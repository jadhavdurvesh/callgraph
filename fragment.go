package callgraph

import (
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

const (
	fragmentHeaderSize = 24
	maxMessageFragments = 1024
	requestAckSequenceBase = uint64(0x10000000)
	responseDataSequenceBase = uint64(0x20000000)
	responseAckSequenceBase = uint64(0x40000000)
)

var (
	ErrMalformedFragment = errors.New("callgraph: malformed FRG1 fragment")
	ErrReassemblyLimit = errors.New("callgraph: reassembly resource limit reached")
)

type messageFragment struct {
	messageID uint64
	index uint16
	count uint16
	total uint32
	offset uint32
	data []byte
}

func encodeFragment(f messageFragment, max int) ([]byte,error) {
	if f.messageID==0 || f.count==0 || int(f.count)>maxMessageFragments || f.index>=f.count ||
		f.total==0 || uint64(f.offset)+uint64(len(f.data))>uint64(f.total) ||
		len(f.data)==0 || len(f.data)>max-fragmentHeaderSize {
		return nil,ErrMalformedFragment
	}
	out:=make([]byte,fragmentHeaderSize+len(f.data))
	copy(out[:4],"FRG1")
	binary.BigEndian.PutUint64(out[4:12],f.messageID)
	binary.BigEndian.PutUint16(out[12:14],f.index)
	binary.BigEndian.PutUint16(out[14:16],f.count)
	binary.BigEndian.PutUint32(out[16:20],f.total)
	binary.BigEndian.PutUint32(out[20:24],f.offset)
	copy(out[24:],f.data)
	return out,nil
}

func decodeFragment(wire []byte,maxMessageBytes int) (messageFragment,error) {
	var f messageFragment
	if len(wire)<=fragmentHeaderSize || string(wire[:4])!="FRG1" { return f,ErrMalformedFragment }
	f.messageID=binary.BigEndian.Uint64(wire[4:12])
	f.index=binary.BigEndian.Uint16(wire[12:14])
	f.count=binary.BigEndian.Uint16(wire[14:16])
	f.total=binary.BigEndian.Uint32(wire[16:20])
	f.offset=binary.BigEndian.Uint32(wire[20:24])
	f.data=wire[24:]
	if f.messageID==0 || f.count==0 || int(f.count)>maxMessageFragments || f.index>=f.count ||
		f.total==0 || uint64(f.total)>uint64(maxMessageBytes) ||
		uint64(f.offset)+uint64(len(f.data))>uint64(f.total) {
		return messageFragment{},ErrMalformedFragment
	}
	return f,nil
}

func splitMessage(data []byte,maxChunk int,messageID uint64) ([]messageFragment,error) {
	if len(data)==0 || maxChunk<=0 || messageID==0 { return nil,ErrMalformedFragment }
	count:=(len(data)+maxChunk-1)/maxChunk
	if count>maxMessageFragments || uint64(len(data))>uint64(^uint32(0)) { return nil,ErrMalformedFragment }
	fragments:=make([]messageFragment,0,count)
	for offset:=0;offset<len(data);offset+=maxChunk {
		end:=min(len(data),offset+maxChunk)
		fragments=append(fragments,messageFragment{messageID:messageID,index:uint16(len(fragments)),count:uint16(count),total:uint32(len(data)),offset:uint32(offset),data:append([]byte(nil),data[offset:end]...)})
	}
	return fragments,nil
}

type fragmentAssembly struct {
	data []byte
	received []bool
	count int
	chunkSize int
	bytesReceived int
	complete bool
	expires time.Time
}

type fragmentReassembler struct {
	mu sync.Mutex
	maxBytes int
	maxMessages int
	ttl time.Duration
	usedBytes int
	messages map[string]*fragmentAssembly
}

func newFragmentReassembler(maxBytes,maxMessages int,ttl time.Duration) *fragmentReassembler {
	return &fragmentReassembler{maxBytes:maxBytes,maxMessages:maxMessages,ttl:ttl,messages:make(map[string]*fragmentAssembly)}
}

func (r *fragmentReassembler) Add(key string,f messageFragment,now time.Time) ([]byte,bool,error) {
	if key=="" || f.total==0 || int(f.total)>r.maxBytes || int(f.count)>maxMessageFragments ||
		f.count==0 || f.index>=f.count || uint64(f.offset)+uint64(len(f.data))>uint64(f.total) || len(f.data)==0 {
		return nil,false,ErrMalformedFragment
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for k,m:=range r.messages {
		if !now.Before(m.expires) { r.usedBytes-=len(m.data); delete(r.messages,k) }
	}
	m:=r.messages[key]
	if m==nil {
		chunkSize:=0
		if f.index==0 { chunkSize=len(f.data) } else if f.offset%uint32(f.index)==0 { chunkSize=int(f.offset/uint32(f.index)) }
		if chunkSize<=0 || (int(f.total)+chunkSize-1)/chunkSize!=int(f.count) ||
			int(f.offset)!=int(f.index)*chunkSize ||
			len(f.data)!=min(chunkSize,int(f.total)-int(f.offset)) {
			return nil,false,ErrMalformedFragment
		}
		if len(r.messages)>=r.maxMessages || int(f.total)>r.maxBytes-r.usedBytes { return nil,false,ErrReassemblyLimit }
		m=&fragmentAssembly{data:make([]byte,int(f.total)),received:make([]bool,int(f.count)),count:int(f.count),chunkSize:chunkSize,expires:now.Add(r.ttl)}
		r.messages[key]=m
		r.usedBytes+=len(m.data)
	} else if len(m.data)!=int(f.total) || m.count!=int(f.count) ||
		int(f.offset)!=int(f.index)*m.chunkSize ||
		len(f.data)!=min(m.chunkSize,int(f.total)-int(f.offset)) {
		r.usedBytes-=len(m.data); delete(r.messages,key)
		return nil,false,ErrMalformedFragment
	}
	start,end:=int(f.offset),int(f.offset)+len(f.data)
	if m.received[int(f.index)] {
		for i,b:=range f.data { if m.data[start+i]!=b { r.usedBytes-=len(m.data);delete(r.messages,key);return nil,false,ErrMalformedFragment } }
		if m.complete { return append([]byte(nil),m.data...),true,nil }
		return nil,false,nil
	}
	for i,received:=range m.received {
		if !received { continue }
		otherStart:=i*m.chunkSize
		otherEnd:=min(len(m.data),otherStart+m.chunkSize)
		if start<otherEnd && otherStart<end { r.usedBytes-=len(m.data);delete(r.messages,key);return nil,false,ErrMalformedFragment }
	}
	copy(m.data[start:end],f.data)
	m.received[int(f.index)]=true
	m.bytesReceived+=len(f.data)
	for _,received:=range m.received { if !received { return nil,false,nil } }
	if m.bytesReceived!=len(m.data) { r.usedBytes-=len(m.data);delete(r.messages,key);return nil,false,ErrMalformedFragment }
	m.complete=true
	return append([]byte(nil),m.data...),true,nil
}

func (r *fragmentReassembler) Stats() (messages,bytes int) {
	r.mu.Lock();defer r.mu.Unlock()
	return len(r.messages),r.usedBytes
}
