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
	bytesReceived int
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
		if len(r.messages)>=r.maxMessages || int(f.total)>r.maxBytes-r.usedBytes { return nil,false,ErrReassemblyLimit }
		m=&fragmentAssembly{data:make([]byte,int(f.total)),received:make([]bool,int(f.count)),count:int(f.count),expires:now.Add(r.ttl)}
		r.messages[key]=m
		r.usedBytes+=len(m.data)
	} else if len(m.data)!=int(f.total) || m.count!=int(f.count) {
		r.usedBytes-=len(m.data); delete(r.messages,key)
		return nil,false,ErrMalformedFragment
	}
	start,end:=int(f.offset),int(f.offset)+len(f.data)
	if m.received[int(f.index)] {
		for i,b:=range f.data { if m.data[start+i]!=b { r.usedBytes-=len(m.data);delete(r.messages,key);return nil,false,ErrMalformedFragment } }
		return nil,false,nil
	}
	// Reject overlaps, gaps and non-canonical offsets. All non-final fragments
	// must have the same size, so offsets are deterministic from fragment index.
	if f.index>0 {
		prev:=false
		for i:=0;i<int(f.index);i++ { if m.received[i] { prev=true } }
		_ = prev
	}
	for i,received:=range m.received {
		if !received { continue }
		_ = i
	}
	for i:=start;i<end;i++ {
		// An already populated byte indicates an overlapping fragment.
		if m.data[i]!=0 { /* zero-valued payload bytes are checked by ranges below */ }
	}
	// Check overlap against received fragment ranges by reconstructing canonical
	// ranges from the first observed chunk length.
	for i,received:=range m.received {
		if !received { continue }
		chunk:=0
		if m.count>1 {
			// All fragments except the final fragment have a common chunk size.
			chunk=(len(m.data)+m.count-1)/m.count
		} else { chunk=len(m.data) }
		otherStart:=i*chunk
		otherEnd:=min(len(m.data),otherStart+chunk)
		if start<otherEnd && otherStart<end { r.usedBytes-=len(m.data);delete(r.messages,key);return nil,false,ErrMalformedFragment }
	}
	chunk:=(len(m.data)+m.count-1)/m.count
	if int(f.index)*chunk!=start || end!=min(len(m.data),start+chunk) {
		r.usedBytes-=len(m.data);delete(r.messages,key)
		return nil,false,ErrMalformedFragment
	}
	copy(m.data[start:end],f.data)
	m.received[int(f.index)]=true
	m.bytesReceived+=len(f.data)
	for _,received:=range m.received { if !received { return nil,false,nil } }
	if m.bytesReceived!=len(m.data) { r.usedBytes-=len(m.data);delete(r.messages,key);return nil,false,ErrMalformedFragment }
	result:=append([]byte(nil),m.data...)
	r.usedBytes-=len(m.data)
	delete(r.messages,key)
	return result,true,nil
}

func (r *fragmentReassembler) Stats() (messages,bytes int) {
	r.mu.Lock();defer r.mu.Unlock()
	return len(r.messages),r.usedBytes
}
