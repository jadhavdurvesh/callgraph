package callgraph

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"
)

func ackCovers(ack AckFrame, sequence uint64) bool {
	if ack.Validate()!=nil { return false }
	for _,r:=range ack.Ranges { if r.First<=sequence && r.Last>=sequence { return true } }
	return false
}

func writeFragmentACK(conn *net.UDPConn,session,ackedSequence,ackSequence uint64,responseDirection bool,mtu int,key []byte,peer *net.UDPAddr) error {
	wire,err:=(AckFrame{LargestAcked:ackedSequence,Ranges:[]AckRange{{First:ackedSequence,Last:ackedSequence}}}).Encode()
	if err!=nil{return err}
	if len(key)==32 { wire,err=sealSecurePayload(key,session,ackSequence,responseDirection,wire);if err!=nil{return err} }
	packet,err:=encodeDatagram(session,ackSequence,wire,mtu);if err!=nil{return err}
	if peer!=nil { _,err=conn.WriteToUDP(packet,peer) } else { _,err=conn.Write(packet) }
	return err
}

func (c *UDPClient) sendRequestFragments(ctx context.Context,conn *net.UDPConn,path *udpPathState,session uint64,mtu int,message []byte,deadline time.Time) ([][]byte,error) {
	messageID,err:=randomUint64();if err!=nil{return err};if messageID==0{messageID=1}
	chunk:=mtu-datagramHeaderSize-secureOverhead(c.config.PreSharedKey)-fragmentHeaderSize
	fragments,err:=splitMessage(message,chunk,messageID);if err!=nil{return err}
	buf:=make([]byte,c.config.MaxDatagramBytes)
	var earlyResponsePackets [][]byte
	for _,fragment:=range fragments {
		plain,err:=encodeFragment(fragment,mtu-datagramHeaderSize-secureOverhead(c.config.PreSharedKey));if err!=nil{return err}
		sequence:=uint64(fragment.index)+1
		wire:=plain
		if len(c.config.PreSharedKey)==32 {wire,err=sealSecurePayload(c.config.PreSharedKey,session,sequence,false,plain);if err!=nil{return err}}
		packet,err:=encodeDatagram(session,sequence,wire,mtu);if err!=nil{return err}
		ackSequence:=requestAckSequenceBase+uint64(fragment.index)
		acknowledged:=false
		for attempt:=0;attempt<=c.config.MaxRetries;attempt++ {
			if err:=ctx.Err();err!=nil{return nil,err}
			if !time.Now().Before(deadline){return nil,context.DeadlineExceeded}
			id,err:=path.reserveSend(ctx,len(packet));if err!=nil{return nil,err}
			attemptDeadline:=time.Now().Add(path.rto());if attemptDeadline.After(deadline){attemptDeadline=deadline}
			if err=conn.SetDeadline(attemptDeadline);err!=nil{path.loss(id);return nil,err}
			if _,err=conn.Write(packet);err!=nil{path.loss(id);return nil,err}
			for {
				n,readErr:=conn.Read(buf)
				if readErr!=nil {
					if ne,ok:=readErr.(net.Error);ok&&ne.Timeout(){break}
					path.loss(id);return readErr
				}
				envelope,decodeErr:=decodeDatagram(buf[:n],c.config.MaxDatagramBytes)
				if decodeErr!=nil||envelope.session!=session{continue}
				if envelope.sequence>=responseDataSequenceBase&&envelope.sequence<responseDataSequenceBase+maxMessageFragments {
					earlyResponsePackets=append(earlyResponsePackets,append([]byte(nil),buf[:n]...))
					continue
				}
				if envelope.sequence!=ackSequence{continue}
				ackPayload:=envelope.payload
				if len(c.config.PreSharedKey)==32 {ackPayload,decodeErr=openSecurePayload(c.config.PreSharedKey,session,ackSequence,true,ackPayload);if decodeErr!=nil{continue}}
				ack,decodeErr:=DecodeACK(ackPayload)
				if decodeErr!=nil||!ackCovers(ack,sequence){continue}
				path.ack(id);acknowledged=true;break
			}
			if acknowledged {break}
			path.loss(id)
		}
		if !acknowledged{return nil,errors.New("callgraph: request fragment acknowledgement limit reached")}
	}
	return earlyResponsePackets,nil
}

func (c *UDPClient) receiveResponseFragments(ctx context.Context,conn *net.UDPConn,session uint64,mtu int,deadline time.Time,early [][]byte) ([]byte,error) {
	reassembler:=newFragmentReassembler(c.config.MaxReassemblyBytes,c.config.MaxReassemblyMessages,c.config.ReassemblyTimeout)
	buf:=make([]byte,c.config.MaxDatagramBytes)
	var messageID uint64
	process:=func(packet []byte)([]byte,bool) {
		envelope,err:=decodeDatagram(packet,c.config.MaxDatagramBytes)
		if err!=nil||envelope.session!=session||envelope.sequence<responseDataSequenceBase||envelope.sequence>=responseDataSequenceBase+maxMessageFragments{return nil,false}
		plain:=envelope.payload
		if len(c.config.PreSharedKey)==32 {plain,err=openSecurePayload(c.config.PreSharedKey,session,envelope.sequence,true,plain);if err!=nil{return nil,false}}
		fragment,err:=decodeFragment(plain,c.config.MaxReassemblyBytes);if err!=nil{return nil,false}
		if envelope.sequence!=responseDataSequenceBase+uint64(fragment.index){return nil,false}
		if messageID==0 {messageID=fragment.messageID} else if messageID!=fragment.messageID {return nil,false}
		key:=strconv.FormatUint(session,10)+"/"+strconv.FormatUint(messageID,10)
		assembled,complete,addErr:=reassembler.Add(key,fragment,time.Now())
		if addErr!=nil{return nil,false}
		ackSequence:=responseAckSequenceBase+uint64(fragment.index)
		_ = conn.SetWriteDeadline(deadline)
		_ = writeFragmentACK(conn,session,envelope.sequence,ackSequence,false,mtu,c.config.PreSharedKey,nil)
		if complete {
			_ = writeFragmentACK(conn,session,envelope.sequence,ackSequence,false,mtu,c.config.PreSharedKey,nil)
			return assembled,true
		}
		return nil,false
	}
	for _,packet:=range early { if assembled,complete:=process(packet);complete{return assembled,nil} }
	for {
		if err:=ctx.Err();err!=nil{return nil,err}
		if !time.Now().Before(deadline){return nil,context.DeadlineExceeded}
		attemptDeadline:=time.Now().Add(c.config.ReadPollInterval);if attemptDeadline.After(deadline){attemptDeadline=deadline}
		if err:=conn.SetReadDeadline(attemptDeadline);err!=nil{return nil,err}
		n,err:=conn.Read(buf)
		if err!=nil {
			if ne,ok:=err.(net.Error);ok&&ne.Timeout(){continue}
			return nil,err
		}
		if assembled,complete:=process(buf[:n]);complete{return assembled,nil}
	}
}

func (s *UDPServer) transmitMessage(ctx context.Context,conn *net.UDPConn,peer *net.UDPAddr,session,messageID uint64,message []byte,path *udpPathState) {
	if messageID==0{return}
	mtu:=path.currentMTU()
	chunk:=mtu-datagramHeaderSize-secureOverhead(s.config.PreSharedKey)-fragmentHeaderSize
	fragments,err:=splitMessage(message,chunk,messageID);if err!=nil{return}
	for _,fragment:=range fragments {
		if ctx.Err()!=nil{return}
		plain,err:=encodeFragment(fragment,mtu-datagramHeaderSize-secureOverhead(s.config.PreSharedKey));if err!=nil{return}
		sequence:=responseDataSequenceBase+uint64(fragment.index)
		wire:=plain
		if len(s.config.PreSharedKey)==32 {wire,err=sealSecurePayload(s.config.PreSharedKey,session,sequence,true,plain);if err!=nil{return}}
		packet,err:=encodeDatagram(session,sequence,wire,mtu);if err!=nil{return}
		if !s.transmitFragment(ctx,conn,peer,session,sequence,packet,path) { return }
	}
}

func (s *UDPServer) transmitFragment(ctx context.Context,conn *net.UDPConn,peer *net.UDPAddr,session,sequence uint64,packet []byte,path *udpPathState) bool {
	key:=pendingResponseKey{session:session,peer:peer.String(),sequence:sequence}
	for attempt:=0;attempt<=s.config.MaxRetries;attempt++ {
		if ctx.Err()!=nil{return false}
		id,err:=path.reserveSend(ctx,len(packet));if err!=nil{return false}
		done:=make(chan struct{})
		s.pendingMu.Lock()
		s.pending[key]=pendingResponse{packet:append([]byte(nil),packet...),path:path,congestionID:id,done:done}
		s.pendingMu.Unlock()
		if _,err=conn.WriteToUDP(packet,peer);err!=nil {
			s.pendingMu.Lock();delete(s.pending,key);s.pendingMu.Unlock();path.loss(id);return false
		}
		timer:=time.NewTimer(path.rto())
		select {
		case <-done:
			timer.Stop();return true
		case <-ctx.Done():
			timer.Stop()
			s.pendingMu.Lock();delete(s.pending,key);s.pendingMu.Unlock()
			path.loss(id);return false
		case <-timer.C:
			s.pendingMu.Lock()
			entry,exists:=s.pending[key]
			if exists&&entry.congestionID==id {delete(s.pending,key)}
			s.pendingMu.Unlock()
			if !exists { return true }
			if entry.congestionID==id {path.loss(id)}
		}
	}
	return false
}
