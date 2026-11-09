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

func (c *UDPClient) sendRequestFragments(ctx context.Context,conn *net.UDPConn,path *udpPathState,session uint64,mtu int,message []byte,deadline time.Time) error {
	messageID,err:=randomUint64();if err!=nil{return err};if messageID==0{messageID=1}
	chunk:=mtu-datagramHeaderSize-secureOverhead(c.config.PreSharedKey)-fragmentHeaderSize
	fragments,err:=splitMessage(message,chunk,messageID);if err!=nil{return err}
	buf:=make([]byte,c.config.MaxDatagramBytes)
	for _,fragment:=range fragments {
		plain,err:=encodeFragment(fragment,mtu-datagramHeaderSize-secureOverhead(c.config.PreSharedKey));if err!=nil{return err}
		sequence:=uint64(fragment.index)+1
		wire:=plain
		if len(c.config.PreSharedKey)==32 {wire,err=sealSecurePayload(c.config.PreSharedKey,session,sequence,false,plain);if err!=nil{return err}}
		packet,err:=encodeDatagram(session,sequence,wire,mtu);if err!=nil{return err}
		ackSequence:=requestAckSequenceBase+uint64(fragment.index)
		acknowledged:=false
		for attempt:=0;attempt<=c.config.MaxRetries;attempt++ {
			if err:=ctx.Err();err!=nil{return err}
			if !time.Now().Before(deadline){return context.DeadlineExceeded}
			id,err:=path.reserveSend(ctx,len(packet));if err!=nil{return err}
			attemptDeadline:=time.Now().Add(path.rto());if attemptDeadline.After(deadline){attemptDeadline=deadline}
			if err=conn.SetDeadline(attemptDeadline);err!=nil{path.loss(id);return err}
			if _,err=conn.Write(packet);err!=nil{path.loss(id);return err}
			for {
				n,readErr:=conn.Read(buf)
				if readErr!=nil {
					if ne,ok:=readErr.(net.Error);ok&&ne.Timeout(){break}
					path.loss(id);return readErr
				}
				envelope,decodeErr:=decodeDatagram(buf[:n],c.config.MaxDatagramBytes)
				if decodeErr!=nil||envelope.session!=session||envelope.sequence!=ackSequence{continue}
				ackPayload:=envelope.payload
				if len(c.config.PreSharedKey)==32 {ackPayload,decodeErr=openSecurePayload(c.config.PreSharedKey,session,ackSequence,true,ackPayload);if decodeErr!=nil{continue}}
				ack,decodeErr:=DecodeACK(ackPayload)
				if decodeErr!=nil||!ackCovers(ack,sequence){continue}
				path.ack(id);acknowledged=true;break
			}
			if acknowledged {break}
			path.loss(id)
		}
		if !acknowledged{return errors.New("callgraph: request fragment acknowledgement limit reached")}
	}
	return nil
}

func (c *UDPClient) receiveResponseFragments(ctx context.Context,conn *net.UDPConn,session uint64,mtu int,deadline time.Time) ([]byte,error) {
	reassembler:=newFragmentReassembler(c.config.MaxReassemblyBytes,c.config.MaxReassemblyMessages,c.config.ReassemblyTimeout)
	buf:=make([]byte,c.config.MaxDatagramBytes)
	var messageID uint64
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
		envelope,err:=decodeDatagram(buf[:n],c.config.MaxDatagramBytes)
		if err!=nil||envelope.session!=session||envelope.sequence<responseDataSequenceBase||envelope.sequence>=responseDataSequenceBase+maxMessageFragments{continue}
		plain:=envelope.payload
		if len(c.config.PreSharedKey)==32 {plain,err=openSecurePayload(c.config.PreSharedKey,session,envelope.sequence,true,plain);if err!=nil{continue}}
		fragment,err:=decodeFragment(plain,c.config.MaxReassemblyBytes);if err!=nil{continue}
		if envelope.sequence!=responseDataSequenceBase+uint64(fragment.index){continue}
		if messageID==0 {messageID=fragment.messageID} else if messageID!=fragment.messageID {continue}
		key:=strconv.FormatUint(session,10)+"/"+strconv.FormatUint(messageID,10)
		assembled,complete,addErr:=reassembler.Add(key,fragment,time.Now())
		if addErr!=nil{continue}
		ackSequence:=responseAckSequenceBase+uint64(fragment.index)
		_ = writeFragmentACK(conn,session,envelope.sequence,ackSequence,false,mtu,c.config.PreSharedKey,nil)
		if complete{return assembled,nil}
	}
}

func (s *UDPServer) transmitMessage(ctx context.Context,conn *net.UDPConn,peer *net.UDPAddr,session uint64,message []byte,path *udpPathState) {
	messageID,err:=randomUint64();if err!=nil{return};if messageID==0{messageID=1}
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
		s.transmitFragment(ctx,conn,peer,session,sequence,packet,path)
	}
}

func (s *UDPServer) transmitFragment(ctx context.Context,conn *net.UDPConn,peer *net.UDPAddr,session,sequence uint64,packet []byte,path *udpPathState) {
	key:=pendingResponseKey{session:session,peer:peer.String(),sequence:sequence}
	for attempt:=0;attempt<=s.config.MaxRetries;attempt++ {
		if ctx.Err()!=nil{return}
		id,err:=path.reserveSend(ctx,len(packet));if err!=nil{return}
		done:=make(chan struct{})
		s.pendingMu.Lock()
		s.pending[key]=pendingResponse{packet:append([]byte(nil),packet...),path:path,congestionID:id,done:done}
		s.pendingMu.Unlock()
		if _,err=conn.WriteToUDP(packet,peer);err!=nil {
			s.pendingMu.Lock();delete(s.pending,key);s.pendingMu.Unlock();path.loss(id);return
		}
		timer:=time.NewTimer(path.rto())
		select {
		case <-done:
			timer.Stop();return
		case <-ctx.Done():
			timer.Stop()
			s.pendingMu.Lock();delete(s.pending,key);s.pendingMu.Unlock()
			path.loss(id);return
		case <-timer.C:
			s.pendingMu.Lock()
			entry,exists:=s.pending[key]
			if exists&&entry.congestionID==id {delete(s.pending,key)}
			s.pendingMu.Unlock()
			if exists&&entry.congestionID==id {path.loss(id)}
		}
	}
}
