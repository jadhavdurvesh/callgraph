package callgraph

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
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
	messageID,err:=randomUint64();if err!=nil{return nil,err};if messageID==0{messageID=1}
	chunk:=mtu-datagramHeaderSize-secureOverhead(c.config.PreSharedKey)-fragmentHeaderSize
	fragments,err:=splitMessage(message,chunk,messageID);if err!=nil{return nil,err}
	sendCtx,cancel:=context.WithDeadline(ctx,deadline);defer cancel()

	type fragmentJob struct { packet []byte; sequence uint64; ackSequence uint64; acked chan struct{} }
	jobs:=make([]fragmentJob,len(fragments))
	for i,fragment:=range fragments {
		plain,e:=encodeFragment(fragment,mtu-datagramHeaderSize-secureOverhead(c.config.PreSharedKey));if e!=nil{return nil,e}
		sequence:=uint64(fragment.index)+1
		wire:=plain
		if len(c.config.PreSharedKey)==32 {wire,e=sealSecurePayload(c.config.PreSharedKey,session,sequence,false,plain);if e!=nil{return nil,e}}
		packet,e:=encodeDatagram(session,sequence,wire,mtu);if e!=nil{return nil,e}
		jobs[i]=fragmentJob{packet:packet,sequence:sequence,ackSequence:requestAckSequenceBase+uint64(fragment.index),acked:make(chan struct{})}
	}

	var mu sync.Mutex
	outstanding:=make(map[uint64]uint64) // data sequence -> congestion controller ID
	responses:=make(chan []byte,maxMessageFragments)
	readerDone:=make(chan struct{})
	var readerErr error
	go func(){
		defer close(readerDone)
		buf:=make([]byte,c.config.MaxDatagramBytes)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(c.config.ReadPollInterval))
			n,e:=conn.Read(buf)
			if e!=nil {
				if ne,ok:=e.(net.Error);ok&&ne.Timeout(){if sendCtx.Err()!=nil{return};continue}
				readerErr=e;return
			}
			packet:=append([]byte(nil),buf[:n]...)
			envelope,e:=decodeDatagram(packet,c.config.MaxDatagramBytes)
			if e!=nil||envelope.session!=session{continue}
			if envelope.sequence>=responseDataSequenceBase&&envelope.sequence<responseDataSequenceBase+maxMessageFragments {
				select {case responses<-packet:default:}
				continue
			}
			if envelope.sequence<requestAckSequenceBase||envelope.sequence>=requestAckSequenceBase+maxMessageFragments{continue}
			index:=envelope.sequence-requestAckSequenceBase
			ackPayload:=envelope.payload
			if len(c.config.PreSharedKey)==32 {ackPayload,e=openSecurePayload(c.config.PreSharedKey,session,envelope.sequence,true,ackPayload);if e!=nil{continue}}
			ack,e:=DecodeACK(ackPayload)
			if e!=nil||!ackCovers(ack,index+1){continue}
			mu.Lock()
			id,ok:=outstanding[index+1]
			if ok {
				delete(outstanding,index+1)
				path.ack(id)
				close(jobs[index].acked)
			}
			mu.Unlock()
		}
	}()

	const sendWindow=8
	sem:=make(chan struct{},sendWindow)
	results:=make(chan error,len(jobs))
	var wg sync.WaitGroup
	for i:=range jobs {
		job:=jobs[i]
		wg.Add(1)
		go func(){
			defer wg.Done()
			select {case sem<-struct{}{}:case <-sendCtx.Done():results<-sendCtx.Err();return}
			defer func(){<-sem}()
			var result error
			for attempt:=0;attempt<=c.config.MaxRetries;attempt++ {
				if err:=sendCtx.Err();err!=nil{result=err;break}
				id,e:=path.reserveSend(sendCtx,len(job.packet));if e!=nil{result=e;break}
				mu.Lock();outstanding[job.sequence]=id;mu.Unlock()
				if _,e=conn.Write(job.packet);e!=nil {
					mu.Lock();delete(outstanding,job.sequence);mu.Unlock();path.loss(id);result=e;break
				}
				timer:=time.NewTimer(path.rto())
				select {
				case <-job.acked:
					if !timer.Stop(){select{case <-timer.C:default:}}
					result=nil;attempt=c.config.MaxRetries+1
				case <-timer.C:
					mu.Lock()
					current,ok:=outstanding[job.sequence]
					if ok&&current==id{delete(outstanding,job.sequence)}
					mu.Unlock()
					if ok&&current==id{path.loss(id)}
					if attempt==c.config.MaxRetries{result=errors.New("callgraph: request fragment acknowledgement limit reached")}
				case <-sendCtx.Done():
					if !timer.Stop(){select{case <-timer.C:default:}}
					mu.Lock();current,ok:=outstanding[job.sequence];if ok&&current==id{delete(outstanding,job.sequence)};mu.Unlock()
					if ok&&current==id{path.loss(id)}
					result=sendCtx.Err();attempt=c.config.MaxRetries+1
				case <-readerDone:
					if !timer.Stop(){select{case <-timer.C:default:}}
					result=readerErr;if result==nil{result=errors.New("callgraph: UDP response reader stopped")}
					attempt=c.config.MaxRetries+1
				}
				if result==nil {break}
			}
			results<-result
		}()
	}
	var firstErr error
	for range jobs {if e:=<-results;e!=nil&&firstErr==nil{firstErr=e;cancel()}}
	wg.Wait()
	cancel()
	<-readerDone
	if firstErr!=nil{return nil,firstErr}
	if readerErr!=nil{return nil,readerErr}
	early:=make([][]byte,0,len(responses))
	for {select{case packet:=<-responses:early=append(early,packet);default:return early,nil}}
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
	const sendWindow=8
	sem:=make(chan struct{},sendWindow)
	var wg sync.WaitGroup
	for _,fragment:=range fragments {
		if ctx.Err()!=nil{break}
		plain,e:=encodeFragment(fragment,mtu-datagramHeaderSize-secureOverhead(s.config.PreSharedKey));if e!=nil{break}
		sequence:=responseDataSequenceBase+uint64(fragment.index)
		wire:=plain
		if len(s.config.PreSharedKey)==32 {wire,e=sealSecurePayload(s.config.PreSharedKey,session,sequence,true,plain);if e!=nil{break}}
		packet,e:=encodeDatagram(session,sequence,wire,mtu);if e!=nil{break}
		select {case sem<-struct{}{}:case <-ctx.Done():break}
		if ctx.Err()!=nil{break}
		wg.Add(1)
		go func(sequence uint64,packet []byte){
			defer wg.Done();defer func(){<-sem}()
			s.transmitFragment(ctx,conn,peer,session,sequence,packet,path)
		}(sequence,packet)
	}
	wg.Wait()
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
