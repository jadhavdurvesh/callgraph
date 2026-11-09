package callgraph

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"
)

const (
	defaultPathDatagramSize = 1200
	maximumProbeDatagramSize = 1452
	mtuProbeHeaderSize = 16
)

type sentPathPacket struct {
	bytes int
	sentAt time.Time
}

// pathCongestion is the Go transport adapter for DATAGRAM's per-path,
// byte-based Reno controller. All methods are protected by udpPathState.ccMu.
type pathCongestion struct {
	maxDatagram int
	minWindow int
	maxWindow int
	cwnd int
	ssthresh int
	bytesInFlight int
	srtt time.Duration
	rttvar time.Duration
	rto time.Duration
	nextSend time.Time
	largestSent uint64
	recoveryEnd uint64
	nextID uint64
	outstanding map[uint64]sentPathPacket
}

func newPathCongestion(maxDatagram int) *pathCongestion {
	if maxDatagram < 576 { maxDatagram = 576 }
	minWindow := 2 * maxDatagram
	maxWindow := 16 * 1024 * 1024
	return &pathCongestion{
		maxDatagram: maxDatagram, minWindow: minWindow, maxWindow: maxWindow,
		cwnd: min(10*defaultPathDatagramSize, maxWindow), ssthresh: maxWindow,
		srtt: 100*time.Millisecond, rttvar: 50*time.Millisecond, rto: 200*time.Millisecond,
		nextID: 1, outstanding: make(map[uint64]sentPathPacket),
	}
}

func (c *pathCongestion) ack(id uint64, now time.Time) {
	packet, ok := c.outstanding[id]
	if !ok { return }
	delete(c.outstanding,id)
	c.bytesInFlight = max(0,c.bytesInFlight-packet.bytes)
	sample := now.Sub(packet.sentAt)
	if sample > 0 {
		if c.srtt == 100*time.Millisecond && c.rttvar == 50*time.Millisecond {
			c.srtt, c.rttvar = sample, sample/2
		} else {
			delta := c.srtt-sample; if delta<0 { delta = -delta }
			c.rttvar = time.Duration(.75*float64(c.rttvar)+.25*float64(delta))
			c.srtt = time.Duration(.875*float64(c.srtt)+.125*float64(sample))
		}
		c.rto = min(60*time.Second,max(200*time.Millisecond,c.srtt+4*c.rttvar))
	}
	if c.cwnd<c.ssthresh { c.cwnd=min(c.maxWindow,c.cwnd+packet.bytes) } else {
		increase := c.maxDatagram*packet.bytes/max(1,c.cwnd)
		c.cwnd=min(c.maxWindow,c.cwnd+max(1,increase))
	}
}

func (c *pathCongestion) loss(id uint64) {
	packet,ok:=c.outstanding[id]; if !ok{return}
	delete(c.outstanding,id); c.bytesInFlight=max(0,c.bytesInFlight-packet.bytes)
	if id>c.recoveryEnd {
		c.ssthresh=max(c.minWindow,c.cwnd/2)
		c.cwnd=c.ssthresh
		c.recoveryEnd=c.largestSent
	}
	c.rto=min(60*time.Second,c.rto*2)
}

func (c *pathCongestion) retransmissionTimeout() time.Duration { return c.rto }

type udpPathState struct {
	mtuMu sync.Mutex
	mtuCurrent int
	mtuUpper int
	mtuReady bool
	ccMu sync.Mutex
	cc *pathCongestion
}

func newUDPPathState(maxDatagram int) *udpPathState {
	if maxDatagram > maximumProbeDatagramSize { maxDatagram = maximumProbeDatagramSize }
	if maxDatagram < 576 { maxDatagram = 576 }
	initial := min(defaultPathDatagramSize,maxDatagram)
	return &udpPathState{mtuCurrent:initial,mtuUpper:maxDatagram,cc:newPathCongestion(maxDatagram)}
}

func (p *udpPathState) currentMTU() int {
	p.mtuMu.Lock(); defer p.mtuMu.Unlock()
	return p.mtuCurrent
}

func (p *udpPathState) setPeerMTU(size int) {
	p.mtuMu.Lock(); defer p.mtuMu.Unlock()
	if size>p.mtuCurrent && size<=p.mtuUpper { p.mtuCurrent=size }
}

func (p *udpPathState) reserveSend(ctx context.Context, size int) (uint64,error) {
	for {
		p.ccMu.Lock()
		c:=p.cc
		now:=time.Now()
		available:=size>0 && size<=c.maxDatagram && size<=c.cwnd-c.bytesInFlight
		paced:=!now.Before(c.nextSend)
		if available && paced {
			if len(c.outstanding)>=4096 { p.ccMu.Unlock(); return 0,errors.New("callgraph: path outstanding-packet limit reached") }
			id:=c.nextID; c.nextID++; if c.nextID==0 { c.nextID=1 }
			c.outstanding[id]=sentPathPacket{bytes:size,sentAt:now}; c.bytesInFlight+=size; c.largestSent=max(c.largestSent,id)
			pacing:=time.Duration(float64(c.srtt)*float64(size)/float64(max(1,c.cwnd))); if pacing<time.Millisecond {pacing=time.Millisecond}; c.nextSend=now.Add(pacing)
			p.ccMu.Unlock(); return id,nil
		}
		wait:=time.Millisecond
		if available && c.nextSend.After(now) { wait=min(wait,time.Until(c.nextSend)) }
		p.ccMu.Unlock()
		timer:=time.NewTimer(wait)
		select { case <-ctx.Done(): timer.Stop(); return 0,ctx.Err(); case <-timer.C: }
	}
}

func (p *udpPathState) ack(id uint64) { p.ccMu.Lock(); p.cc.ack(id,time.Now()); p.ccMu.Unlock() }
func (p *udpPathState) loss(id uint64) { p.ccMu.Lock(); p.cc.loss(id); p.ccMu.Unlock() }
func (p *udpPathState) rto() time.Duration { p.ccMu.Lock(); defer p.ccMu.Unlock(); return p.cc.retransmissionTimeout() }

func (p *udpPathState) discoverMTU(ctx context.Context, conn *net.UDPConn, cfg UDPConfig, deadline time.Time) int {
	p.mtuMu.Lock()
	defer p.mtuMu.Unlock()
	if p.mtuReady { return p.mtuCurrent }
	if p.mtuUpper>cfg.MaxDatagramBytes { p.mtuUpper=cfg.MaxDatagramBytes }
	if p.mtuCurrent>p.mtuUpper { p.mtuCurrent=p.mtuUpper }
	for p.mtuCurrent<p.mtuUpper && ctx.Err()==nil && time.Now().Before(deadline) {
		size:=p.mtuCurrent+(p.mtuUpper-p.mtuCurrent+1)/2
		session,err:=randomUint64(); if err!=nil {break}
		probeID,err:=randomUint64(); if err!=nil {break}
		plain:=make([]byte,size-datagramHeaderSize-secureOverhead(cfg.PreSharedKey))
		copy(plain[:4],"MTP1"); binary.BigEndian.PutUint64(plain[4:12],probeID); binary.BigEndian.PutUint32(plain[12:16],uint32(size))
		for i:=mtuProbeHeaderSize;i<len(plain);i++ {plain[i]=byte((uint64(i)*31+probeID)&0xff)}
		wire:=plain
		if len(cfg.PreSharedKey)==32 {wire,err=sealSecurePayload(cfg.PreSharedKey,session,3,false,plain);if err!=nil{break}}
		packet,err:=encodeDatagram(session,3,wire,size);if err!=nil{break}
		probeDeadline:=time.Now().Add(cfg.RetryInterval);if probeDeadline.After(deadline){probeDeadline=deadline}
		if err=conn.SetDeadline(probeDeadline);err!=nil{break}
		if _,err=conn.Write(packet);err!=nil{p.mtuUpper=size-1;continue}
		buf:=make([]byte,cfg.MaxDatagramBytes)
		n,readErr:=conn.Read(buf)
		if readErr==nil {
			env,decErr:=decodeDatagram(buf[:n],cfg.MaxDatagramBytes)
			if decErr==nil && env.session==session && env.sequence==4 {
				response:=env.payload
				if len(cfg.PreSharedKey)==32 {response,decErr=openSecurePayload(cfg.PreSharedKey,session,4,true,response)}
				if decErr==nil && len(response)>=mtuProbeHeaderSize && string(response[:4])=="MTA1" &&
					binary.BigEndian.Uint64(response[4:12])==probeID && int(binary.BigEndian.Uint32(response[12:16]))==size && n==size {
					p.mtuCurrent=size
					continue
				}
			}
		}
		p.mtuUpper=size-1
		if p.mtuUpper<p.mtuCurrent {p.mtuUpper=p.mtuCurrent}
	}
	p.mtuReady=true
	_ = conn.SetDeadline(time.Time{})
	return p.mtuCurrent
}

func (s *UDPServer) handleMTUProbe(conn *net.UDPConn, peer *net.UDPAddr, packet []byte, envelope datagramEnvelope, plain []byte, path *udpPathState) {
	if len(plain)<mtuProbeHeaderSize || string(plain[:4])!="MTP1" {return}
	probeID:=binary.BigEndian.Uint64(plain[4:12])
	size:=int(binary.BigEndian.Uint32(plain[12:16]))
	if size!=len(packet) || size> s.config.MaxDatagramBytes || size<datagramHeaderSize+mtuProbeHeaderSize+secureOverhead(s.config.PreSharedKey) {return}
	path.setPeerMTU(size)
	response:=make([]byte,size-datagramHeaderSize-secureOverhead(s.config.PreSharedKey))
	copy(response[:4],"MTA1");binary.BigEndian.PutUint64(response[4:12],probeID);binary.BigEndian.PutUint32(response[12:16],uint32(size))
	for i:=mtuProbeHeaderSize;i<len(response);i++ {response[i]=byte((uint64(i)*31+probeID)&0xff)}
	wire:=response
	var err error
	if len(s.config.PreSharedKey)==32 {wire,err=sealSecurePayload(s.config.PreSharedKey,envelope.session,4,true,response);if err!=nil{return}}
	out,err:=encodeDatagram(envelope.session,4,wire,size);if err!=nil{return}
	_,_=conn.WriteToUDP(out,peer)
}

func peerPathKey(peer *net.UDPAddr) string {
	if peer==nil{return ""}
	if peer.IP==nil{return peer.String()}
	return peer.IP.String()+"%"+peer.Zone
}
