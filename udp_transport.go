package callgraph

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"
)

const (
	datagramHeaderSize = 24
	datagramVersion    = 1
	octetHeaderSize    = 10
	octetVersion       = 1
	kindRequest        = 1
	kindResponse       = 2
	kindError          = 3
	mtuProbeRequestSequence = uint64(0x0fffff00)
	mtuProbeResponseSequence = uint64(0x0fffff01)
)

var (
	ErrMalformedDatagram = errors.New("callgraph: malformed DATAGRAM envelope")
	ErrMalformedOCTET = errors.New("callgraph: malformed OCTET frame")
	ErrUnexpectedMessage = errors.New("callgraph: unexpected RPC message kind")
	ErrRemoteOperation = errors.New("callgraph: remote operation failed")
	ErrOperationVersion = errors.New("callgraph: operation version mismatch")
)

// UDPConfig bounds every datagram and defines a finite default request budget.
type UDPConfig struct {
	MaxPayloadBytes int
	MaxDatagramBytes int
	RequestTimeout time.Duration
	ReadPollInterval time.Duration
	MaxConcurrentRequests int
	MaxReplayEntries int
	MaxReplayBytes int
	MaxReassemblyBytes int
	MaxReassemblyMessages int
	ReassemblyTimeout time.Duration
	MaxRetries int
	RetryInterval time.Duration
	PreSharedKey []byte
	RequireEncryption bool
}

func DefaultUDPConfig() UDPConfig {
	return UDPConfig{MaxPayloadBytes: 60 * 1024, MaxDatagramBytes: 65_507, RequestTimeout: 5 * time.Second, ReadPollInterval: 100 * time.Millisecond, MaxConcurrentRequests: 128, MaxReplayEntries: 8192, MaxReplayBytes: 8 * 1024 * 1024, MaxReassemblyBytes: 8 * 1024 * 1024, MaxReassemblyMessages: 256, ReassemblyTimeout: 15 * time.Second, MaxRetries: 2, RetryInterval: 250 * time.Millisecond}
}

func (c UDPConfig) validate() error {
	if c.MaxPayloadBytes <= 0 || c.MaxDatagramBytes < datagramHeaderSize+octetHeaderSize+FrameHeaderSize+1 || c.MaxDatagramBytes > 65_507 || c.RequestTimeout <= 0 || c.ReadPollInterval <= 0 || c.MaxConcurrentRequests <= 0 || c.MaxReplayEntries <= 0 || c.MaxReplayBytes <= 0 || c.MaxReassemblyBytes < c.MaxPayloadBytes+FrameHeaderSize+octetHeaderSize || c.MaxReassemblyMessages <= 0 || c.ReassemblyTimeout <= 0 || c.MaxRetries < 0 || c.RetryInterval <= 0 || (len(c.PreSharedKey) != 0 && len(c.PreSharedKey) != 32) || (c.RequireEncryption && len(c.PreSharedKey) != 32) {
		return errors.New("callgraph: invalid UDP transport limits")
	}
	overhead := datagramHeaderSize + octetHeaderSize + FrameHeaderSize
	if len(c.PreSharedKey) == 32 { overhead += 20 }
	if c.MaxPayloadBytes+overhead > c.MaxDatagramBytes {
		return errors.New("callgraph: payload limit exceeds datagram limit")
	}
	return nil
}

func encodeOCTET(payload []byte, max int) ([]byte, error) {
	if max < 0 || len(payload) > max || uint64(len(payload)) > uint64(^uint32(0)) { return nil, ErrPayloadTooLarge }
	out := make([]byte, octetHeaderSize+len(payload))
	copy(out[:4], "OCT1")
	out[4] = octetVersion
	binary.BigEndian.PutUint32(out[6:10], uint32(len(payload)))
	copy(out[10:], payload)
	return out, nil
}

func decodeOCTET(wire []byte, max int) ([]byte, error) {
	if len(wire) < octetHeaderSize || string(wire[:4]) != "OCT1" || wire[4] != octetVersion { return nil, ErrMalformedOCTET }
	n := uint64(binary.BigEndian.Uint32(wire[6:10]))
	if max < 0 || n > uint64(max) || uint64(len(wire)) != uint64(octetHeaderSize)+n { return nil, ErrMalformedOCTET }
	return wire[10:], nil
}

type datagramEnvelope struct {
	session uint64
	sequence uint64
	payload []byte
}

func encodeDatagram(session, sequence uint64, payload []byte, max int) ([]byte, error) {
	if max < datagramHeaderSize || len(payload) > max-datagramHeaderSize || len(payload) > 65_535 { return nil, ErrPayloadTooLarge }
	out := make([]byte, datagramHeaderSize+len(payload))
	copy(out[:4], "DGM1")
	out[4] = datagramVersion
	binary.BigEndian.PutUint64(out[6:14], session)
	binary.BigEndian.PutUint64(out[14:22], sequence)
	binary.BigEndian.PutUint16(out[22:24], uint16(len(payload)))
	copy(out[24:], payload)
	return out, nil
}

func decodeDatagram(wire []byte, max int) (datagramEnvelope, error) {
	var out datagramEnvelope
	if len(wire) < datagramHeaderSize || len(wire) > max || string(wire[:4]) != "DGM1" || wire[4] != datagramVersion { return out, ErrMalformedDatagram }
	n := int(binary.BigEndian.Uint16(wire[22:24]))
	if len(wire) != datagramHeaderSize+n { return out, ErrMalformedDatagram }
	out.session = binary.BigEndian.Uint64(wire[6:14])
	out.sequence = binary.BigEndian.Uint64(wire[14:22])
	out.payload = wire[24:]
	return out, nil
}


// sealSecurePayload applies AES-256-GCM to a complete OCTET frame. The 96-bit
// nonce is session || (sequence with response-direction bit); sequence values
// are restricted to 31 bits to keep request and response nonces disjoint.
func sealSecurePayload(key []byte, session uint64, sequence uint64, response bool, plaintext []byte) ([]byte, error) {
	if len(key) != 32 || sequence > 0x7fffffff { return nil, ErrMalformedDatagram }
	block, err := aes.NewCipher(key); if err != nil { return nil, err }
	aead, err := cipher.NewGCM(block); if err != nil { return nil, err }
	var nonce [12]byte
	binary.BigEndian.PutUint64(nonce[:8], session)
	seq := uint32(sequence); if response { seq |= 0x80000000 }
	binary.BigEndian.PutUint32(nonce[8:], seq)
	var aad [12]byte
	copy(aad[:], nonce[:])
	sealed := aead.Seal(nil, nonce[:], plaintext, aad[:])
	out := make([]byte, 4+len(sealed)); copy(out[:4], "SEC1"); copy(out[4:], sealed)
	return out, nil
}

func openSecurePayload(key []byte, session uint64, sequence uint64, response bool, wire []byte) ([]byte, error) {
	if len(key) != 32 || sequence > 0x7fffffff || len(wire) < 4+16 || string(wire[:4]) != "SEC1" { return nil, ErrMalformedDatagram }
	block, err := aes.NewCipher(key); if err != nil { return nil, err }
	aead, err := cipher.NewGCM(block); if err != nil { return nil, err }
	var nonce [12]byte
	binary.BigEndian.PutUint64(nonce[:8], session)
	seq := uint32(sequence); if response { seq |= 0x80000000 }
	binary.BigEndian.PutUint32(nonce[8:], seq)
	var aad [12]byte; copy(aad[:], nonce[:])
	return aead.Open(nil, nonce[:], wire[4:], aad[:])
}

// UDPClient sends CALLGRAPH frames inside OCTET frames inside DATAGRAM envelopes.
type CallOptions struct { RetrySafe bool }
type UDPClient struct { config UDPConfig; pathsMu sync.Mutex; paths map[string]*udpPathState }

func NewUDPClient(config UDPConfig) (*UDPClient, error) {
	if err := config.validate(); err != nil { return nil, err }
	return &UDPClient{config: config, paths: make(map[string]*udpPathState)}, nil
}

func (c *UDPClient) Call(ctx context.Context, address string, operationID uint32, major, minor uint16, payload []byte) ([]byte, error) {
	return c.CallWithOptions(ctx, address, operationID, major, minor, payload, CallOptions{})
}

// CallWithOptions retransmits only when the caller explicitly declares the
// operation retry-safe. Non-idempotent operations must not be retried blindly.
func (c *UDPClient) pathFor(address string) *udpPathState {
	c.pathsMu.Lock()
	defer c.pathsMu.Unlock()
	path := c.paths[address]
	if path == nil { path = newUDPPathState(c.config.MaxDatagramBytes); if len(c.paths)<maximumClientPathStates { c.paths[address] = path } }
	return path
}

func (c *UDPClient) CallWithOptions(ctx context.Context, address string, operationID uint32, major, minor uint16, payload []byte, options CallOptions) ([]byte, error) {
	if len(payload)>c.config.MaxPayloadBytes{return nil,ErrPayloadTooLarge}
	remote,err:=net.ResolveUDPAddr("udp",address);if err!=nil{return nil,err}
	conn,err:=net.DialUDP("udp",nil,remote);if err!=nil{return nil,err};defer conn.Close()
	path:=c.pathFor(remote.String())
	budget:=c.config.RequestTimeout
	if deadline,ok:=ctx.Deadline();ok {remaining:=time.Until(deadline);if remaining<=0{return nil,context.DeadlineExceeded};if remaining<budget{budget=remaining}}
	if err:=ctx.Err();err!=nil{return nil,err}
	requestID,err:=random16();if err!=nil{return nil,err}
	session,err:=randomUint64();if err!=nil{return nil,err}
	deadline:=time.Now().Add(budget)
	if parentDeadline,ok:=ctx.Deadline();ok&&parentDeadline.Before(deadline){deadline=parentDeadline}
	mtu:=path.discoverMTU(ctx,conn,c.config,time.Now().Add(min(budget,2*c.config.RetryInterval)))
	frame,err:=EncodeFrame(Frame{Kind:kindRequest,RequestID:requestID,OperationID:operationID,Major:major,Minor:minor,DeadlineMillis:durationMillis(budget),Payload:payload},c.config.MaxPayloadBytes)
	if err!=nil{return nil,err}
	octets,err:=encodeOCTET(frame,c.config.MaxPayloadBytes+FrameHeaderSize);if err!=nil{return nil,err}
	if err=c.sendRequestFragments(ctx,conn,path,session,mtu,octets,deadline);err!=nil{return nil,err}
	responseWire,err:=c.receiveResponseFragments(ctx,conn,session,mtu);if err!=nil{return nil,err}
	rpcWire,err:=decodeOCTET(responseWire,c.config.MaxPayloadBytes+FrameHeaderSize);if err!=nil{return nil,err}
	response,err:=DecodeFrame(rpcWire,c.config.MaxPayloadBytes);if err!=nil{return nil,err}
	if response.RequestID!=requestID{return nil,ErrMalformedFrame}
	switch response.Kind {
	case kindResponse:return response.Payload,nil
	case kindError:
		code,message,decodeErr:=DecodeErrorPayload(response.Payload);if decodeErr!=nil{return nil,decodeErr}
		return nil,&RemoteError{Code:code,Message:message}
	default:return nil,ErrUnexpectedMessage
	}
}

// UDPServer dispatches registered CALLGRAPH operations over the shared wire profiles.
type replayKey struct { session uint64; messageID uint64 }
type replayEntry struct { expires time.Time; response []byte }
type pendingResponseKey struct { session uint64; peer string; sequence uint64 }
type pendingResponse struct { packet []byte; path *udpPathState; congestionID uint64; done chan struct{} }

type UDPServer struct {
	runtime *Runtime
	config UDPConfig
	mu sync.RWMutex
	operations map[uint32]Descriptor
	replayMu sync.Mutex
	replay map[replayKey]replayEntry
	replayBytes int
	reassembler *fragmentReassembler
	pendingMu sync.Mutex
	pending map[pendingResponseKey]pendingResponse
	pathsMu sync.Mutex
	paths map[string]*udpPathState
	workers chan struct{}
	wg sync.WaitGroup
}

func NewUDPServer(runtime *Runtime, config UDPConfig) (*UDPServer, error) {
	if runtime == nil { return nil, errors.New("callgraph: runtime is required") }
	if err := config.validate(); err != nil { return nil, err }
	return &UDPServer{runtime: runtime, config: config, operations: make(map[uint32]Descriptor), replay: make(map[replayKey]replayEntry), pending: make(map[pendingResponseKey]pendingResponse), reassembler:newFragmentReassembler(config.MaxReassemblyBytes,config.MaxReassemblyMessages,config.ReassemblyTimeout), paths: make(map[string]*udpPathState), workers: make(chan struct{}, config.MaxConcurrentRequests)}, nil
}

func (s *UDPServer) RegisterOperation(d Descriptor, authorize Authorizer, handler Handler) error {
	if err := d.Validate(); err != nil { return err }
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.operations[d.ID]; exists { return ErrDuplicateMethod }
	if err := s.runtime.RegisterOperation(d, authorize, handler); err != nil { return err }
	s.operations[d.ID] = d
	return nil
}

func (s *UDPServer) pathFor(peer *net.UDPAddr) *udpPathState {
	key := peerPathKey(peer)
	s.pathsMu.Lock()
	defer s.pathsMu.Unlock()
	path := s.paths[key]
	if path == nil { path = newUDPPathState(s.config.MaxDatagramBytes); if len(s.paths)<maximumServerPathStates { s.paths[key] = path } }
	return path
}


// Serve runs until ctx is cancelled. It polls the socket so cancellation is bounded.
func (s *UDPServer) Serve(ctx context.Context, conn *net.UDPConn) error {
	if conn==nil{return errors.New("callgraph: UDP socket is required")}
	buf:=make([]byte,s.config.MaxDatagramBytes+1)
	for ctx.Err()==nil {
		_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadPollInterval))
		n,peer,err:=conn.ReadFromUDP(buf)
		if err!=nil {if ne,ok:=err.(net.Error);ok&&ne.Timeout(){continue};if ctx.Err()!=nil{break};return err}
		if n>s.config.MaxDatagramBytes{continue}
		packet:=append([]byte(nil),buf[:n]...)
		envelope,decodeErr:=decodeDatagram(packet,s.config.MaxDatagramBytes);if decodeErr!=nil{continue}
		plain:=envelope.payload
		if len(s.config.PreSharedKey)==32 {plain,decodeErr=openSecurePayload(s.config.PreSharedKey,envelope.session,envelope.sequence,false,envelope.payload);if decodeErr!=nil{continue}} else if s.config.RequireEncryption {continue}
		path:=s.pathFor(peer)
		if len(plain)>=4&&string(plain[:4])=="MTP1" {s.handleMTUProbe(conn,peer,packet,envelope,plain,path);continue}
		if len(plain)>=4&&string(plain[:4])=="ACK1" {
			ack,ackErr:=DecodeACK(plain);if ackErr==nil{s.acknowledgeResponse(peer,envelope.session,envelope.sequence,ack)}
			continue
		}
		if len(plain)<4||string(plain[:4])!="FRG1"{continue}
		fragment,fragmentErr:=decodeFragment(plain,s.config.MaxReassemblyBytes);if fragmentErr!=nil{continue}
		if envelope.sequence!=uint64(fragment.index)+1{continue}
		reassemblyKey:=peer.String()+"/"+strconv.FormatUint(envelope.session,10)+"/"+strconv.FormatUint(fragment.messageID,10)
		assembled,complete,addErr:=s.reassembler.Add(reassemblyKey,fragment,time.Now())
		if addErr!=nil{continue}
		if !complete {
			_ = writeFragmentACK(conn,envelope.session,envelope.sequence,requestAckSequenceBase+uint64(fragment.index),true,path.currentMTU(),s.config.PreSharedKey)
			continue
		}
		select {
		case s.workers<-struct{}{}:
			accepted,cached:=s.acceptReplay(envelope.session,fragment.messageID)
			if !accepted {
				<-s.workers
				_ = writeFragmentACK(conn,envelope.session,envelope.sequence,requestAckSequenceBase+uint64(fragment.index),true,path.currentMTU(),s.config.PreSharedKey)
				if len(cached)>0 {
					select {case s.workers<-struct{}{}:
						s.wg.Add(1)
						go func(peer *net.UDPAddr,session uint64,response []byte,path *udpPathState){
							defer s.wg.Done();defer func(){<-s.workers}()
							s.transmitMessage(ctx,conn,peer,session,response,path)
						}(peer,envelope.session,cached,path)
					default:}
				}
				continue
			}
			_ = writeFragmentACK(conn,envelope.session,envelope.sequence,requestAckSequenceBase+uint64(fragment.index),true,path.currentMTU(),s.config.PreSharedKey)
			s.wg.Add(1)
			go func(peer *net.UDPAddr,session,messageID uint64,assembled []byte,path *udpPathState){
				defer s.wg.Done();defer func(){<-s.workers}()
				wire:=assembled
				if len(s.config.PreSharedKey)==32 {
					var sealErr error
					wire,sealErr=sealSecurePayload(s.config.PreSharedKey,session,1,false,assembled)
					if sealErr!=nil{return}
				}
				requestPacket,encodeErr:=encodeDatagram(session,1,wire,s.config.MaxDatagramBytes);if encodeErr!=nil{return}
				response:=s.handlePacket(ctx,requestPacket);if len(response)==0{return}
				s.storeReplayResponse(session,messageID,response)
				s.transmitMessage(ctx,conn,peer,session,response,path)
			}(peer,envelope.session,fragment.messageID,assembled,path)
		default:
			// Do not acknowledge the final fragment while execution capacity is
			// exhausted. The sender retries it; the completed assembly is retained.
		}
	}
	s.wg.Wait()
	return ctx.Err()
}

func (s *UDPServer) acknowledgeResponse(peer *net.UDPAddr,session,ackSequence uint64,ack AckFrame) {
	if ack.Validate()!=nil||ack.LargestAcked<responseDataSequenceBase||
		ack.LargestAcked>=responseDataSequenceBase+maxMessageFragments||
		ackSequence!=responseAckSequenceBase+(ack.LargestAcked-responseDataSequenceBase){return}
	for _,r:=range ack.Ranges {
		if r.First<responseDataSequenceBase||r.Last>=responseDataSequenceBase+maxMessageFragments{return}
	}
	s.pendingMu.Lock()
	var acknowledged []pendingResponse
	for key,entry:=range s.pending {
		if key.session!=session||key.peer!=peer.String()||!ackCovers(ack,key.sequence){continue}
		delete(s.pending,key)
		acknowledged=append(acknowledged,entry)
	}
	s.pendingMu.Unlock()
	for _,entry:=range acknowledged {
		if entry.path!=nil&&entry.congestionID!=0{entry.path.ack(entry.congestionID)}
		if entry.done!=nil{close(entry.done)}
	}
}

func (s *UDPServer) acceptReplay(session,messageID uint64) (bool,[]byte) {
	now:=time.Now()
	key:=replayKey{session:session,messageID:messageID}
	s.replayMu.Lock();defer s.replayMu.Unlock()
	if entry,exists:=s.replay[key];exists {
		if now.Before(entry.expires){return false,append([]byte(nil),entry.response...)}
		s.replayBytes-=len(entry.response);delete(s.replay,key)
	}
	if len(s.replay)>=s.config.MaxReplayEntries {
		for k,entry:=range s.replay {if !now.Before(entry.expires){s.replayBytes-=len(entry.response);delete(s.replay,k)}}
		if len(s.replay)>=s.config.MaxReplayEntries{return false,nil}
	}
	s.replay[key]=replayEntry{expires:now.Add(2*s.config.RequestTimeout)}
	return true,nil
}

func (s *UDPServer) storeReplayResponse(session,messageID uint64,response []byte) {
	key:=replayKey{session:session,messageID:messageID}
	s.replayMu.Lock();defer s.replayMu.Unlock()
	entry,ok:=s.replay[key];if !ok{return}
	if len(response)>s.config.MaxReplayBytes-s.replayBytes+len(entry.response){return}
	s.replayBytes-=len(entry.response)
	entry.response=append([]byte(nil),response...)
	s.replayBytes+=len(entry.response)
	s.replay[key]=entry
}

func (s *UDPServer) handlePacket(parent context.Context, packet []byte) []byte {
	envelope, err := decodeDatagram(packet, s.config.MaxDatagramBytes)
	if err != nil { return nil }
	wirePayload := envelope.payload
	if len(s.config.PreSharedKey) == 32 {
		wirePayload, err = openSecurePayload(s.config.PreSharedKey, envelope.session, envelope.sequence, false, wirePayload)
		if err != nil { return nil }
	} else if s.config.RequireEncryption { return nil }
	rpcWire, err := decodeOCTET(wirePayload, s.config.MaxDatagramBytes)
	if err != nil { return nil }
	request, err := DecodeFrame(rpcWire, s.config.MaxPayloadBytes)
	if err != nil || request.Kind != kindRequest || request.OperationID == 0 { return nil }

	s.mu.RLock()
	descriptor, exists := s.operations[request.OperationID]
	s.mu.RUnlock()
	kind := uint8(kindResponse)
	var result []byte
	if !exists {
		kind = kindError
		result = encodeErrorPayload(ErrorCodeMethodNotFound, ErrMethodNotFound.Error())
	} else if descriptor.Major != request.Major {
		kind = kindError
		result = encodeErrorPayload(ErrorCodeOperationVersion, ErrOperationVersion.Error())
	} else {
		budget := time.Duration(request.DeadlineMillis) * time.Millisecond
		if budget <= 0 || budget > descriptor.MaxDeadline { budget = descriptor.MaxDeadline }
		ctx, cancel := context.WithTimeout(parent, budget)
		result, err = s.runtime.InvokeOperation(ctx, descriptor, request.Payload)
		cancel()
		if err != nil {
			kind = kindError
			code := ErrorCodeInternal
			if errors.Is(err, ErrUnauthorized) { code = ErrorCodeUnauthorized }
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrDeadlineExceeded) { code = ErrorCodeDeadlineExceeded }
			if errors.Is(err, ErrPayloadTooLarge) { code = ErrorCodePayloadLimit }
			result = encodeErrorPayload(code, err.Error())
		}
	}
	responseFrame, err := EncodeFrame(Frame{Kind: kind, RequestID: request.RequestID, OperationID: request.OperationID, Major: request.Major, Minor: request.Minor, Payload: result}, s.config.MaxPayloadBytes)
	if err != nil { return nil }
	octets, err := encodeOCTET(responseFrame, s.config.MaxPayloadBytes+FrameHeaderSize)
	if err != nil { return nil }
	return octets
}

func secureOverhead(key []byte) int { if len(key)==32 { return 20 }; return 0 }

func durationMillis(d time.Duration) uint32 {
	ms := d / time.Millisecond
	if ms < 1 { return 1 }
	if ms > time.Duration(^uint32(0)) { return ^uint32(0) }
	return uint32(ms)
}

func random16() ([16]byte, error) { var id [16]byte; _, err := rand.Read(id[:]); return id, err }
func randomUint64() (uint64, error) { var b [8]byte; if _, err := rand.Read(b[:]); err != nil { return 0, err }; return binary.BigEndian.Uint64(b[:]), nil }
