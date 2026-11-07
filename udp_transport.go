package callgraph

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
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
}

func DefaultUDPConfig() UDPConfig {
	return UDPConfig{MaxPayloadBytes: 60 * 1024, MaxDatagramBytes: 65_507, RequestTimeout: 5 * time.Second, ReadPollInterval: 100 * time.Millisecond}
}

func (c UDPConfig) validate() error {
	if c.MaxPayloadBytes <= 0 || c.MaxDatagramBytes < datagramHeaderSize+octetHeaderSize+FrameHeaderSize+1 || c.MaxDatagramBytes > 65_507 || c.RequestTimeout <= 0 || c.ReadPollInterval <= 0 {
		return errors.New("callgraph: invalid UDP transport limits")
	}
	if c.MaxPayloadBytes+datagramHeaderSize+octetHeaderSize+FrameHeaderSize > c.MaxDatagramBytes {
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

// UDPClient sends CALLGRAPH frames inside OCTET frames inside DATAGRAM envelopes.
type UDPClient struct { config UDPConfig }

func NewUDPClient(config UDPConfig) (*UDPClient, error) {
	if err := config.validate(); err != nil { return nil, err }
	return &UDPClient{config: config}, nil
}

func (c *UDPClient) Call(ctx context.Context, address string, operationID uint32, major, minor uint16, payload []byte) ([]byte, error) {
	if len(payload) > c.config.MaxPayloadBytes { return nil, ErrPayloadTooLarge }
	remote, err := net.ResolveUDPAddr("udp", address)
	if err != nil { return nil, err }
	conn, err := net.DialUDP("udp", nil, remote)
	if err != nil { return nil, err }
	defer conn.Close()

	budget := c.config.RequestTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 { return nil, context.DeadlineExceeded }
		if remaining < budget { budget = remaining }
	}
	if err := ctx.Err(); err != nil { return nil, err }
	requestID, err := random16()
	if err != nil { return nil, err }
	session, err := randomUint64()
	if err != nil { return nil, err }

	frame, err := EncodeFrame(Frame{Kind: kindRequest, RequestID: requestID, OperationID: operationID, Major: major, Minor: minor, DeadlineMillis: durationMillis(budget), Payload: payload}, c.config.MaxPayloadBytes)
	if err != nil { return nil, err }
	octets, err := encodeOCTET(frame, c.config.MaxDatagramBytes-datagramHeaderSize-octetHeaderSize)
	if err != nil { return nil, err }
	packet, err := encodeDatagram(session, 1, octets, c.config.MaxDatagramBytes)
	if err != nil { return nil, err }

	deadline := time.Now().Add(budget)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) { deadline = parentDeadline }
	if err := conn.SetDeadline(deadline); err != nil { return nil, err }
	if _, err := conn.Write(packet); err != nil { return nil, err }
	buf := make([]byte, c.config.MaxDatagramBytes)
	n, err := conn.Read(buf)
	if err != nil {
		if ctx.Err() != nil { return nil, ctx.Err() }
		return nil, err
	}
	envelope, err := decodeDatagram(buf[:n], c.config.MaxDatagramBytes)
	if err != nil { return nil, err }
	if envelope.session != session || envelope.sequence != 1 { return nil, ErrMalformedDatagram }
	rpcWire, err := decodeOCTET(envelope.payload, c.config.MaxDatagramBytes)
	if err != nil { return nil, err }
	response, err := DecodeFrame(rpcWire, c.config.MaxPayloadBytes)
	if err != nil { return nil, err }
	if response.RequestID != requestID { return nil, ErrMalformedFrame }
	switch response.Kind {
	case kindResponse:
		return response.Payload, nil
	case kindError:
		return nil, fmt.Errorf("%w: %s", ErrRemoteOperation, string(response.Payload))
	default:
		return nil, ErrUnexpectedMessage
	}
}

// UDPServer dispatches registered CALLGRAPH operations over the shared wire profiles.
type UDPServer struct {
	runtime *Runtime
	config UDPConfig
	mu sync.RWMutex
	operations map[uint32]Descriptor
}

func NewUDPServer(runtime *Runtime, config UDPConfig) (*UDPServer, error) {
	if runtime == nil { return nil, errors.New("callgraph: runtime is required") }
	if err := config.validate(); err != nil { return nil, err }
	return &UDPServer{runtime: runtime, config: config, operations: make(map[uint32]Descriptor)}, nil
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

// Serve runs until ctx is cancelled. It polls the socket so cancellation is bounded.
func (s *UDPServer) Serve(ctx context.Context, conn *net.UDPConn) error {
	if conn == nil { return errors.New("callgraph: UDP socket is required") }
	buf := make([]byte, s.config.MaxDatagramBytes+1)
	for {
		if err := ctx.Err(); err != nil { return err }
		_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadPollInterval))
		n, peer, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() { continue }
			if ctx.Err() != nil { return ctx.Err() }
			return err
		}
		if n > s.config.MaxDatagramBytes { continue }
		response := s.handlePacket(ctx, buf[:n])
		if len(response) > 0 { _, _ = conn.WriteToUDP(response, peer) }
	}
}

func (s *UDPServer) handlePacket(parent context.Context, packet []byte) []byte {
	envelope, err := decodeDatagram(packet, s.config.MaxDatagramBytes)
	if err != nil { return nil }
	rpcWire, err := decodeOCTET(envelope.payload, s.config.MaxDatagramBytes)
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
		result = []byte(ErrMethodNotFound.Error())
	} else if descriptor.Major != request.Major {
		kind = kindError
		result = []byte(ErrOperationVersion.Error())
	} else {
		budget := time.Duration(request.DeadlineMillis) * time.Millisecond
		if budget <= 0 || budget > descriptor.MaxDeadline { budget = descriptor.MaxDeadline }
		ctx, cancel := context.WithTimeout(parent, budget)
		result, err = s.runtime.InvokeOperation(ctx, descriptor, request.Payload)
		cancel()
		if err != nil { kind = kindError; result = []byte(err.Error()) }
	}
	responseFrame, err := EncodeFrame(Frame{Kind: kind, RequestID: request.RequestID, OperationID: request.OperationID, Major: request.Major, Minor: request.Minor, Payload: result}, s.config.MaxPayloadBytes)
	if err != nil { return nil }
	octets, err := encodeOCTET(responseFrame, s.config.MaxDatagramBytes-datagramHeaderSize-octetHeaderSize)
	if err != nil { return nil }
	response, err := encodeDatagram(envelope.session, envelope.sequence, octets, s.config.MaxDatagramBytes)
	if err != nil { return nil }
	return response
}

func durationMillis(d time.Duration) uint32 {
	ms := d / time.Millisecond
	if ms < 1 { return 1 }
	if ms > time.Duration(^uint32(0)) { return ^uint32(0) }
	return uint32(ms)
}

func random16() ([16]byte, error) { var id [16]byte; _, err := rand.Read(id[:]); return id, err }
func randomUint64() (uint64, error) { var b [8]byte; if _, err := rand.Read(b[:]); err != nil { return 0, err }; return binary.BigEndian.Uint64(b[:]), nil }
