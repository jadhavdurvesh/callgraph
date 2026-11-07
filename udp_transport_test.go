package callgraph

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func startTestUDPServer(t *testing.T, register func(*UDPServer) error) (string, context.CancelFunc) {
	t.Helper()
	runtime, err := New(DefaultConfig())
	if err != nil { t.Fatal(err) }
	config := DefaultUDPConfig()
	server, err := NewUDPServer(runtime, config)
	if err != nil { t.Fatal(err) }
	if err := register(server); err != nil { t.Fatal(err) }
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	conn, err := net.ListenUDP("udp", addr)
	if err != nil { t.Fatal(err) }
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = server.Serve(ctx, conn); _ = conn.Close() }()
	t.Cleanup(func() { cancel(); _ = conn.Close() })
	return conn.LocalAddr().String(), cancel
}

func TestUDPCallEndToEnd(t *testing.T) {
	address, _ := startTestUDPServer(t, func(s *UDPServer) error {
		d := Descriptor{Name:"math.add-one", ID:OperationID("example.math.add-one"), Major:1, Minor:0, Idempotency:ReadOnly, AuthorizationScope:"math:read", MaxDeadline:2*time.Second}
		return s.RegisterOperation(d, func(context.Context,string) error { return nil }, func(_ context.Context, p []byte) ([]byte,error) {
			return append(append([]byte(nil), p...), '!'), nil
		})
	})
	client, err := NewUDPClient(DefaultUDPConfig())
	if err != nil { t.Fatal(err) }
	got, err := client.Call(context.Background(), address, OperationID("example.math.add-one"), 1, 0, []byte("hello"))
	if err != nil { t.Fatal(err) }
	if string(got) != "hello!" { t.Fatalf("response = %q", got) }
}

func TestUDPCallReportsUnknownOperation(t *testing.T) {
	address, _ := startTestUDPServer(t, func(*UDPServer) error { return nil })
	client, _ := NewUDPClient(DefaultUDPConfig())
	_, err := client.Call(context.Background(), address, 99, 1, 0, []byte("x"))
	if !errors.Is(err, ErrRemoteOperation) { t.Fatalf("expected remote error, got %v", err) }
}

func TestUDPCallUsesContextDeadline(t *testing.T) {
	address, _ := startTestUDPServer(t, func(s *UDPServer) error {
		d := Descriptor{Name:"slow", ID:77, Major:1, Idempotency:ReadOnly, AuthorizationScope:"test", MaxDeadline:2*time.Second}
		return s.RegisterOperation(d, func(context.Context,string) error{return nil}, func(ctx context.Context,_ []byte)([]byte,error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
	})
	client, _ := NewUDPClient(DefaultUDPConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := client.Call(ctx,address,77,1,0,nil)
	if err == nil || !strings.Contains(err.Error(),"deadline") { t.Fatalf("expected deadline error, got %v",err) }
}

func TestNestedWireProfilesAreStrict(t *testing.T) {
	rpc, err := EncodeFrame(Frame{Kind:kindRequest,OperationID:7,Payload:[]byte("payload")},1024)
	if err != nil { t.Fatal(err) }
	octets, err := encodeOCTET(rpc,1024)
	if err != nil { t.Fatal(err) }
	packet, err := encodeDatagram(3,1,octets,4096)
	if err != nil { t.Fatal(err) }
	envelope, err := decodeDatagram(packet,4096)
	if err != nil { t.Fatal(err) }
	decodedOCTET, err := decodeOCTET(envelope.payload,1024)
	if err != nil { t.Fatal(err) }
	decodedRPC, err := DecodeFrame(decodedOCTET,1024)
	if err != nil { t.Fatal(err) }
	if decodedRPC.OperationID != 7 || string(decodedRPC.Payload)!="payload" { t.Fatalf("unexpected RPC frame: %#v",decodedRPC) }
}
