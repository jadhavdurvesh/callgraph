package callgraph

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
)

func TestAESGCMWirePayloadAuthenticatesAndSeparatesDirections(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	plain := []byte("OCT1 test payload")
	sealed, err := sealSecurePayload(key, 0x12345678, 1, false, plain)
	if err != nil { t.Fatal(err) }
	opened, err := openSecurePayload(key, 0x12345678, 1, false, sealed)
	if err != nil || !bytes.Equal(opened, plain) { t.Fatalf("open failed: %v", err) }
	if _, err := openSecurePayload(bytes.Repeat([]byte{0x24},32), 0x12345678, 1, false, sealed); err == nil {
		t.Fatal("wrong key must fail authentication")
	}
	if _, err := openSecurePayload(key, 0x12345678, 1, true, sealed); err == nil {
		t.Fatal("response nonce domain must not authenticate request ciphertext")
	}
	sealed[len(sealed)-1] ^= 1
	if _, err := openSecurePayload(key, 0x12345678, 1, false, sealed); err == nil {
		t.Fatal("modified ciphertext must fail authentication")
	}
}

func TestSecureUDPCallEndToEnd(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	runtime, err := New(DefaultConfig()); if err != nil { t.Fatal(err) }
	serverConfig := DefaultUDPConfig()
	serverConfig.PreSharedKey = key
	serverConfig.RequireEncryption = true
	server, err := NewUDPServer(runtime, serverConfig); if err != nil { t.Fatal(err) }
	d := Descriptor{Name:"secure.echo", ID:77, Major:1, Idempotency:ReadOnly, AuthorizationScope:"test:echo", MaxDeadline:time.Second}
	if err := server.RegisterOperation(d, func(context.Context,string) error{return nil}, func(_ context.Context,p []byte)([]byte,error){return append([]byte("secure:"),p...),nil}); err != nil { t.Fatal(err) }
	addr, err := net.ResolveUDPAddr("udp","127.0.0.1:0"); if err != nil { t.Fatal(err) }
	conn, err := net.ListenUDP("udp",addr); if err != nil { t.Fatal(err) }
	ctx,cancel:=context.WithCancel(context.Background()); defer cancel()
	go func(){ _=server.Serve(ctx,conn) }()
	clientConfig:=DefaultUDPConfig(); clientConfig.PreSharedKey=key
	client,err:=NewUDPClient(clientConfig); if err!=nil {t.Fatal(err)}
	got,err:=client.Call(context.Background(),conn.LocalAddr().String(),77,1,0,[]byte("payload"))
	if err!=nil {t.Fatal(err)}
	if string(got)!="secure:payload" {t.Fatalf("response %q",got)}
	_ = conn.Close()
}
