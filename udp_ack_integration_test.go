package callgraph

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestUDPResponseIsAcknowledgedAndNotReexecuted(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false:"plaintext",true:"encrypted"}[encrypted], func(t *testing.T) {
			runtime, err := New(DefaultConfig()); if err != nil { t.Fatal(err) }
			cfg := DefaultUDPConfig()
			cfg.RetryInterval = 40*time.Millisecond
			cfg.MaxRetries = 3
			if encrypted { cfg.PreSharedKey = make([]byte,32); for i := range cfg.PreSharedKey { cfg.PreSharedKey[i] = byte(i+1) }; cfg.RequireEncryption = true }
			server, err := NewUDPServer(runtime,cfg); if err != nil { t.Fatal(err) }
			var executions atomic.Int32
			d := Descriptor{Name:"counter.once",ID:OperationID("test.counter.once"),Major:1,Idempotency:ReadOnly,AuthorizationScope:"test:read",MaxDeadline:time.Second}
			if err := server.RegisterOperation(d,func(context.Context,string)error{return nil},func(context.Context,[]byte)([]byte,error){executions.Add(1);return []byte("ok"),nil}); err != nil { t.Fatal(err) }
			conn, err := net.ListenUDP("udp",&net.UDPAddr{IP:net.ParseIP("127.0.0.1")}); if err != nil { t.Fatal(err) }
			ctx,cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func(){ _ = server.Serve(ctx,conn); close(done) }()
			clientCfg := cfg
			client, err := NewUDPClient(clientCfg); if err != nil { t.Fatal(err) }
			result, err := client.Call(context.Background(),conn.LocalAddr().String(),d.ID,1,0,[]byte("x"))
			if err != nil { t.Fatal(err) }
			if string(result)!="ok" { t.Fatalf("response = %q",result) }
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				server.pendingMu.Lock(); pending := len(server.pending); server.pendingMu.Unlock()
				if pending==0 { break }
				time.Sleep(5*time.Millisecond)
			}
			server.pendingMu.Lock(); pending := len(server.pending); server.pendingMu.Unlock()
			if pending!=0 { t.Fatalf("response was not acknowledged; pending=%d",pending) }
			if got:=executions.Load(); got!=1 { t.Fatalf("operation executed %d times",got) }
			cancel(); _ = conn.Close(); <-done
		})
	}
}
