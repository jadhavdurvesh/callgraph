package callgraph

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
	"sync/atomic"
)

func TestUDPClientServerLargeFragmentedRequestAndResponse(t *testing.T) {
	for _,encrypted:=range []bool{false,true} {
		name:="plaintext";if encrypted{name="aes-gcm"}
		t.Run(name,func(t *testing.T){
			runtime,err:=New(DefaultConfig());if err!=nil{t.Fatal(err)}
			cfg:=DefaultUDPConfig()
			cfg.MaxDatagramBytes=1200 // exercise reassembly below the path MTU
			cfg.RequestTimeout=8*time.Second
			cfg.RetryInterval=40*time.Millisecond
			cfg.MaxRetries=4
			if encrypted {
				cfg.PreSharedKey=bytes.Repeat([]byte{0x5a},32)
				cfg.RequireEncryption=true
			}
			server,err:=NewUDPServer(runtime,cfg);if err!=nil{t.Fatal(err)}
			expected:=bytes.Repeat([]byte("large-callgraph-payload-"),1800)
			responsePrefix:=[]byte("server-response:")
			var calls atomic.Int32
			d:=Descriptor{Name:"fragment.echo",ID:OperationID("test.fragment.echo"),Major:1,Idempotency:ReadOnly,AuthorizationScope:"test:fragment",MaxDeadline:5*time.Second}
			if err:=server.RegisterOperation(d,func(context.Context,string)error{return nil},func(_ context.Context,p []byte)([]byte,error){
				calls.Add(1)
				return append(append([]byte(nil),responsePrefix...),p...),nil
			});err!=nil{t.Fatal(err)}
			conn,err:=net.ListenUDP("udp",&net.UDPAddr{IP:net.ParseIP("127.0.0.1")});if err!=nil{t.Fatal(err)}
			ctx,cancel:=context.WithCancel(context.Background())
			done:=make(chan struct{})
			go func(){_ = server.Serve(ctx,conn);close(done)}()
			defer func(){cancel();_ = conn.Close();<-done}()
			client,err:=NewUDPClient(cfg);if err!=nil{t.Fatal(err)}
			callCtx,callCancel:=context.WithTimeout(context.Background(),8*time.Second);defer callCancel()
			got,err:=client.Call(callCtx,conn.LocalAddr().String(),d.ID,1,0,expected)
			if err!=nil {
				server.pendingMu.Lock();pending:=len(server.pending);server.pendingMu.Unlock()
				server.replayMu.Lock();cachedBytes:=0;inFlight:=0;replayEntries:=len(server.replay);for _,entry:=range server.replay{cachedBytes+=len(entry.response);if entry.inFlight{inFlight++}};server.replayMu.Unlock()
				server.reassembler.mu.Lock();assemblies:=len(server.reassembler.messages);received:=0;receivedBytes:=0;for _,assembly:=range server.reassembler.messages{for _,ok:=range assembly.received{if ok{received++}};receivedBytes+=assembly.bytesReceived};server.reassembler.mu.Unlock()
				t.Fatalf("call failed: %v; handler calls=%d pending fragments=%d cached response bytes=%d in-flight messages=%d replay entries=%d assemblies=%d received fragments=%d received bytes=%d",err,calls.Load(),pending,cachedBytes,inFlight,replayEntries,assemblies,received,receivedBytes)
			}
			want:=append(append([]byte(nil),responsePrefix...),expected...)
			if !bytes.Equal(got,want){t.Fatalf("fragmented response mismatch: got %d bytes, want %d",len(got),len(want))}
			if messages,used:=server.reassembler.Stats();messages==0||used<len(expected){t.Fatalf("server did not retain bounded completed assembly: messages=%d bytes=%d",messages,used)}
		})
	}
}
