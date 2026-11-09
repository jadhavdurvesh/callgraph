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
			if err!=nil{t.Fatal(err)}
			want:=append(append([]byte(nil),responsePrefix...),expected...)
			if !bytes.Equal(got,want){t.Fatalf("fragmented response mismatch: got %d bytes, want %d",len(got),len(want))}
			if calls.Load()!=1{t.Fatalf("handler executed %d times, want exactly once",calls.Load())}
			if messages,used:=server.reassembler.Stats();messages==0||used<len(expected){t.Fatalf("server did not retain bounded completed assembly: messages=%d bytes=%d",messages,used)}
		})
	}
}
