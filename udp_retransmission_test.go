package callgraph

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestUDPServerClearsPendingFragmentsAfterACK(t *testing.T) {
	runtime,err:=New(DefaultConfig());if err!=nil{t.Fatal(err)}
	cfg:=DefaultUDPConfig();cfg.RetryInterval=30*time.Millisecond;cfg.MaxRetries=4
	server,err:=NewUDPServer(runtime,cfg);if err!=nil{t.Fatal(err)}
	d:=Descriptor{Name:"echo",ID:OperationID("test.echo"),Major:1,Idempotency:ReadOnly,AuthorizationScope:"test:read",MaxDeadline:time.Second}
	if err:=server.RegisterOperation(d,func(context.Context,string)error{return nil},func(_ context.Context,p []byte)([]byte,error){return p,nil});err!=nil{t.Fatal(err)}
	conn,err:=net.ListenUDP("udp",&net.UDPAddr{IP:net.ParseIP("127.0.0.1")});if err!=nil{t.Fatal(err)}
	ctx,cancel:=context.WithCancel(context.Background());done:=make(chan struct{})
	go func(){_ = server.Serve(ctx,conn);close(done)}()
	defer func(){cancel();_ = conn.Close();<-done}()
	client,err:=NewUDPClient(cfg);if err!=nil{t.Fatal(err)}
	got,err:=client.Call(context.Background(),conn.LocalAddr().String(),d.ID,1,0,[]byte("hello"))
	if err!=nil{t.Fatal(err)}
	if string(got)!="hello"{t.Fatalf("response %q",got)}
	deadline:=time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		server.pendingMu.Lock();pending:=len(server.pending);server.pendingMu.Unlock()
		if pending==0 {break}
		time.Sleep(5*time.Millisecond)
	}
	server.pendingMu.Lock();pending:=len(server.pending);server.pendingMu.Unlock()
	if pending!=0{t.Fatalf("server retained %d acknowledged response fragments",pending)}
	server.pathsMu.Lock();var path *udpPathState
	for _,candidate:=range server.paths{path=candidate;break}
	server.pathsMu.Unlock()
	if path==nil{t.Fatal("server path state missing")}
	path.ccMu.Lock();flight:=path.cc.bytesInFlight;outstanding:=len(path.cc.outstanding);path.ccMu.Unlock()
	if flight!=0||outstanding!=0{t.Fatalf("ACK failed to release congestion accounting: bytes=%d packets=%d",flight,outstanding)}
}
