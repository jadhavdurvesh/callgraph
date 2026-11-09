package callgraph

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestUDPPathMTUDiscoveryAndCongestionFeedback(t *testing.T) {
	runtime,err:=New(DefaultConfig());if err!=nil{t.Fatal(err)}
	cfg:=DefaultUDPConfig();cfg.RetryInterval=30*time.Millisecond;cfg.MaxRetries=3
	server,err:=NewUDPServer(runtime,cfg);if err!=nil{t.Fatal(err)}
	var calls atomic.Int32
	d:=Descriptor{Name:"path.echo",ID:OperationID("test.path.echo"),Major:1,Idempotency:ReadOnly,AuthorizationScope:"test:read",MaxDeadline:time.Second}
	if err:=server.RegisterOperation(d,func(context.Context,string)error{return nil},func(_ context.Context,p []byte)([]byte,error){calls.Add(1);return p,nil});err!=nil{t.Fatal(err)}
	conn,err:=net.ListenUDP("udp",&net.UDPAddr{IP:net.ParseIP("127.0.0.1")});if err!=nil{t.Fatal(err)}
	ctx,cancel:=context.WithCancel(context.Background());done:=make(chan struct{})
	go func(){_ = server.Serve(ctx,conn);close(done)}()
	defer func(){cancel();_ = conn.Close();<-done}()
	client,err:=NewUDPClient(cfg);if err!=nil{t.Fatal(err)}
	result,err:=client.Call(context.Background(),conn.LocalAddr().String(),d.ID,1,0,[]byte("path-state"))
	if err!=nil{t.Fatal(err)}
	if string(result)!="path-state"{t.Fatalf("response %q",result)}
	client.pathsMu.Lock();clientPath:=client.paths[conn.LocalAddr().String()];client.pathsMu.Unlock()
	if clientPath==nil{t.Fatal("client path state was not retained")}
	if got:=clientPath.currentMTU();got<=defaultPathDatagramSize{t.Fatalf("MTU discovery did not increase baseline: %d",got)}
	server.pathsMu.Lock();serverPath:=server.paths[peerPathKey(&net.UDPAddr{IP:net.ParseIP("127.0.0.1")})];server.pathsMu.Unlock()
	if serverPath==nil{t.Fatal("server path state was not retained")}
	serverPath.ccMu.Lock();serverWindow:=serverPath.cc.cwnd;serverFlight:=serverPath.cc.bytesInFlight;serverOutstanding:=len(serverPath.cc.outstanding);serverPath.ccMu.Unlock()
	if serverWindow<=0{t.Fatalf("invalid congestion window: %d",serverWindow)}
	deadline:=time.Now().Add(time.Second)
	for time.Now().Before(deadline)&&serverOutstanding!=0{time.Sleep(5*time.Millisecond);serverPath.ccMu.Lock();serverOutstanding=len(serverPath.cc.outstanding);serverFlight=serverPath.cc.bytesInFlight;serverPath.ccMu.Unlock()}
	if serverOutstanding!=0||serverFlight!=0{t.Fatalf("ACK did not release server in-flight accounting: packets=%d bytes=%d",serverOutstanding,serverFlight)}
	if got:=calls.Load();got!=1{t.Fatalf("handler ran %d times",got)}
}
