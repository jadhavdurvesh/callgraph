package callgraph

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
)

func TestUDPClientSlidingWindowSendsMultipleFragmentsBeforeACK(t *testing.T) {
	server,err:=net.ListenUDP("udp",&net.UDPAddr{IP:net.ParseIP("127.0.0.1")})
	if err!=nil{t.Fatal(err)}
	defer server.Close()
	clientConn,err:=net.DialUDP("udp",nil,server.LocalAddr().(*net.UDPAddr))
	if err!=nil{t.Fatal(err)}
	defer clientConn.Close()

	cfg:=DefaultUDPConfig()
	cfg.MaxDatagramBytes=1200
	cfg.MaxRetries=2
	cfg.RetryInterval=100*time.Millisecond
	client,err:=NewUDPClient(cfg)
	if err!=nil{t.Fatal(err)}
	path:=newUDPPathState(1200)
	message:=bytes.Repeat([]byte("window-test-"),300)
	ctx,cancel:=context.WithTimeout(context.Background(),3*time.Second)
	defer cancel()
	clientDone:=make(chan error,1)
	go func(){_,sendErr:=client.sendRequestFragments(ctx,clientConn,path,0x1234,1200,message,time.Now().Add(3*time.Second));clientDone<-sendErr}()

	buf:=make([]byte,1200)
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	first:=make(map[uint64]bool)
	var peer *net.UDPAddr
	for len(first)<2 {
		n,from,e:=server.ReadFromUDP(buf)
		if e!=nil{t.Fatalf("expected two in-flight fragments before ACK: %v",e)}
		env,e:=decodeDatagram(buf[:n],1200);if e!=nil{t.Fatal(e)}
		first[env.sequence]=true
		peer=from
	}
	if !first[1]||!first[2]{t.Fatalf("expected fragment sequences 1 and 2 in first window, got %v",first)}
	for _,seq:=range []uint64{1,2} {
		if err:=writeFragmentACK(server,0x1234,seq,requestAckSequenceBase+seq-1,true,1200,nil,peer);err!=nil{t.Fatal(err)}
	}
	n,_,err:=server.ReadFromUDP(buf)
	if err!=nil{t.Fatalf("third fragment was not released after ACKs: %v",err)}
	env,err:=decodeDatagram(buf[:n],1200);if err!=nil{t.Fatal(err)}
	if env.sequence!=3{t.Fatalf("expected third fragment sequence 3, got %d",env.sequence)}
	if err:=writeFragmentACK(server,0x1234,3,requestAckSequenceBase+2,true,1200,nil,peer);err!=nil{t.Fatal(err)}
	if err:=<-clientDone;err!=nil{t.Fatal(err)}
}
