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
	// Receive all four fragments before sending any ACK. This proves the
	// sender no longer waits stop-and-wait for each individual fragment.
	for len(first)<4 {
		n,from,e:=server.ReadFromUDP(buf)
		if e!=nil{t.Fatalf("expected four in-flight fragments before ACK: %v",e)}
		env,e:=decodeDatagram(buf[:n],1200);if e!=nil{t.Fatal(e)}
		if env.sequence<1||env.sequence>4{t.Fatalf("unexpected request fragment sequence %d",env.sequence)}
		first[env.sequence]=true
		peer=from
	}
	if len(first)!=4{t.Fatalf("expected all four fragments in flight, got %v",first)}
	for seq:=uint64(1);seq<=4;seq++ {
		if err:=writeFragmentACK(server,0x1234,seq,requestAckSequenceBase+seq-1,true,1200,nil,peer);err!=nil{t.Fatal(err)}
	}
	if err:=<-clientDone;err!=nil{t.Fatal(err)}
}
