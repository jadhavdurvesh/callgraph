package callgraph

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestUDPClientRetriesWithoutReexecutingOperation(t *testing.T) {
	runtime, err := New(DefaultConfig())
	if err != nil { t.Fatal(err) }
	serverConfig := DefaultUDPConfig()
	serverConfig.RequestTimeout = time.Second
	server, err := NewUDPServer(runtime, serverConfig)
	if err != nil { t.Fatal(err) }
	var calls atomic.Int32
	d := Descriptor{Name:"retry.once", ID:91, Major:1, Idempotency:ReadOnly, AuthorizationScope:"test:retry", MaxDeadline:time.Second}
	if err := server.RegisterOperation(d, func(context.Context,string) error{return nil}, func(_ context.Context,p []byte)([]byte,error){
		calls.Add(1)
		return append([]byte("ack:"),p...),nil
	}); err != nil { t.Fatal(err) }

	serverConn, err := net.ListenUDP("udp",&net.UDPAddr{IP:net.ParseIP("127.0.0.1"),Port:0})
	if err != nil { t.Fatal(err) }
	serverCtx, stopServer := context.WithCancel(context.Background())
	defer stopServer()
	go func(){ _=server.Serve(serverCtx,serverConn) }()
	defer serverConn.Close()

	proxy, err := net.ListenUDP("udp",&net.UDPAddr{IP:net.ParseIP("127.0.0.1"),Port:0})
	if err != nil { t.Fatal(err) }
	defer proxy.Close()
	upstream, err := net.DialUDP("udp",nil,serverConn.LocalAddr().(*net.UDPAddr))
	if err != nil { t.Fatal(err) }
	defer upstream.Close()

	proxyDone := make(chan error,1)
	go func(){
		buf:=make([]byte,65507)
		for {
			n,peer,e:=proxy.ReadFromUDP(buf);if e!=nil{proxyDone<-e;return}
			packet:=append([]byte(nil),buf[:n]...)
			env,decodeErr:=decodeDatagram(packet,65507);if decodeErr!=nil{proxyDone<-decodeErr;return}
			if env.sequence==mtuProbeRequestSequence {
				if _,e=upstream.Write(packet);e!=nil{proxyDone<-e;return}
				n,e=upstream.Read(buf);if e!=nil{proxyDone<-e;return}
				if _,e=proxy.WriteToUDP(buf[:n],peer);e!=nil{proxyDone<-e;return}
				continue
			}
			if _,e=upstream.Write(packet);e!=nil{proxyDone<-e;return}
			// Drop the first response fragment. The server must retransmit
			// that same fragment until its fragment-level ACK is received.
			n,e=upstream.Read(buf);if e!=nil{proxyDone<-e;return}
			n,e=upstream.Read(buf);if e!=nil{proxyDone<-e;return}
			if _,e=proxy.WriteToUDP(buf[:n],peer);e!=nil{proxyDone<-e;return}
			n,peer2,e:=proxy.ReadFromUDP(buf);if e!=nil{proxyDone<-e;return}
			if peer2.String()!=peer.String(){proxyDone<-context.Canceled;return}
			if _,e=upstream.Write(buf[:n]);e!=nil{proxyDone<-e;return}
			proxyDone<-nil;return
		}
	}()
	clientConfig:=DefaultUDPConfig()
	clientConfig.RequestTimeout=time.Second
	clientConfig.RetryInterval=30*time.Millisecond
	clientConfig.MaxRetries=2
	client,err:=NewUDPClient(clientConfig);if err!=nil{t.Fatal(err)}
	got,err:=client.CallWithOptions(context.Background(),proxy.LocalAddr().String(),91,1,0,[]byte("hello"),CallOptions{RetrySafe:true})
	if err!=nil{t.Fatal(err)}
	if string(got)!="ack:hello"{t.Fatalf("response %q",got)}
	if err:=<-proxyDone;err!=nil{t.Fatal(err)}
	if calls.Load()!=1{t.Fatalf("operation executed %d times, want exactly once",calls.Load())}
}
