package callgraph

import (
	"net"
	"testing"
	"time"
)

func TestFragmentACKUsesDistinctSequenceAndReachesPeer(t *testing.T) {
	server,err:=net.ListenUDP("udp",&net.UDPAddr{IP:net.ParseIP("127.0.0.1")});if err!=nil{t.Fatal(err)}
	defer server.Close()
	client,err:=net.DialUDP("udp",nil,server.LocalAddr().(*net.UDPAddr));if err!=nil{t.Fatal(err)}
	defer client.Close()
	const session=0x12345678
	request,err:=encodeDatagram(session,1,[]byte("request"),1200);if err!=nil{t.Fatal(err)}
	if _,err=client.Write(request);err!=nil{t.Fatal(err)}
	done:=make(chan error,1)
	go func(){
		buf:=make([]byte,1200)
		_,peer,readErr:=server.ReadFromUDP(buf);if readErr!=nil{done<-readErr;return}
		done<-writeFragmentACK(server,session,1,requestAckSequenceBase,true,1200,nil,peer)
	}()
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	buf:=make([]byte,1200)
	n,err:=client.Read(buf);if err!=nil{t.Fatal(err)}
	envelope,err:=decodeDatagram(buf[:n],1200);if err!=nil{t.Fatal(err)}
	if envelope.session!=session||envelope.sequence!=requestAckSequenceBase{t.Fatalf("unexpected ACK envelope: %+v",envelope)}
	ack,err:=DecodeACK(envelope.payload);if err!=nil{t.Fatal(err)}
	if !ackCovers(ack,1){t.Fatalf("ACK did not cover request sequence 1: %+v",ack)}
	if err:=<-done;err!=nil{t.Fatal(err)}
}
