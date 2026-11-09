package callgraph

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestUDPServerRetransmitsIdenticalResponseUntilACK(t *testing.T) {
	runtime, err := New(DefaultConfig()); if err != nil { t.Fatal(err) }
	cfg := DefaultUDPConfig(); cfg.RetryInterval = 30*time.Millisecond; cfg.MaxRetries = 4
	server, err := NewUDPServer(runtime,cfg); if err != nil { t.Fatal(err) }
	d := Descriptor{Name:"echo",ID:OperationID("test.echo"),Major:1,Idempotency:ReadOnly,AuthorizationScope:"test:read",MaxDeadline:time.Second}
	if err:=server.RegisterOperation(d,func(context.Context,string)error{return nil},func(_ context.Context,p []byte)([]byte,error){return p,nil});err!=nil{t.Fatal(err)}
	conn,err:=net.ListenUDP("udp",&net.UDPAddr{IP:net.ParseIP("127.0.0.1")});if err!=nil{t.Fatal(err)}
	ctx,cancel:=context.WithCancel(context.Background());done:=make(chan struct{})
	go func(){_ = server.Serve(ctx,conn);close(done)}()
	defer func(){cancel();_ = conn.Close();<-done}()
	client,err:=net.DialUDP("udp",nil,conn.LocalAddr().(*net.UDPAddr));if err!=nil{t.Fatal(err)}
	defer client.Close()
	requestID,_:=random16();const session=0x1122334455667788
	frame,err:=EncodeFrame(Frame{Kind:kindRequest,RequestID:requestID,OperationID:d.ID,Major:1,DeadlineMillis:1000,Payload:[]byte("hello")},cfg.MaxPayloadBytes);if err!=nil{t.Fatal(err)}
	octets,err:=encodeOCTET(frame,cfg.MaxDatagramBytes-datagramHeaderSize-octetHeaderSize);if err!=nil{t.Fatal(err)}
	packet,err:=encodeDatagram(session,1,octets,cfg.MaxDatagramBytes);if err!=nil{t.Fatal(err)}
	if _,err=client.Write(packet);err!=nil{t.Fatal(err)}
	buf:=make([]byte,cfg.MaxDatagramBytes)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	n,err:=client.Read(buf);if err!=nil{t.Fatal(err)}
	first:=append([]byte(nil),buf[:n]...)
	envelope,err:=decodeDatagram(first,cfg.MaxDatagramBytes);if err!=nil{t.Fatal(err)}
	if envelope.session!=session||envelope.sequence!=2{t.Fatalf("unexpected response envelope: %+v",envelope)}
	_ = client.SetReadDeadline(time.Now().Add(250*time.Millisecond))
	n,err=client.Read(buf);if err!=nil{t.Fatalf("expected retransmitted response: %v",err)}
	second:=append([]byte(nil),buf[:n]...)
	if string(first)!=string(second){t.Fatal("retransmitted response bytes changed")}
	ack,err:=(AckFrame{LargestAcked:2,Ranges:[]AckRange{{First:2,Last:2}}}).Encode();if err!=nil{t.Fatal(err)}
	ackPacket,err:=encodeDatagram(session,2,ack,cfg.MaxDatagramBytes);if err!=nil{t.Fatal(err)}
	if _,err=client.Write(ackPacket);err!=nil{t.Fatal(err)}
	_ = client.SetReadDeadline(time.Now().Add(150*time.Millisecond))
	if n,err=client.Read(buf);err==nil{t.Fatalf("received unexpected response after ACK: %d bytes",n)} else if ne,ok:=err.(net.Error);!ok||!ne.Timeout(){t.Fatalf("unexpected read error after ACK: %v",err)}
}
