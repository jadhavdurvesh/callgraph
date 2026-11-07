package callgraph
import("bytes";"testing")
func TestFrameRoundTrip(t *testing.T){f:=Frame{Kind:1,OperationID:42,Major:1,Minor:2,DeadlineMillis:500,Payload:[]byte("hello")};f.RequestID[0]=9;b,e:=EncodeFrame(f,1024);if e!=nil{t.Fatal(e)};g,e:=DecodeFrame(b,1024);if e!=nil{t.Fatal(e)};if g.OperationID!=42||g.RequestID[0]!=9||!bytes.Equal(g.Payload,f.Payload){t.Fatalf("bad frame %#v",g)}}
func TestFrameRejectsTrailingData(t *testing.T){b,_:=EncodeFrame(Frame{Payload:[]byte("x")},10);b=append(b,0);if _,e:=DecodeFrame(b,10);e==nil{t.Fatal("expected malformed frame")}}
