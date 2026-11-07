package callgraph
import("bytes";"encoding/hex";"testing")
func TestOCTETGoldenVector(t *testing.T){want,_:=hex.DecodeString("4f435431010000000568656c6c6f");got,e:=EncodeOCTET(0,[]byte("hello"),1024);if e!=nil{t.Fatal(e)};if !bytes.Equal(got,want){t.Fatalf("wire mismatch %x",got)};f,e:=DecodeOCTET(want,1024);if e!=nil{t.Fatal(e)};if string(f.Payload)!="hello"{t.Fatalf("payload %q",f.Payload)}}
func TestOCTETRejectsTrailingBytes(t *testing.T){b,_:=EncodeOCTET(0,[]byte("x"),10);b=append(b,0);if _,e:=DecodeOCTET(b,10);e==nil{t.Fatal("expected strict length rejection")}}
