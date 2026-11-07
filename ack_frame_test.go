package callgraph

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

const ackGoldenHex = "41434b31010000000000000000000064000000fa0200000000000000000000620000000000000064000000000000005a000000000000005e"

func TestACKGoldenVectorMatchesDATAGRAMRustCodec(t *testing.T) {
	frame := AckFrame{LargestAcked:100, AckDelayMicros:250, Ranges:[]AckRange{{First:98,Last:100},{First:90,Last:94}}}
	want, err := hex.DecodeString(ackGoldenHex); if err != nil { t.Fatal(err) }
	got, err := frame.Encode(); if err != nil { t.Fatal(err) }
	if !bytes.Equal(got,want) { t.Fatalf("wire mismatch\n got %x\nwant %x",got,want) }
	decoded, err := DecodeACK(want); if err != nil { t.Fatal(err) }
	if decoded.LargestAcked != frame.LargestAcked || decoded.AckDelayMicros != frame.AckDelayMicros || len(decoded.Ranges)!=2 || decoded.Ranges[0]!=frame.Ranges[0] || decoded.Ranges[1]!=frame.Ranges[1] {
		t.Fatalf("decoded frame mismatch: %+v",decoded)
	}
}

func TestACKRejectsMalformedAndUnboundedFrames(t *testing.T) {
	if _,err:=DecodeACK([]byte("ACK1")); !errors.Is(err,ErrMalformedACK) { t.Fatalf("got %v",err) }
	frame:=AckFrame{LargestAcked:10,Ranges:[]AckRange{{First:8,Last:10},{First:9,Last:9}}}
	if _,err:=frame.Encode(); !errors.Is(err,ErrMalformedACK) { t.Fatalf("got %v",err) }
}
