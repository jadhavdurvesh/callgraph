package callgraph

import (
	"bytes"
	"testing"
	"time"
)

func TestFragmentSplitAndReassembleOutOfOrder(t *testing.T) {
	input:=bytes.Repeat([]byte("callgraph-fragmentation-"),700)
	fragments,err:=splitMessage(input,997,42);if err!=nil{t.Fatal(err)}
	r:=newFragmentReassembler(1<<20,8,time.Minute)
	var got []byte
	for i:=len(fragments)-1;i>=0;i-- {
		var complete bool
		got,complete,err=r.Add("peer/session/message",fragments[i],time.Now())
		if err!=nil{t.Fatal(err)}
		if complete && !bytes.Equal(got,input){t.Fatal("reassembled message differs")}
	}
	if !bytes.Equal(got,input){t.Fatal("message was not reassembled")}
	// A retransmitted fragment after completion must remain recognizable and
	// must not allocate a second assembly or lose the completed payload.
	again,complete,err:=r.Add("peer/session/message",fragments[0],time.Now())
	if err!=nil||!complete||!bytes.Equal(again,input){t.Fatalf("duplicate complete fragment: complete=%v err=%v",complete,err)}
	messages,used:=r.Stats()
	if messages!=1||used!=len(input){t.Fatalf("unexpected retained assembly: messages=%d bytes=%d",messages,used)}
}

func TestFragmentReassemblerRejectsConflictingAndExpiredMessages(t *testing.T) {
	fragments,err:=splitMessage(bytes.Repeat([]byte{7},2048),512,99);if err!=nil{t.Fatal(err)}
	r:=newFragmentReassembler(4096,2,10*time.Millisecond)
	now:=time.Now()
	if _,_,err=r.Add("x",fragments[0],now);err!=nil{t.Fatal(err)}
	conflict:=fragments[0];conflict.data=append([]byte(nil),conflict.data...);conflict.data[0]^=0xff
	if _,_,err=r.Add("x",conflict,now);err!=ErrMalformedFragment{t.Fatalf("want malformed fragment, got %v",err)}
	if _,_,err=r.Add("y",fragments[0],now);err!=nil{t.Fatal(err)}
	if _,_,err=r.Add("z",fragments[0],now.Add(20*time.Millisecond));err!=nil{t.Fatal(err)}
	messages,used:=r.Stats()
	if messages!=1||used!=2048{t.Fatalf("expired assembly not reclaimed: messages=%d bytes=%d",messages,used)}
}

func TestFragmentCodecRejectsOutOfBoundsAndOversizedCounts(t *testing.T) {
	f:=messageFragment{messageID:1,index:0,count:1,total:4,offset:3,data:[]byte{1,2}}
	if _,err:=encodeFragment(f,128);err==nil{t.Fatal("expected out-of-bounds fragment rejection")}
	f=messageFragment{messageID:1,index:0,count:maxMessageFragments+1,total:4,data:[]byte{1}}
	if _,err:=encodeFragment(f,128);err==nil{t.Fatal("expected fragment count rejection")}
}
