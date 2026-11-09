package callgraph

import (
	"bytes"
	"testing"
	"time"
)

func TestUDPReplayWindowRejectsDuplicateMessageID(t *testing.T) {
	server:=&UDPServer{config:DefaultUDPConfig(),replay:make(map[replayKey]replayEntry)}
	if accepted,_,_,_,_:=server.acceptReplay(0x1234,11);!accepted{t.Fatal("first message must be accepted")}
	if accepted,_,_,_,_:=server.acceptReplay(0x1234,11);accepted{t.Fatal("duplicate message must be rejected")}
	if accepted,_,_,_,_:=server.acceptReplay(0x1234,12);!accepted{t.Fatal("new message ID must be accepted")}
	if accepted,_,_,_,_:=server.acceptReplay(0x5678,11);!accepted{t.Fatal("same message ID in new session must be accepted")}
}

func TestUDPReplayCacheReturnsPreviouslyGeneratedResponse(t *testing.T) {
	server:=&UDPServer{config:DefaultUDPConfig(),replay:make(map[replayKey]replayEntry)}
	if accepted,_,_,_,_:=server.acceptReplay(7,3);!accepted{t.Fatal("first message rejected")}
	response:=[]byte("cached response")
	server.storeReplayResponse(7,3,response)
	server.finishReplayTransmission(7,3)
	accepted,cached,known,retransmit,busy:=server.acceptReplay(7,3)
	if accepted||!known||!retransmit||busy{t.Fatal("duplicate should claim cached response retransmission")}
	if !bytes.Equal(cached,response){t.Fatalf("cached response %q",cached)}
	cached[0]='X'
	_,cachedAgain,_,_,_:=server.acceptReplay(7,3)
	if !bytes.Equal(cachedAgain,response){t.Fatal("cache must return a defensive copy")}
}

func TestUDPReplayWindowExpiresEntriesAndEnforcesCapacity(t *testing.T) {
	cfg:=DefaultUDPConfig();cfg.RequestTimeout=time.Millisecond;cfg.MaxReplayEntries=1
	server:=&UDPServer{config:cfg,replay:make(map[replayKey]replayEntry)}
	if accepted,_,_,_,_:=server.acceptReplay(1,1);!accepted{t.Fatal("first message rejected")}
	if accepted,_,_,_,_:=server.acceptReplay(2,1);accepted{t.Fatal("full replay table must fail closed")}
	time.Sleep(3*time.Millisecond)
	if accepted,_,_,_,_:=server.acceptReplay(2,1);!accepted{t.Fatal("expired entry must be evicted")}
}

func TestUDPReplayResponseCacheHasByteBudget(t *testing.T) {
	cfg:=DefaultUDPConfig();cfg.MaxReplayBytes=5
	server:=&UDPServer{config:cfg,replay:make(map[replayKey]replayEntry)}
	if ok,_,_,_,_:=server.acceptReplay(1,1);!ok{t.Fatal("first message rejected")}
	if ok,_,_,_,_:=server.acceptReplay(2,1);!ok{t.Fatal("second message rejected")}
	server.storeReplayResponse(1,1,[]byte("12345"))
	server.storeReplayResponse(2,1,[]byte("67890"))
	if server.replayBytes!=5{t.Fatalf("cached %d bytes, want 5",server.replayBytes)}
	_,cachedFirst,_,_,_:=server.acceptReplay(1,1)
	if len(cachedFirst)!=5{t.Fatalf("first response should remain cached: %q",cachedFirst)}
	_,cachedSecond,_,_,_:=server.acceptReplay(2,1)
	if len(cachedSecond)!=0{t.Fatalf("second response should not exceed cache budget: %q",cachedSecond)}
}
