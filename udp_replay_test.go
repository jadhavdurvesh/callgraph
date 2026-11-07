package callgraph

import (
	"bytes"
	"testing"
	"time"
)

func TestUDPReplayWindowRejectsDuplicateSessionSequence(t *testing.T) {
	server := &UDPServer{config: DefaultUDPConfig(), replay: make(map[replayKey]replayEntry)}
	envelope := datagramEnvelope{session: 0x1234, sequence: 1}
	if accepted, _ := server.acceptReplay(envelope); !accepted { t.Fatal("first datagram must be accepted") }
	if accepted, _ := server.acceptReplay(envelope); accepted { t.Fatal("duplicate datagram must be rejected") }
	if accepted, _ := server.acceptReplay(datagramEnvelope{session: envelope.session, sequence: 2}); !accepted { t.Fatal("new sequence must be accepted") }
	if accepted, _ := server.acceptReplay(datagramEnvelope{session: 0x5678, sequence: 1}); !accepted { t.Fatal("new session must be accepted") }
}

func TestUDPReplayCacheReturnsPreviouslyGeneratedResponse(t *testing.T) {
	server := &UDPServer{config: DefaultUDPConfig(), replay: make(map[replayKey]replayEntry)}
	envelope := datagramEnvelope{session: 7, sequence: 3}
	if accepted, _ := server.acceptReplay(envelope); !accepted { t.Fatal("first packet rejected") }
	response := []byte("cached response")
	server.storeReplayResponse(envelope, response)
	accepted, cached := server.acceptReplay(envelope)
	if accepted { t.Fatal("duplicate should not be re-executed") }
	if !bytes.Equal(cached, response) { t.Fatalf("cached response %q", cached) }
	cached[0] = 'X'
	_, cachedAgain := server.acceptReplay(envelope)
	if !bytes.Equal(cachedAgain, response) { t.Fatal("cache must return a defensive copy") }
}

func TestUDPReplayWindowExpiresEntriesAndEnforcesCapacity(t *testing.T) {
	cfg := DefaultUDPConfig()
	cfg.RequestTimeout = time.Millisecond
	cfg.MaxReplayEntries = 1
	server := &UDPServer{config: cfg, replay: make(map[replayKey]replayEntry)}
	first := datagramEnvelope{session: 1, sequence: 1}
	if accepted, _ := server.acceptReplay(first); !accepted { t.Fatal("first packet rejected") }
	if accepted, _ := server.acceptReplay(datagramEnvelope{session: 2, sequence: 1}); accepted { t.Fatal("full replay table must fail closed") }
	time.Sleep(3 * time.Millisecond)
	if accepted, _ := server.acceptReplay(datagramEnvelope{session: 2, sequence: 1}); !accepted { t.Fatal("expired entry must be evicted") }
}

func TestUDPReplayResponseCacheHasByteBudget(t *testing.T) {
	cfg := DefaultUDPConfig()
	cfg.MaxReplayBytes = 5
	server := &UDPServer{config: cfg, replay: make(map[replayKey]replayEntry)}
	first := datagramEnvelope{session: 1, sequence: 1}
	second := datagramEnvelope{session: 2, sequence: 1}
	if ok, _ := server.acceptReplay(first); !ok { t.Fatal("first packet rejected") }
	if ok, _ := server.acceptReplay(second); !ok { t.Fatal("second packet rejected") }
	server.storeReplayResponse(first, []byte("12345"))
	server.storeReplayResponse(second, []byte("67890"))
	if server.replayBytes != 5 { t.Fatalf("cached %d bytes, want 5", server.replayBytes) }
	_, cachedFirst := server.acceptReplay(first)
	if len(cachedFirst) != 5 { t.Fatalf("first response should remain cached: %q", cachedFirst) }
	_, cachedSecond := server.acceptReplay(second)
	if len(cachedSecond) != 0 { t.Fatalf("second response should not exceed cache budget: %q", cachedSecond) }
}
