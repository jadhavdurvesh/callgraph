package callgraph

import (
	"testing"
	"time"
)

func TestUDPReplayWindowRejectsDuplicateSessionSequence(t *testing.T) {
	cfg := DefaultUDPConfig()
	server := &UDPServer{
		config: cfg,
		replay: make(map[replayKey]time.Time),
	}
	envelope := datagramEnvelope{session: 0x1234, sequence: 1}
	if !server.acceptReplay(envelope) {
		t.Fatal("first datagram must be accepted")
	}
	if server.acceptReplay(envelope) {
		t.Fatal("duplicate datagram must be rejected")
	}
	if !server.acceptReplay(datagramEnvelope{session: envelope.session, sequence: 2}) {
		t.Fatal("new sequence in same session must be accepted")
	}
	if !server.acceptReplay(datagramEnvelope{session: 0x5678, sequence: 1}) {
		t.Fatal("new session must be accepted")
	}
}

func TestUDPReplayWindowExpiresEntriesAndEnforcesCapacity(t *testing.T) {
	cfg := DefaultUDPConfig()
	cfg.RequestTimeout = time.Millisecond
	cfg.MaxReplayEntries = 1
	server := &UDPServer{config: cfg, replay: make(map[replayKey]time.Time)}
	first := datagramEnvelope{session: 1, sequence: 1}
	if !server.acceptReplay(first) {
		t.Fatal("first datagram must be accepted")
	}
	if server.acceptReplay(datagramEnvelope{session: 2, sequence: 1}) {
		t.Fatal("full replay table must fail closed")
	}
	time.Sleep(3 * time.Millisecond)
	if !server.acceptReplay(datagramEnvelope{session: 2, sequence: 1}) {
		t.Fatal("expired entries must be evicted")
	}
}
