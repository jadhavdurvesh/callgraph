package callgraph

import (
	"errors"
	"testing"
)

func TestStructuredErrorPayloadRoundTrip(t *testing.T) {
	wire := encodeErrorPayload(ErrorCodeOperationVersion, "major version mismatch")
	code, message, err := DecodeErrorPayload(wire)
	if err != nil { t.Fatal(err) }
	if code != ErrorCodeOperationVersion || message != "major version mismatch" {
		t.Fatalf("decoded error = (%d, %q)", code, message)
	}
	wire[len(wire)-1] ^= 1
	if _, _, err := DecodeErrorPayload(wire); err != nil {
		// Message bytes are opaque text; a valid length remains structurally valid.
		t.Fatalf("unexpected structural error: %v", err)
	}
	if _, _, err := DecodeErrorPayload([]byte("ERR1\x00")); !errors.Is(err, ErrMalformedErrorPayload) {
		t.Fatalf("expected malformed error payload, got %v", err)
	}
}

func TestRemoteErrorMatchesSentinel(t *testing.T) {
	err := &RemoteError{Code: ErrorCodeMethodNotFound, Message: "missing"}
	if !errors.Is(err, ErrRemoteOperation) { t.Fatal("remote error must match sentinel") }
}
