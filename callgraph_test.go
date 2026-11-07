package callgraph

import (
	"context"
	"errors"
	"testing"
)

func TestInvoke(t *testing.T) {
	r, err := New(DefaultConfig())
	if err != nil { t.Fatal(err) }
	if err := r.Register("robot.status", func(_ context.Context, input []byte) ([]byte, error) {
		return append([]byte("status:"), input...), nil
	}); err != nil { t.Fatal(err) }
	got, err := r.Invoke(context.Background(), "robot.status", []byte("FB-01"))
	if err != nil { t.Fatal(err) }
	if string(got) != "status:FB-01" { t.Fatalf("unexpected response %q", got) }
}

func TestRejectsUnknownAndOversized(t *testing.T) {
	cfg := Config{MaxRequestBytes: 4, MaxResponseBytes: 4}
	r, _ := New(cfg)
	if _, err := r.Invoke(context.Background(), "missing", nil); !errors.Is(err, ErrMethodNotFound) {
		t.Fatalf("expected method not found, got %v", err)
	}
	if _, err := r.Invoke(context.Background(), "missing", []byte("12345")); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("expected payload too large, got %v", err)
	}
}
