package main

import (
	"context"
	"encoding/hex"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jadhavdurvesh/callgraph"
)

const integrationOperationID uint32 = 0x4A311001

func main() {
	address := os.Getenv("CALLGRAPH_LISTEN")
	if address == "" {
		address = "127.0.0.1:19000"
	}
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	runtime, err := callgraph.New(callgraph.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	transportConfig := callgraph.DefaultUDPConfig()
	if encodedKey := os.Getenv("CALLGRAPH_PSK_HEX"); encodedKey != "" {
		key, decodeErr := hex.DecodeString(encodedKey)
		if decodeErr != nil || len(key) != 32 { log.Fatal("CALLGRAPH_PSK_HEX must contain exactly 32 bytes encoded as 64 hex characters") }
		transportConfig.PreSharedKey = key
		transportConfig.RequireEncryption = true
	}
	server, err := callgraph.NewUDPServer(runtime, transportConfig)
	if err != nil {
		log.Fatal(err)
	}
	descriptor := callgraph.Descriptor{
		Name: "integration.echo",
		ID: integrationOperationID,
		Major: 1,
		Minor: 0,
		Idempotency: callgraph.ReadOnly,
		AuthorizationScope: "integration:echo",
		MaxDeadline: 3 * time.Second,
	}
	authorize := func(context.Context, string) error { return nil }
	handler := func(_ context.Context, payload []byte) ([]byte, error) {
		return append([]byte("go:"), payload...), nil
	}
	if err := server.RegisterOperation(descriptor, authorize, handler); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("CALLGRAPH interoperability server listening on %s", conn.LocalAddr())
	if err := server.Serve(ctx, conn); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
