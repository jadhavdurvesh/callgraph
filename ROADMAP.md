# CALLGRAPH roadmap

- [x] Transport-independent operation registry and dispatch.
- [x] Versioned operation descriptors and stable operation IDs.
- [x] Request correlation IDs and deadline-aware execution.
- [x] Authorization hook and operation scopes.
- [x] CALLGRAPH frame codec and OCTET/DATAGRAM wire adapters.
- [x] UDP client/server with bounded payloads and concurrent workers.
- [x] Optional AES-256-GCM PSK protection shared with the Rust CONTRACT client.
- [x] Bounded retransmission, duplicate suppression and cached response replay.
- [x] Initial structured error payload and stable remote error codes (1001–1005, 1500).
- [ ] Cancellation propagation beyond the server-side handler context.
- [x] ACK1 codec with shared Rust/Go golden wire vector.
- [ ] Integrate ACK generation/processing, congestion control, MTU discovery and fair scheduling.
- [ ] Key rotation negotiation, forward secrecy and certificate identity.
- [ ] Streaming/backpressure profile, fuzzing and benchmarks.

The current UDP profile is an experimental PSK-based research implementation. It is not a reviewed production transport standard.
