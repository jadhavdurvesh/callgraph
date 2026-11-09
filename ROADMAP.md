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
- [x] ACK-driven bounded response retransmission with client ACK generation and server ACK processing.
- [x] Integrate per-peer congestion window, pacing, RTT/RTO estimation and ACK feedback into the Go UDP scheduler.
- [x] Integrate per-peer challenge/response path MTU discovery with conservative 1200-byte baseline and 1452-byte ceiling.
- [ ] Add fragmentation/reassembly for application payloads larger than the discovered path budget.
- [ ] Add fair scheduling, ECN and multi-path fairness tests.
- [ ] Add retry-safe request-loss integration tests and bidirectional delivery acknowledgements.
- [ ] Key rotation negotiation, forward secrecy and certificate identity.
- [ ] Streaming/backpressure profile, fuzzing and benchmarks.

The current UDP profile is an experimental PSK-based research implementation. It is not a reviewed production transport standard.
