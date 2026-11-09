# CALLGRAPH roadmap

- [x] Transport-independent operation registry and dispatch.
- [x] Versioned operation descriptors and stable operation IDs.
- [x] Request correlation IDs and deadline-aware execution.
- [x] Authorization hook and operation scopes.
- [x] CALLGRAPH frame codec and OCTET/DATAGRAM wire adapters.
- [x] UDP client/server with bounded payloads and concurrent workers.
- [x] Optional AES-256-GCM PSK protection shared with the Rust CONTRACT client.
- [x] Initial structured error payload and stable remote error codes (1001–1005, 1500).
- [x] ACK1 codec with shared Rust/Go golden wire vector.
- [x] Per-peer congestion window, pacing, RTT/RTO estimation and ACK feedback.
- [x] Per-peer challenge/response path MTU discovery with conservative 1200-byte baseline and 1452-byte ceiling.
- [x] FRG1 fragmentation and reassembly for large request and response messages.
- [x] Per-fragment ACKs, bounded retransmission and duplicate-fragment recognition.
- [x] Message-ID replay protection and cached response retransmission without re-executing the handler.
- [x] Bounded reassembly memory, assembly count, fragment count and expiry.
- [x] Plaintext and AES-GCM large-message integration tests, plus response-fragment loss/retransmission coverage.
- [ ] Cancellation propagation beyond the server-side handler context.
- [ ] Sliding-window delivery, streaming/backpressure and fair scheduling.
- [ ] ECN and multi-path fairness tests.
- [ ] Key rotation negotiation, forward secrecy and certificate identity.
- [ ] Fuzzing, adversarial network tests, interoperability vectors and benchmarks.
- [ ] Formal protocol and threat-model review.

The current UDP profile is an experimental PSK-based research implementation. It is not a reviewed production transport standard.
