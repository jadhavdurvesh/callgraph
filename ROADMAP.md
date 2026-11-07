# CALLGRAPH roadmap

- [x] Transport-independent operation registry and dispatch.
- [x] Versioned operation descriptors and stable operation IDs.
- [x] Request correlation IDs and deadline-aware execution.
- [x] Authorization hook and operation scopes.
- [x] CALLGRAPH frame codec and OCTET/DATAGRAM wire adapters.
- [x] UDP client/server with bounded payloads and concurrent workers.
- [x] Optional AES-256-GCM PSK protection shared with the Rust CONTRACT client.
- [x] Bounded retransmission, duplicate suppression and cached response replay.
- [ ] Structured, typed error catalogue and stable error codes.
- [ ] Cancellation propagation beyond the server-side handler context.
- [ ] Congestion control, MTU discovery and fair scheduling.
- [ ] Key rotation negotiation, forward secrecy and certificate identity.
- [ ] Streaming/backpressure profile, fuzzing and benchmarks.

The current UDP profile is an experimental PSK-based research implementation. It is not a reviewed production transport standard.
