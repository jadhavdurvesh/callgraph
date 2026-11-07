# CALLGRAPH roadmap

- [x] Transport-independent method registry.
- [x] Context-aware handler dispatch.
- [x] Request and response size limits.
- [x] Duplicate and unknown method handling.
- [ ] Versioned method descriptors and stable operation IDs.
- [ ] Request correlation, deadlines and cancellation semantics.
- [ ] Structured error model.
- [ ] Authentication and authorization hooks.
- [ ] OCTET codec adapter.
- [ ] DATAGRAM transport adapter.
- [ ] Retry/idempotency and replay protection specification.
- [ ] Streaming/backpressure profile and benchmarks.

The initial implementation is a local dispatcher, not a remotely accessible RPC service.
