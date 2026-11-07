# CALLGRAPH

**A research-oriented remote procedure execution runtime.**

CALLGRAPH defines typed remote operations, request/response envelopes, deadlines, cancellation, identity and authorization hooks, and bounded execution behavior across process boundaries.

## Design boundaries

- Own remote invocation semantics, method registration, request correlation, deadlines, cancellation, and error taxonomy.
- Keep payload encoding in OCTET through a codec interface.
- Keep network I/O in DATAGRAM or another explicitly selected transport adapter.
- Keep application-level HTTP routing in CONTRACT.
- Do not use remote calls for hard real-time control loops or emergency-stop dependencies.

## Initial implementation

- Language: Go.
- Initial scope: transport-independent invocation registry, request validation, context-aware dispatch, and tests.
- Later work: OCTET codec adapter, DATAGRAM transport adapter, authentication/authorization, replay protection, and streaming semantics.

## Status

Early development. The initial runtime is not yet a networked RPC implementation.

## Related technologies

- [CONTRACT](https://github.com/jadhavdurvesh/contract) — API framework.
- [OCTET](https://github.com/jadhavdurvesh/octet) — binary data protocol.
- [DATAGRAM](https://github.com/jadhavdurvesh/datagram) — datagram networking.
