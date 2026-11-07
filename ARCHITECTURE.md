# CALLGRAPH architecture

## Scope
CALLGRAPH owns remote-operation semantics independently of any socket or serialization implementation. This initial package is a local dispatch runtime; it is not yet a network RPC stack.

## Core model
- Method registry with duplicate-registration rejection.
- Context-aware handler signature.
- Request and response payload bounds.
- Explicit unknown-method, cancellation and handler errors.

## Planned protocol/runtime layers
1. Stable operation identity and versioning.
2. Request correlation IDs and deadlines.
3. Cancellation propagation and structured error taxonomy.
4. Authorization, peer identity and replay protection.
5. OCTET codec adapter.
6. DATAGRAM transport adapter.
7. Streaming and backpressure profile.

## Safety boundary
Never put high-frequency actuator control or the only emergency-stop path behind remote procedure execution. Remote operations are supervisory and must be locally authorized.
