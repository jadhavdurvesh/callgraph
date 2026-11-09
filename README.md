# CALLGRAPH

CALLGRAPH is a typed remote-operation runtime. Its UDP profile transports CALLGRAPH frames inside OCTET serialization frames inside DATAGRAM envelopes.

## Wire stack

`CGR1 → OCT1 → DGM1 → UDP/IP`

## Current transport capabilities

- Bounded datagram and application payload sizes.
- Bounded concurrent operation execution.
- Per-session sequence replay rejection with bounded cache.
- Cached responses for duplicate requests, enabling safe retransmission without re-executing a completed operation.
- Configurable bounded retries and request deadlines. Retransmission is opt-in through `CallOptions{RetrySafe: true}`; only use it for operations safe to repeat.
- Stable structured remote error codes: 1001 method missing, 1002 major-version mismatch, 1003 unauthorized, 1004 deadline exceeded, 1005 payload limit, and 1500 internal error.
- ACK1 codec interoperable with the Rust DATAGRAM implementation, with up to eight bounded acknowledgement ranges.
- ACK-driven server response retransmission: response packet 2 is resent on an adaptive bounded timer until the client returns ACK1 (authenticated by AES-256-GCM when PSK encryption is enabled).
- Per-peer path congestion control: byte-based congestion window, pacing, RTT/RTO estimation, ACK feedback and bounded in-flight packet metadata.
- Per-peer MTU discovery: challenge/response probes search up to a conservative 1452-byte UDP datagram ceiling, starting from a 1200-byte baseline.
- Application datagrams are bounded by the discovered path size; fragmentation/reassembly is not yet implemented.
- Optional AES-256-GCM authenticated encryption using a 32-byte pre-shared key.
- Separate nonce domains for request and response directions.

## Enabling encryption

Configure both peers with the same 32-byte key. For the interoperability server:

```sh
export CALLGRAPH_PSK_HEX="$(openssl rand -hex 32)"
go run ./cmd/interop-server
```

The client must be provisioned with the same key through a secret manager or protected configuration. Do not commit keys to source control, logs, or CI artifacts. `RequireEncryption` rejects unencrypted requests when no key is present.

The current profile uses a 64-bit random session identifier and a 31-bit sequence space per direction. Rotate pre-shared keys periodically and cap the number of sessions per key. This is a PSK profile, not a public-key handshake or certificate infrastructure.

## Limitations

This is a research implementation, not yet a production transport standard. ACK1 currently acknowledges server responses, and the client treats a valid response as implicit acknowledgement of its request. Congestion control and MTU discovery are now integrated into the Go scheduler, but the reliability profile remains stop-and-wait per request rather than a general bidirectional reliable transport. The current MTU probe challenge is not cryptographically authenticated without PSK mode; use PSK mode on untrusted networks. Fragmentation/reassembly, ECN, fairness testing, key rotation negotiation, forward secrecy, certificate identity, and a formal threat-model review remain outstanding. Key rotation negotiation, forward secrecy, certificate-based identity, and a formal threat-model review also remain outstanding. Retransmission is bounded and response caching is in-memory. UDP overload can cause packet drops. Do not use it for hard real-time or emergency-stop control.

## Repositories

- [CONTRACT](https://github.com/jadhavdurvesh/contract) — API and HTTP application layer.
- [OCTET](https://github.com/jadhavdurvesh/octet) — binary serialization profile.
- [DATAGRAM](https://github.com/jadhavdurvesh/datagram) — UDP endpoint and envelope primitives.
