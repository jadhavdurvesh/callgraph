# CALLGRAPH

CALLGRAPH is a typed remote-operation runtime. Its UDP profile transports CALLGRAPH frames inside OCTET serialization frames inside DATAGRAM envelopes.

## Wire stack

`CGR1 → OCT1 → DGM1 → UDP/IP`

## Current transport capabilities

- Bounded datagram and application payload sizes.
- Bounded concurrent operation execution.
- Per-session sequence replay rejection with bounded cache.
- Cached responses for duplicate requests, enabling safe retransmission without re-executing a completed operation.
- Configurable bounded retries and request deadlines.
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

This is a research implementation, not yet a production transport standard. It does not yet implement congestion control, path MTU discovery, key rotation negotiation, forward secrecy, certificate-based identity, or a formal threat-model review. Retransmission is bounded and response caching is in-memory. UDP overload can cause packet drops. Do not use it for hard real-time or emergency-stop control.

## Repositories

- [CONTRACT](https://github.com/jadhavdurvesh/contract) — API and HTTP application layer.
- [OCTET](https://github.com/jadhavdurvesh/octet) — binary serialization profile.
- [DATAGRAM](https://github.com/jadhavdurvesh/datagram) — UDP endpoint and envelope primitives.
