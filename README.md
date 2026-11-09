# CALLGRAPH

CALLGRAPH is a typed remote-operation runtime. Its UDP profile carries CALLGRAPH request/response frames through OCTET serialization, FRG1 message fragmentation, and DATAGRAM UDP envelopes.

## Wire stack

`CGR1 → OCT1 → FRG1 → DGM1 → UDP/IP`

ACK1 frames travel in DGM1 envelopes in the reverse direction.

## Current transport capabilities

- Typed operation registry, versioned descriptors, request correlation IDs, deadlines, authorization hooks and stable remote error codes.
- Bounded UDP datagrams, application payloads, concurrent execution, replay entries and response-cache bytes.
- FRG1 fragmentation and reassembly for requests and responses larger than the current path datagram budget.
- Bounded selective-repeat fragment windows (up to 8 fragments in flight), independent ACK processing and retransmission of missing fragments using the adaptive path RTO.
- Duplicate-fragment suppression and message-ID replay protection; duplicate calls do not execute a completed operation again.
- Bounded reassembly: 1,024 fragments per message, 8 MiB aggregate reassembly memory and 256 concurrent assemblies by default, with a 15-second assembly expiry.
- ACK1 codec interoperable with the Rust DATAGRAM implementation, including up to eight bounded acknowledgement ranges.
- Per-peer congestion control: byte-based congestion window, pacing, RTT/RTO estimation, ACK feedback and bounded in-flight packet metadata.
- Per-peer MTU discovery with a 1,200-byte baseline and a conservative 1,452-byte UDP datagram ceiling.
- Optional AES-256-GCM authenticated encryption using a 32-byte pre-shared key.
- Separate sequence ranges for request data, request acknowledgements, response data, response acknowledgements and MTU probes to avoid AES-GCM nonce reuse.
- Integration tests for large fragmented requests and responses with plaintext and AES-GCM, plus response-fragment loss and retransmission.

## Fragmentation profile

Each FRG1 fragment carries a message ID, fragment index/count, total message length, byte offset and fragment data. Fragments are reassembled before the CALLGRAPH frame is decoded or dispatched. The sender can keep up to eight fragments in flight, subject to the per-path congestion window and pacing. Each fragment is acknowledged independently and only missing fragments are retransmitted. See [FRAGMENTATION.md](FRAGMENTATION.md) for the wire layout, sequence allocation and resource limits.

This is a bounded selective-repeat window, not a streaming protocol. ACK range batching, streaming/backpressure and fair scheduling remain future work.

## Streaming API groundwork

CALLGRAPH also exposes a transport-independent `NewStreamPipe(ctx)` API for incremental in-process byte transfer with synchronous backpressure and context cancellation. This is groundwork for streaming handlers; it is not yet integrated into the UDP wire protocol, which still uses bounded whole-message reassembly. See [STREAMING.md](STREAMING.md) for its scope and limitations.

## Enabling encryption

Configure both peers with the same 32-byte key. For the interoperability server:

```sh
export CALLGRAPH_PSK_HEX="$(openssl rand -hex 32)"
go run ./cmd/interop-server
```

The client must be provisioned with the same key through a secret manager or protected configuration. Do not commit keys to source control, logs, or CI artifacts. `RequireEncryption` rejects unencrypted requests when no key is present.

The current profile uses a 64-bit random session identifier and direction-separated 31-bit sequence numbers. Rotate pre-shared keys periodically and cap the number of sessions per key. This is a PSK profile, not a public-key handshake or certificate infrastructure.

## Configuration

- `MaxPayloadBytes`: maximum application payload in a CALLGRAPH frame.
- `MaxDatagramBytes`: maximum UDP datagram size accepted by the local endpoint; it is independent of the application payload limit.
- `MaxReassemblyBytes`: aggregate memory budget for incomplete and recently completed messages.
- `MaxReassemblyMessages`: maximum simultaneous reassembly entries.
- `ReassemblyTimeout`: expiry for incomplete/completed fragment assemblies.
- `MaxRetries` and `RetryInterval`: bounded fragment retransmission policy; the path RTO adapts using measured RTT and loss.

## Limitations and security status

This is an experimental research transport, not a reviewed production transport standard. Fragment transmission uses a bounded 8-fragment selective-repeat window; streaming, backpressure, ACK range batching, ECN, fair scheduling, multi-path fairness, key rotation negotiation, forward secrecy, certificate identity, and a formal threat-model review remain outstanding. MTU discovery is capped at 1,452 bytes. Without PSK encryption, fragment and MTU-probe integrity is not cryptographically authenticated. Response replay caching is in-memory and bounded; process restart loses it. UDP overload can cause packet drops. Do not use this profile for hard real-time or emergency-stop control.

## Repositories

- [CONTRACT](https://github.com/jadhavdurvesh/contract) — API and HTTP application layer.
- [OCTET](https://github.com/jadhavdurvesh/octet) — binary serialization profile.
- [DATAGRAM](https://github.com/jadhavdurvesh/datagram) — UDP endpoint and envelope primitives.
