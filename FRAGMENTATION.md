# CALLGRAPH FRG1 fragmentation profile

Status: experimental implementation profile, version 1

## Purpose

FRG1 carries one bounded CALLGRAPH/OCTET message across multiple DATAGRAM UDP envelopes when the encoded message exceeds the current path datagram budget. Reassembly occurs before CALLGRAPH frame decoding and operation dispatch.

This profile is stop-and-wait: the sender transmits one fragment and waits for its ACK before advancing to the next fragment. It does not define streaming, a sliding window, or arbitrary multiplexing of multiple messages within one session.

## Encapsulation

```text
CALLGRAPH frame (CGR1)
  └─ OCTET serialization frame (OCT1)
       └─ FRG1 message fragment
            └─ DATAGRAM envelope (DGM1)
                 └─ UDP/IP
```

ACK1 acknowledgement frames are carried directly in DGM1 envelopes.

## FRG1 header

All integer fields are unsigned, big-endian. The fixed header is 24 bytes.

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | Magic | ASCII `FRG1` |
| 4 | 8 | Message ID | Random non-zero 64-bit identifier, stable for this request and its response |
| 12 | 2 | Fragment index | Zero-based index |
| 14 | 2 | Fragment count | Total fragments in the message |
| 16 | 4 | Total length | Reassembled message length in bytes |
| 20 | 4 | Offset | Byte offset of this fragment in the complete message |
| 24 | variable | Data | Fragment bytes |

The fragment data must be non-empty. Fragment count is 1–1,024. Total length is non-zero and cannot exceed the configured reassembly budget. Fragment offsets are canonical: all non-final fragments have a common chunk size, and the final fragment occupies the remaining bytes. Overlaps, inconsistent metadata and conflicting duplicates are rejected.

## Sequence allocation

The envelope sequence field is also used as the AES-256-GCM nonce input. The implementation reserves non-overlapping ranges for each direction and packet class.

| Packet class | Direction | Envelope sequence |
|---|---|---|
| Request data | Client → server | `1 + fragment_index` |
| MTU probe request | Client → server | `0x0FFFFF00` |
| MTU probe response | Server → client | `0x0FFFFF01` |
| Request-fragment ACK | Server → client | `0x10000000 + fragment_index` |
| Response data | Server → client | `0x20000000 + fragment_index` |
| Response-fragment ACK | Client → server | `0x40000000 + fragment_index` |

The response uses the request's message ID. Cached response retransmission therefore reproduces the same fragment header and ciphertext for a given session/sequence, rather than reusing a nonce with different plaintext. Fragment retransmission repeats the exact original datagram bytes.

## ACK1 semantics

An ACK1 frame acknowledges the data sequence in its `LargestAcked` field and bounded ACK ranges. In this profile, each fragment is acknowledged independently with a single-fragment range. The ACK envelope sequence is derived from the data fragment index using the corresponding ACK sequence base.

The receiver sends an ACK after validating and recording a fragment. Duplicate fragments are acknowledged again. A sender retransmits an unacknowledged fragment using the current per-path retransmission timeout, up to `MaxRetries`.

## Reassembly and replay behavior

- Reassembly key: peer address + session ID + message ID.
- Reassembly occurs before CALLGRAPH frame decoding or handler execution.
- Completed assemblies are retained until expiry so retransmitted fragments can be recognized and acknowledged.
- The server replay cache is keyed by session ID + message ID.
- The handler is invoked at most once per cached message ID. Duplicate messages while the original response is in flight are acknowledged without starting a second response sender. Once a response is cached and its original transmission ends, a duplicate may claim a bounded cached-response retransmission.
- The replay cache and reassembly table are in-memory; process restart loses both.

## Default resource limits

| Limit | Default |
|---|---:|
| Application payload | 60 KiB |
| Reassembly memory budget | 8 MiB |
| Simultaneous reassembly entries | 256 |
| Reassembly expiry | 15 seconds |
| Fragments per message | 1,024 |
| Replay entries | 8,192 |
| Cached response bytes | 8 MiB |
| Path datagram baseline | 1,200 bytes |
| MTU discovery ceiling | 1,452 bytes |

These are configurable where exposed by `UDPConfig`; the 1,024-fragment cap and MTU ceiling are profile constants.

## Security notes

With a configured 32-byte PSK, fragment payloads and ACK1 frames are protected using AES-256-GCM. The sequence ranges above, plus the separate request/response direction bit, prevent nonce overlap between packet classes. Retransmissions must repeat the exact same ciphertext for a given session and sequence.

Without PSK mode, the profile provides no cryptographic fragment integrity or peer authentication. Random message IDs and sequence separation are not authentication. The MTU probe response is challenge-validated but is not cryptographically authenticated without PSK mode.

## Known limitations

The implementation uses stop-and-wait fragment delivery. It does not yet define sliding-window flow control, streaming/backpressure, selective-repeat batching, ECN, congestion fairness across competing flows, multipath, negotiated key rotation, forward secrecy, certificate identity, or a formal threat model. The profile is experimental and must not be used as the sole safety channel for emergency-stop or hard real-time control.
