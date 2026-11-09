# CALLGRAPH streaming API groundwork

Status: local in-process primitive; not yet a wire-level streaming protocol.

## Stream pipe

`NewStreamPipe(ctx)` creates a `*StreamReader` and `*StreamWriter` pair backed by Go's synchronous pipe.

- Data is consumed incrementally; the implementation does not enqueue the entire payload in an application-owned buffer.
- A writer blocks until a reader consumes the bytes. This is the backpressure mechanism.
- Cancelling the parent context unblocks blocked reads and writes.
- Closing the writer produces EOF after buffered pipe data is consumed.
- `CloseWithError` propagates a terminal error to the opposite endpoint.
- The primitive is transport-independent and can be used by local handlers while the network stream framing is designed.

## Example

```go
reader, writer := callgraph.NewStreamPipe(ctx)
defer reader.Close()
defer writer.Close()

go func() {
    defer writer.Close()
    _, _ = io.Copy(writer, source)
}()

_, err := io.Copy(destination, reader)
```

## Scope and limitations

This API is not yet integrated into CALLGRAPH's UDP RPC frames. The existing RPC path still accepts and assembles bounded whole-message payloads. The pipe is not a network stream, does not provide remote half-close, stream IDs, multiplexing, per-stream flow-control windows, checksums, or resumability. Those require a versioned wire-level stream profile and corresponding cross-language implementations. The current pipe uses synchronous in-process backpressure and has no configurable byte window because it does not buffer an application queue.
