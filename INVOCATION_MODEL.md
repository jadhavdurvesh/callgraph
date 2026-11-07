# CALLGRAPH invocation model — draft 0.2

## Operation descriptor
Each operation has:
- stable operation name;
- stable 32-bit operation ID assigned by a registry;
- major/minor contract version;
- request and response schema identifiers;
- idempotency classification;
- authorization scope;
- default and maximum deadline.

IDs must never be silently reassigned. Registry changes are versioned and published.

## Invocation envelope
A request contains protocol version, request ID, operation ID, contract version, deadline, trace/correlation context, and OCTET-encoded payload. A response contains the same request ID, status, structured error or result payload.

## Cancellation and deadlines
- Deadlines are absolute within a defined clock model or relative durations established at the receiving boundary; the transport adapter must define conversion.
- Cancellation is cooperative through Go context.
- A handler that ignores context cancellation may continue executing; runtime cancellation is not forced preemption.
- Expired work is rejected before handler invocation where possible.

## Idempotency and retries
Operations declare one of: read-only, idempotent-write, non-idempotent.
- Retry is automatic only for read-only operations unless a higher-level idempotency key protocol is implemented.
- Non-idempotent calls must not be automatically retried after ambiguous delivery.
- A request ID supports correlation and replay detection but does not by itself guarantee exactly-once execution.

## Errors
Errors are classified as invalid request, unauthenticated, unauthorized, method not found, deadline exceeded, cancelled, resource exhausted, unavailable, conflict, and internal. Transport errors remain distinct from application errors.

## Security
The runtime must authenticate caller identity and authorize each operation before dispatch when used remotely. Replay windows, per-peer quotas and bounded concurrency are required before network exposure.
