package callgraph

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrInvalidDescriptor = errors.New("callgraph: invalid operation descriptor")
	ErrDeadlineExceeded = errors.New("callgraph: deadline exceeded")
	ErrUnauthorized = errors.New("callgraph: unauthorized")
)

type Idempotency uint8
const (
	ReadOnly Idempotency = iota
	IdempotentWrite
	NonIdempotent
)

type Descriptor struct {
	Name string
	ID uint32
	Major uint16
	Minor uint16
	Idempotency Idempotency
	AuthorizationScope string
	MaxDeadline time.Duration
}

func (d Descriptor) Validate() error {
	if d.Name == "" || d.ID == 0 || d.Major == 0 || d.MaxDeadline <= 0 || d.Idempotency > NonIdempotent {
		return ErrInvalidDescriptor
	}
	return nil
}

// OperationID returns a stable candidate ID. Registries must detect collisions,
// publish assignments, and never silently reassign an ID.
func OperationID(qualifiedName string) uint32 {
	sum := sha256.Sum256([]byte(qualifiedName))
	id := binary.BigEndian.Uint32(sum[:4])
	if id == 0 { return 1 }
	return id
}

type Authorizer func(context.Context, string) error

func (r *Runtime) RegisterOperation(d Descriptor, authorize Authorizer, handler Handler) error {
	if err := d.Validate(); err != nil { return err }
	if authorize == nil { return ErrUnauthorized }
	if handler == nil { return ErrNilHandler }
	wrapped := func(ctx context.Context, payload []byte) ([]byte, error) {
		if err := authorize(ctx, d.AuthorizationScope); err != nil { return nil, err }
		return handler(ctx, payload)
	}
	return r.Register(d.Name, wrapped)
}

func (r *Runtime) InvokeOperation(ctx context.Context, d Descriptor, payload []byte) ([]byte, error) {
	if err := d.Validate(); err != nil { return nil, err }
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= 0 {
		return nil, ErrDeadlineExceeded
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > d.MaxDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.MaxDeadline)
		defer cancel()
	}
	if err := ctx.Err(); err != nil { return nil, err }
	response, err := r.Invoke(ctx, d.Name, payload)
	if errors.Is(err, context.DeadlineExceeded) { return nil, ErrDeadlineExceeded }
	return response, err
}
