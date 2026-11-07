// Package callgraph provides transport-independent typed-operation dispatch.
package callgraph

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrMethodNotFound = errors.New("callgraph: method not found")
	ErrDuplicateMethod = errors.New("callgraph: duplicate method")
	ErrPayloadTooLarge = errors.New("callgraph: payload too large")
	ErrNilHandler = errors.New("callgraph: nil handler")
)

type Handler func(context.Context, []byte) ([]byte, error)

type Config struct {
	MaxRequestBytes  int
	MaxResponseBytes int
}

func DefaultConfig() Config { return Config{MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20} }

type Runtime struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	config   Config
}

func New(config Config) (*Runtime, error) {
	if config.MaxRequestBytes <= 0 || config.MaxResponseBytes <= 0 {
		return nil, fmt.Errorf("callgraph: payload limits must be positive")
	}
	return &Runtime{handlers: make(map[string]Handler), config: config}, nil
}

func (r *Runtime) Register(method string, handler Handler) error {
	if method == "" { return errors.New("callgraph: method name is required") }
	if handler == nil { return ErrNilHandler }
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[method]; exists { return ErrDuplicateMethod }
	r.handlers[method] = handler
	return nil
}

func (r *Runtime) Invoke(ctx context.Context, method string, payload []byte) ([]byte, error) {
	if len(payload) > r.config.MaxRequestBytes { return nil, ErrPayloadTooLarge }
	r.mu.RLock()
	handler, ok := r.handlers[method]
	r.mu.RUnlock()
	if !ok { return nil, ErrMethodNotFound }
	if err := ctx.Err(); err != nil { return nil, err }
	response, err := handler(ctx, payload)
	if err != nil { return nil, err }
	if len(response) > r.config.MaxResponseBytes { return nil, ErrPayloadTooLarge }
	return response, nil
}
