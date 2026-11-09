package callgraph

import (
	"context"
	"io"
	"sync"
)

// StreamReader is the read half of a bounded, backpressured byte stream.
type StreamReader struct {
	pipe *io.PipeReader
	done chan struct{}
	stop context.CancelFunc
	once sync.Once
	doneOnce *sync.Once
}

// StreamWriter is the write half of a bounded, backpressured byte stream.
type StreamWriter struct {
	pipe *io.PipeWriter
	done chan struct{}
	stop context.CancelFunc
	once sync.Once
	doneOnce *sync.Once
}

// NewStreamPipe creates a transport-independent full-duplex-independent
// byte stream with synchronous backpressure. Writes cannot accumulate in an
// unbounded application queue: each Write is coupled to reads from the peer.
// The supplied context cancels blocked reads and writes.
func NewStreamPipe(parent context.Context) (*StreamReader, *StreamWriter) {
	if parent == nil { parent = context.Background() }
	ctx, cancel := context.WithCancel(parent)
	reader, writer := io.Pipe()
	done := make(chan struct{})
	doneOnce := &sync.Once{}
	r := &StreamReader{pipe:reader,done:done,stop:cancel,doneOnce:doneOnce}
	w := &StreamWriter{pipe:writer,done:done,stop:cancel,doneOnce:doneOnce}
	go func() {
		select {
		case <-ctx.Done():
			_ = reader.CloseWithError(ctx.Err())
			_ = writer.CloseWithError(ctx.Err())
		case <-done:
		}
	}()
	return r,w
}

func (r *StreamReader) Read(p []byte) (int,error) {
	if r==nil||r.pipe==nil{return 0,io.ErrClosedPipe}
	return r.pipe.Read(p)
}

func (r *StreamReader) Close() error {
	if r==nil||r.pipe==nil{return nil}
	var err error
	r.once.Do(func(){err=r.pipe.Close();r.stop();r.doneOnce.Do(func(){close(r.done)})})
	return err
}

func (r *StreamReader) CloseWithError(cause error) error {
	if r==nil||r.pipe==nil{return nil}
	var err error
	r.once.Do(func(){err=r.pipe.CloseWithError(cause);r.stop();close(r.done)})
	return err
}

func (w *StreamWriter) Write(p []byte) (int,error) {
	if w==nil||w.pipe==nil{return 0,io.ErrClosedPipe}
	return w.pipe.Write(p)
}

func (w *StreamWriter) Close() error {
	if w==nil||w.pipe==nil{return nil}
	var err error
	w.once.Do(func(){err=w.pipe.Close();w.stop();w.doneOnce.Do(func(){close(w.done)})})
	return err
}

func (w *StreamWriter) CloseWithError(cause error) error {
	if w==nil||w.pipe==nil{return nil}
	var err error
	w.once.Do(func(){err=w.pipe.CloseWithError(cause);w.stop();close(w.done)})
	return err
}
