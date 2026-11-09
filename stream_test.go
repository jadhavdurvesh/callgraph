package callgraph

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestStreamPipeTransfersIncrementallyWithBackpressure(t *testing.T) {
	reader,writer:=NewStreamPipe(context.Background())
	defer reader.Close()
	defer writer.Close()
	payload:=bytes.Repeat([]byte("callgraph-stream-"),16*1024)
	writeDone:=make(chan error,1)
	go func(){_,err:=writer.Write(payload);if err==nil{err=writer.Close()};writeDone<-err}()
	// A pipe writer cannot complete until a reader consumes the bytes.
	select {
	case err:=<-writeDone:t.Fatalf("writer completed without a reader: %v",err)
	case <-time.After(20*time.Millisecond):
	}
	got:=make([]byte,0,len(payload))
	buf:=make([]byte,4096)
	for {
		n,err:=reader.Read(buf)
		if n>0{got=append(got,buf[:n]...)}
		if err==io.EOF{break}
		if err!=nil{t.Fatal(err)}
	}
	if !bytes.Equal(got,payload){t.Fatalf("stream mismatch: got %d bytes, want %d",len(got),len(payload))}
	if err:=<-writeDone;err!=nil{t.Fatal(err)}
}

func TestStreamPipeContextCancellationUnblocksWriter(t *testing.T) {
	ctx,cancel:=context.WithCancel(context.Background())
	reader,writer:=NewStreamPipe(ctx)
	defer reader.Close()
	writeDone:=make(chan error,1)
	go func(){_,err:=writer.Write(bytes.Repeat([]byte("blocked"),4096));writeDone<-err}()
	select {
	case err:=<-writeDone:t.Fatalf("writer unexpectedly completed before cancellation: %v",err)
	case <-time.After(20*time.Millisecond):
	}
	cancel()
	select {
	case err:=<-writeDone:
		if !errors.Is(err,context.Canceled){t.Fatalf("expected context cancellation, got %v",err)}
	case <-time.After(time.Second):t.Fatal("writer remained blocked after context cancellation")
	}
}

func TestStreamPipeClosePropagatesEOF(t *testing.T) {
	reader,writer:=NewStreamPipe(context.Background())
	writeDone:=make(chan error,1)
	go func(){_,err:=writer.Write([]byte("done"));if err==nil{err=writer.Close()};writeDone<-err}()
	got,err:=io.ReadAll(reader)
	if err!=nil{t.Fatal(err)}
	if string(got)!="done"{t.Fatalf("got %q",got)}
	if err:=<-writeDone;err!=nil{t.Fatal(err)}
	if err:=reader.Close();err!=nil{t.Fatal(err)}
}
