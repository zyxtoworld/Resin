package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

type temporaryAcceptFailureListener struct {
	net.Listener
	remaining int
	attempts  chan struct{}
	mu        sync.Mutex
}

func (l *temporaryAcceptFailureListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.remaining > 0 {
		l.remaining--
		l.mu.Unlock()
		l.attempts <- struct{}{}
		return nil, temporaryNetError{err: errors.New("temporary accept failure")}
	}
	l.mu.Unlock()
	return l.Listener.Accept()
}

func TestInboundDemux_ShutdownInterruptsTemporaryAcceptBackoff(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	listener := &temporaryAcceptFailureListener{
		Listener:  base,
		remaining: 8,
		attempts:  make(chan struct{}, 8),
	}
	t.Cleanup(func() { _ = listener.Close() })
	demux := newInboundDemuxServer(&http.Server{}, nil)
	t.Cleanup(func() { _ = demux.Shutdown(context.Background()) })
	serveDone := make(chan error, 1)
	go func() { serveDone <- demux.Serve(listener) }()

	for i := 0; i < 8; i++ {
		select {
		case <-listener.attempts:
		case <-time.After(time.Second):
			t.Fatalf("temporary accept attempt %d did not occur", i+1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := demux.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown: %v", err)
	}

	select {
	case err := <-serveDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Serve remained in temporary accept backoff after Shutdown returned")
	}
}
