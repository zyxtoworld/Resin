package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/service"
)

type blockingEndpointHarness struct {
	manager        *endpointRuntimeManager
	handlerEntered chan struct{}
	handlerRelease chan struct{}
	handlerDone    chan struct{}
	clientDone     chan error
	releaseOnce    sync.Once
}

func newBlockingEndpointHarness(t *testing.T, handlerBody func()) *blockingEndpointHarness {
	t.Helper()
	port := reserveTestPorts(t, 1)[0]
	h := &blockingEndpointHarness{
		handlerEntered: make(chan struct{}),
		handlerRelease: make(chan struct{}),
		handlerDone:    make(chan struct{}),
		clientDone:     make(chan error, 1),
	}
	var enteredOnce sync.Once
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		enteredOnce.Do(func() { close(h.handlerEntered) })
		<-h.handlerRelease
		handlerBody()
		close(h.handlerDone)
		w.WriteHeader(http.StatusNoContent)
	})
	h.manager = newEndpointRuntimeManager("127.0.0.1", "", nil, nil, apiHandler, nil, nil, nil)
	endpoint := service.NewDefaultEndpoint(port)
	if err := h.manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("ApplyEndpoint: %v", err)
	}
	h.manager.Start()
	go func() {
		resp, err := http.Get(formatListenURL("127.0.0.1", port) + "/api/")
		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		h.clientDone <- err
	}()
	select {
	case <-h.handlerEntered:
	case <-time.After(time.Second):
		t.Fatal("blocking HTTP handler did not start")
	}
	t.Cleanup(func() {
		h.release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = h.manager.Shutdown(ctx)
	})
	return h
}

func (h *blockingEndpointHarness) release() {
	if h == nil {
		return
	}
	h.releaseOnce.Do(func() { close(h.handlerRelease) })
}

func (h *blockingEndpointHarness) waitHandler(t *testing.T) {
	t.Helper()
	select {
	case <-h.handlerDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not finish")
	}
	select {
	case <-h.clientDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP client did not finish")
	}
}

func TestResinAppHTTPDrainHonorsShutdownDeadline(t *testing.T) {
	h := newBlockingEndpointHarness(t, func() {})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := h.manager.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want deadline exceeded", err)
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- drainHTTPHandlersBeforeSinks(ctx, h.manager, nil) }()
	select {
	case err := <-drainDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handler drain error = %v, want deadline exceeded", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("app handler drain ignored the shutdown deadline")
	}
	h.release()
	h.waitHandler(t)
}

func TestEndpointRuntimeManager_WaitForHTTPHandlersAfterShutdownUsesRetiredRuntime(t *testing.T) {
	h := newBlockingEndpointHarness(t, func() {})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := h.manager.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want deadline exceeded", err)
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- h.manager.WaitForHTTPHandlers(context.Background()) }()
	select {
	case err := <-drainDone:
		t.Fatalf("handler drain returned before release: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	h.release()
	h.waitHandler(t)
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("WaitForHTTPHandlers: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler drain did not finish after release")
	}
}
