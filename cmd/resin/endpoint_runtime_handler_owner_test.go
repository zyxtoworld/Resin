package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
)

// A runtime retirement may finish its bounded graceful-stop attempt while an
// HTTP handler still ignores cancellation. The later application handler
// barrier must still find that runtime through the retirement owner.
func TestEndpointRuntimeManager_WaitForHTTPHandlersRetainsTimedOutRetirement(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	handlerStarted := make(chan struct{})
	handlerRelease := make(chan struct{})
	var startOnce sync.Once
	var releaseOnce sync.Once

	manager := newEndpointRuntimeManager(
		"127.0.0.1",
		"",
		nil,
		nil,
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			startOnce.Do(func() { close(handlerStarted) })
			<-handlerRelease
			w.WriteHeader(http.StatusNoContent)
		}),
		nil,
		nil,
		nil,
	)

	endpoint := model.Endpoint{
		ID:              "retirement-handler-owner",
		Port:            port,
		Enabled:         true,
		AllowManagement: true,
	}
	if err := manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("ApplyEndpoint: %v", err)
	}
	manager.Start()

	client, err := net.Dial("tcp", formatListenAddress("127.0.0.1", port))
	if err != nil {
		t.Fatalf("dial endpoint: %v", err)
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(handlerRelease) })
		_ = client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})

	if _, err := io.WriteString(client, "GET /api/ HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatalf("write request: %v", err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start")
	}

	manager.RemoveEndpoint(endpoint.ID)
	manager.mu.Lock()
	runtime := manager.runtimes[endpoint.ID]
	manager.mu.Unlock()
	if runtime != nil {
		t.Fatal("removed runtime still present")
	}

	// RemoveEndpoint owns the production 5s bounded retirement. Wait for its
	// completion signal instead of sleeping; the handler itself remains gated.
	manager.mu.Lock()
	var retirement *endpointRuntimeRetirement
	for _, candidate := range manager.retirements {
		retirement = candidate
		break
	}
	manager.mu.Unlock()
	if retirement == nil {
		t.Fatal("retirement owner was not registered")
	}
	select {
	case <-retirement.done:
	case <-time.After(15 * time.Second):
		t.Fatal("bounded runtime retirement did not complete")
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- manager.WaitForHTTPHandlers(context.Background()) }()
	select {
	case err := <-drainDone:
		t.Fatalf("handler drain returned before the retired handler was released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(handlerRelease) })
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("WaitForHTTPHandlers after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler drain did not finish after release")
	}
}
