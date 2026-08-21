package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func TestForwardProxy_PlatformRetryBudgetDoesNotCutOffAcceptedStream(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = int64(40 * time.Millisecond)
	upstreamFirstSeen := make(chan struct{})
	releaseUpstream := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: first\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(upstreamFirstSeen)
		select {
		case <-releaseUpstream:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: late\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer upstream.Close()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseUpstream) }) }
	defer release()

	baseHash := nodeHashForBudgetE2E()
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base entry not found")
	}
	setProxyE2EEntryDialTarget(t, baseEntry, upstream.Listener.Addr().String())

	proxyHandler := NewForwardProxy(ForwardProxyConfig{
		ProxyToken:          "tok",
		Router:              env.router,
		Pool:                env.pool,
		RequestTotalTimeout: 2 * time.Second,
	})
	request := httptest.NewRequest(http.MethodGet, "http://example.com/events", nil)
	request.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		proxyHandler.ServeHTTP(response, request)
		close(done)
	}()

	select {
	case <-upstreamFirstSeen:
	case <-time.After(time.Second):
		t.Fatal("forward upstream did not send its first event")
	}
	timer := time.NewTimer(120 * time.Millisecond)
	select {
	case <-done:
		timer.Stop()
		t.Fatal("forward handler ended when the retry budget expired after response acceptance")
	case <-timer.C:
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forward handler did not finish after stream release")
	}
	if response.Code != http.StatusOK || response.Body.String() != "data: first\n\ndata: late\n\n" {
		t.Fatalf("forward stream: status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestReverseProxy_PlatformRetryBudgetDoesNotCutOffAcceptedStream(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = int64(40 * time.Millisecond)
	upstreamFirstSeen := make(chan struct{})
	releaseUpstream := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: first\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(upstreamFirstSeen)
		select {
		case <-releaseUpstream:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: late\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer upstream.Close()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseUpstream) }) }
	defer release()

	baseHash := nodeHashForBudgetE2E()
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base entry not found")
	}
	setProxyE2EEntryDialTarget(t, baseEntry, upstream.Listener.Addr().String())

	proxyHandler := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:          "tok",
		Router:              env.router,
		Pool:                env.pool,
		PlatformLookup:      env.pool,
		RequestTotalTimeout: 2 * time.Second,
	})
	targetHost := strings.TrimPrefix(upstream.URL, "http://")
	request := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/tok/plat:account/http/%s/events", targetHost), nil)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		proxyHandler.ServeHTTP(response, request)
		close(done)
	}()

	select {
	case <-upstreamFirstSeen:
	case <-time.After(time.Second):
		t.Fatal("reverse upstream did not send its first event")
	}
	timer := time.NewTimer(120 * time.Millisecond)
	select {
	case <-done:
		timer.Stop()
		t.Fatal("reverse handler ended when the retry budget expired after response acceptance")
	case <-timer.C:
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reverse handler did not finish after stream release")
	}
	if response.Code != http.StatusOK || response.Body.String() != "data: first\n\ndata: late\n\n" {
		t.Fatalf("reverse stream: status=%d body=%q", response.Code, response.Body.String())
	}
}

func nodeHashForBudgetE2E() node.Hash {
	return node.HashFromRawOptions([]byte(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
}
