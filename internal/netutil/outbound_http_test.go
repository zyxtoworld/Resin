package netutil

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/testutil"
)

func TestHTTPGetViaOutbound_RejectsOversizedResponseBody(t *testing.T) {
	body := bytes.Repeat([]byte{'x'}, maxResourceBodyBytes+1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	got, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{})
	if err == nil {
		t.Fatalf("oversized response was accepted: %d bytes", len(got))
	}
	if got != nil {
		t.Fatalf("oversized response returned body: %d bytes", len(got))
	}
}

func TestHTTPGetViaOutbound_RequireStatusOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: true,
	})
	if err == nil {
		t.Fatal("expected non-200 status to return error")
	}
	if !strings.Contains(err.Error(), "unexpected status 404") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

func TestHTTPGetViaOutbound_ErrorsRedactURLCredentials(t *testing.T) {
	const (
		userinfoSecret = "status-userinfo-secret"
		pathSecret     = "status-path-secret"
		querySecret    = "status-query-secret"
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	rawURL := strings.Replace(srv.URL, "http://", "http://subscriber:"+userinfoSecret+"@", 1) +
		"/sub/" + pathSecret + "?token=" + querySecret

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}

	_, _, err = HTTPGetViaOutbound(context.Background(), ob, rawURL, OutboundHTTPOptions{
		RequireStatusOK: true,
	})
	if err == nil {
		t.Fatal("expected non-200 status to return error")
	}
	if !strings.Contains(err.Error(), "unexpected status 503") {
		t.Fatalf("status error lost status diagnostic: %v", err)
	}
	if !strings.Contains(err.Error(), srv.URL) {
		t.Fatalf("status error lost origin diagnostic: %v", err)
	}
	for _, secret := range []string{userinfoSecret, pathSecret, querySecret, "subscriber:"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("status error exposed URL secret %q: %v", secret, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = HTTPGetViaOutbound(ctx, ob, rawURL, OutboundHTTPOptions{})
	if err == nil {
		t.Fatal("expected canceled request to return error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("request error lost context cancellation: %v", err)
	}
	for _, secret := range []string{userinfoSecret, pathSecret, querySecret, "subscriber:"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("request error exposed URL secret %q: %v", secret, err)
		}
	}

	malformedURL := strings.Replace(srv.URL, "http://", "http://subscriber:malformed-userinfo@", 1) +
		"/sub/%zz?token=malformed-query"
	_, _, err = HTTPGetViaOutbound(context.Background(), ob, malformedURL, OutboundHTTPOptions{})
	if err == nil {
		t.Fatal("expected malformed URL to return error")
	}
	for _, secret := range []string{"malformed-userinfo", "malformed-query", "/sub/%zz", "subscriber:"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("malformed URL error exposed URL secret %q: %v", secret, err)
		}
	}
}

func TestHTTPGetViaOutbound_AllowNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("probe-body"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	body, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: false,
	})
	if err != nil {
		t.Fatalf("expected non-200 response to pass through, got: %v", err)
	}
	if string(body) != "probe-body" {
		t.Fatalf("unexpected body %q", string(body))
	}
}

func TestConnCloseHook_CloseIsIdempotentAndConcurrentSafe(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	var onCloseCount atomic.Int32
	hook := &connCloseHook{
		Conn: client,
		onClose: func() {
			onCloseCount.Add(1)
		},
	}

	const closers = 32
	var wg sync.WaitGroup
	wg.Add(closers)
	for i := 0; i < closers; i++ {
		go func() {
			defer wg.Done()
			_ = hook.Close()
		}()
	}
	wg.Wait()

	if got := onCloseCount.Load(); got != 1 {
		t.Fatalf("onClose called %d times, want 1", got)
	}
}

type observedCloseConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *observedCloseConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestConnCloseHook_ClosesUnderlyingBeforeCallback(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	base := &observedCloseConn{Conn: client}
	callbackObserved := make(chan bool, 1)
	hook := &connCloseHook{
		Conn: base,
		onClose: func() {
			callbackObserved <- base.closed.Load()
		},
	}

	if err := hook.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case closed := <-callbackObserved:
		if !closed {
			t.Fatal("close callback ran before the underlying connection closed")
		}
	case <-time.After(time.Second):
		t.Fatal("close callback was not invoked")
	}
}
