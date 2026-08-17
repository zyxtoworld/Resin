package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/platform"
)

func TestReverseRetryRoundTripper_CancelAfterResponseBodyDoesNotReplay(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retryable", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusBadGateway},
			Body:        &model.PlatformResponseBodyMatch{Op: "contains", Value: "retryable"},
		},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/api")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &cancelOnEOFBody{reader: strings.NewReader("retryable"), cancel: cancel}
	var calls atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				_, _ = io.ReadAll(req.Body)
				calls.Add(1)
				return &http.Response{
					StatusCode: http.StatusBadGateway,
					Header:     make(http.Header),
					Body:       body,
				}, nil
			})
		},
	}
	req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(`{"stream":true}`)).WithContext(ctx)
	resp, err := retry.RoundTrip(req)
	if !errors.Is(err, context.Canceled) || resp != nil {
		t.Fatalf("canceled retry: resp=%v err=%v", resp, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("canceled retry attempts: got %d, want 1", got)
	}
	if got := body.closeCalls.Load(); got != 1 {
		t.Fatalf("canceled response body close calls: got %d, want 1", got)
	}
}

func TestReverseRetryRoundTripper_OversizedRequestBodyDoesNotReplay(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retryable", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusBadGateway},
			Body:        &model.PlatformResponseBodyMatch{Op: "contains", Value: "retryable"},
		},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/api")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}

	var calls atomic.Int32
	responseBody := io.NopCloser(strings.NewReader("retryable"))
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				_, _ = io.ReadAll(req.Body)
				calls.Add(1)
				return &http.Response{StatusCode: http.StatusBadGateway, Header: make(http.Header), Body: responseBody}, nil
			})
		},
	}
	payload := strings.Repeat("x", responseRuleRetryBodyLimit+1)
	req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(payload))
	resp, err := retry.RoundTrip(req)
	if err != nil || resp == nil {
		t.Fatalf("oversized retry: resp=%v err=%v", resp, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("oversized retry status: got %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("oversized retry attempts: got %d, want 1", got)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type cancelOnEOFBody struct {
	reader     io.Reader
	cancel     context.CancelFunc
	closeCalls atomic.Int32
}

func (b *cancelOnEOFBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err == io.EOF {
		b.cancel()
	}
	return n, err
}

func (b *cancelOnEOFBody) Close() error {
	b.closeCalls.Add(1)
	return nil
}
