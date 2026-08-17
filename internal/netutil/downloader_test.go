package netutil

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestDirectDownloader_RequestErrorRedactsURLCredentials(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{name: "network", cause: errors.New("connection refused")},
		{name: "canceled", cause: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &DirectDownloader{
				Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return nil, &url.Error{Op: req.Method, URL: req.URL.String(), Err: tc.cause}
				})},
				TimeoutFn:   func() time.Duration { return 0 },
				UserAgentFn: func() string { return "" },
			}

			gotErr := func() error {
				_, err := d.Download(context.Background(), "https://subscriber:download-secret@example.com/subscription")
				return err
			}()
			if gotErr == nil {
				t.Fatal("expected request error")
			}
			if strings.Contains(gotErr.Error(), "download-secret") || strings.Contains(gotErr.Error(), "subscriber:") {
				t.Fatal("request error exposed URL credentials")
			}
			if !errors.Is(gotErr, tc.cause) {
				t.Fatalf("request error lost its underlying cause: %v", gotErr)
			}
		})
	}
}

func TestDirectDownloader_StatusErrorStoresOnlyURLOrigin(t *testing.T) {
	const rawURL = "https://subscriber:status-secret@example.com/sub/path-secret?token=query-secret"
	d := &DirectDownloader{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("unavailable")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		})},
		TimeoutFn:   func() time.Duration { return 0 },
		UserAgentFn: func() string { return "" },
	}

	_, err := d.Download(context.Background(), rawURL)
	if err == nil {
		t.Fatal("expected status error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T: %v", err, err)
	}
	if statusErr.URL != "https://example.com" {
		t.Fatalf("status error retained non-origin URL: %q", statusErr.URL)
	}
	if strings.Contains(err.Error(), "status-secret") || strings.Contains(err.Error(), "query-secret") || strings.Contains(err.Error(), "path-secret") {
		t.Fatalf("status error exposed URL secret: %v", err)
	}
}

func TestDirectDownloader_MalformedURLRedactsCredentials(t *testing.T) {
	const rawURL = "https://subscriber:malformed-userinfo@example.com/sub/%zz?token=malformed-query"
	d := &DirectDownloader{
		Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("malformed URL reached transport")
			return nil, nil
		})},
		TimeoutFn:   func() time.Duration { return 0 },
		UserAgentFn: func() string { return "" },
	}

	_, err := d.Download(context.Background(), rawURL)
	if err == nil {
		t.Fatal("expected malformed URL error")
	}
	for _, secret := range []string{"malformed-userinfo", "malformed-query", "/sub/%zz", "subscriber:"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("malformed URL error exposed %q: %v", secret, err)
		}
	}
}

func TestDirectDownloader_RejectsOversizedResponseBody(t *testing.T) {
	body := bytes.Repeat([]byte{'x'}, maxResourceBodyBytes+1)
	d := &DirectDownloader{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(body)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		})},
		TimeoutFn:   func() time.Duration { return 0 },
		UserAgentFn: func() string { return "" },
	}

	got, err := d.Download(context.Background(), "https://resource.example/subscription")
	if err == nil {
		t.Fatalf("oversized response was accepted: %d bytes", len(got))
	}
	if got != nil {
		t.Fatalf("oversized response returned body: %d bytes", len(got))
	}
}

func TestDirectDownloader_ContextDeadlineOverridesFallbackTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	d := NewDirectDownloader(
		func() time.Duration { return 20 * time.Millisecond },
		func() string { return "" },
	)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	body, err := d.Download(ctx, srv.URL)
	if err != nil {
		t.Fatalf("download should succeed with caller deadline, got err=%v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("body: got %q, want %q", string(body), "ok")
	}
}

func TestDirectDownloader_FallbackTimeoutWithoutContextDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	d := NewDirectDownloader(
		func() time.Duration { return 20 * time.Millisecond },
		func() string { return "" },
	)

	_, err := d.Download(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestDirectDownloader_DynamicTimeoutPulled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	timeout := 200 * time.Millisecond
	d := NewDirectDownloader(
		func() time.Duration { return timeout },
		func() string { return "" },
	)

	if _, err := d.Download(context.Background(), srv.URL); err != nil {
		t.Fatalf("download should succeed with long timeout, got %v", err)
	}

	timeout = 20 * time.Millisecond
	_, err := d.Download(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected timeout error after shrinking dynamic timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestDirectDownloader_DynamicUserAgentPulled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Header.Get("User-Agent")))
	}))
	defer srv.Close()

	ua := "agent-a"
	d := NewDirectDownloader(
		func() time.Duration { return 0 },
		func() string { return ua },
	)

	body, err := d.Download(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("first download failed: %v", err)
	}
	if string(body) != "agent-a" {
		t.Fatalf("expected first UA agent-a, got %q", string(body))
	}

	ua = "agent-b"
	body, err = d.Download(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("second download failed: %v", err)
	}
	if string(body) != "agent-b" {
		t.Fatalf("expected second UA agent-b, got %q", string(body))
	}
}
