package netutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func TestAttemptResultDistinguishesTimeoutCancellationAndError(t *testing.T) {
	deadlineCtx, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()

	cancelCtx, cancelCaller := context.WithCancel(context.Background())
	cancelCaller()

	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want string
	}{
		{name: "nil", ctx: context.Background(), want: "ok"},
		{name: "direct deadline error", ctx: context.Background(), err: context.DeadlineExceeded, want: "timeout"},
		{name: "direct canceled error", ctx: context.Background(), err: context.Canceled, want: "canceled"},
		{name: "deadline context", ctx: deadlineCtx, err: errors.New("transport failed"), want: "timeout"},
		{name: "caller canceled context", ctx: cancelCtx, err: errors.New("transport failed"), want: "canceled"},
		{name: "ordinary error", ctx: context.Background(), err: errors.New("transport failed"), want: "error"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := attemptResult(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("attemptResult() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRetryDownloaderAttemptEventsAreSafeLogProjection(t *testing.T) {
	const (
		userinfoSecret = "attempt-userinfo-unique-secret"
		pathSecret     = "attempt-path-unique-secret"
		querySecret    = "attempt-query-unique-secret"
		rawError       = "attempt-raw-error-unique-sentinel"
		rawNodeOptions = `{"type":"node","password":"attempt-node-options-unique-secret"}`
	)
	selection := NodeSelection{Hash: node.HashFromRawOptions([]byte(rawNodeOptions))}
	rawURL := "https://subscriber:" + userinfoSecret + "@example.test/sub/" + pathSecret + "?token=" + querySecret

	var (
		eventsMu sync.Mutex
		events   []AttemptEvent
		logLines []string
	)
	observer := func(event AttemptEvent) {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		events = append(events, event)
		logLines = append(logLines, fmt.Sprintf(
			"resource_attempt request_id=%d platform_id=%s attempt=%d kind=%s node_id=%s phase=%s elapsed_ms=%d result=%s",
			event.RequestID,
			event.PlatformID,
			event.Attempt,
			event.Kind,
			event.NodeID,
			event.Phase,
			event.Elapsed/time.Millisecond,
			event.Result,
		))
	}

	r := &RetryDownloader{
		Direct: downloaderFunc(func(context.Context, string) ([]byte, error) {
			return nil, fmt.Errorf("direct transport: %s", rawError)
		}),
		TotalTimeout:     time.Second,
		MaxProxyAttempts: 1,
		PlatformID:       "platform-safe-observation",
		AttemptObserver:  observer,
		NodePicker: func(context.Context, string, []NodeSelection) (NodeSelection, error) {
			return selection, nil
		},
		ProxyFetch: func(context.Context, NodeSelection, string) ([]byte, error) {
			return nil, fmt.Errorf("proxy transport: %s", rawError)
		},
	}

	_, _ = r.Download(context.Background(), rawURL)

	eventsMu.Lock()
	defer eventsMu.Unlock()
	if len(events) == 0 || len(logLines) != len(events) {
		t.Fatalf("attempt observation count = %d, log projection count = %d", len(events), len(logLines))
	}
	serialized, err := json.Marshal(events)
	if err != nil {
		t.Fatalf("marshal attempt events: %v", err)
	}
	projection := string(serialized) + "\n" + strings.Join(logLines, "\n")
	for _, secret := range []string{userinfoSecret, pathSecret, querySecret, rawError, rawNodeOptions, "attempt-node-options-unique-secret"} {
		if strings.Contains(projection, secret) {
			t.Fatalf("attempt projection exposed secret %q", secret)
		}
	}

	proxyEvents := 0
	for _, event := range events {
		if event.Kind == AttemptKindProxy {
			proxyEvents++
			if event.NodeID != selection.Hash.Hex() {
				t.Fatalf("proxy event node id = %q, want stable hash %q", event.NodeID, selection.Hash.Hex())
			}
		}
		if event.Kind == AttemptKindDirect && event.NodeID != "" {
			t.Fatalf("direct event exposed node id %q", event.NodeID)
		}
	}
	if proxyEvents == 0 {
		t.Fatal("expected a proxy attempt event")
	}
}
