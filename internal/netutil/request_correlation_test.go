package netutil

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func TestRetryDownloaderPropagatesRequestCorrelationIDToAttemptEvents(t *testing.T) {
	const correlationID = "550e8400-e29b-41d4-a716-446655440000"

	var (
		mu     sync.Mutex
		events []AttemptEvent
	)
	r := &RetryDownloader{
		Direct: downloaderFunc(func(context.Context, string) ([]byte, error) {
			return nil, errors.New("direct failed")
		}),
		TotalTimeout:     time.Second,
		MaxProxyAttempts: 1,
		AttemptObserver: func(event AttemptEvent) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
		NodePicker: func(context.Context, string, []NodeSelection) (NodeSelection, error) {
			return NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"correlation-node"}`))}, nil
		},
		ProxyFetch: func(context.Context, NodeSelection, string) ([]byte, error) {
			return nil, errors.New("proxy failed")
		},
	}

	ctx := WithRequestCorrelationID(context.Background(), correlationID)
	_, _ = r.Download(ctx, "https://example.test/subscription")

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("expected attempt events")
	}
	for _, event := range events {
		if event.CorrelationID != correlationID {
			t.Fatalf("attempt event correlation_id = %q, want %q: %+v", event.CorrelationID, correlationID, event)
		}
	}
}

func TestWithRequestCorrelationIDRejectsLogUnsafeValues(t *testing.T) {
	for _, id := range []string{
		"",
		"550e8400-e29b-41d4-a716-446655440000\nmalicious",
		"550e8400-e29b-41d4-a716-446655440000" + strings.Repeat("x", 128),
		"refresh-correlation-test",
	} {
		ctx := WithRequestCorrelationID(context.Background(), id)
		if got := RequestCorrelationID(ctx); got != "" {
			t.Fatalf("unsafe correlation ID %q was retained as %q", id, got)
		}
	}
}
