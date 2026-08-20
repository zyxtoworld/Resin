package netutil

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

type downloaderFunc func(ctx context.Context, url string) ([]byte, error)

func (f downloaderFunc) Download(ctx context.Context, url string) ([]byte, error) {
	return f(ctx, url)
}

func TestRetryDownloader_RetryOnSelectedHTTPStatusError(t *testing.T) {
	retryableStatusCodes := []int{403, 429, 500, 502, 503, 504}

	for _, statusCode := range retryableStatusCodes {
		t.Run(strconv.Itoa(statusCode), func(t *testing.T) {
			var pickerCalls, proxyCalls int

			r := &RetryDownloader{
				Direct: downloaderFunc(func(_ context.Context, url string) ([]byte, error) {
					return nil, &HTTPStatusError{StatusCode: statusCode, URL: url}
				}),
				NodePicker: func(_ context.Context, _ string, _ []NodeSelection) (NodeSelection, error) {
					pickerCalls++
					return NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"retry-node"}`))}, nil
				},
				ProxyFetch: func(_ context.Context, _ NodeSelection, _ string) ([]byte, error) {
					proxyCalls++
					return []byte("proxy"), nil
				},
			}

			body, err := r.Download(context.Background(), "https://example.com")
			if err != nil {
				t.Fatalf("expected proxy retry success, got %v", err)
			}
			if string(body) != "proxy" {
				t.Fatalf("unexpected body %q", string(body))
			}
			if pickerCalls != 1 || proxyCalls != 1 {
				t.Fatalf("expected single successful retry, got picker=%d proxy=%d", pickerCalls, proxyCalls)
			}
		})
	}
}

func TestRetryDownloader_NoRetryOnNonRetryableHTTPStatusError(t *testing.T) {
	var pickerCalls, proxyCalls int

	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, url string) ([]byte, error) {
			return nil, &HTTPStatusError{StatusCode: 404, URL: url}
		}),
		NodePicker: func(_ context.Context, _ string, _ []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			return NodeSelection{}, nil
		},
		ProxyFetch: func(_ context.Context, _ NodeSelection, _ string) ([]byte, error) {
			proxyCalls++
			return []byte("proxy"), nil
		},
	}

	_, err := r.Download(context.Background(), "https://example.com")
	if err == nil {
		t.Fatal("expected direct error")
	}
	if pickerCalls != 0 || proxyCalls != 0 {
		t.Fatalf("expected no proxy retry, got picker=%d proxy=%d", pickerCalls, proxyCalls)
	}
}

func TestRetryDownloader_NoRetryOnNonRetryableError(t *testing.T) {
	var pickerCalls, proxyCalls int
	inner := errors.New("bad url")

	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, _ string) ([]byte, error) {
			return nil, &NonRetryableError{Err: inner}
		}),
		NodePicker: func(_ context.Context, _ string, _ []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			return NodeSelection{}, nil
		},
		ProxyFetch: func(_ context.Context, _ NodeSelection, _ string) ([]byte, error) {
			proxyCalls++
			return []byte("proxy"), nil
		},
	}

	_, err := r.Download(context.Background(), "::::")
	if err == nil {
		t.Fatal("expected direct error")
	}
	if !errors.Is(err, inner) {
		t.Fatalf("expected wrapped inner error, got: %v", err)
	}
	if pickerCalls != 0 || proxyCalls != 0 {
		t.Fatalf("expected no proxy retry, got picker=%d proxy=%d", pickerCalls, proxyCalls)
	}
}

func TestRetryDownloader_RetryOnNetworkError(t *testing.T) {
	var pickerCalls, proxyCalls int

	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, _ string) ([]byte, error) {
			return nil, context.DeadlineExceeded
		}),
		NodePicker: func(_ context.Context, _ string, _ []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			return NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"retry-node"}`))}, nil
		},
		ProxyFetch: func(_ context.Context, _ NodeSelection, _ string) ([]byte, error) {
			proxyCalls++
			return []byte("via-proxy"), nil
		},
	}

	body, err := r.Download(context.Background(), "https://example.com")
	if err != nil {
		t.Fatalf("expected proxy retry success, got %v", err)
	}
	if string(body) != "via-proxy" {
		t.Fatalf("unexpected body %q", string(body))
	}
	if pickerCalls != 1 || proxyCalls != 1 {
		t.Fatalf("expected single successful retry, got picker=%d proxy=%d", pickerCalls, proxyCalls)
	}
}

func TestRetryDownloader_NoRetryWhenContextDone(t *testing.T) {
	var pickerCalls int
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, _ string) ([]byte, error) {
			return nil, context.Canceled
		}),
		NodePicker: func(_ context.Context, _ string, _ []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			return NodeSelection{}, nil
		},
	}

	_, err := r.Download(ctx, "https://example.com")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if pickerCalls != 0 {
		t.Fatalf("expected no retry when context is done, got picker calls=%d", pickerCalls)
	}
}

func TestRetryDownloader_CancelInterruptsBlockedNodePicker(t *testing.T) {
	pickerEntered := make(chan struct{})
	releasePicker := make(chan struct{})
	pickerCalls := 0
	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, url string) ([]byte, error) {
			return nil, &HTTPStatusError{StatusCode: 503, URL: url}
		}),
		NodePicker: func(ctx context.Context, _ string, _ []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			if pickerCalls == 1 {
				close(pickerEntered)
			}
			select {
			case <-releasePicker:
				return NodeSelection{}, nil
			case <-ctx.Done():
				return NodeSelection{}, ctx.Err()
			}
		},
		ProxyFetch: func(_ context.Context, _ NodeSelection, _ string) ([]byte, error) {
			return nil, errors.New("proxy failed")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := r.Download(ctx, "https://example.com")
		result <- err
	}()
	select {
	case <-pickerEntered:
	case <-time.After(time.Second):
		t.Fatal("node picker did not start")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Download error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		close(releasePicker)
		select {
		case <-result:
		case <-time.After(time.Second):
		}
		t.Fatal("Download remained blocked in NodePicker after context cancellation")
	}
}

func TestRetryDownloader_ProxyRetriesExhaustedReturnsDirectError(t *testing.T) {
	var pickerCalls, proxyCalls int
	directErr := context.DeadlineExceeded
	selections := []NodeSelection{
		{Hash: node.HashFromRawOptions([]byte(`{"id":"retry-node-a"}`))},
		{Hash: node.HashFromRawOptions([]byte(`{"id":"retry-node-b"}`))},
	}

	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, _ string) ([]byte, error) {
			return nil, directErr
		}),
		NodePicker: func(_ context.Context, _ string, attempted []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			if len(attempted) >= len(selections) {
				return NodeSelection{}, errors.New("no more proxy candidates")
			}
			return selections[len(attempted)], nil
		},
		ProxyFetch: func(_ context.Context, _ NodeSelection, _ string) ([]byte, error) {
			proxyCalls++
			return nil, errors.New("proxy failed")
		},
	}

	_, err := r.Download(context.Background(), "https://example.com")
	if !errors.Is(err, directErr) {
		t.Fatalf("expected original direct error, got %v", err)
	}
	if pickerCalls != 3 {
		t.Fatalf("expected one final picker exhaustion check, got %d picker attempts", pickerCalls)
	}
	if proxyCalls != 2 {
		t.Fatalf("expected 2 proxy fetch attempts, got %d", proxyCalls)
	}
}

func TestRetryDownloader_DoesNotRepeatFailedSelection(t *testing.T) {
	selection := NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"single-retry-node"}`))}
	var pickerCalls, proxyCalls int

	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, _ string) ([]byte, error) {
			return nil, &HTTPStatusError{StatusCode: 503, URL: "https://example.com"}
		}),
		NodePicker: func(_ context.Context, _ string, attempted []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			if len(attempted) != 0 {
				return NodeSelection{}, errors.New("no available proxy candidates")
			}
			return selection, nil
		},
		ProxyFetch: func(_ context.Context, _ NodeSelection, _ string) ([]byte, error) {
			proxyCalls++
			return nil, errors.New("proxy failed")
		},
	}

	_, err := r.Download(context.Background(), "https://example.com")
	if err == nil {
		t.Fatal("expected failed download")
	}
	if pickerCalls != 2 || proxyCalls != 1 {
		t.Fatalf("failed selection was retried: picker=%d proxy=%d", pickerCalls, proxyCalls)
	}
}

func TestRetryDownloader_UsesNextSelectionAfterFirstFailure(t *testing.T) {
	first := NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"next-node-a"}`))}
	second := NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"next-node-b"}`))}
	var pickerCalls, proxyCalls int

	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, _ string) ([]byte, error) {
			return nil, &HTTPStatusError{StatusCode: 503, URL: "https://example.com"}
		}),
		NodePicker: func(_ context.Context, _ string, attempted []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			switch len(attempted) {
			case 0:
				return first, nil
			case 1:
				if attempted[0].Hash != first.Hash {
					t.Fatalf("picker received unexpected first attempt: %v", attempted)
				}
				return second, nil
			default:
				return NodeSelection{}, errors.New("no more proxy candidates")
			}
		},
		ProxyFetch: func(_ context.Context, selection NodeSelection, _ string) ([]byte, error) {
			proxyCalls++
			if selection.Hash == first.Hash {
				return nil, errors.New("first proxy failed")
			}
			if selection.Hash == second.Hash {
				return []byte("via-second"), nil
			}
			return nil, errors.New("unexpected proxy candidate")
		},
	}

	body, err := r.Download(context.Background(), "https://example.com")
	if err != nil {
		t.Fatalf("expected second proxy candidate to succeed, got %v", err)
	}
	if string(body) != "via-second" {
		t.Fatalf("unexpected body %q", body)
	}
	if pickerCalls != 2 || proxyCalls != 2 {
		t.Fatalf("expected two distinct proxy attempts, picker=%d proxy=%d", pickerCalls, proxyCalls)
	}
}

func TestRetryDownloader_AttemptTimeoutLeavesCallerBudgetForProxy(t *testing.T) {
	var pickerCalls, proxyCalls int

	r := &RetryDownloader{
		Direct: downloaderFunc(func(ctx context.Context, _ string) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}),
		AttemptTimeoutCap: 100 * time.Millisecond,
		NodePicker: func(_ context.Context, _ string, _ []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			return NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"retry-node-deadline"}`))}, nil
		},
		ProxyFetch: func(ctx context.Context, _ NodeSelection, _ string) ([]byte, error) {
			proxyCalls++
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return []byte("via-proxy"), nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	body, err := r.Download(ctx, "https://example.com")
	if err != nil {
		t.Fatalf("expected remaining caller budget to permit proxy, got %v", err)
	}
	if string(body) != "via-proxy" {
		t.Fatalf("body = %q, want proxy response", body)
	}
	if pickerCalls != 1 || proxyCalls != 1 {
		t.Fatalf("expected one proxy retry, got picker=%d proxy=%d", pickerCalls, proxyCalls)
	}
}

func TestRetryDownloader_AttemptTimeoutCapStillApplies(t *testing.T) {
	var pickerCalls, proxyCalls int

	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, _ string) ([]byte, error) {
			return nil, context.DeadlineExceeded
		}),
		AttemptTimeoutCap: 20 * time.Millisecond,
		NodePicker: func(_ context.Context, _ string, attempted []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			if len(attempted) == 0 {
				return NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"retry-node-attempt-timeout-a"}`))}, nil
			}
			return NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"retry-node-attempt-timeout-b"}`))}, nil
		},
		ProxyFetch: func(ctx context.Context, _ NodeSelection, _ string) ([]byte, error) {
			proxyCalls++
			if _, ok := ctx.Deadline(); !ok {
				return nil, errors.New("missing per-attempt deadline")
			}
			if proxyCalls == 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return []byte("via-proxy"), nil
		},
	}

	body, err := r.Download(context.Background(), "https://example.com")
	if err != nil {
		t.Fatalf("expected proxy retry success, got %v", err)
	}
	if string(body) != "via-proxy" {
		t.Fatalf("unexpected body %q", string(body))
	}
	if pickerCalls != 2 || proxyCalls != 2 {
		t.Fatalf("expected two timed attempts, got picker=%d proxy=%d", pickerCalls, proxyCalls)
	}
}

func TestRetryDownloader_AttemptCapAllowsDelayedRetryableDirectResponse(t *testing.T) {
	const (
		totalBudget = 120 * time.Millisecond
		attemptCap  = 60 * time.Millisecond
	)

	selection := NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"delayed-status-node"}`))}
	directEntered := make(chan time.Duration, 1)
	releaseDirect := make(chan struct{})
	var directReturnedStatus atomic.Bool

	r := &RetryDownloader{
		Direct: downloaderFunc(func(ctx context.Context, _ string) ([]byte, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				return nil, errors.New("direct attempt has no deadline")
			}
			remaining := time.Until(deadline)
			directEntered <- remaining
			if remaining < 45*time.Millisecond {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			<-releaseDirect
			directReturnedStatus.Store(true)
			return nil, &HTTPStatusError{StatusCode: 500, URL: "https://example.com"}
		}),
		TotalTimeout:      totalBudget,
		AttemptTimeoutCap: attemptCap,
		MaxProxyAttempts:  3,
		NodePicker: func(context.Context, string, []NodeSelection) (NodeSelection, error) {
			if !directReturnedStatus.Load() {
				return NodeSelection{}, errors.New("direct response did not reach retryable status")
			}
			return selection, nil
		},
		ProxyFetch: func(context.Context, NodeSelection, string) ([]byte, error) {
			return []byte("proxy-after-delayed-status"), nil
		},
	}

	result := make(chan struct {
		body []byte
		err  error
	}, 1)
	go func() {
		body, err := r.Download(context.Background(), "https://example.com")
		result <- struct {
			body []byte
			err  error
		}{body: body, err: err}
	}()

	select {
	case remaining := <-directEntered:
		if remaining < 45*time.Millisecond {
			select {
			case got := <-result:
				t.Fatalf("direct budget was pre-divided before retryable response: err=%v body=%q", got.err, got.body)
			case <-time.After(time.Second):
				t.Fatal("old direct attempt did not finish within watchdog")
			}
		}
		close(releaseDirect)
	case <-time.After(time.Second):
		t.Fatal("direct attempt did not start")
	}

	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("download failed after delayed retryable response: %v", got.err)
		}
		if string(got.body) != "proxy-after-delayed-status" {
			t.Fatalf("body = %q, want proxy-after-delayed-status", got.body)
		}
	case <-time.After(time.Second):
		t.Fatal("download did not finish within watchdog")
	}
}

func TestRetryDownloader_TotalBudgetReachesThirdDistinctProxyAfterDirectFailure(t *testing.T) {
	selections := []NodeSelection{
		{Hash: node.HashFromRawOptions([]byte(`{"id":"budget-node-a"}`))},
		{Hash: node.HashFromRawOptions([]byte(`{"id":"budget-node-b"}`))},
		{Hash: node.HashFromRawOptions([]byte(`{"id":"budget-node-c"}`))},
	}
	seen := make([]node.Hash, 0, len(selections))
	var events []AttemptEvent

	r := &RetryDownloader{
		Direct: downloaderFunc(func(ctx context.Context, _ string) ([]byte, error) {
			if _, ok := ctx.Deadline(); !ok {
				return nil, errors.New("direct attempt has no request budget")
			}
			return nil, context.DeadlineExceeded
		}),
		TotalTimeout:     150 * time.Millisecond,
		MaxProxyAttempts: 3,
		PlatformID:       "default-platform",
		AttemptObserver: func(event AttemptEvent) {
			events = append(events, event)
		},
		NodePicker: func(_ context.Context, _ string, attempted []NodeSelection) (NodeSelection, error) {
			if len(attempted) >= len(selections) {
				return NodeSelection{}, errors.New("proxy candidates exhausted")
			}
			return selections[len(attempted)], nil
		},
		ProxyFetch: func(ctx context.Context, selection NodeSelection, _ string) ([]byte, error) {
			seen = append(seen, selection.Hash)
			if selection.Hash == selections[2].Hash {
				return []byte("third-proxy-success"), nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	started := time.Now()
	body, err := r.Download(context.Background(), "https://user:secret@example.test/sub?token=secret")
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}
	if string(body) != "third-proxy-success" {
		t.Fatalf("body = %q, want third proxy response", body)
	}
	if len(seen) != 3 {
		t.Fatalf("proxy attempts = %d, want 3", len(seen))
	}
	for i := range seen {
		for j := i + 1; j < len(seen); j++ {
			if seen[i] == seen[j] {
				t.Fatalf("proxy candidate %d was repeated", i)
			}
		}
	}
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond+100*time.Millisecond {
		t.Fatalf("download exceeded request budget: %v", elapsed)
	}
	if len(events) == 0 {
		t.Fatal("attempt observer received no events")
	}
	for _, event := range events {
		if event.RequestID == 0 || event.PlatformID == "" || event.Attempt < 0 || event.Kind == "" || event.Phase == "" || event.Result == "" {
			t.Fatalf("incomplete attempt event: %+v", event)
		}
		if event.NodeID == "" && event.Kind == AttemptKindProxy {
			t.Fatalf("proxy event has no stable node id: %+v", event)
		}
	}
}

func TestRetryDownloader_AllProxyCandidatesFailWithinOneRequestBudget(t *testing.T) {
	selections := []NodeSelection{
		{Hash: node.HashFromRawOptions([]byte(`{"id":"bounded-node-a"}`))},
		{Hash: node.HashFromRawOptions([]byte(`{"id":"bounded-node-b"}`))},
		{Hash: node.HashFromRawOptions([]byte(`{"id":"bounded-node-c"}`))},
	}
	var seen []node.Hash
	var pickerCalls int
	directErr := context.DeadlineExceeded

	r := &RetryDownloader{
		Direct: downloaderFunc(func(ctx context.Context, _ string) ([]byte, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatalf("direct attempt did not receive request deadline")
			}
			return nil, directErr
		}),
		TotalTimeout:     120 * time.Millisecond,
		MaxProxyAttempts: 3,
		NodePicker: func(_ context.Context, _ string, attempted []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			if len(attempted) >= len(selections) {
				return NodeSelection{}, errors.New("proxy candidates exhausted")
			}
			return selections[len(attempted)], nil
		},
		ProxyFetch: func(ctx context.Context, selection NodeSelection, _ string) ([]byte, error) {
			seen = append(seen, selection.Hash)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	started := time.Now()
	_, err := r.Download(context.Background(), "https://example.test/sub")
	if !errors.Is(err, directErr) {
		t.Fatalf("error = %v, want original direct error", err)
	}
	if len(seen) != len(selections) {
		t.Fatalf("proxy attempts = %d, want %d", len(seen), len(selections))
	}
	if pickerCalls != len(selections) {
		t.Fatalf("picker calls = %d, want %d", pickerCalls, len(selections))
	}
	if elapsed := time.Since(started); elapsed > 120*time.Millisecond+100*time.Millisecond {
		t.Fatalf("all-failed download exceeded request budget: %v", elapsed)
	}
}

func TestRetryDownloader_CancelInterruptsProxyAttempt(t *testing.T) {
	selection := NodeSelection{Hash: node.HashFromRawOptions([]byte(`{"id":"cancel-proxy-node"}`))}
	proxyEntered := make(chan struct{})
	var pickerCalls int
	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, _ string) ([]byte, error) {
			return nil, &HTTPStatusError{StatusCode: 503, URL: "https://example.test/sub"}
		}),
		TotalTimeout:     time.Second,
		MaxProxyAttempts: 4,
		NodePicker: func(_ context.Context, _ string, attempted []NodeSelection) (NodeSelection, error) {
			pickerCalls++
			if len(attempted) != 0 {
				return NodeSelection{}, errors.New("unexpected second picker call")
			}
			return selection, nil
		},
		ProxyFetch: func(ctx context.Context, _ NodeSelection, _ string) ([]byte, error) {
			close(proxyEntered)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := r.Download(ctx, "https://example.test/sub")
		result <- err
	}()
	select {
	case <-proxyEntered:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("proxy attempt did not start")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("download error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("proxy attempt did not stop after cancellation")
	}
	if pickerCalls != 1 {
		t.Fatalf("picker calls = %d, want 1", pickerCalls)
	}
}
