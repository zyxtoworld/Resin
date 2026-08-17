package netutil

import (
	"context"
	"errors"
	"strconv"
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
	if pickerCalls != 2 {
		t.Fatalf("expected 2 picker attempts, got %d", pickerCalls)
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

func TestRetryDownloader_NoRetryWhenCallerDeadlineExceeded(t *testing.T) {
	var pickerCalls, proxyCalls int

	r := &RetryDownloader{
		Direct: downloaderFunc(func(ctx context.Context, _ string) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}),
		ProxyAttemptTimeout: 100 * time.Millisecond,
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := r.Download(ctx, "https://example.com")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if pickerCalls != 0 || proxyCalls != 0 {
		t.Fatalf("expected no proxy retry after caller deadline, got picker=%d proxy=%d", pickerCalls, proxyCalls)
	}
}

func TestRetryDownloader_ProxyAttemptTimeoutStillApplies(t *testing.T) {
	var pickerCalls, proxyCalls int

	r := &RetryDownloader{
		Direct: downloaderFunc(func(_ context.Context, _ string) ([]byte, error) {
			return nil, context.DeadlineExceeded
		}),
		ProxyAttemptTimeout: 20 * time.Millisecond,
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
