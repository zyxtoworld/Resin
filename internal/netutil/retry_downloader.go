package netutil

import (
	"context"
	"errors"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

// NodeSelection carries the exact pool entry that passed a picker’s filters.
// The entry is an identity token, not a resource lease; the fetcher must
// compare it with a fresh pool lookup before loading any outbound.
type NodeSelection struct {
	Hash  node.Hash
	Entry *node.NodeEntry
}

// NodePicker selects one candidate that is not in attempted. The attempted
// slice belongs to the current download and is not reused across requests;
// the picker must apply it before returning a candidate.
type NodePicker func(ctx context.Context, target string, attempted []NodeSelection) (NodeSelection, error)

const maxProxyAttempts = 2

// RetryDownloader decorates a Downloader with proxy retry logic.
type RetryDownloader struct {
	Direct Downloader
	// ProxyAttemptTimeout caps each proxy retry attempt duration.
	// If <= 0, it falls back to DirectDownloader's dynamic timeout when available,
	// otherwise 30s.
	ProxyAttemptTimeout time.Duration
	NodePicker          NodePicker
	ProxyFetch          func(ctx context.Context, selection NodeSelection, url string) ([]byte, error)
}

// Download attempts direct download first, then falls back to proxy retries.
func (r *RetryDownloader) Download(ctx context.Context, url string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	body, err := r.Direct.Download(ctx, url)
	if err == nil {
		return body, nil
	}

	if !shouldRetryViaProxy(err) {
		return nil, err
	}

	if r.NodePicker == nil || r.ProxyFetch == nil {
		return nil, err
	}

	// Respect caller cancellation/deadline: don't extend lifecycle beyond caller ctx.
	if ctx.Err() != nil {
		return nil, err
	}

	attemptTimeout := r.proxyAttemptTimeout()

	attempted := make([]NodeSelection, 0, maxProxyAttempts)
	for attempt := 0; attempt < maxProxyAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, err
		}

		pickerAttempted := append([]NodeSelection(nil), attempted...)
		selection, pickErr := r.NodePicker(ctx, url, pickerAttempted)
		if pickErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if errors.Is(pickErr, context.Canceled) || errors.Is(pickErr, context.DeadlineExceeded) {
				return nil, pickErr
			}
			continue
		}
		if selectionAlreadyAttempted(selection, attempted) {
			// A picker that ignores the attempted set cannot provide a bounded
			// retry sequence. Fail closed instead of looping on one node.
			break
		}
		attempted = append(attempted, selection)

		attemptCtx := ctx
		cancel := func() {}
		if attemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, attemptTimeout)
		}
		body, fetchErr := r.ProxyFetch(attemptCtx, selection, url)
		cancel()
		if fetchErr == nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return body, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	return nil, err
}

func selectionAlreadyAttempted(selection NodeSelection, attempted []NodeSelection) bool {
	for _, previous := range attempted {
		if selection.Hash != node.Zero && selection.Hash == previous.Hash {
			return true
		}
		if selection.Entry != nil && selection.Entry == previous.Entry {
			return true
		}
	}
	return false
}

func shouldRetryViaProxy(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}

	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		return shouldRetryHTTPStatusCode(statusErr.StatusCode)
	}

	var nonRetryable *NonRetryableError
	return !errors.As(err, &nonRetryable)
}

func shouldRetryHTTPStatusCode(statusCode int) bool {
	switch statusCode {
	case 403, 429, 500, 502, 503, 504:
		return true
	default:
		return false
	}
}

func (r *RetryDownloader) proxyAttemptTimeout() time.Duration {
	if r.ProxyAttemptTimeout > 0 {
		return r.ProxyAttemptTimeout
	}
	if direct, ok := r.Direct.(*DirectDownloader); ok && direct != nil {
		timeout := direct.currentTimeout()
		if timeout > 0 {
			return timeout
		}
	}
	return 30 * time.Second
}
