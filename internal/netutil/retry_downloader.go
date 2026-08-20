package netutil

import (
	"context"
	"errors"
	"sync/atomic"
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

const (
	// A resource refresh may try a small, finite set of healthy candidates. The
	// request deadline, not this cap, remains the primary budget.
	defaultMaxProxyAttempts  = 4
	maxProxyAttemptsLimit    = 8
	defaultRetryTotalTimeout = 60 * time.Second
)

// RetryDownloader decorates a Downloader with proxy retry logic.
type RetryDownloader struct {
	Direct Downloader
	// TotalTimeout is the sole request-level budget. A caller deadline still
	// wins when it is earlier. If unset, the direct downloader timeout or the
	// package default supplies the budget.
	TotalTimeout time.Duration
	// AttemptTimeoutCap optionally limits an individual direct/proxy slice. The
	// actual slice is always bounded by the remaining request budget.
	AttemptTimeoutCap time.Duration
	// MaxProxyAttempts is a finite safety cap for distinct proxy candidates.
	// Zero selects the package default; values above the hard limit are capped.
	MaxProxyAttempts int
	PlatformID       string
	AttemptObserver  AttemptObserver
	NodePicker       NodePicker
	ProxyFetch       func(ctx context.Context, selection NodeSelection, url string) ([]byte, error)
	nextRequestID    atomic.Uint64
}

// Download attempts direct download first, then falls back to proxy retries.
func (r *RetryDownloader) Download(ctx context.Context, url string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	requestCtx, cancelRequest := context.WithTimeout(ctx, r.totalTimeout())
	defer cancelRequest()

	requestID := r.nextRequestID.Add(1)
	proxyAttempts := r.maxProxyAttempts()
	directState := &attemptState{
		requestID:  requestID,
		platformID: r.PlatformID,
		attempt:    1,
		kind:       AttemptKindDirect,
		started:    time.Now(),
		observe:    r.AttemptObserver,
	}
	directSlots := proxyAttempts + 1
	if r.AttemptTimeoutCap > 0 {
		// With an explicit per-attempt cap, direct gets one capped slice;
		// proxy slots are reserved only after direct fails. Without a cap,
		// retain the historical total-budget split so a blocked direct
		// attempt cannot consume every proxy opportunity.
		directSlots = 1
	}
	directBudget := r.attemptBudget(requestCtx, directSlots)
	if directBudget <= 0 {
		return nil, requestContextError(ctx, requestCtx)
	}
	directCtx, cancelDirect := context.WithTimeout(requestCtx, directBudget)
	directCtx = withAttemptState(directCtx, directState)
	var err error
	var body []byte
	if r.Direct == nil {
		err = errors.New("direct downloader is nil")
	} else {
		body, err = r.Direct.Download(directCtx, url)
	}
	emitAttemptPhaseForState(directState, AttemptPhaseComplete, attemptResult(directCtx, err))
	cancelDirect()
	if err == nil {
		if ctxErr := requestContextError(ctx, requestCtx); ctxErr != nil {
			return nil, ctxErr
		}
		return body, nil
	}
	if ctxErr := requestContextError(ctx, requestCtx); ctxErr != nil {
		return nil, ctxErr
	}

	if !shouldRetryViaProxy(err) {
		return nil, err
	}

	if r.NodePicker == nil || r.ProxyFetch == nil {
		return nil, err
	}

	attempted := make([]NodeSelection, 0, proxyAttempts)
	for proxyAttempt := 0; proxyAttempt < proxyAttempts; proxyAttempt++ {
		if ctxErr := requestContextError(ctx, requestCtx); ctxErr != nil {
			return nil, ctxErr
		}

		pickerAttempted := append([]NodeSelection(nil), attempted...)
		selection, pickErr := r.NodePicker(requestCtx, url, pickerAttempted)
		if pickErr != nil {
			if errors.Is(pickErr, context.Canceled) || errors.Is(pickErr, context.DeadlineExceeded) {
				return nil, pickErr
			}
			break
		}
		if selectionAlreadyAttempted(selection, attempted) {
			// A picker that ignores the attempted set cannot provide a bounded
			// retry sequence. Fail closed instead of looping on one node.
			break
		}
		if selection.Hash == node.Zero {
			// A retry candidate without a stable node identity cannot be
			// bounded or correlated safely.
			break
		}
		attempted = append(attempted, selection)

		attemptState := &attemptState{
			requestID:  requestID,
			platformID: r.PlatformID,
			attempt:    proxyAttempt + 2,
			kind:       AttemptKindProxy,
			nodeID:     selection.Hash.Hex(),
			started:    time.Now(),
			observe:    r.AttemptObserver,
		}
		attemptBudget := r.attemptBudget(requestCtx, proxyAttempts-proxyAttempt)
		if attemptBudget <= 0 {
			break
		}
		attemptCtx, cancel := context.WithTimeout(requestCtx, attemptBudget)
		attemptCtx = withAttemptState(attemptCtx, attemptState)
		body, fetchErr := r.ProxyFetch(attemptCtx, selection, url)
		emitAttemptPhaseForState(attemptState, AttemptPhaseComplete, attemptResult(attemptCtx, fetchErr))
		cancel()
		if fetchErr == nil {
			if ctxErr := requestContextError(ctx, requestCtx); ctxErr != nil {
				return nil, ctxErr
			}
			return body, nil
		}
		if ctxErr := requestContextError(ctx, requestCtx); ctxErr != nil {
			return nil, ctxErr
		}
	}

	if ctxErr := requestContextError(ctx, requestCtx); ctxErr != nil {
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

func (r *RetryDownloader) totalTimeout() time.Duration {
	if r.TotalTimeout > 0 {
		return r.TotalTimeout
	}
	if direct, ok := r.Direct.(*DirectDownloader); ok && direct != nil && direct.TimeoutFn != nil {
		if timeout := direct.currentTimeout(); timeout > 0 {
			return timeout
		}
	}
	return defaultRetryTotalTimeout
}

func (r *RetryDownloader) maxProxyAttempts() int {
	attempts := r.MaxProxyAttempts
	if attempts <= 0 {
		attempts = defaultMaxProxyAttempts
	}
	if attempts > maxProxyAttemptsLimit {
		return maxProxyAttemptsLimit
	}
	return attempts
}

func (r *RetryDownloader) attemptBudget(ctx context.Context, slots int) time.Duration {
	if slots <= 0 {
		return 0
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return r.AttemptTimeoutCap
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0
	}
	budget := remaining / time.Duration(slots)
	if r.AttemptTimeoutCap > 0 && budget > r.AttemptTimeoutCap {
		budget = r.AttemptTimeoutCap
	}
	return budget
}

func requestContextError(caller, request context.Context) error {
	if err := caller.Err(); err != nil {
		return err
	}
	return request.Err()
}
