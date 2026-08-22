package topology

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// RefreshStage is a finite, secret-free phase of one subscription refresh.
type RefreshStage string

const (
	RefreshStageStart                       RefreshStage = "start"
	RefreshStageFetchStart                  RefreshStage = "fetch_start"
	RefreshStageFetchEnd                    RefreshStage = "fetch_end"
	RefreshStageParseStart                  RefreshStage = "parse_start"
	RefreshStageParseEnd                    RefreshStage = "parse_end"
	RefreshStageApplyStart                  RefreshStage = "apply_start"
	RefreshStageApplyEnd                    RefreshStage = "apply_end"
	RefreshStageRuntimePreparationScheduled RefreshStage = "runtime_preparation_scheduled"
	RefreshStageRuntimePreparationStart     RefreshStage = "runtime_preparation_start"
	RefreshStageRuntimePreparationEnd       RefreshStage = "runtime_preparation_end"
	RefreshStageFinished                    RefreshStage = "finished"
)

// RefreshEvent is a bounded observation for one subscription refresh. It
// deliberately contains no source URL, response body, node options, or raw
// error text.
type RefreshEvent struct {
	CorrelationID string
	// ParentCorrelationID links an asynchronous runtime-preparation lifecycle
	// back to the committed refresh without making preparation events trailing
	// stages of that refresh's commit trace.
	ParentCorrelationID    string
	SubscriptionID         string
	AttemptSeq             int64
	Stage                  RefreshStage
	SourceType             string
	Elapsed                time.Duration
	Result                 string
	CallerDeadlineSet      bool
	CallerDeadline         time.Time
	FetchTotalTimeout      time.Duration
	FetchAttemptTimeoutCap time.Duration
	PreparedNodeCount      int
	// RuntimePreparation* are fixed-size outcome counters. They are populated
	// only on the independent runtime-preparation correlation, so a terminal
	// "error" can be distinguished from queue drops, coalescing, and stale
	// generation invalidation without claiming every node became ready.
	RuntimePreparationTotal     int
	RuntimePreparationCompleted int
	RuntimePreparationErrors    int
	RuntimePreparationStale     int
	RuntimePreparationDropped   int
	RuntimePreparationCoalesced int
}

// RuntimePreparationSummary is a bounded result summary for one preparation
// batch. It contains counts only; per-node details remain in node state/logs.
type RuntimePreparationSummary struct {
	Total     int
	Completed int
	Errors    int
	Stale     int
	Dropped   int
	Coalesced int
}

// RefreshObserver receives one safe event at a time. Implementations must be
// concurrency-safe because background refreshes may run in parallel.
type RefreshObserver func(RefreshEvent)

type refreshTrace struct {
	correlationID          string
	parentCorrelationID    string
	subscriptionID         string
	attemptSeq             int64
	started                time.Time
	callerDeadlineSet      bool
	callerDeadline         time.Time
	fetchTotalTimeout      time.Duration
	fetchAttemptTimeoutCap time.Duration
	sourceType             string
	observe                RefreshObserver
}

func newRefreshTrace(
	ctx context.Context,
	subscriptionID string,
	attemptSeq int64,
	fetchTotalTimeout time.Duration,
	fetchAttemptTimeoutCap time.Duration,
	observe RefreshObserver,
) *refreshTrace {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline, hasDeadline := ctx.Deadline()
	return &refreshTrace{
		correlationID:          uuid.NewString(),
		subscriptionID:         subscriptionID,
		attemptSeq:             attemptSeq,
		started:                time.Now(),
		callerDeadlineSet:      hasDeadline,
		callerDeadline:         deadline,
		fetchTotalTimeout:      fetchTotalTimeout,
		fetchAttemptTimeoutCap: fetchAttemptTimeoutCap,
		observe:                observe,
	}
}

func newRuntimePreparationTrace(parent *refreshTrace) *refreshTrace {
	if parent == nil {
		return newRefreshTrace(context.Background(), "", 0, 0, 0, nil)
	}
	trace := newRefreshTrace(
		context.Background(),
		parent.subscriptionID,
		parent.attemptSeq,
		parent.fetchTotalTimeout,
		parent.fetchAttemptTimeoutCap,
		parent.observe,
	)
	trace.parentCorrelationID = parent.correlationID
	trace.sourceType = parent.sourceType
	return trace
}

func (t *refreshTrace) emit(stage RefreshStage, result string, preparedNodeCount int) {
	t.emitWithPreparationSummary(stage, result, preparedNodeCount, RuntimePreparationSummary{})
}

func (t *refreshTrace) emitWithPreparationSummary(
	stage RefreshStage,
	result string,
	preparedNodeCount int,
	summary RuntimePreparationSummary,
) {
	if t == nil || t.observe == nil {
		return
	}
	t.observe(RefreshEvent{
		CorrelationID:               t.correlationID,
		ParentCorrelationID:         t.parentCorrelationID,
		SubscriptionID:              t.subscriptionID,
		AttemptSeq:                  t.attemptSeq,
		Stage:                       stage,
		SourceType:                  t.sourceType,
		Elapsed:                     time.Since(t.started),
		Result:                      result,
		CallerDeadlineSet:           t.callerDeadlineSet,
		CallerDeadline:              t.callerDeadline,
		FetchTotalTimeout:           t.fetchTotalTimeout,
		FetchAttemptTimeoutCap:      t.fetchAttemptTimeoutCap,
		PreparedNodeCount:           preparedNodeCount,
		RuntimePreparationTotal:     summary.Total,
		RuntimePreparationCompleted: summary.Completed,
		RuntimePreparationErrors:    summary.Errors,
		RuntimePreparationStale:     summary.Stale,
		RuntimePreparationDropped:   summary.Dropped,
		RuntimePreparationCoalesced: summary.Coalesced,
	})
}

func refreshResult(ctx context.Context, err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if ctx != nil {
		switch {
		case errors.Is(ctx.Err(), context.Canceled):
			return "canceled"
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			return "timeout"
		}
	}
	return "error"
}
