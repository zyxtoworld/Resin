package netutil

import (
	"context"
	"errors"
	"strconv"
	"time"
)

// AttemptKind identifies the transport path used by one resource attempt.
// These values are deliberately finite and contain no request data.
type AttemptKind string

const (
	AttemptKindDirect AttemptKind = "direct"
	AttemptKindProxy  AttemptKind = "proxy"
)

// AttemptPhase identifies the last completed transport phase observed for an
// attempt. It is safe to emit because it contains no URL, body, or error text.
type AttemptPhase string

const (
	AttemptPhaseDial     AttemptPhase = "dial"
	AttemptPhaseTLS      AttemptPhase = "tls"
	AttemptPhaseHeaders  AttemptPhase = "headers"
	AttemptPhaseBody     AttemptPhase = "body"
	AttemptPhaseComplete AttemptPhase = "complete"
)

// AttemptEvent is the bounded, non-secret observation emitted by a resource
// download. NodeID is a stable node hash, never an address or raw options.
type AttemptEvent struct {
	RequestID  uint64
	PlatformID string
	Attempt    int
	Kind       AttemptKind
	NodeID     string
	Phase      AttemptPhase
	Elapsed    time.Duration
	Result     string
}

// AttemptObserver receives one safe event at a time. Observers must not
// retain mutable request state or block the download path indefinitely.
type AttemptObserver func(AttemptEvent)

type attemptState struct {
	requestID  uint64
	platformID string
	attempt    int
	kind       AttemptKind
	nodeID     string
	started    time.Time
	observe    AttemptObserver
}

type attemptStateContextKey struct{}

func withAttemptState(ctx context.Context, state *attemptState) context.Context {
	return context.WithValue(ctx, attemptStateContextKey{}, state)
}

func attemptStateFromContext(ctx context.Context) *attemptState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(attemptStateContextKey{}).(*attemptState)
	return state
}

func emitAttemptPhase(ctx context.Context, phase AttemptPhase, result string) {
	emitAttemptPhaseForState(attemptStateFromContext(ctx), phase, result)
}

func emitAttemptPhaseForState(state *attemptState, phase AttemptPhase, result string) {
	if state == nil || state.observe == nil {
		return
	}
	state.observe(AttemptEvent{
		RequestID:  state.requestID,
		PlatformID: state.platformID,
		Attempt:    state.attempt,
		Kind:       state.kind,
		NodeID:     state.nodeID,
		Phase:      phase,
		Elapsed:    time.Since(state.started),
		Result:     result,
	})
}

func attemptResult(ctx context.Context, err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if ctx != nil && ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return "canceled"
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "timeout"
		}
	}
	return "error"
}

func attemptStatusResult(statusCode int) string {
	return "status_" + strconv.Itoa(statusCode)
}
