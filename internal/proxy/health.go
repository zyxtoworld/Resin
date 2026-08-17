package proxy

import (
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// HealthRecorder abstracts request health feedback reporting.
// The entry captured during routing is part of every writeback contract: a
// request may outlive removal and recreation of the same hash.
type HealthRecorder interface {
	RecordResultForEntry(hash node.Hash, expected *node.NodeEntry, success bool) bool
	RecordLatencyForEntry(hash node.Hash, expected *node.NodeEntry, rawTarget string, latency *time.Duration) bool
	RecordPassiveResultForEntry(platformID string, hash node.Hash, expected *node.NodeEntry, success bool) bool
}

// routePassiveHealthRecorder consumes the immutable passive-breaker policy
// captured by routing. Production's topology pool implements it; test and
// integration recorders fall back to the legacy platform-ID method below.
type routePassiveHealthRecorder interface {
	RecordPassiveResultForRoute(
		platformID string,
		passiveCircuitBreakerDisabled bool,
		hash node.Hash,
		expected *node.NodeEntry,
		success bool,
	) bool
}

// HealthWriteOwner admits asynchronous proxy health writebacks and provides a
// shutdown barrier for them. Admission is closed before waiting, so a write
// cannot be added after Wait returns. The owner is shared by forward,
// reverse, and SOCKS/tunnel handlers in production.
type HealthWriteOwner struct {
	target HealthRecorder

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup

	// waitStarted is package-private test coordination. It is closed before
	// Wait blocks on admitted writes and is not part of the public contract.
	waitStarted     chan struct{}
	waitStartedOnce sync.Once
}

// NewHealthWriteOwner creates a lifecycle owner around a health recorder.
func NewHealthWriteOwner(target HealthRecorder) *HealthWriteOwner {
	return &HealthWriteOwner{
		target:      target,
		waitStarted: make(chan struct{}),
	}
}

func (o *HealthWriteOwner) RecordResultForEntry(
	hash node.Hash,
	expected *node.NodeEntry,
	success bool,
) bool {
	if o == nil || o.target == nil {
		return false
	}
	var applied bool
	o.runSync(func() {
		applied = o.target.RecordResultForEntry(hash, expected, success)
	})
	return applied
}

func (o *HealthWriteOwner) RecordLatencyForEntry(
	hash node.Hash,
	expected *node.NodeEntry,
	rawTarget string,
	latency *time.Duration,
) bool {
	if o == nil || o.target == nil {
		return false
	}
	var applied bool
	o.runSync(func() {
		applied = o.target.RecordLatencyForEntry(hash, expected, rawTarget, latency)
	})
	return applied
}

func (o *HealthWriteOwner) RecordPassiveResultForEntry(
	platformID string,
	hash node.Hash,
	expected *node.NodeEntry,
	success bool,
) bool {
	if o == nil || o.target == nil {
		return false
	}
	var applied bool
	o.runSync(func() {
		applied = o.target.RecordPassiveResultForEntry(platformID, hash, expected, success)
	})
	return applied
}

func (o *HealthWriteOwner) RecordPassiveResultForRoute(
	platformID string,
	passiveCircuitBreakerDisabled bool,
	hash node.Hash,
	expected *node.NodeEntry,
	success bool,
) bool {
	if o == nil || o.target == nil {
		return false
	}
	var applied bool
	o.runSync(func() {
		if recorder, ok := o.target.(routePassiveHealthRecorder); ok {
			applied = recorder.RecordPassiveResultForRoute(
				platformID,
				passiveCircuitBreakerDisabled,
				hash,
				expected,
				success,
			)
			return
		}
		applied = o.target.RecordPassiveResultForEntry(platformID, hash, expected, success)
	})
	return applied
}

// runSync admits one synchronous write unless shutdown has closed the owner.
// It uses the same admission counter as asynchronous writes, so CloseAndWait
// also covers callers that use the HealthRecorder interface directly.
func (o *HealthWriteOwner) runSync(fn func()) bool {
	if o == nil || fn == nil {
		return false
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return false
	}
	o.wg.Add(1)
	o.mu.Unlock()
	defer o.wg.Done()
	fn()
	return true
}

// submit admits one asynchronous write unless shutdown has closed the owner.
// The WaitGroup Add occurs under the same mutex as closed, which makes
// CloseAndWait safe against concurrent submissions.
func (o *HealthWriteOwner) submit(fn func()) {
	if o == nil || fn == nil {
		return
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.wg.Add(1)
	o.mu.Unlock()
	go func() {
		defer o.wg.Done()
		fn()
	}()
}

// CloseAndWait closes admission and waits for every already-admitted write.
// Calls after the first one are idempotent and wait for the same in-flight
// work.
func (o *HealthWriteOwner) CloseAndWait() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
	o.waitStartedOnce.Do(func() { close(o.waitStarted) })
	o.wg.Wait()
}

type healthWriteSubmitter interface {
	submit(func())
}

func submitHealthWrite(health HealthRecorder, fn func()) {
	if health == nil || fn == nil {
		return
	}
	if submitter, ok := health.(healthWriteSubmitter); ok {
		submitter.submit(fn)
		return
	}
	// Non-production test/integration recorders retain the historical async
	// behavior; production wiring uses HealthWriteOwner above.
	go fn()
}

// directHealthRecorder unwraps the owner only inside an already admitted
// callback. Calling the owner's public HealthRecorder methods there would
// perform a second admission and could be rejected after shutdown has closed
// new work, even though this callback was admitted before shutdown.
func directHealthRecorder(health HealthRecorder) HealthRecorder {
	if owner, ok := health.(*HealthWriteOwner); ok && owner != nil {
		return owner.target
	}
	return health
}

func recordPassiveResultAsync(health HealthRecorder, route routing.RouteResult, expected *node.NodeEntry, success bool) {
	if health == nil {
		return
	}
	target := directHealthRecorder(health)
	if recorder, ok := target.(routePassiveHealthRecorder); ok && route.PlatformID != "" {
		submitHealthWrite(health, func() {
			recorder.RecordPassiveResultForRoute(
				route.PlatformID,
				route.PassiveCircuitBreakerDisabled,
				route.NodeHash,
				expected,
				success,
			)
		})
		return
	}
	submitHealthWrite(health, func() {
		target.RecordPassiveResultForEntry(route.PlatformID, route.NodeHash, expected, success)
	})
}

func recordLatencyAsync(health HealthRecorder, hash node.Hash, expected *node.NodeEntry, rawTarget string, latency *time.Duration) {
	if health == nil {
		return
	}
	target := directHealthRecorder(health)
	submitHealthWrite(health, func() {
		target.RecordLatencyForEntry(hash, expected, rawTarget, latency)
	})
}
