// Package runtimeguard provides short, linearized admission gates for
// deferred runtime work. External builders and fetchers must never hold the
// gate; only the final publication/writeback callback runs under it.
package runtimeguard

import "sync"

// TokenID is the stable comparable identity of one gate generation. It is
// intended for coalescing maps/queues; do not use *Guard pointer identity for
// that purpose because callers may capture the same generation repeatedly.
type TokenID struct {
	gate  *Gate
	epoch uint64
}

// Gate serializes lifecycle invalidation with short final commits.
type Gate struct {
	mu      sync.Mutex
	epoch   uint64
	current *Guard
}

// Guard is an immutable token captured from a Gate generation.
type Guard struct {
	gate  *Gate
	epoch uint64
}

// NewGate creates a valid gate with an initial generation.
func NewGate() *Gate {
	return &Gate{epoch: 1}
}

// Capture returns the stable token for the current generation. Capturing is a
// short operation; the returned token may be carried across external work.
func (g *Gate) Capture() *Guard {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current == nil {
		g.current = &Guard{gate: g, epoch: g.epoch}
	}
	return g.current
}

// Invalidate advances the generation. It waits for a final Commit already in
// progress, so once it returns no older commit can publish afterward.
func (g *Gate) Invalidate() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.epoch++
	g.current = nil
	g.mu.Unlock()
}

// Mutate invalidates the previous generation and applies the lifecycle state
// transition while holding the same short gate. The callback must only mutate
// in-memory lifecycle state; it must not perform external work.
func (g *Gate) Mutate(fn func()) {
	if g == nil {
		if fn != nil {
			fn()
		}
		return
	}
	g.mu.Lock()
	g.epoch++
	g.current = nil
	if fn != nil {
		fn()
	}
	g.mu.Unlock()
}

// Commit executes fn only if token is still the current generation. The gate
// remains held for the whole callback, so Invalidate cannot return between
// the generation check and the final publication/writeback.
func (g *Gate) Commit(token *Guard, fn func()) bool {
	if g == nil || token == nil || fn == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if token.gate != g || token.epoch != g.epoch || token != g.current {
		return false
	}
	fn()
	return true
}

// Allowed is a non-committing admission hint. Final side effects must use
// Commit; this method alone intentionally provides no publication guarantee.
func (g *Guard) Allowed() bool {
	if g == nil || g.gate == nil {
		return false
	}
	g.gate.mu.Lock()
	defer g.gate.mu.Unlock()
	return g == g.gate.current && g.epoch == g.gate.epoch
}

// Commit runs fn at the token's gate finalization point.
func (g *Guard) Commit(fn func()) bool {
	if g == nil {
		return false
	}
	return g.gate.Commit(g, fn)
}

// Identity returns a stable comparable key for this token generation.
func (g *Guard) Identity() TokenID {
	if g == nil {
		return TokenID{}
	}
	return TokenID{gate: g.gate, epoch: g.epoch}
}
