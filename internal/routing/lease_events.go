package routing

import (
	"sort"
	"sync"
)

// ticketedLeaseEvent records the ticket assigned at the state mutation's
// linearization point. Events are delivered in ticket order after the
// lifecycle lock has been released.
type ticketedLeaseEvent struct {
	seq   uint64
	event LeaseEvent
}

// leaseEventBatch owns only the events produced by one Router operation. It
// never owns the Router's sequence mutex while a callback runs, so unrelated
// state mutations remain concurrent.
//
// A callback may re-enter Router read APIs. It must not call a Router mutation
// that can emit another lease event: such a call would wait for its ticket
// while the current callback is still running and deadlock. The production
// callback is persistence/metrics wiring and does not do that.
type leaseEventBatch struct {
	router *Router
	mu     sync.Mutex
	events []ticketedLeaseEvent
}

type leaseEventMutation struct {
	batch     *leaseEventBatch
	events    []LeaseEvent
	committed bool
}

type leaseEventSink interface {
	add(LeaseEvent)
}

func (r *Router) newLeaseEventBatch() *leaseEventBatch {
	if r == nil {
		return nil
	}
	return &leaseEventBatch{router: r}
}

func (b *leaseEventBatch) newMutation() *leaseEventMutation {
	if b == nil || b.router == nil {
		return nil
	}
	return &leaseEventMutation{batch: b}
}

func (m *leaseEventMutation) add(event LeaseEvent) {
	if m == nil || m.committed {
		return
	}
	m.events = append(m.events, event)
}

// commit must be called inside the state mutation's Compute callback, after
// all events for that mutation have been collected. It reserves one
// contiguous ticket range under the short sequence lock; callbacks still run
// only later, after lifecycleMu is released.
func (m *leaseEventMutation) commit() {
	if m == nil || m.committed || m.batch == nil || len(m.events) == 0 {
		if m != nil {
			m.committed = true
		}
		return
	}
	m.committed = true
	b := m.batch
	b.router.leaseEventsMu.Lock()
	first := b.router.nextLeaseEventSeq + 1
	b.router.nextLeaseEventSeq += uint64(len(m.events))
	ticketed := make([]ticketedLeaseEvent, len(m.events))
	for i, event := range m.events {
		ticketed[i] = ticketedLeaseEvent{seq: first + uint64(i), event: event}
	}
	b.router.leaseEventsMu.Unlock()

	b.mu.Lock()
	b.events = append(b.events, ticketed...)
	b.mu.Unlock()
	if hook := b.router.afterLeaseEventBatchTicketHook; hook != nil {
		hook()
	}
}

// finish must be called after the caller releases lifecycleMu. It delivers
// this batch synchronously in ticket order. Each callback runs without either
// the lifecycle lock or the sequence lock held.
func (b *leaseEventBatch) finish() {
	if b == nil || b.router == nil {
		return
	}
	b.mu.Lock()
	events := append([]ticketedLeaseEvent(nil), b.events...)
	b.mu.Unlock()
	sort.Slice(events, func(i, j int) bool { return events[i].seq < events[j].seq })

	var firstPanic any
	for _, queued := range events {
		if recovered, panicked := b.router.deliverLeaseEvent(queued); panicked && firstPanic == nil {
			firstPanic = recovered
		}
	}
	if firstPanic != nil {
		panic(firstPanic)
	}
}

func (r *Router) deliverLeaseEvent(queued ticketedLeaseEvent) (recovered any, panicked bool) {
	r.leaseEventsMu.Lock()
	for r.deliveredLeaseEventSeq+1 != queued.seq {
		r.leaseEventsCond.Wait()
	}
	r.leaseEventsMu.Unlock()

	defer func() {
		if value := recover(); value != nil {
			recovered = value
			panicked = true
		}
		r.leaseEventsMu.Lock()
		if queued.seq == r.deliveredLeaseEventSeq+1 {
			r.deliveredLeaseEventSeq = queued.seq
		}
		r.leaseEventsCond.Broadcast()
		r.leaseEventsMu.Unlock()
	}()

	if r.onLeaseEvent != nil {
		r.onLeaseEvent(queued.event)
	}
	return nil, false
}

// WaitForLeaseEvents is an explicit synchronous barrier for callers that
// need the dirty-set/metrics side effects to be observable before returning.
// It is intentionally not callable from OnLeaseEvent; callbacks are only
// allowed to re-enter Router read APIs.
func (r *Router) WaitForLeaseEvents() {
	if r == nil {
		return
	}
	r.leaseEventsMu.Lock()
	target := r.nextLeaseEventSeq
	for r.deliveredLeaseEventSeq < target {
		r.leaseEventsCond.Wait()
	}
	r.leaseEventsMu.Unlock()
}
