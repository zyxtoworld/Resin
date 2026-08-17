package routing

import (
	"net/netip"

	"github.com/Resinat/Resin/internal/node"
)

type LeaseEventType int

const (
	LeaseCreate LeaseEventType = iota
	LeaseTouch
	LeaseReplace
	LeaseRemove
	LeaseExpire
)

type LeaseEvent struct {
	Type        LeaseEventType
	PlatformID  string
	Account     string
	NodeHash    node.Hash
	EgressIP    netip.Addr
	CreatedAtNs int64 // set on remove/expire for lifetime calculation
}

// LeaseEventFunc is invoked synchronously after the routing lifecycle lock is
// released. A handler may re-enter Router read APIs, but must not call a
// Router mutation that emits another lease event. The caller waits for the
// handler, preserving dirty-set and metrics durability/backpressure.
type LeaseEventFunc func(event LeaseEvent)
