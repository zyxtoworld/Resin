package routing

// PlatformRoutingState encapsulates the routing state for a single platform.
// This struct is stored in the Router's state map.
type PlatformRoutingState struct {
	Leases            *LeaseTable
	IPLoadStats       *IPLoadStats
	ResponseCooldowns *ResponseCooldowns
	generation        uint64
}

// NewPlatformRoutingState creates a new state instance.
func NewPlatformRoutingState() *PlatformRoutingState {
	return newPlatformRoutingState(0)
}

func newPlatformRoutingState(generation uint64) *PlatformRoutingState {
	stats := NewIPLoadStats()
	return &PlatformRoutingState{
		Leases:            NewLeaseTable(stats),
		IPLoadStats:       stats,
		ResponseCooldowns: NewResponseCooldowns(),
		generation:        generation,
	}
}
