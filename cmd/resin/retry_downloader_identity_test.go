package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type retryIdentityOutbound struct {
	dialCalls atomic.Int32
}

func (o *retryIdentityOutbound) Type() string           { return "retry-identity" }
func (o *retryIdentityOutbound) Tag() string            { return "retry-identity" }
func (o *retryIdentityOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (o *retryIdentityOutbound) Dependencies() []string { return nil }
func (o *retryIdentityOutbound) Close() error           { return nil }
func (o *retryIdentityOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	var lc net.ListenConfig
	return lc.ListenPacket(context.Background(), "udp", "")
}
func (o *retryIdentityOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	o.dialCalls.Add(1)
	return (&net.Dialer{}).DialContext(ctx, network, destination.String())
}

var _ adapter.Outbound = (*retryIdentityOutbound)(nil)

func TestWireRetryDownloaderRejectsSameHashReplacementBetweenPickAndFetch(t *testing.T) {
	subManager := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"retry-identity-sub",
		"Retry Identity Sub",
		"https://example.com/retry-identity-sub",
		true,
		false,
	)
	subManager.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})
	defaultPlatform := platform.NewPlatformWithTagFilter(
		platform.DefaultPlatformID,
		platform.DefaultPlatformName,
		node.TagFilter{Must: []*regexp.Regexp{regexp.MustCompile(`^Retry Identity Sub/retry$`)}},
		nil,
	)
	if err := pool.RegisterPlatform(defaultPlatform); err != nil {
		t.Fatalf("register default platform: %v", err)
	}

	raw := json.RawMessage(`{"type":"retry-identity"}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"retry"}})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old entry missing")
	}
	oldEntry.SetEgressIP(netip.MustParseAddr("203.0.113.241"))
	oldEntry.LatencyTable.Update("example.com", 25*time.Millisecond, 10*time.Minute)
	oldOutbound := &retryIdentityOutbound{}
	var oldOutboundAdapter adapter.Outbound = oldOutbound
	oldEntry.Outbound.Store(&oldOutboundAdapter)
	pool.RecordResult(hash, true)
	pool.NotifyNodeDirty(hash)
	if !defaultPlatform.View().Contains(hash) {
		t.Fatal("old entry was not published in Default view")
	}

	newOutbound := &retryIdentityOutbound{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("replacement-must-not-be-used"))
	}))
	defer server.Close()

	manager := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	app := &resinApp{topoRuntime: &topologyRuntime{pool: pool, outboundMgr: manager}}
	retryDL := &netutil.RetryDownloader{
		Direct: retryTestDownloaderFunc(func(_ context.Context, url string) ([]byte, error) {
			return nil, &netutil.HTTPStatusError{StatusCode: 503, URL: url}
		}),
	}
	app.wireRetryDownloader(retryDL)
	productionPicker := retryDL.NodePicker
	picked := make(chan struct{})
	allowFetch := make(chan struct{})
	retryDL.NodePicker = func(ctx context.Context, target string, attempted []netutil.NodeSelection) (netutil.NodeSelection, error) {
		selection, err := productionPicker(ctx, target, attempted)
		if err == nil {
			close(picked)
			<-allowFetch
		}
		return selection, err
	}

	resultCh := make(chan struct {
		body []byte
		err  error
	}, 1)
	go func() {
		body, err := retryDL.Download(context.Background(), server.URL)
		resultCh <- struct {
			body []byte
			err  error
		}{body: body, err: err}
	}()

	select {
	case <-picked:
	case <-time.After(time.Second):
		t.Fatal("production picker did not return a node")
	}

	// Remove the selected entry and recreate the same hash with a tag excluded
	// by Default. The old hash-only chain still fetches this replacement.
	pool.RemoveNodeFromSub(hash, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"excluded"}})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash replacement was not created")
	}
	newEntry.SetEgressIP(netip.MustParseAddr("203.0.113.242"))
	newEntry.LatencyTable.Update("example.com", 25*time.Millisecond, 10*time.Minute)
	var newOutboundAdapter adapter.Outbound = newOutbound
	newEntry.Outbound.Store(&newOutboundAdapter)
	pool.RecordResult(hash, true)
	pool.NotifyNodeDirty(hash)
	if defaultPlatform.View().Contains(hash) {
		t.Fatal("replacement entry unexpectedly remained in Default view")
	}
	close(allowFetch)

	select {
	case got := <-resultCh:
		if got.err == nil || len(got.body) != 0 {
			t.Fatalf("retry chain used a replacement after identity changed: body=%q err=%v", got.body, got.err)
		}
		if newOutbound.dialCalls.Load() != 0 || oldOutbound.dialCalls.Load() != 0 {
			t.Fatalf("stale/replacement outbound was used: old=%d new=%d", oldOutbound.dialCalls.Load(), newOutbound.dialCalls.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("retry download did not return")
	}
}
