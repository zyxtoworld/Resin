package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

type retryTestDownloaderFunc func(context.Context, string) ([]byte, error)

func (f retryTestDownloaderFunc) Download(ctx context.Context, url string) ([]byte, error) {
	return f(ctx, url)
}

func newRetryWiringTestRuntime(
	t *testing.T,
	geoEntered chan<- struct{},
	allowGeo <-chan struct{},
) (*topology.GlobalNodePool, *routing.Router, node.Hash) {
	t.Helper()

	subManager := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"retry-sub",
		"Retry Sub",
		"https://example.com/sub",
		true,
		false,
	)
	subManager.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subManager.Lookup,
		GeoLookup: func(netip.Addr) string {
			if geoEntered != nil {
				select {
				case geoEntered <- struct{}{}:
				default:
				}
				<-allowGeo
			}
			return "us"
		},
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})
	defaultPlatform := platform.NewPlatform(
		platform.DefaultPlatformID,
		platform.DefaultPlatformName,
		nil,
		nil,
	)
	if err := pool.RegisterPlatform(defaultPlatform); err != nil {
		t.Fatalf("register default platform: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.40","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"retry"}})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("retry node %s missing", hash.Hex())
	}
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.40"))
	entry.LatencyTable.Update("example.com", 25*time.Millisecond, 10*time.Minute)
	pool.RecordResult(hash, true)
	pool.NotifyNodeDirty(hash)
	if !defaultPlatform.View().Contains(hash) {
		t.Fatalf("retry node %s was not made routable", hash.Hex())
	}

	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return []string{"example.com"} },
		P2CWindow:   func() time.Duration { return 10 * time.Minute },
	})
	return pool, router, hash
}

func TestWireRetryDownloader_CancelDoesNotLeaveRouteRequest(t *testing.T) {
	geoEntered := make(chan struct{}, 1)
	allowGeo := make(chan struct{})
	pool, router, retryHash := newRetryWiringTestRuntime(t, geoEntered, allowGeo)
	routeReachedAfterCancel := make(chan struct{}, 1)
	router = routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return []string{"example.com"} },
		P2CWindow:   func() time.Duration { return 10 * time.Minute },
		NodeTagResolver: func(node.Hash, *node.NodeEntry) string {
			select {
			case routeReachedAfterCancel <- struct{}{}:
			default:
			}
			return ""
		},
	})

	app := &resinApp{
		topoRuntime: &topologyRuntime{
			pool:   pool,
			router: router,
		},
	}
	retryDL := &netutil.RetryDownloader{
		Direct: retryTestDownloaderFunc(func(_ context.Context, url string) ([]byte, error) {
			return nil, &netutil.HTTPStatusError{StatusCode: 503, URL: url}
		}),
	}
	app.wireRetryDownloader(retryDL)

	replacementDone := make(chan error, 1)
	go func() {
		next := platform.NewPlatform(
			platform.DefaultPlatformID,
			platform.DefaultPlatformName,
			nil,
			[]string{"jp"},
		)
		replacementDone <- pool.ReplacePlatform(next)
	}()
	select {
	case <-geoEntered:
	case <-time.After(time.Second):
		t.Fatal("platform replacement did not enter its rebuild")
	}

	proxyEntered := make(chan struct{})
	selectedHash := make(chan node.Hash, 1)
	var proxyOnce sync.Once
	// Keep the fetch in the real retry path until the caller cancels. The
	// picker itself still comes from wireRetryDownloader.
	retryDL.ProxyFetch = func(ctx context.Context, selection netutil.NodeSelection, _ string) ([]byte, error) {
		selectedHash <- selection.Hash
		proxyOnce.Do(func() { close(proxyEntered) })
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := retryDL.Download(ctx, "https://example.com/retry")
		result <- err
	}()

	select {
	case <-proxyEntered:
		// The fixed picker does not enter Router at all. The replacement can
		// remain in its real platform rebuild critical section while the
		// download is canceled.
		cancel()
		if got := <-selectedHash; got != retryHash {
			t.Fatalf("picker selected %s while replacement was held, want complete old view node %s", got.Hex(), retryHash.Hex())
		}
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Download error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Download did not return after cancellation")
		}
		select {
		case <-routeReachedAfterCancel:
			t.Fatal("wireRetryDownloader left a background RouteRequest after cancellation")
		default:
		}
		close(allowGeo)
		select {
		case err := <-replacementDone:
			if err != nil {
				t.Fatalf("platform replacement: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("platform replacement did not complete after release")
		}
		if _, err := retryDL.NodePicker(context.Background(), "https://example.com/retry"); !errors.Is(err, topology.ErrNoAvailableOutbound) {
			t.Fatalf("picker after publishing filtered replacement = %v, want ErrNoAvailableOutbound", err)
		}
	case <-time.After(time.Second):
		close(allowGeo)
		<-replacementDone
		select {
		case <-result:
		case <-time.After(time.Second):
		}
		t.Fatal("retry download reached neither the production route nor proxy fetch")
	}
}
