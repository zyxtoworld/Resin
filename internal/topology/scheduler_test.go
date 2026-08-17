package topology

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/netip"
	neturl "net/url"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
)

// makeMockFetcher returns a Fetcher that serves the given response.
func makeMockFetcher(body []byte, err error) func(context.Context, string) ([]byte, error) {
	return func(context.Context, string) ([]byte, error) {
		return body, err
	}
}

func makeSubscriptionJSON(outbounds ...string) []byte {
	arr := "["
	for i, o := range outbounds {
		if i > 0 {
			arr += ","
		}
		arr += o
	}
	arr += "]"
	return []byte(`{"outbounds":` + arr + `}`)
}

func newTestScheduler(subMgr *SubscriptionManager, pool *GlobalNodePool, fetcher func(context.Context, string) ([]byte, error)) *SubscriptionScheduler {
	return NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher:    fetcher,
	})
}

func TestScheduler_TickDoesNotOverflowHugeUpdateInterval(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("huge-interval", "Huge interval", "http://example.com", true, false)
	sub.SetFetchConfig(sub.URL(), math.MaxInt64)
	sub.LastCheckedNs.Store(time.Now().UnixNano())
	subMgr.Register(sub)

	var fetchCalls atomic.Int32
	sched := newTestScheduler(subMgr, newTestPool(subMgr), func(context.Context, string) ([]byte, error) {
		fetchCalls.Add(1)
		return nil, errors.New("unexpected refresh")
	})

	sched.tick()
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("huge update interval was treated as immediately due after overflow: fetch calls=%d", got)
	}
}

type controlledDeadlineContext struct {
	done chan struct{}
	once sync.Once
}

func (c *controlledDeadlineContext) Deadline() (time.Time, bool) {
	return time.Now().Add(time.Hour), true
}

func (c *controlledDeadlineContext) Done() <-chan struct{} {
	return c.done
}

func (c *controlledDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *controlledDeadlineContext) Value(any) any { return nil }

func (c *controlledDeadlineContext) expire() {
	c.once.Do(func() { close(c.done) })
}

// --- Test: UpdateSubscription success path ---

func TestScheduler_UpdateSubscription_Success(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	sub.SetFetchConfig(sub.URL(), int64(time.Hour))
	subMgr.Register(sub)

	pool := newTestPool(subMgr)

	body := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"us-1","server":"1.1.1.1","server_port":443}`,
		`{"type":"vmess","tag":"jp-1","server":"2.2.2.2","server_port":443}`,
	)
	fetcher := makeMockFetcher(body, nil)
	sched := newTestScheduler(subMgr, pool, fetcher)

	sched.UpdateSubscription(sub)

	// Verify pool has 2 nodes.
	if pool.Size() != 2 {
		t.Fatalf("expected 2 nodes in pool, got %d", pool.Size())
	}

	// Verify ManagedNodes.
	count := 0
	sub.ManagedNodes().RangeNodes(func(_ node.Hash, _ subscription.ManagedNode) bool {
		count++
		return true
	})
	if count != 2 {
		t.Fatalf("expected 2 managed nodes, got %d", count)
	}

	// Verify timestamps updated.
	if sub.LastCheckedNs.Load() == 0 {
		t.Fatal("LastCheckedNs should be set")
	}
	if sub.LastUpdatedNs.Load() == 0 {
		t.Fatal("LastUpdatedNs should be set")
	}
	if sub.GetLastError() != "" {
		t.Fatalf("LastError should be empty, got %s", sub.GetLastError())
	}
}

func TestScheduler_UpdateSubscription_DownloadViaHTTPServer(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)

	const rawOutbound = `{"type":"shadowsocks","tag":"http-node","server":"1.1.1.1","server_port":443,"method":"aes-256-gcm","password":"secret"}`
	body := makeSubscriptionJSON(rawOutbound)

	subUserAgent := "resin-scheduler-e2e"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ua := r.Header.Get("User-Agent"); ua != subUserAgent {
			t.Fatalf("user-agent: got %q, want %q", ua, subUserAgent)
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	sub := subscription.NewSubscription("s1", "TestSub", srv.URL+"/sub", true, false)
	subMgr.Register(sub)

	downloader := netutil.NewDirectDownloader(
		func() time.Duration { return time.Second },
		func() string { return subUserAgent },
	)
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Downloader: downloader,
	})

	sched.UpdateSubscription(sub)

	if sub.GetLastError() != "" {
		t.Fatalf("unexpected last error: %q", sub.GetLastError())
	}
	if sub.LastCheckedNs.Load() == 0 {
		t.Fatal("LastCheckedNs should be set")
	}
	if sub.LastUpdatedNs.Load() == 0 {
		t.Fatal("LastUpdatedNs should be set")
	}

	hash := node.HashFromRawOptions([]byte(rawOutbound))
	if _, ok := sub.ManagedNodes().LoadNode(hash); !ok {
		t.Fatalf("managed nodes should contain %s", hash.Hex())
	}
	if _, ok := pool.GetEntry(hash); !ok {
		t.Fatalf("pool should contain %s", hash.Hex())
	}
}

// --- Test: UpdateSubscription fetch failure ---

func TestScheduler_UpdateSubscription_FetchFailure(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	fetcher := makeMockFetcher(nil, errors.New("network error"))
	sched := newTestScheduler(subMgr, pool, fetcher)

	sched.UpdateSubscription(sub)

	if sub.GetLastError() != "network error" {
		t.Fatalf("expected 'network error', got %q", sub.GetLastError())
	}
	if sub.LastCheckedNs.Load() == 0 {
		t.Fatal("LastCheckedNs should be set on failure")
	}
	if pool.Size() != 0 {
		t.Fatalf("pool should be empty after fetch failure, got %d", pool.Size())
	}
}

// --- Test: UpdateSubscription parse failure ---

func TestScheduler_UpdateSubscription_ParseFailure(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	fetcher := makeMockFetcher([]byte(`not valid json`), nil)
	sched := newTestScheduler(subMgr, pool, fetcher)

	sched.UpdateSubscription(sub)

	if sub.GetLastError() == "" {
		t.Fatal("LastError should be set on parse failure")
	}
	if pool.Size() != 0 {
		t.Fatal("pool should be empty after parse failure")
	}
}

func TestScheduler_UpdateSubscription_LocalSubscription_SuccessWithoutFetcher(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "LocalSub", "", true, false)
	sub.SetFetchConfig("", int64(time.Hour))
	sub.SetSourceType(subscription.SourceTypeLocal)
	sub.SetContent(`{"outbounds":[{"type":"shadowsocks","tag":"local-ss","server":"1.1.1.1","server_port":443}]}`)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	// If fetcher gets called for local subscription, this test should fail.
	fetcher := makeMockFetcher(nil, errors.New("fetch should not be called for local subscription"))
	sched := newTestScheduler(subMgr, pool, fetcher)

	sched.UpdateSubscription(sub)

	if sub.GetLastError() != "" {
		t.Fatalf("unexpected last error: %q", sub.GetLastError())
	}
	if pool.Size() != 1 {
		t.Fatalf("expected 1 node in pool, got %d", pool.Size())
	}
}

func TestScheduler_RefreshDoesNotPublishMixedInputConfigGeneration(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("mixed-input", "LocalSub", "", true, false)
	sub.SetFetchConfig("", int64(time.Hour))
	sub.SetSourceType(subscription.SourceTypeLocal)
	oldRaw := `{"type":"shadowsocks","tag":"old-input","server":"1.1.1.1","server_port":443}`
	newRaw := `{"type":"shadowsocks","tag":"new-input","server":"2.2.2.2","server_port":443}`
	sub.SetContent(string(makeSubscriptionJSON(oldRaw)))
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(nil, errors.New("local fetch must not run")))
	fieldsRead := make(chan struct{})
	allowVersionRead := make(chan struct{})
	sched.beforeRefreshConfigVersionHook = func() {
		close(fieldsRead)
		<-allowVersionRead
	}

	refreshDone := make(chan struct{})
	go func() {
		sched.UpdateSubscription(sub)
		close(refreshDone)
	}()
	select {
	case <-fieldsRead:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach the mixed-input barrier")
	}

	// This is the same operation-lock/field-setter contract used by the
	// control-plane PATCH path. It changes the refresh input after the old
	// fields were read but before the old implementation reads ConfigVersion.
	sub.WithOpLock(func() {
		sub.SetContent(string(makeSubscriptionJSON(newRaw)))
	})
	close(allowVersionRead)

	select {
	case <-refreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not finish after releasing the barrier")
	}

	oldHash := node.HashFromRawOptions([]byte(oldRaw))
	newHash := node.HashFromRawOptions([]byte(newRaw))
	if _, ok := sub.ManagedNodes().LoadNode(oldHash); ok {
		t.Fatal("refresh published old content after the input config generation changed")
	}
	if _, ok := pool.GetEntry(oldHash); ok {
		t.Fatal("pool published old content after the input config generation changed")
	}
	if _, ok := sub.ManagedNodes().LoadNode(newHash); ok {
		t.Fatal("refresh published the post-mutation content without a new refresh")
	}
	if _, ok := pool.GetEntry(newHash); ok {
		t.Fatal("pool published the post-mutation content without a new refresh")
	}
}

func TestScheduler_RefreshContextCancellationInterruptsConfigSnapshotLock(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("snapshot-cancel", "LocalSub", "", true, false)
	sub.SetFetchConfig("", int64(time.Hour))
	sub.SetSourceType(subscription.SourceTypeLocal)
	sub.SetContent(string(makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"snapshot-cancel","server":"1.1.1.1","server_port":443}`,
	)))
	subMgr.Register(sub)

	sched := newTestScheduler(subMgr, newTestPool(subMgr), nil)
	snapshotStarted := make(chan struct{})
	sched.beforeRefreshConfigSnapshotHook = func() { close(snapshotStarted) }

	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	go sub.WithOpLock(func() {
		close(lockHeld)
		<-releaseLock
	})
	<-lockHeld
	defer close(releaseLock)

	ctx, cancel := context.WithCancel(context.Background())
	refreshDone := make(chan struct{})
	go func() {
		sched.UpdateSubscriptionContext(ctx, sub)
		close(refreshDone)
	}()
	select {
	case <-snapshotStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach the config snapshot boundary")
	}
	cancel()

	select {
	case <-refreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh ignored caller cancellation while waiting for config snapshot lock")
	}
}

func TestScheduler_UpdateSubscription_LocalSubscription_ParseFailure(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "LocalSub", "", true, false)
	sub.SetFetchConfig("", int64(time.Hour))
	sub.SetSourceType(subscription.SourceTypeLocal)
	sub.SetContent("not valid subscription content")
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	fetcher := makeMockFetcher(nil, errors.New("fetch should not be called for local subscription"))
	sched := newTestScheduler(subMgr, pool, fetcher)

	sched.UpdateSubscription(sub)

	if sub.GetLastError() == "" {
		t.Fatal("expected last error on local parse failure")
	}
	if pool.Size() != 0 {
		t.Fatalf("expected empty pool, got %d", pool.Size())
	}
}

func TestScheduler_UpdateSubscription_AllUnsupportedPayloadRetainsExistingNodes(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	oldRaw := `{"type":"shadowsocks","tag":"old-node","server":"1.1.1.1","server_port":443}`
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(makeSubscriptionJSON(oldRaw), nil))
	sched.UpdateSubscription(sub)

	oldHash := node.HashFromRawOptions([]byte(oldRaw))
	if _, ok := pool.GetEntry(oldHash); !ok {
		t.Fatal("old node should exist before the invalid refresh")
	}

	for _, invalidPayload := range [][]byte{
		[]byte(`{"outbounds":[{"type":"direct"},{"type":"block"}]}`),
		[]byte(`{"outbounds":[]}`),
	} {
		// A syntactically valid recognized subscription with no supported nodes
		// is not a trustworthy replacement for the last known-good node set.
		sched.Fetcher = makeMockFetcher(invalidPayload, nil)
		sched.UpdateSubscription(sub)

		if _, ok := pool.GetEntry(oldHash); !ok {
			t.Fatal("zero-node refresh must not remove the last known-good node")
		}
		if _, ok := sub.ManagedNodes().LoadNode(oldHash); !ok {
			t.Fatal("zero-node refresh must not replace ManagedNodes with an empty set")
		}
	}
}

func TestScheduler_UpdateSubscription_StatusErrorRedactsWholeURL(t *testing.T) {
	canaries := []struct {
		url    string
		secret string
	}{
		{
			url:    "https://subscriber:userinfo-secret@example.com/subscription",
			secret: "userinfo-secret",
		},
		{
			url:    "https://example.com/subscription?token=query-secret&format=json",
			secret: "query-secret",
		},
		{
			url:    "https://example.com/subscription/path-secret",
			secret: "path-secret",
		},
	}

	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", canaries[0].url, true, false)
	subMgr.Register(sub)
	p := newTestPool(subMgr)
	sched := newTestScheduler(subMgr, p, nil)

	for _, canary := range canaries {
		sub.SetFetchConfig(canary.url, int64(time.Hour))
		sched.Fetcher = makeMockFetcher(nil, &netutil.HTTPStatusError{StatusCode: 503, URL: canary.url})
		if !sched.UpdateSubscription(sub) {
			t.Fatal("refresh was not admitted")
		}
		got := sub.GetLastError()
		if strings.Contains(got, canary.secret) || strings.Contains(got, "subscriber:") || strings.Contains(got, "?token=") || strings.Contains(got, "/path-secret") {
			t.Fatal("subscription LastError exposed URL data beyond its origin")
		}
		if !strings.Contains(got, "503") || !strings.Contains(got, "https://example.com") {
			t.Fatalf("LastError lost safe status/origin diagnostics: %q", got)
		}
	}
}

type subscriptionRoundTripFunc func(*http.Request) (*http.Response, error)

func (f subscriptionRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestScheduler_UpdateSubscription_RequestErrorRedactsWholeURL(t *testing.T) {
	canaries := []struct {
		url    string
		secret string
	}{
		{
			url:    "https://subscriber:userinfo-secret@example.com/subscription",
			secret: "userinfo-secret",
		},
		{
			url:    "https://example.com/subscription?token=query-secret&format=json",
			secret: "query-secret",
		},
		{
			url:    "https://example.com/subscription/path-secret",
			secret: "path-secret",
		},
	}
	requestErr := errors.New("connection refused")
	downloader := &netutil.DirectDownloader{
		Client: &http.Client{Transport: subscriptionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, &neturl.Error{Op: req.Method, URL: req.URL.String(), Err: requestErr}
		})},
		TimeoutFn:   func() time.Duration { return 0 },
		UserAgentFn: func() string { return "" },
	}
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", canaries[0].url, true, false)
	subMgr.Register(sub)
	p := newTestPool(subMgr)
	sched := NewSubscriptionScheduler(SchedulerConfig{SubManager: subMgr, Pool: p, Downloader: downloader})

	for _, canary := range canaries {
		sub.SetFetchConfig(canary.url, int64(time.Hour))
		if !sched.UpdateSubscription(sub) {
			t.Fatal("refresh was not admitted")
		}
		got := sub.GetLastError()
		if strings.Contains(got, canary.secret) || strings.Contains(got, "subscriber:") || strings.Contains(got, "?token=") || strings.Contains(got, "/path-secret") {
			t.Fatal("subscription LastError exposed URL data beyond its origin")
		}
		if !strings.Contains(got, "connection refused") || !strings.Contains(got, "https://example.com") {
			t.Fatalf("LastError lost safe request diagnostics: %q", got)
		}
	}
}

func TestScheduler_UpdateSubscription_MalformedURLRedactsWholeURL(t *testing.T) {
	const rawURL = "https://subscriber:malformed-userinfo@example.com/sub/%zz?token=malformed-query"
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", rawURL, true, false)
	subMgr.Register(sub)
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       newTestPool(subMgr),
		Downloader: &netutil.DirectDownloader{
			TimeoutFn:   func() time.Duration { return 0 },
			UserAgentFn: func() string { return "" },
		},
	})

	if !sched.UpdateSubscription(sub) {
		t.Fatal("refresh was not admitted")
	}
	got := sub.GetLastError()
	for _, secret := range []string{"malformed-userinfo", "malformed-query", "/sub/%zz", "subscriber:"} {
		if strings.Contains(got, secret) {
			t.Fatalf("subscription LastError exposed %q: %q", secret, got)
		}
	}
	if !strings.Contains(got, "invalid request URL [redacted-url]") {
		t.Fatalf("LastError lost safe malformed-URL diagnostics: %q", got)
	}
}

// --- Test: Swap-before-add/remove ordering ---

func TestScheduler_UpdateSubscription_SwapBeforePoolMutation(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	// Preload with initial nodes.
	initialBody := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"node-a","server":"1.1.1.1","server_port":443}`,
	)
	pool := newTestPool(subMgr)
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(initialBody, nil))
	sched.UpdateSubscription(sub)

	oldHash := node.HashFromRawOptions([]byte(`{"type":"shadowsocks","tag":"node-a","server":"1.1.1.1","server_port":443}`))

	// Now update to different nodes.
	newBody := makeSubscriptionJSON(
		`{"type":"vmess","tag":"node-b","server":"2.2.2.2","server_port":443}`,
	)
	sched.Fetcher = makeMockFetcher(newBody, nil)
	sched.UpdateSubscription(sub)

	newHash := node.HashFromRawOptions([]byte(`{"type":"vmess","tag":"node-b","server":"2.2.2.2","server_port":443}`))

	// Old node should be removed, new node should be added.
	if _, ok := pool.GetEntry(oldHash); ok {
		t.Fatal("old node should have been removed from pool")
	}
	if _, ok := pool.GetEntry(newHash); !ok {
		t.Fatal("new node should be in pool")
	}

	// ManagedNodes should only have new hash.
	if _, ok := sub.ManagedNodes().LoadNode(oldHash); ok {
		t.Fatal("old hash should not be in ManagedNodes")
	}
	if _, ok := sub.ManagedNodes().LoadNode(newHash); !ok {
		t.Fatal("new hash should be in ManagedNodes")
	}
}

// --- Test: Idempotent update (same data) ---

func TestScheduler_UpdateSubscription_Idempotent(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	body := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"us-1","server":"1.1.1.1","server_port":443}`,
	)
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(body, nil))

	// Update three times with same data.
	sched.UpdateSubscription(sub)
	sched.UpdateSubscription(sub)
	sched.UpdateSubscription(sub)

	if pool.Size() != 1 {
		t.Fatalf("expected 1 node after idempotent updates, got %d", pool.Size())
	}

	h := node.HashFromRawOptions([]byte(`{"type":"shadowsocks","tag":"us-1","server":"1.1.1.1","server_port":443}`))
	entry, _ := pool.GetEntry(h)
	if entry.SubscriptionCount() != 1 {
		t.Fatalf("expected 1 sub ref, got %d", entry.SubscriptionCount())
	}
}

func TestScheduler_UpdateSubscription_KeepEvictedDoesNotReAddToPool(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	body := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"evicted-node","server":"1.1.1.1","server_port":443}`,
	)
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(body, nil))
	sched.UpdateSubscription(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"shadowsocks","tag":"evicted-node","server":"1.1.1.1","server_port":443}`))
	if _, ok := pool.GetEntry(hash); !ok {
		t.Fatal("node should exist in pool after initial refresh")
	}

	// Simulate eviction: keep hash in managed view, but remove runtime pool ref.
	sub.WithOpLock(func() {
		managed, ok := sub.ManagedNodes().LoadNode(hash)
		if !ok {
			t.Fatal("managed node should exist before eviction mark")
		}
		managed.Evicted = true
		sub.ManagedNodes().StoreNode(hash, managed)
		pool.RemoveNodeFromSub(hash, sub.ID)
	})

	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("evicted node should be removed from pool")
	}

	// Same subscription content keeps the hash, but evicted keep must not be re-added.
	sched.UpdateSubscription(sub)

	managed, ok := sub.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatal("evicted keep hash should remain in managed view")
	}
	if !managed.Evicted {
		t.Fatal("evicted keep hash should preserve Evicted=true")
	}
	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("evicted keep hash should not be re-added to pool on refresh")
	}
}

func TestScheduler_UpdateSubscription_IncrementalAliveModeKeepsHealthyOldNodes(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	sub.SetIncrementalAliveNodes(true)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	initialRaw := `{"type":"shadowsocks","tag":"healthy-old","server":"1.1.1.1","server_port":443}`
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(makeSubscriptionJSON(initialRaw), nil))
	sched.UpdateSubscription(sub)

	oldHash := node.HashFromRawOptions([]byte(initialRaw))
	oldEntry, ok := pool.GetEntry(oldHash)
	if !ok {
		t.Fatal("old node should exist after initial refresh")
	}
	outbound := testutil.NewNoopOutbound()
	oldEntry.Outbound.Store(&outbound)
	oldEntry.CircuitOpenSince.Store(0)
	oldEntry.SetLastError("")

	newRaw := `{"type":"vmess","tag":"new-node","server":"2.2.2.2","server_port":443}`
	sched.Fetcher = makeMockFetcher(makeSubscriptionJSON(newRaw), nil)
	sched.UpdateSubscription(sub)

	newHash := node.HashFromRawOptions([]byte(newRaw))
	if _, ok := sub.ManagedNodes().LoadNode(oldHash); !ok {
		t.Fatal("healthy old node should be retained in incremental mode")
	}
	if _, ok := sub.ManagedNodes().LoadNode(newHash); !ok {
		t.Fatal("new node should be present after incremental refresh")
	}
	if _, ok := pool.GetEntry(oldHash); !ok {
		t.Fatal("healthy old node should remain in pool")
	}
	if _, ok := pool.GetEntry(newHash); !ok {
		t.Fatal("new node should be added to pool")
	}
}

func TestScheduler_UpdateSubscription_IncrementalAliveModeRemovesUnhealthyOldNodes(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	sub.SetIncrementalAliveNodes(true)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	initialRaw := `{"type":"shadowsocks","tag":"bad-old","server":"1.1.1.1","server_port":443}`
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(makeSubscriptionJSON(initialRaw), nil))
	sched.UpdateSubscription(sub)

	oldHash := node.HashFromRawOptions([]byte(initialRaw))
	oldEntry, ok := pool.GetEntry(oldHash)
	if !ok {
		t.Fatal("old node should exist after initial refresh")
	}
	oldEntry.CircuitOpenSince.Store(time.Now().Add(-time.Minute).UnixNano())

	newRaw := `{"type":"vmess","tag":"new-node","server":"2.2.2.2","server_port":443}`
	sched.Fetcher = makeMockFetcher(makeSubscriptionJSON(newRaw), nil)
	sched.UpdateSubscription(sub)

	newHash := node.HashFromRawOptions([]byte(newRaw))
	if _, ok := sub.ManagedNodes().LoadNode(oldHash); ok {
		t.Fatal("unhealthy old node should be removed in incremental mode")
	}
	if _, ok := pool.GetEntry(oldHash); ok {
		t.Fatal("unhealthy old node should be removed from pool")
	}
	if _, ok := sub.ManagedNodes().LoadNode(newHash); !ok {
		t.Fatal("new node should still be present")
	}
}

// --- Test: Rename triggers re-filter ---

func TestScheduler_RenameSubscription(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "OldName", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	body := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"us-1","server":"1.1.1.1","server_port":443}`,
	)
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(body, nil))
	sched.UpdateSubscription(sub)

	// Rename should update name and re-add all hashes (triggering platform re-filter).
	sched.RenameSubscription(sub, "NewName")

	if sub.Name() != "NewName" {
		t.Fatalf("expected NewName, got %s", sub.Name())
	}

	// Pool should still have 1 node.
	if pool.Size() != 1 {
		t.Fatalf("expected 1 node after rename, got %d", pool.Size())
	}
}

func TestScheduler_RenameSubscription_SkipsEvictedManagedNodes(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "OldName", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	body := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"evicted-node","server":"1.1.1.1","server_port":443}`,
	)
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(body, nil))
	sched.UpdateSubscription(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"shadowsocks","tag":"evicted-node","server":"1.1.1.1","server_port":443}`))
	sub.WithOpLock(func() {
		managed, ok := sub.ManagedNodes().LoadNode(hash)
		if !ok {
			t.Fatal("managed node should exist before eviction mark")
		}
		managed.Evicted = true
		sub.ManagedNodes().StoreNode(hash, managed)
		pool.RemoveNodeFromSub(hash, sub.ID)
	})
	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("evicted node should be removed before rename")
	}

	sched.RenameSubscription(sub, "NewName")

	if sub.Name() != "NewName" {
		t.Fatalf("expected NewName, got %s", sub.Name())
	}
	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("rename should not re-add evicted managed nodes")
	}
}

// --- Test: Failure path is serialized with WithSubLock ---

func TestScheduler_FailurePath_Serialized(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)

	var fetchCount atomic.Int32
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		fetchCount.Add(1)
		return nil, errors.New("fail")
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	// Run concurrent updates — all failure paths should serialize via WithSubLock.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sched.UpdateSubscription(sub)
		}()
	}
	wg.Wait()

	if fetchCount.Load() != 20 {
		t.Fatalf("expected 20 fetch attempts, got %d", fetchCount.Load())
	}
	if sub.GetLastError() != "fail" {
		t.Fatalf("expected 'fail' error, got %q", sub.GetLastError())
	}
}

func TestScheduler_StaleFailureDoesNotOverrideNewerSuccess(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	successBody := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"ok-node","server":"1.1.1.1","server_port":443}`,
	)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			return nil, errors.New("stale failure")
		}
		return successBody, nil
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		sched.UpdateSubscription(sub) // older attempt: blocks in fetcher and eventually fails.
	}()

	<-firstStarted

	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		sched.UpdateSubscription(sub) // newer attempt: succeeds first.
	}()
	<-done2

	close(releaseFirst)
	<-done1

	if calls.Load() != 2 {
		t.Fatalf("expected 2 fetch calls, got %d", calls.Load())
	}
	if sub.GetLastError() != "" {
		t.Fatalf("stale failure must not overwrite newer success, got error=%q", sub.GetLastError())
	}
	if sub.LastUpdatedNs.Load() == 0 {
		t.Fatal("newer success should set LastUpdatedNs")
	}
	if pool.Size() != 1 {
		t.Fatalf("expected 1 node in pool after newer success, got %d", pool.Size())
	}

	h := node.HashFromRawOptions([]byte(`{"type":"shadowsocks","tag":"ok-node","server":"1.1.1.1","server_port":443}`))
	if _, ok := sub.ManagedNodes().LoadNode(h); !ok {
		t.Fatal("managed nodes should contain hash from newer successful update")
	}
}

func TestScheduler_StaleFailureDoesNotOverrideNewerFailure(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondDone := make(chan struct{})
	var calls atomic.Int32
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			return nil, errors.New("stale failure")
		}
		close(secondDone)
		return nil, errors.New("newer failure")
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		sched.UpdateSubscription(sub)
	}()

	<-firstStarted
	secondDoneCall := make(chan struct{})
	go func() {
		sched.UpdateSubscription(sub)
		close(secondDoneCall)
	}()

	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("newer failure did not reach fetcher")
	}
	select {
	case <-secondDoneCall:
	case <-time.After(time.Second):
		t.Fatal("newer failure did not finish")
	}

	close(releaseFirst)
	<-firstDone

	if got := sub.GetLastError(); got != "newer failure" {
		t.Fatalf("stale failure overwrote newer failure: got %q", got)
	}
}

func TestScheduler_StaleSuccessDoesNotOverrideNewerFailure(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	oldBody := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"old-node","server":"1.1.1.1","server_port":443}`,
	)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondDone := make(chan struct{})
	var calls atomic.Int32
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			return oldBody, nil
		}
		close(secondDone)
		return nil, errors.New("newer failure")
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		sched.UpdateSubscription(sub)
	}()

	<-firstStarted
	secondDoneCall := make(chan struct{})
	go func() {
		sched.UpdateSubscription(sub)
		close(secondDoneCall)
	}()

	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("newer failure did not reach fetcher")
	}
	select {
	case <-secondDoneCall:
	case <-time.After(time.Second):
		t.Fatal("newer failure did not finish")
	}

	close(releaseFirst)
	<-firstDone

	if got := sub.GetLastError(); got != "newer failure" {
		t.Fatalf("stale success cleared newer failure: got %q", got)
	}
	if pool.Size() != 0 {
		t.Fatalf("stale success repopulated nodes after newer failure: size=%d", pool.Size())
	}
}

func TestScheduler_StaleSuccessDoesNotOverrideNewerSuccess(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	oldBody := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"old-node","server":"1.1.1.1","server_port":443}`,
	)
	newBody := makeSubscriptionJSON(
		`{"type":"vmess","tag":"new-node","server":"2.2.2.2","server_port":443}`,
	)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			return oldBody, nil // older attempt returns stale success
		}
		return newBody, nil // newer attempt succeeds first
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		sched.UpdateSubscription(sub) // older attempt: blocked in fetcher, then stale success
	}()

	<-firstStarted

	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		sched.UpdateSubscription(sub) // newer attempt: success lands first
	}()
	<-done2

	close(releaseFirst)
	<-done1

	if calls.Load() != 2 {
		t.Fatalf("expected 2 fetch calls, got %d", calls.Load())
	}
	if sub.GetLastError() != "" {
		t.Fatalf("expected empty last error, got %q", sub.GetLastError())
	}

	// Newer success should win; stale old success must be ignored.
	if pool.Size() != 1 {
		t.Fatalf("expected exactly 1 node in pool, got %d", pool.Size())
	}
	oldHash := node.HashFromRawOptions([]byte(`{"type":"shadowsocks","tag":"old-node","server":"1.1.1.1","server_port":443}`))
	newHash := node.HashFromRawOptions([]byte(`{"type":"vmess","tag":"new-node","server":"2.2.2.2","server_port":443}`))
	if _, ok := pool.GetEntry(newHash); !ok {
		t.Fatal("expected new node to remain after stale-success race")
	}
	if _, ok := pool.GetEntry(oldHash); ok {
		t.Fatal("stale old success should not overwrite newer subscription state")
	}
	if _, ok := sub.ManagedNodes().LoadNode(newHash); !ok {
		t.Fatal("managed nodes should contain new hash")
	}
	if _, ok := sub.ManagedNodes().LoadNode(oldHash); ok {
		t.Fatal("managed nodes should not contain old hash after stale-success race")
	}
}

// --- Test: Concurrent update + rename serialization ---

func TestScheduler_ConcurrentUpdateAndRename(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Name0", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)

	body := makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"node","server":"1.1.1.1","server_port":443}`,
	)
	sched := newTestScheduler(subMgr, pool, makeMockFetcher(body, nil))

	var wg sync.WaitGroup
	// Concurrent updates.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sched.UpdateSubscription(sub)
		}()
	}
	// Concurrent renames.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sched.RenameSubscription(sub, fmt.Sprintf("Name%d", n))
		}(i)
	}
	wg.Wait()

	// Pool should have exactly 1 node.
	if pool.Size() != 1 {
		t.Fatalf("expected 1 node after concurrent ops, got %d", pool.Size())
	}

	h := node.HashFromRawOptions([]byte(`{"type":"shadowsocks","tag":"node","server":"1.1.1.1","server_port":443}`))
	entry, _ := pool.GetEntry(h)
	if entry.SubscriptionCount() != 1 {
		t.Fatalf("expected 1 sub ref, got %d", entry.SubscriptionCount())
	}
}

// --- Test: Due check logic ---

func TestScheduler_DueCheck(t *testing.T) {
	subMgr := NewSubscriptionManager()

	// Sub that was checked 2 hours ago with 1-hour interval → due.
	dueSub := subscription.NewSubscription("s1", "Due", "http://example.com", true, false)
	dueSub.SetFetchConfig(dueSub.URL(), int64(time.Hour))
	dueSub.LastCheckedNs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	subMgr.Register(dueSub)

	// Sub that was just checked → not due.
	notDueSub := subscription.NewSubscription("s2", "NotDue", "http://example.com", true, false)
	notDueSub.SetFetchConfig(notDueSub.URL(), int64(time.Hour))
	notDueSub.LastCheckedNs.Store(time.Now().UnixNano())
	subMgr.Register(notDueSub)

	pool := newTestPool(subMgr)
	var fetchedURLs sync.Map
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		fetchedURLs.Store(url, true)
		return makeSubscriptionJSON(), nil
	}
	_ = newTestScheduler(subMgr, pool, fetcher)

	// Simulate the due check from scheduler.run().
	now := time.Now().UnixNano()
	var dueIDs []string
	subMgr.Range(func(id string, sub *subscription.Subscription) bool {
		if !sub.Enabled() {
			return true
		}
		if subscriptionDueAt(sub.LastCheckedNs.Load(), sub.UpdateIntervalNs(), now) {
			dueIDs = append(dueIDs, id)
		}
		return true
	})

	if len(dueIDs) != 1 {
		t.Fatalf("expected 1 due sub, got %d: %v", len(dueIDs), dueIDs)
	}
	if dueIDs[0] != "s1" {
		t.Fatalf("expected s1 to be due, got %s", dueIDs[0])
	}
}

func TestScheduler_Tick_UpdatesDueSubscriptionsInParallel(t *testing.T) {
	subMgr := NewSubscriptionManager()

	due1 := subscription.NewSubscription("s1", "Due1", "http://example.com/due1", true, false)
	due1.SetFetchConfig(due1.URL(), int64(time.Hour))
	due1.LastCheckedNs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	subMgr.Register(due1)

	due2 := subscription.NewSubscription("s2", "Due2", "http://example.com/due2", true, false)
	due2.SetFetchConfig(due2.URL(), int64(time.Hour))
	due2.LastCheckedNs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	subMgr.Register(due2)

	notDue := subscription.NewSubscription("s3", "NotDue", "http://example.com/not-due", true, false)
	notDue.SetFetchConfig(notDue.URL(), int64(time.Hour))
	notDue.LastCheckedNs.Store(time.Now().UnixNano())
	subMgr.Register(notDue)

	pool := newTestPool(subMgr)
	releaseFetch := make(chan struct{})
	allStarted := make(chan struct{})
	var started atomic.Int32
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		if started.Add(1) == 2 {
			close(allStarted)
		}
		<-releaseFetch
		return makeSubscriptionJSON(), nil
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	done := make(chan struct{})
	go func() {
		sched.tick()
		close(done)
	}()

	select {
	case <-allStarted:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected due subscription refreshes to start in parallel")
	}

	select {
	case <-done:
		t.Fatal("tick should wait for in-flight due refreshes")
	default:
	}

	close(releaseFetch)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("tick did not finish after fetchers were released")
	}

	if got := started.Load(); got != 2 {
		t.Fatalf("expected 2 fetch attempts for due subscriptions, got %d", got)
	}
}

func TestScheduler_ForceRefreshAllAsync_ReturnsImmediately(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Async", "http://example.com/async", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		close(fetchStarted)
		<-releaseFetch
		return makeSubscriptionJSON(), nil
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	returned := make(chan struct{})
	go func() {
		sched.ForceRefreshAllAsync()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ForceRefreshAllAsync should return immediately")
	}

	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not start")
	}

	close(releaseFetch)
	sched.Stop()
}

func TestScheduler_StopAfterWorkerCheckRejectsFetcherAdmission(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-stop-race", "Stop race", "https://example.com/sub", true, false)
	subMgr.Register(sub)
	p := newTestPool(subMgr)
	var fetchCalls atomic.Int32
	fetchStarted := make(chan struct{})
	fetcher := func(ctx context.Context, _ string) ([]byte, error) {
		fetchCalls.Add(1)
		close(fetchStarted)
		return makeSubscriptionJSON(), ctx.Err()
	}
	sched := newTestScheduler(subMgr, p, fetcher)
	fetchAdmissionReached := make(chan struct{})
	releaseBeforeFetcher := make(chan struct{})
	sched.beforeRefreshUpdateHook = func() {
		close(fetchAdmissionReached)
		<-releaseBeforeFetcher
	}

	done := make(chan struct{})
	go func() {
		sched.ForceRefreshAll()
		close(done)
	}()
	select {
	case <-fetchAdmissionReached:
	case <-time.After(time.Second):
		t.Fatal("refresh did not reach fetch admission")
	}

	stopDone := make(chan struct{})
	go func() {
		sched.Stop()
		close(stopDone)
	}()
	select {
	case <-sched.stopCh:
	case <-time.After(time.Second):
		t.Fatal("Stop did not close stop channel")
	}
	close(releaseBeforeFetcher)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ForceRefreshAll did not finish")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish")
	}
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("Fetcher was called after stop admission closed: got %d calls", got)
	}
	select {
	case <-fetchStarted:
		t.Fatal("Fetcher unexpectedly started")
	default:
	}
}

func TestScheduler_RequestDeadlineDoesNotRecordFailure(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-request-deadline", "Request deadline", "https://example.com/sub", true, false)
	subMgr.Register(sub)
	p := newTestPool(subMgr)

	const initialChecked = int64(123)
	sub.LastCheckedNs.Store(initialChecked)
	sub.SetLastError("")
	fetchStarted := make(chan struct{})
	sched := newTestScheduler(subMgr, p, func(ctx context.Context, _ string) ([]byte, error) {
		close(fetchStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	var updateCallbacks atomic.Int32
	sched.onSubUpdated = func(*subscription.Subscription) {
		updateCallbacks.Add(1)
	}

	requestCtx := &controlledDeadlineContext{done: make(chan struct{})}
	result := make(chan bool, 1)
	go func() {
		result <- sched.UpdateSubscriptionContext(requestCtx, sub)
	}()

	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("refresh did not reach fetcher")
	}
	requestCtx.expire()

	select {
	case accepted := <-result:
		if !accepted {
			t.Fatal("admitted refresh unexpectedly rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("refresh did not return after request deadline")
	}

	if got := sub.GetLastError(); got != "" {
		t.Fatalf("request deadline was recorded as subscription failure: %q", got)
	}
	if got := sub.LastCheckedNs.Load(); got != initialChecked {
		t.Fatalf("request deadline changed LastCheckedNs: got %d, want %d", got, initialChecked)
	}
	if got := updateCallbacks.Load(); got != 0 {
		t.Fatalf("request deadline triggered %d update callbacks, want 0", got)
	}
}

func TestScheduler_FetcherDeadlineWhileCallerAliveRecordsFailure(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-fetcher-deadline", "Fetcher deadline", "https://example.com/sub", true, false)
	subMgr.Register(sub)
	p := newTestPool(subMgr)
	sched := newTestScheduler(subMgr, p, func(context.Context, string) ([]byte, error) {
		return nil, context.DeadlineExceeded
	})

	if !sched.UpdateSubscriptionContext(context.Background(), sub) {
		t.Fatal("refresh was not admitted")
	}
	if got := sub.GetLastError(); got != context.DeadlineExceeded.Error() {
		t.Fatalf("fetcher deadline was not recorded while caller was alive: got %q", got)
	}
}

func TestScheduler_RequestDeadlineBeforeParseDoesNotRecordFailure(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-parse-deadline", "Parse deadline", "", true, false)
	sub.SetSourceType(subscription.SourceTypeLocal)
	sub.SetContent("not a subscription")
	subMgr.Register(sub)
	p := newTestPool(subMgr)

	const initialChecked = int64(456)
	sub.LastCheckedNs.Store(initialChecked)
	sub.SetLastError("")
	requestCtx := &controlledDeadlineContext{done: make(chan struct{})}
	sched := newTestScheduler(subMgr, p, makeMockFetcher(nil, errors.New("local refresh must not fetch")))
	sched.beforeRefreshParseHook = requestCtx.expire

	if !sched.UpdateSubscriptionContext(requestCtx, sub) {
		t.Fatal("refresh was not admitted")
	}
	if got := sub.GetLastError(); got != "" {
		t.Fatalf("request deadline before parse was recorded as failure: %q", got)
	}
	if got := sub.LastCheckedNs.Load(); got != initialChecked {
		t.Fatalf("request deadline before parse changed LastCheckedNs: got %d, want %d", got, initialChecked)
	}
}

func TestScheduler_StopWaitsForDirectLocalRefresh(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-local-stop", "Local stop", "", true, false)
	sub.SetSourceType(subscription.SourceTypeLocal)
	sub.SetContent(string(makeSubscriptionJSON(
		`{"type":"shadowsocks","tag":"local-stop","server":"1.1.1.1","server_port":443}`,
	)))
	subMgr.Register(sub)
	p := newTestPool(subMgr)
	sched := newTestScheduler(subMgr, p, makeMockFetcher(nil, errors.New("local refresh must not fetch")))
	reachedRefresh := make(chan struct{})
	releaseRefresh := make(chan struct{})
	sched.beforeRefreshUpdateHook = func() {
		close(reachedRefresh)
		<-releaseRefresh
	}

	refreshDone := make(chan struct{})
	go func() {
		sched.UpdateSubscription(sub)
		close(refreshDone)
	}()
	select {
	case <-reachedRefresh:
	case <-time.After(time.Second):
		t.Fatal("local refresh did not reach the controlled admission boundary")
	}

	stopDone := make(chan struct{})
	go func() {
		sched.Stop()
		close(stopDone)
	}()
	select {
	case <-sched.stopCh:
	case <-time.After(time.Second):
		t.Fatal("Stop did not signal the scheduler")
	}
	select {
	case <-stopDone:
		t.Fatal("Stop returned while a direct local refresh was still admitted")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseRefresh)
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("local refresh did not finish after release")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not join the direct local refresh")
	}
	if got := p.Size(); got != 1 {
		t.Fatalf("expected admitted local refresh to publish one node, got %d", got)
	}
}

func TestScheduler_StopAfterFetcherAdmissionCancelsCustomFetcher(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-fetch-cancel", "Fetch cancel", "https://example.com/sub", true, false)
	subMgr.Register(sub)
	p := newTestPool(subMgr)
	var fetchCalls atomic.Int32
	fetchStarted := make(chan struct{})
	fetcher := func(ctx context.Context, _ string) ([]byte, error) {
		fetchCalls.Add(1)
		close(fetchStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	sched := newTestScheduler(subMgr, p, fetcher)

	sched.UpdateSubscriptionAsync(sub)
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("custom Fetcher was not admitted")
	}

	stopDone := make(chan struct{})
	go func() {
		sched.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel and join the admitted custom Fetcher")
	}
	if got := fetchCalls.Load(); got != 1 {
		t.Fatalf("expected exactly one admitted Fetcher call, got %d", got)
	}
}

func TestScheduler_ForceRefreshAll_UpdatesSubscriptionsInParallel(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub1 := subscription.NewSubscription("s1", "One", "http://example.com/one", true, false)
	sub2 := subscription.NewSubscription("s2", "Two", "http://example.com/two", true, false)
	subMgr.Register(sub1)
	subMgr.Register(sub2)

	pool := newTestPool(subMgr)
	releaseFetch := make(chan struct{})
	allStarted := make(chan struct{})
	var started atomic.Int32
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		if started.Add(1) == 2 {
			close(allStarted)
		}
		<-releaseFetch
		return makeSubscriptionJSON(), nil
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	done := make(chan struct{})
	go func() {
		sched.ForceRefreshAll()
		close(done)
	}()

	select {
	case <-allStarted:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected both subscription refreshes to start in parallel")
	}

	select {
	case <-done:
		t.Fatal("ForceRefreshAll should wait for in-flight refreshes")
	default:
	}

	close(releaseFetch)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ForceRefreshAll did not finish after fetchers were released")
	}

	if got := started.Load(); got != 2 {
		t.Fatalf("expected 2 fetch attempts, got %d", got)
	}
}

func TestScheduler_ForceRefreshAll_LimitsConcurrentUpdates(t *testing.T) {
	oldMaxProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	subMgr := NewSubscriptionManager()
	for i := 0; i < 6; i++ {
		subMgr.Register(subscription.NewSubscription(
			fmt.Sprintf("s%d", i),
			fmt.Sprintf("Sub-%d", i),
			fmt.Sprintf("http://example.com/%d", i),
			true,
			false,
		))
	}

	pool := newTestPool(subMgr)
	releaseFetch := make(chan struct{})
	var started atomic.Int32
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		current := inFlight.Add(1)
		for {
			prev := maxInFlight.Load()
			if current <= prev || maxInFlight.CompareAndSwap(prev, current) {
				break
			}
		}

		started.Add(1)
		<-releaseFetch
		inFlight.Add(-1)
		return makeSubscriptionJSON(), nil
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	done := make(chan struct{})
	go func() {
		sched.ForceRefreshAll()
		close(done)
	}()

	deadline := time.After(300 * time.Millisecond)
	for started.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("expected at least 2 fetch attempts, got %d", started.Load())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	time.Sleep(50 * time.Millisecond)
	if got := started.Load(); got != 2 {
		t.Fatalf("expected worker limit to cap in-flight starts at 2 before release, got %d", got)
	}

	close(releaseFetch)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ForceRefreshAll did not finish after releasing fetchers")
	}

	if got := maxInFlight.Load(); got > 2 {
		t.Fatalf("expected max in-flight fetches <= 2, got %d", got)
	}
	if got := started.Load(); got != 6 {
		t.Fatalf("expected all 6 subscriptions to be refreshed, got %d", got)
	}
}

func TestScheduler_ForceRefreshAll_AfterStopDoesNotFetch(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "One", "http://example.com/one", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	var calls atomic.Int32
	fetcher := func(_ context.Context, url string) ([]byte, error) {
		calls.Add(1)
		return makeSubscriptionJSON(), nil
	}
	sched := newTestScheduler(subMgr, pool, fetcher)

	sched.Stop()
	sched.ForceRefreshAll()

	if got := calls.Load(); got != 0 {
		t.Fatalf("expected no fetch attempts after stop, got %d", got)
	}
}

func TestScheduler_StopCancelsInFlightForceRefreshDownload(t *testing.T) {
	subMgr := NewSubscriptionManager()

	requestStarted := make(chan struct{})
	requestDone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
		close(requestDone)
	}))
	defer srv.Close()

	sub := subscription.NewSubscription("s1", "One", srv.URL, true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	downloader := netutil.NewDirectDownloader(
		func() time.Duration { return 30 * time.Second },
		func() string { return "resin-scheduler-stop-test" },
	)
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Downloader: downloader,
	})

	sched.ForceRefreshAllAsync()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("expected in-flight download to start")
	}

	stopped := make(chan struct{})
	go func() {
		sched.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler Stop should cancel in-flight download and return promptly")
	}

	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("expected HTTP handler context to be canceled after scheduler stop")
	}
}

// --- Test: Persistence callback invoked ---

func TestScheduler_OnSubUpdated_Called(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	body := makeSubscriptionJSON(`{"type":"shadowsocks","tag":"n","server":"1.1.1.1","server_port":443}`)

	var callCount atomic.Int32
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher:    makeMockFetcher(body, nil),
		OnSubUpdated: func(s *subscription.Subscription) {
			callCount.Add(1)
		},
	})

	sched.UpdateSubscription(sub)
	if callCount.Load() != 1 {
		t.Fatalf("expected onSubUpdated to be called once, got %d", callCount.Load())
	}

	// Also on failure.
	sched.Fetcher = makeMockFetcher(nil, errors.New("fail"))
	sched.UpdateSubscription(sub)
	if callCount.Load() != 2 {
		t.Fatalf("expected onSubUpdated to be called on failure too, got %d", callCount.Load())
	}
}

// --- Test: Disabled ephemeral sub still gets evicted ---

func TestEphemeralCleaner_DisabledEphemeralStillEvicted(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "EphSub", "url", false, true) // disabled + ephemeral
	sub.SetEphemeralNodeEvictDelayNs(int64(1 * time.Minute))
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node-1"}})
	sub.SwapManagedNodes(mn)
	pool.AddNodeFromSub(h, raw, "s1")

	entry, _ := pool.GetEntry(h)
	entry.CircuitOpenSince.Store(time.Now().Add(-2 * time.Minute).UnixNano())

	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.sweep()

	// Disabled ephemeral sub should still be cleaned.
	if pool.Size() != 0 {
		t.Fatal("disabled ephemeral sub should still have circuit-broken nodes evicted")
	}
}

// --- Test: ReadOnlyView compile-time constraint ---

func TestPlatform_ReadOnlyView_Interface(t *testing.T) {
	plat := platform.NewPlatform("p1", "Test", nil, nil)

	// View() returns ReadOnlyView, not *RoutableView.
	// This compile-time type assignment verifies the interface constraint.
	var view platform.ReadOnlyView = plat.View()

	// Read methods work.
	if view.Size() != 0 {
		t.Fatalf("empty view should have size 0, got %d", view.Size())
	}
	if view.Contains(node.Hash{}) {
		t.Fatal("empty view should not contain anything")
	}

	// The key constraint: callers with only a ReadOnlyView cannot call
	// Add/Remove/Clear — there are no such methods on the interface.
	// This is enforced at compile time, not runtime.
	// (Go's type assertion can still recover the concrete type, but API
	// consumers using the interface type cannot accidentally mutate.)
	t.Log("ReadOnlyView interface correctly restricts mutation methods at the type level")
}

// Ensure ReadOnlyView is correctly implemented by RoutableView.
var _ platform.ReadOnlyView = (*platform.RoutableView)(nil)

// --- Test: SetSubscriptionEnabled rebuilds platform views ---

func TestScheduler_SetSubscriptionEnabled_RebuildsPlatformViews(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Provider", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)

	// Platform with regex matching the tag "us-node" via sub name "Provider".
	plat := platform.NewPlatform("p1", "US", []*regexp.Regexp{regexp.MustCompile("us")}, nil)
	pool.RegisterPlatform(plat)

	raw := json.RawMessage(`{"type":"shadowsocks","server":"1.1.1.1","server_port":443}`)
	h := node.HashFromRawOptions(raw)

	// Set up managed nodes and make the node fully routable.
	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"us-node"}})
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(h, raw, "s1")
	entry, _ := pool.GetEntry(h)
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	pool.RecordResult(h, true)

	// Trigger rebuild so node appears in view.
	pool.RebuildAllPlatforms()
	if plat.View().Size() != 1 {
		t.Fatalf("expected node in view while enabled, got %d", plat.View().Size())
	}

	// Create scheduler for SetSubscriptionEnabled.
	sched := newTestScheduler(subMgr, pool, nil)

	// Disable → node should disappear from platform view.
	sched.SetSubscriptionEnabled(sub, false)
	if sub.Enabled() {
		t.Fatal("sub should be disabled")
	}
	if plat.View().Size() != 0 {
		t.Fatalf("expected 0 nodes in view after disable, got %d", plat.View().Size())
	}

	// Re-enable → node should reappear.
	sched.SetSubscriptionEnabled(sub, true)
	if !sub.Enabled() {
		t.Fatal("sub should be re-enabled")
	}
	if plat.View().Size() != 1 {
		t.Fatalf("expected node in view after re-enable, got %d", plat.View().Size())
	}
}

func TestScheduler_SetSubscriptionEnabled_RebuildsPlatformViews_EmptyRegex(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Provider", "http://example.com", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)

	// Empty regex should still require at least one enabled subscription.
	plat := platform.NewPlatform("p1", "All", nil, nil)
	pool.RegisterPlatform(plat)

	raw := json.RawMessage(`{"type":"shadowsocks","server":"1.1.1.1","server_port":443}`)
	h := node.HashFromRawOptions(raw)

	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node-a"}})
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(h, raw, "s1")
	entry, _ := pool.GetEntry(h)
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	pool.RecordResult(h, true)

	pool.RebuildAllPlatforms()
	if plat.View().Size() != 1 {
		t.Fatalf("expected node in view while enabled, got %d", plat.View().Size())
	}

	sched := newTestScheduler(subMgr, pool, nil)

	sched.SetSubscriptionEnabled(sub, false)
	if plat.View().Size() != 0 {
		t.Fatalf("expected 0 nodes in view after disable with empty regex, got %d", plat.View().Size())
	}

	sched.SetSubscriptionEnabled(sub, true)
	if plat.View().Size() != 1 {
		t.Fatalf("expected node in view after re-enable with empty regex, got %d", plat.View().Size())
	}
}

func TestScheduler_SetSubscriptionEnabled_ReenabledCallbackForActiveNodesOnly(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Provider", "http://example.com", false, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)

	rawLive := json.RawMessage(`{"type":"shadowsocks","server":"1.1.1.1","server_port":443}`)
	hLive := node.HashFromRawOptions(rawLive)
	rawStale := json.RawMessage(`{"type":"shadowsocks","server":"2.2.2.2","server_port":443}`)
	hStale := node.HashFromRawOptions(rawStale)

	mn := subscription.NewManagedNodes()
	mn.StoreNode(hLive, subscription.ManagedNode{Tags: []string{"live"}})
	mn.StoreNode(hStale, subscription.ManagedNode{Tags: []string{"stale"}}) // not in pool
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(hLive, rawLive, sub.ID)

	var got []node.Hash
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		OnSubReenabledNode: func(h node.Hash, _ *node.NodeEntry) {
			got = append(got, h)
		},
	})

	sched.SetSubscriptionEnabled(sub, true)

	if len(got) != 1 {
		t.Fatalf("reenabled callback count = %d, want 1", len(got))
	}
	if got[0] != hLive {
		t.Fatalf("reenabled callback hash = %s, want %s", got[0].Hex(), hLive.Hex())
	}
}

func TestScheduler_SetSubscriptionEnabled_ReenabledOutboundPublishesNode(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-reenable-view", "Provider", "http://example.com", false, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	plat := platform.NewPlatform("p-reenable-view", "Reenable", nil, nil)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	raw := json.RawMessage(`{"type":"shadowsocks","server":"1.1.1.1","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	mn := subscription.NewManagedNodes()
	mn.StoreNode(hash, subscription.ManagedNode{Tags: []string{"ready"}})
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(hash, raw, sub.ID)
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	if !pool.RecordResultForEntry(hash, entry, true) {
		t.Fatal("failed to mark node healthy")
	}
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	pool.RebuildAllPlatforms()
	if got := plat.View().Size(); got != 0 {
		t.Fatalf("disabled node view size = %d, want 0", got)
	}

	outboundMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		OnSubReenabledNode: func(h node.Hash, expected *node.NodeEntry) {
			outboundMgr.EnsureNodeOutboundForEntry(h, expected)
		},
	})

	sched.SetSubscriptionEnabled(sub, true)

	if !entry.HasOutbound() {
		t.Fatal("reenabled node did not receive outbound")
	}
	if got := plat.View().Size(); got != 1 {
		t.Fatalf("reenabled node view size = %d, want 1", got)
	}
	if !plat.View().Contains(hash) {
		t.Fatalf("reenabled node %s missing from platform view", hash.Hex())
	}
}

func TestScheduler_ReenabledRuntimePreparationDoesNotCrossEntryGeneration(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-reenable-generation", "Provider", "http://example.com", false, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"reenable-generation","server":"198.51.100.77","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	managed := subscription.NewManagedNodes()
	managed.StoreNode(hash, subscription.ManagedNode{Tags: []string{"reenable-generation"}})
	sub.SwapManagedNodes(managed)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	entryA, ok := pool.GetEntry(hash)
	if !ok || entryA == nil {
		t.Fatal("failed to create entry A")
	}

	var callbackCount atomic.Int32
	scheduler := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		OnSubReenabledNode: func(node.Hash, *node.NodeEntry) {
			callbackCount.Add(1)
		},
	})
	scheduler.beforeReenabledRuntimeHook = func() {
		pool.RemoveNodeFromSub(hash, sub.ID)
		pool.AddNodeFromSub(hash, raw, sub.ID)
		entryB, ok := pool.GetEntry(hash)
		if !ok || entryB == nil || entryB == entryA {
			t.Fatalf("same-hash replacement did not create entry B")
		}
	}

	scheduler.SetSubscriptionEnabled(sub, true)
	if got := callbackCount.Load(); got != 0 {
		t.Fatalf("stale re-enable callback count = %d, want 0", got)
	}
}

func TestScheduler_SetSubscriptionEnabled_ReenabledCallbackSkipsAlreadyEnabledSharedNodes(t *testing.T) {
	subMgr := NewSubscriptionManager()
	subA := subscription.NewSubscription("s-a", "Provider-A", "http://example.com/a", false, false)
	subB := subscription.NewSubscription("s-b", "Provider-B", "http://example.com/b", true, false)
	subMgr.Register(subA)
	subMgr.Register(subB)

	pool := newTestPool(subMgr)

	rawShared := json.RawMessage(`{"type":"shadowsocks","server":"1.1.1.1","server_port":443}`)
	hShared := node.HashFromRawOptions(rawShared)
	rawRecovered := json.RawMessage(`{"type":"shadowsocks","server":"2.2.2.2","server_port":443}`)
	hRecovered := node.HashFromRawOptions(rawRecovered)

	managedA := subscription.NewManagedNodes()
	managedA.StoreNode(hShared, subscription.ManagedNode{Tags: []string{"shared"}})
	managedA.StoreNode(hRecovered, subscription.ManagedNode{Tags: []string{"recovered"}})
	subA.SwapManagedNodes(managedA)

	managedB := subscription.NewManagedNodes()
	managedB.StoreNode(hShared, subscription.ManagedNode{Tags: []string{"shared-b"}})
	subB.SwapManagedNodes(managedB)

	pool.AddNodeFromSub(hShared, rawShared, subA.ID)
	pool.AddNodeFromSub(hShared, rawShared, subB.ID)
	pool.AddNodeFromSub(hRecovered, rawRecovered, subA.ID)

	var got []node.Hash
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		OnSubReenabledNode: func(h node.Hash, _ *node.NodeEntry) {
			got = append(got, h)
		},
	})

	sched.SetSubscriptionEnabled(subA, true)

	if len(got) != 1 {
		t.Fatalf("reenabled callback count = %d, want 1", len(got))
	}
	if got[0] != hRecovered {
		t.Fatalf("reenabled callback hash = %s, want %s", got[0].Hex(), hRecovered.Hex())
	}
}
