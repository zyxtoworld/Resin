package state

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
)

// newTestEngine sets up a full StateEngine with both DBs in temp dirs.
func newTestEngine(t *testing.T) (*StateEngine, string, string) {
	t.Helper()
	stateDir := t.TempDir()
	cacheDir := t.TempDir()

	engine, closer, err := PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closer.Close() })
	return engine, stateDir, cacheDir
}

// --- Strong persist round-trip ---

func TestEngine_StrongPersist_ConfigSurvivesRestart(t *testing.T) {
	stateDir := t.TempDir()
	cacheDir := t.TempDir()

	// First boot: save config.
	engine1, closer1, err := PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.NewDefaultRuntimeConfig()
	cfg.MaxConsecutiveFailures = 9
	if err := engine1.SaveSystemConfig(cfg, 1, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	closer1.Close()

	// Second boot: config should survive.
	engine2, closer2, err := PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	defer closer2.Close()

	loaded, ver, err := engine2.GetSystemConfig()
	if err != nil {
		t.Fatal(err)
	}
	if ver != 1 || loaded.MaxConsecutiveFailures != 9 {
		t.Fatalf("config did not survive restart: ver=%d, max_failures=%d", ver, loaded.MaxConsecutiveFailures)
	}
}

func TestEngine_StrongPersist_PlatformSurvivesRestart(t *testing.T) {
	stateDir := t.TempDir()
	cacheDir := t.TempDir()

	engine1, closer1, err := PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatal(err)
	}

	p := model.Platform{
		ID: "p1", Name: "MyPlatform", StickyTTLNs: 5000,
		RegexFilters: []string{}, RegionFilters: []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY", AllocationPolicy: "BALANCED",
		UpdatedAtNs: time.Now().UnixNano(),
	}
	if err := engine1.UpsertPlatform(p); err != nil {
		t.Fatal(err)
	}
	closer1.Close()

	engine2, closer2, err := PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	defer closer2.Close()

	got, err := engine2.GetPlatform("p1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "MyPlatform" {
		t.Fatalf("platform did not survive: %+v", got)
	}
}

// --- Weak persist restart test ---

func TestEngine_WeakPersist_CacheDataSurvivesRestart(t *testing.T) {
	stateDir := t.TempDir()
	cacheDir := t.TempDir()

	// First boot: set up state.db refs + flush weak data.
	engine1, closer1, err := PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatal(err)
	}

	// Create required state references for consistency repair to keep our data.
	engine1.UpsertSubscription(model.Subscription{
		ID: "s1", Name: "Sub1", URL: "https://example.com",
		UpdateIntervalNs: 30_000_000_000, Enabled: true, Ephemeral: false,
		EphemeralNodeEvictDelayNs: int64(72 * time.Hour), CreatedAtNs: 1, UpdatedAtNs: 1,
	})
	engine1.UpsertPlatform(model.Platform{
		ID: "p1", Name: "P1", StickyTTLNs: 1000,
		RegexFilters: []string{}, RegionFilters: []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY", AllocationPolicy: "BALANCED",
		UpdatedAtNs: 1,
	})

	// In-memory stores.
	nodeStore := map[string]*model.NodeStatic{
		"n1": {Hash: "n1", RawOptions: json.RawMessage(`{"type":"ss"}`), CreatedAtNs: 100},
	}
	dynamicStore := map[string]*model.NodeDynamic{
		"n1": {Hash: "n1", FailureCount: 5, EgressIP: "10.0.0.1"},
	}
	subNodeStore := map[model.SubscriptionNodeKey]*model.SubscriptionNode{
		{SubscriptionID: "s1", NodeHash: "n1"}: {SubscriptionID: "s1", NodeHash: "n1", Tags: []string{"fast"}},
	}
	latencyStore := map[model.NodeLatencyKey]*model.NodeLatency{
		{NodeHash: "n1", Domain: "google.com"}: {NodeHash: "n1", Domain: "google.com", EwmaNs: 42000, LastUpdatedNs: 999},
	}
	leaseStore := map[model.LeaseKey]*model.Lease{
		{PlatformID: "p1", Account: "user1"}: {PlatformID: "p1", Account: "user1", NodeHash: "n1", CreatedAtNs: 777, ExpiryNs: 99999, LastAccessedNs: 888},
	}

	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nodeStore[h] },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return dynamicStore[h] },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return latencyStore[k] },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return leaseStore[k] },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return subNodeStore[k] },
	}

	engine1.MarkNodeStatic("n1")
	engine1.MarkSubscriptionNode("s1", "n1")
	engine1.MarkNodeDynamic("n1")
	engine1.MarkNodeLatency("n1", "google.com")
	engine1.MarkLease("p1", "user1")
	engine1.FlushDirtySets(readers)
	closer1.Close()

	// Second boot: data should survive restart + consistency repair.
	engine2, closer2, err := PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	defer closer2.Close()

	nodes, _ := engine2.LoadAllNodesStatic()
	if len(nodes) != 1 || nodes[0].Hash != "n1" {
		t.Fatalf("nodes_static did not survive restart: %+v", nodes)
	}

	dyn, _ := engine2.LoadAllNodesDynamic()
	if len(dyn) != 1 || dyn[0].FailureCount != 5 {
		t.Fatalf("nodes_dynamic did not survive restart: %+v", dyn)
	}

	sns, _ := engine2.LoadAllSubscriptionNodes()
	if len(sns) != 1 || !reflect.DeepEqual(sns[0].Tags, []string{"fast"}) {
		t.Fatalf("subscription_nodes did not survive restart: %+v", sns)
	}

	lat, _ := engine2.LoadAllNodeLatency()
	if len(lat) != 1 || lat[0].EwmaNs != 42000 {
		t.Fatalf("node_latency did not survive restart: %+v", lat)
	}

	leases, _ := engine2.LoadAllLeases()
	if len(leases) != 1 || leases[0].Account != "user1" {
		t.Fatalf("leases did not survive restart: %+v", leases)
	}
	if leases[0].CreatedAtNs != 777 {
		t.Fatalf("lease created_at_ns did not survive restart: %+v", leases)
	}
}

// --- Weak persist: dirty mark → flush → verify ---

func TestEngine_WeakPersist_FlushAndLoad(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	// Simulate in-memory store.
	nodeStore := map[string]*model.NodeStatic{
		"hash-a": {Hash: "hash-a", RawOptions: json.RawMessage(`{"type":"ss"}`), CreatedAtNs: 100},
		"hash-b": {Hash: "hash-b", RawOptions: json.RawMessage(`{"type":"vmess"}`), CreatedAtNs: 200},
	}
	subNodeStore := map[model.SubscriptionNodeKey]*model.SubscriptionNode{
		{SubscriptionID: "s1", NodeHash: "hash-a"}: {SubscriptionID: "s1", NodeHash: "hash-a", Tags: []string{"tag1"}},
	}
	dynamicStore := map[string]*model.NodeDynamic{
		"hash-a": {Hash: "hash-a", FailureCount: 2, EgressIP: "1.1.1.1"},
	}

	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nodeStore[h] },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return dynamicStore[h] },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return subNodeStore[k] },
	}

	// Mark dirty.
	engine.MarkNodeStatic("hash-a")
	engine.MarkNodeStatic("hash-b")
	engine.MarkSubscriptionNode("s1", "hash-a")
	engine.MarkNodeDynamic("hash-a")

	if engine.DirtyCount() != 4 {
		t.Fatalf("expected 4 dirty, got %d", engine.DirtyCount())
	}

	// Flush.
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatal(err)
	}

	if engine.DirtyCount() != 0 {
		t.Fatalf("expected 0 dirty after flush, got %d", engine.DirtyCount())
	}

	// Verify in DB.
	nodes, _ := engine.LoadAllNodesStatic()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes in DB, got %d", len(nodes))
	}

	sns, _ := engine.LoadAllSubscriptionNodes()
	if len(sns) != 1 {
		t.Fatalf("expected 1 sub_node, got %d", len(sns))
	}

	dyn, _ := engine.LoadAllNodesDynamic()
	if len(dyn) != 1 || dyn[0].FailureCount != 2 {
		t.Fatalf("unexpected dynamic: %+v", dyn)
	}
}

func TestEngine_WeakPersist_DeleteFlush(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	nodeStore := map[string]*model.NodeStatic{
		"hash-a": {Hash: "hash-a", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 100},
	}

	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nodeStore[h] },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	// Insert first.
	engine.MarkNodeStatic("hash-a")
	engine.FlushDirtySets(readers)

	nodes, _ := engine.LoadAllNodesStatic()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	// Now delete.
	delete(nodeStore, "hash-a")
	engine.MarkNodeStaticDelete("hash-a")
	engine.FlushDirtySets(readers)

	nodes, _ = engine.LoadAllNodesStatic()
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes after delete flush, got %d", len(nodes))
	}
}

func TestEngine_WeakPersist_UpsertMissTreatedAsDelete(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	// Insert a node first.
	nodeStore := map[string]*model.NodeStatic{
		"hash-a": {Hash: "hash-a", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 100},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nodeStore[h] },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	engine.MarkNodeStatic("hash-a")
	engine.FlushDirtySets(readers)

	// Mark upsert but reader returns nil (object deleted from memory between mark and flush).
	delete(nodeStore, "hash-a")
	engine.MarkNodeStatic("hash-a")
	engine.FlushDirtySets(readers)

	nodes, _ := engine.LoadAllNodesStatic()
	if len(nodes) != 0 {
		t.Fatalf("expected upsert-miss to be treated as delete, got %d nodes", len(nodes))
	}
}

// --- Concurrent Mark + Flush + Restart stability ---

func TestEngine_ConcurrentMarkAndFlush(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	var mu sync.Mutex
	nodeStore := make(map[string]*model.NodeStatic)
	for i := 0; i < 100; i++ {
		h := fmt.Sprintf("node-%d", i)
		nodeStore[h] = &model.NodeStatic{Hash: h, RawOptions: json.RawMessage(`{}`), CreatedAtNs: int64(i)}
	}

	readers := CacheReaders{
		ReadNodeStatic: func(h string) *model.NodeStatic {
			mu.Lock()
			defer mu.Unlock()
			return nodeStore[h]
		},
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	var wg sync.WaitGroup

	// Writers: mark dirty concurrently.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				engine.MarkNodeStatic(fmt.Sprintf("node-%d", base*10+j))
			}
		}(i)
	}

	// Flushers: flush concurrently.
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				engine.FlushDirtySets(readers)
			}
		}()
	}

	wg.Wait()

	// Final flush.
	engine.FlushDirtySets(readers)

	// Verify no data loss: all 100 nodes should be in DB.
	nodes, _ := engine.LoadAllNodesStatic()
	if len(nodes) != 100 {
		t.Fatalf("expected 100 nodes, got %d (some lost in concurrent flush)", len(nodes))
	}
}

func TestEngine_FlushWaitsForCompoundDirtyWriteBoundary(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	const hash = "compound-dirty-node"
	if err := engine.CacheRepo.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hash,
		RawOptions:  json.RawMessage(`{"type":"compound"}`),
		CreatedAtNs: 1,
	}}); err != nil {
		t.Fatalf("seed static node: %v", err)
	}
	if err := engine.CacheRepo.BulkUpsertNodesDynamic([]model.NodeDynamic{{
		Hash:         hash,
		FailureCount: 1,
	}}); err != nil {
		t.Fatalf("seed dynamic node: %v", err)
	}

	staticMarked := make(chan struct{})
	allowDynamic := make(chan struct{})
	mutationDone := make(chan bool, 1)
	go func() {
		mutationDone <- engine.WithDirtyWriteAdmission(func(admission *DirtyWriteAdmission) {
			if !admission.MarkNodeStaticDelete(hash) {
				t.Errorf("static delete was not admitted")
			}
			close(staticMarked)
			<-allowDynamic
			if !admission.MarkNodeDynamicDelete(hash) {
				t.Errorf("dynamic delete was not admitted")
			}
		})
	}()
	select {
	case <-staticMarked:
	case <-time.After(time.Second):
		t.Fatal("compound dirty mutation did not reach its first mark")
	}

	readers := CacheReaders{
		ReadNodeStatic:       func(string) *model.NodeStatic { return nil },
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	flushDone := make(chan error, 1)
	go func() { flushDone <- engine.FlushDirtySets(readers) }()
	select {
	case err := <-flushDone:
		t.Fatalf("flush crossed an active compound dirty mutation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(allowDynamic)
	select {
	case ok := <-mutationDone:
		if !ok {
			t.Fatal("compound dirty mutation was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("compound dirty mutation did not finish")
	}
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("flush after compound mutation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("flush did not finish after compound mutation")
	}

	static, err := engine.CacheRepo.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("load static nodes: %v", err)
	}
	if len(static) != 0 {
		t.Fatalf("static delete was not flushed atomically: %+v", static)
	}
	dynamic, err := engine.CacheRepo.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("load dynamic nodes: %v", err)
	}
	if len(dynamic) != 0 {
		t.Fatalf("dynamic delete was not flushed atomically: %+v", dynamic)
	}
}

func TestEngine_ConcurrentFlushesDoNotCommitOlderSnapshotAfterNewer(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	var current atomic.Pointer[model.NodeStatic]
	current.Store(&model.NodeStatic{
		Hash:        "ordered-node",
		RawOptions:  json.RawMessage(`{"version":1}`),
		CreatedAtNs: 1,
	})
	engine.MarkNodeStatic("ordered-node")

	firstRead := make(chan struct{})
	releaseFirstRead := make(chan struct{})
	var readCalls atomic.Int32
	readStatic := func(hash string) *model.NodeStatic {
		if hash != "ordered-node" {
			return nil
		}
		if readCalls.Add(1) == 1 {
			value := current.Load()
			if value == nil {
				return nil
			}
			copy := *value
			copy.RawOptions = append(json.RawMessage(nil), value.RawOptions...)
			close(firstRead)
			<-releaseFirstRead
			return &copy
		}
		value := current.Load()
		if value == nil {
			return nil
		}
		copy := *value
		copy.RawOptions = append(json.RawMessage(nil), value.RawOptions...)
		return &copy
	}
	readers := CacheReaders{
		ReadNodeStatic:       readStatic,
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	firstFlushDone := make(chan struct{})
	go func() {
		if err := engine.FlushDirtySets(readers); err != nil {
			t.Errorf("first flush: %v", err)
		}
		close(firstFlushDone)
	}()
	select {
	case <-firstRead:
	case <-time.After(time.Second):
		t.Fatal("first flush did not reach the controlled reader")
	}

	current.Store(&model.NodeStatic{
		Hash:        "ordered-node",
		RawOptions:  json.RawMessage(`{"version":2}`),
		CreatedAtNs: 2,
	})
	engine.MarkNodeStatic("ordered-node")

	secondFlushDone := make(chan struct{})
	go func() {
		if err := engine.FlushDirtySets(readers); err != nil {
			t.Errorf("second flush: %v", err)
		}
		close(secondFlushDone)
	}()
	select {
	case <-secondFlushDone:
		t.Fatal("newer flush committed while older flush still held the flush owner")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirstRead)

	select {
	case <-firstFlushDone:
	case <-time.After(time.Second):
		t.Fatal("older flush did not finish after release")
	}
	select {
	case <-secondFlushDone:
	case <-time.After(time.Second):
		t.Fatal("newer flush did not finish after older flush released")
	}

	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || string(nodes[0].RawOptions) != `{"version":2}` {
		t.Fatalf("older concurrent flush overwrote newer value: %+v", nodes)
	}
	if got := engine.DirtyCount(); got != 0 {
		t.Fatalf("expected no dirty entries after ordered flushes, got %d", got)
	}
}

func TestEngine_ConcurrentFlushesDoNotCommitOlderNodeTripletAfterNewer(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	const hash = "ordered-node-triplet"

	var currentStatic atomic.Pointer[model.NodeStatic]
	var currentDynamic atomic.Pointer[model.NodeDynamic]
	var currentLatency atomic.Pointer[model.NodeLatency]
	currentStatic.Store(&model.NodeStatic{
		Hash:        hash,
		RawOptions:  json.RawMessage(`{"version":1}`),
		CreatedAtNs: 1,
	})
	currentDynamic.Store(&model.NodeDynamic{
		Hash:         hash,
		FailureCount: 1,
		EgressIP:     "203.0.113.1",
	})
	currentLatency.Store(&model.NodeLatency{
		NodeHash:      hash,
		Domain:        "ordered.example",
		EwmaNs:        100,
		LastUpdatedNs: 10,
	})
	if !engine.MarkNodeStatic(hash) || !engine.MarkNodeDynamic(hash) ||
		!engine.MarkNodeLatency(hash, "ordered.example") {
		t.Fatal("initial triplet dirty marks were rejected")
	}

	firstLatencyRead := make(chan struct{})
	releaseFirstLatencyRead := make(chan struct{})
	var latencyReads atomic.Int32
	readers := CacheReaders{
		ReadNodeStatic: func(readHash string) *model.NodeStatic {
			value := currentStatic.Load()
			if readHash != hash || value == nil {
				return nil
			}
			copy := *value
			copy.RawOptions = append(json.RawMessage(nil), value.RawOptions...)
			return &copy
		},
		ReadNodeDynamic: func(readHash string) *model.NodeDynamic {
			value := currentDynamic.Load()
			if readHash != hash || value == nil {
				return nil
			}
			copy := *value
			return &copy
		},
		ReadNodeLatency: func(key NodeLatencyDirtyKey) *model.NodeLatency {
			value := currentLatency.Load()
			if key.NodeHash != hash || key.Domain != "ordered.example" || value == nil {
				return nil
			}
			copy := *value
			if latencyReads.Add(1) == 1 {
				close(firstLatencyRead)
				<-releaseFirstLatencyRead
			}
			return &copy
		},
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- engine.FlushDirtySets(readers) }()
	select {
	case <-firstLatencyRead:
	case <-time.After(time.Second):
		t.Fatal("first flush did not reach the triplet reader barrier")
	}

	currentStatic.Store(&model.NodeStatic{
		Hash:        hash,
		RawOptions:  json.RawMessage(`{"version":2}`),
		CreatedAtNs: 2,
	})
	currentDynamic.Store(&model.NodeDynamic{
		Hash:         hash,
		FailureCount: 2,
		EgressIP:     "203.0.113.2",
	})
	currentLatency.Store(&model.NodeLatency{
		NodeHash:      hash,
		Domain:        "ordered.example",
		EwmaNs:        200,
		LastUpdatedNs: 20,
	})
	if !engine.MarkNodeStatic(hash) || !engine.MarkNodeDynamic(hash) ||
		!engine.MarkNodeLatency(hash, "ordered.example") {
		t.Fatal("newer triplet dirty marks were rejected")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- engine.FlushDirtySets(readers) }()
	select {
	case err := <-secondDone:
		t.Fatalf("newer flush committed while older triplet read was blocked: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirstLatencyRead)
	if err := <-firstDone; err != nil {
		t.Fatalf("first triplet flush: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second triplet flush: %v", err)
	}

	staticRows, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("load static rows: %v", err)
	}
	dynamicRows, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("load dynamic rows: %v", err)
	}
	latencyRows, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("load latency rows: %v", err)
	}
	if len(staticRows) != 1 || string(staticRows[0].RawOptions) != `{"version":2}` || staticRows[0].CreatedAtNs != 2 {
		t.Fatalf("older static ticket overwrote newer value: %+v", staticRows)
	}
	if len(dynamicRows) != 1 || dynamicRows[0].FailureCount != 2 || dynamicRows[0].EgressIP != "203.0.113.2" {
		t.Fatalf("older dynamic ticket overwrote newer value: %+v", dynamicRows)
	}
	if len(latencyRows) != 1 || latencyRows[0].EwmaNs != 200 || latencyRows[0].LastUpdatedNs != 20 {
		t.Fatalf("older latency ticket overwrote newer value: %+v", latencyRows)
	}
	if got := engine.DirtyCount(); got != 0 {
		t.Fatalf("expected no dirty triplet entries after ordered flushes, got %d", got)
	}
}
