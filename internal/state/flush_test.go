package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
)

func TestCacheRepo_FlushTxContext_DiscardsConnectionWhenBusyTimeoutRestoreFails(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	engine.CacheRepo.db.SetMaxOpenConns(1)
	engine.CacheRepo.beforeContextConnResetHook = func(conn *sql.Conn) {
		if err := conn.Raw(func(driverConn any) error {
			closer, ok := driverConn.(interface{ Close() error })
			if !ok {
				return errors.New("driver connection does not expose Close")
			}
			return closer.Close()
		}); err != nil {
			t.Fatalf("close raw driver connection: %v", err)
		}
	}

	first := model.NodeStatic{Hash: "restore-failure-1", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1}
	if err := engine.CacheRepo.FlushTxContext(context.Background(), FlushOps{
		UpsertNodesStatic: []model.NodeStatic{first},
	}); err != nil {
		t.Fatalf("FlushTxContext: %v", err)
	}

	second := model.NodeStatic{Hash: "restore-failure-2", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 2}
	if err := engine.CacheRepo.FlushTx(FlushOps{
		UpsertNodesStatic: []model.NodeStatic{second},
	}); err != nil {
		t.Fatalf("ordinary FlushTx after failed restore: %v", err)
	}
	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("nodes after failed restore = %+v, want two rows", nodes)
	}
}

func TestCacheRepo_FlushTxContextWaitsForLockReleasedBeforeDeadline(t *testing.T) {
	engine, _, cacheDir := newTestEngine(t)
	blocker, err := OpenDB(filepath.Join(cacheDir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO nodes_static (hash, raw_options_json, created_at_ns) VALUES (?, ?, ?)", "held-until-release", "{}", 1); err != nil {
		tx.Rollback()
		blocker.Close()
		t.Fatalf("blocker lock write: %v", err)
	}

	var releaseOnce sync.Once
	released := make(chan struct{})
	release := func() {
		releaseOnce.Do(func() {
			_ = tx.Rollback()
			_ = blocker.Close()
			close(released)
		})
	}
	defer release()

	beginReady := make(chan struct{})
	allowBegin := make(chan struct{})
	engine.CacheRepo.beforeContextTxBeginHook = func() {
		close(beginReady)
		<-allowBegin
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- engine.CacheRepo.FlushTxContext(ctx, FlushOps{
			UpsertNodesStatic: []model.NodeStatic{{Hash: "waited-node", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 2}},
		})
	}()
	select {
	case <-beginReady:
	case <-time.After(time.Second):
		t.Fatal("flush did not reach transaction begin gate")
	}
	close(allowBegin)
	timer := time.AfterFunc(250*time.Millisecond, release)
	defer timer.Stop()

	select {
	case err := <-done:
		t.Fatalf("flush returned before the held lock was released: %v", err)
	case <-released:
	}
	if err := <-done; err != nil {
		t.Fatalf("FlushTxContext after lock release: %v", err)
	}
	rows, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(rows) != 1 || rows[0].Hash != "waited-node" {
		t.Fatalf("rows after lock release = %+v, want flushed row", rows)
	}
}

func TestCacheRepo_FlushTxContextDeadlineDoesNotLeaveBackgroundWrite(t *testing.T) {
	engine, _, cacheDir := newTestEngine(t)
	blocker, err := OpenDB(filepath.Join(cacheDir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO nodes_static (hash, raw_options_json, created_at_ns) VALUES (?, ?, ?)", "held-past-deadline", "{}", 1); err != nil {
		tx.Rollback()
		blocker.Close()
		t.Fatalf("blocker lock write: %v", err)
	}

	beginReady := make(chan struct{})
	allowBegin := make(chan struct{})
	engine.CacheRepo.beforeContextTxBeginHook = func() {
		close(beginReady)
		<-allowBegin
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- engine.CacheRepo.FlushTxContext(ctx, FlushOps{
			UpsertNodesStatic: []model.NodeStatic{{Hash: "must-not-write", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 2}},
		})
	}()
	select {
	case <-beginReady:
	case <-time.After(time.Second):
		t.Fatal("flush did not reach transaction begin gate")
	}
	close(allowBegin)
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("FlushTxContext error = %v, want context deadline/cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FlushTxContext ignored its deadline")
	}
	_ = tx.Rollback()
	_ = blocker.Close()
	rows, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	for _, row := range rows {
		if row.Hash == "must-not-write" {
			t.Fatalf("canceled FlushTxContext wrote after returning: %+v", row)
		}
	}
}

func TestFlushWorker_ThresholdTriggered(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	nodeStore := map[string]*model.NodeStatic{
		"n1": {Hash: "n1", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1},
		"n2": {Hash: "n2", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 2},
		"n3": {Hash: "n3", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 3},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nodeStore[h] },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	// Threshold = 2, interval very long, check tick short.
	w := NewCacheFlushWorker(
		engine,
		readers,
		func() int { return 2 },
		func() time.Duration { return 1 * time.Hour },
		50*time.Millisecond,
	)
	w.Start()

	// Mark 3 entries (above threshold of 2).
	engine.MarkNodeStatic("n1")
	engine.MarkNodeStatic("n2")
	engine.MarkNodeStatic("n3")

	// Wait for flush cycle.
	time.Sleep(300 * time.Millisecond)

	// Check: dirty count should be 0 (flushed).
	if dc := engine.DirtyCount(); dc != 0 {
		t.Fatalf("expected dirty count 0 after threshold flush, got %d", dc)
	}

	// Verify in DB.
	nodes, _ := engine.LoadAllNodesStatic()
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes in DB, got %d", len(nodes))
	}

	w.Stop()
}

func TestFlushWorker_PeriodicTriggered(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	nodeStore := map[string]*model.NodeStatic{
		"n1": {Hash: "n1", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nodeStore[h] },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	// Threshold very high (won't trigger), interval short (will trigger).
	w := NewCacheFlushWorker(
		engine,
		readers,
		func() int { return 10000 },
		func() time.Duration { return 100 * time.Millisecond },
		50*time.Millisecond,
	)
	w.Start()

	// Mark 1 entry (below threshold of 10000).
	engine.MarkNodeStatic("n1")

	// Wait longer than interval for periodic flush.
	time.Sleep(400 * time.Millisecond)

	if dc := engine.DirtyCount(); dc != 0 {
		t.Fatalf("expected dirty count 0 after periodic flush, got %d", dc)
	}

	w.Stop()
}

func TestFlushWorker_SkipsEmptyDirty(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nil },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	// Very short interval — if not skipping empty, would spam flushes.
	w := NewCacheFlushWorker(
		engine,
		readers,
		func() int { return 1 },
		func() time.Duration { return 10 * time.Millisecond },
		5*time.Millisecond,
	)
	w.Start()

	// No dirty marks. Let it run a few cycles.
	time.Sleep(100 * time.Millisecond)

	// Still 0 dirty.
	if dc := engine.DirtyCount(); dc != 0 {
		t.Fatalf("expected 0, got %d", dc)
	}

	w.Stop()
}

func TestFlushWorker_StopFinalFlush(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	nodeStore := map[string]*model.NodeStatic{
		"n1": {Hash: "n1", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nodeStore[h] },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	// Very high threshold + very long interval → won't auto-flush.
	w := NewCacheFlushWorker(
		engine,
		readers,
		func() int { return 10000 },
		func() time.Duration { return 1 * time.Hour },
		50*time.Millisecond,
	)
	w.Start()

	engine.MarkNodeStatic("n1")
	time.Sleep(100 * time.Millisecond)

	// Still dirty.
	if dc := engine.DirtyCount(); dc != 1 {
		t.Fatalf("expected 1 dirty before stop, got %d", dc)
	}

	// Stop should trigger final flush.
	w.Stop()

	if dc := engine.DirtyCount(); dc != 0 {
		t.Fatalf("expected 0 dirty after stop (final flush), got %d", dc)
	}

	nodes, _ := engine.LoadAllNodesStatic()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after final flush, got %d", len(nodes))
	}
}

func TestFlushWorker_StopContextHonorsDeadlineDuringFinalFlush(t *testing.T) {
	engine, _, cacheDir := newTestEngine(t)
	nodeStore := map[string]*model.NodeStatic{
		"blocked-final-flush": {Hash: "blocked-final-flush", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(hash string) *model.NodeStatic { return nodeStore[hash] },
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 10000 }, func() time.Duration { return time.Hour }, time.Hour)
	if !engine.MarkNodeStatic("blocked-final-flush") {
		t.Fatal("initial dirty mark was rejected")
	}

	blocker, err := OpenDB(filepath.Join(cacheDir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO nodes_static (hash, raw_options_json, created_at_ns) VALUES (?, ?, ?)", "held-lock", "{}", 1); err != nil {
		tx.Rollback()
		blocker.Close()
		t.Fatalf("blocker lock write: %v", err)
	}
	var releaseBlockerOnce sync.Once
	releaseBlocker := func() {
		releaseBlockerOnce.Do(func() {
			_ = tx.Rollback()
			_ = blocker.Close()
		})
	}
	defer releaseBlocker()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- w.StopContext(ctx) }()

	select {
	case err := <-stopDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext error = %v, want context deadline exceeded", err)
		}
	case <-time.After(250 * time.Millisecond):
		releaseBlocker()
		select {
		case <-stopDone:
		case <-time.After(7 * time.Second):
		}
		t.Fatal("StopContext ignored shutdown deadline during final cache flush")
	}
	releaseBlocker()
	secondDone := make(chan error, 1)
	go func() { secondDone <- w.StopContext(context.Background()) }()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second StopContext error = %v, want completed owner", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second StopContext did not observe the completed stop owner")
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("deadline final flush dirty count = %d, want 0 after owner retry", dirty)
	}

}

func TestFlushWorker_StopContextHonorsDeadlineDuringBlockedFinalReader(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	readers := CacheReaders{
		ReadNodeStatic: func(hash string) *model.NodeStatic {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return &model.NodeStatic{
				Hash:        hash,
				RawOptions:  json.RawMessage(`{}`),
				CreatedAtNs: 1,
			}
		},
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 10000 }, func() time.Duration { return time.Hour }, time.Hour)
	if !engine.MarkNodeStatic("blocked-final-reader") {
		t.Fatal("initial dirty mark was rejected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- w.StopContext(ctx) }()

	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		<-stopDone
		t.Fatal("final flush did not enter the blocking reader")
	}
	<-ctx.Done()

	select {
	case err := <-stopDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext error = %v, want context deadline exceeded", err)
		}
	case <-time.After(time.Second):
		close(release)
		<-stopDone
		t.Fatal("StopContext waited past its deadline for a non-cooperative final reader")
	}
	close(release)
	secondDone := make(chan error, 1)
	go func() { secondDone <- w.StopContext(context.Background()) }()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second StopContext error = %v, want completed owner", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second StopContext did not observe the completed stop owner")
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("canceled final reader dirty count after release = %d, want 0", dirty)
	}
}

func TestFlushWorker_StopContextHonorsDeadlineDuringBlockedRunReader(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	readers := CacheReaders{
		ReadNodeStatic: func(hash string) *model.NodeStatic {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return &model.NodeStatic{
				Hash:        hash,
				RawOptions:  json.RawMessage(`{}`),
				CreatedAtNs: 1,
			}
		},
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 1 }, func() time.Duration { return time.Hour }, time.Millisecond)
	if !engine.MarkNodeStatic("blocked-run-reader") {
		t.Fatal("initial dirty mark was rejected")
	}
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	w.Start()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("periodic flush did not enter the blocking reader")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- w.StopContext(ctx) }()
	<-ctx.Done()
	select {
	case err := <-stopDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext error = %v, want context deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StopContext waited past its deadline for a non-cooperative run reader")
	}

	releaseOnce.Do(func() { close(release) })
	secondDone := make(chan error, 1)
	go func() { secondDone <- w.StopContext(context.Background()) }()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second StopContext error = %v, want completed owner", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second StopContext did not observe the completed stop owner")
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("canceled run reader dirty count after release = %d, want 0", dirty)
	}
}

func TestFlushWorker_StopContextCallersUseOwnContexts(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	readers := CacheReaders{
		ReadNodeStatic: func(hash string) *model.NodeStatic {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return &model.NodeStatic{
				Hash:        hash,
				RawOptions:  json.RawMessage(`{}`),
				CreatedAtNs: 1,
			}
		},
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 10000 }, func() time.Duration { return time.Hour }, time.Hour)
	if !engine.MarkNodeStatic("stop-context-callers") {
		t.Fatal("initial dirty mark was rejected")
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- w.StopContext(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		<-firstDone
		t.Fatal("first stop owner did not enter the blocking reader")
	}

	secondCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() { secondDone <- w.StopContext(secondCtx) }()
	<-secondCtx.Done()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second StopContext error = %v, want its own deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second StopContext did not honor its own deadline")
	}
	select {
	case err := <-firstDone:
		t.Fatalf("first StopContext returned while its reader was blocked: %v", err)
	default:
	}

	close(release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first StopContext: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first StopContext did not finish after reader release")
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("shared stop owner left dirty entries after successful final flush: %d", dirty)
	}
	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Hash != "stop-context-callers" {
		t.Fatalf("shared stop owner final flush result = %+v", nodes)
	}
}

func TestFlushWorker_StopTimeoutStillCompletesFinalPersistence(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	nodeStore := map[string]*model.NodeStatic{
		"timeout-final-persistence": {
			Hash:        "timeout-final-persistence",
			RawOptions:  json.RawMessage(`{}`),
			CreatedAtNs: 1,
		},
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	readers := CacheReaders{
		ReadNodeStatic: func(hash string) *model.NodeStatic {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return nodeStore[hash]
		},
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 10_000 }, func() time.Duration { return time.Hour }, time.Hour)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if !engine.MarkNodeStatic("timeout-final-persistence") {
		t.Fatal("initial dirty mark was rejected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- w.StopContext(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("final flush did not enter the blocking reader")
	}
	<-ctx.Done()
	select {
	case err := <-firstDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first StopContext error = %v, want context deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first StopContext did not honor its caller deadline")
	}

	releaseOnce.Do(func() { close(release) })
	secondDone := make(chan error, 1)
	go func() { secondDone <- w.StopContext(context.Background()) }()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("stop owner error after waiter timeout = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stop owner did not finish after reader release")
	}

	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("final persistence left %d dirty entries", dirty)
	}
	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Hash != "timeout-final-persistence" {
		t.Fatalf("final persistence after waiter timeout = %+v", nodes)
	}
}

func TestFlushWorker_StopContextCanceledBeforeFinalFlushStillPersistsBatch(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	nodeStore := map[string]*model.NodeStatic{
		"canceled-final-flush": {Hash: "canceled-final-flush", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(hash string) *model.NodeStatic { return nodeStore[hash] },
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 10000 }, func() time.Duration { return time.Hour }, time.Hour)
	if !engine.MarkNodeStatic("canceled-final-flush") {
		t.Fatal("initial dirty mark was rejected")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.StopContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("StopContext error = %v, want context canceled", err)
	}
	if err := w.StopContext(context.Background()); err != nil {
		t.Fatalf("stop owner after canceled waiter: %v", err)
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("canceled final flush dirty count = %d, want 0", dirty)
	}
	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Hash != "canceled-final-flush" {
		t.Fatalf("canceled final flush did not write cache row: %+v", nodes)
	}
}

func TestFlushWorker_StopContextConcurrentCallersShareOwner(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	nodeStore := map[string]*model.NodeStatic{
		"concurrent-stop": {Hash: "concurrent-stop", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(hash string) *model.NodeStatic { return nodeStore[hash] },
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 10000 }, func() time.Duration { return time.Hour }, time.Hour)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	engine.beforeDirtyWriteHook = func() {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}
	markDone := make(chan bool, 1)
	go func() { markDone <- engine.MarkNodeStatic("concurrent-stop") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dirty mark did not enter admission")
	}

	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() { firstDone <- w.StopContext(context.Background()) }()
	go func() { secondDone <- w.StopContext(context.Background()) }()
	select {
	case err := <-firstDone:
		t.Fatalf("first StopContext returned while dirty mark was blocked: %v", err)
	case err := <-secondDone:
		t.Fatalf("second StopContext returned while owner was blocked: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case accepted := <-markDone:
		if !accepted {
			t.Fatal("admitted dirty mark was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("dirty mark did not finish")
	}
	for name, done := range map[string]chan error{"first": firstDone, "second": secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s StopContext: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s StopContext did not finish", name)
		}
	}
	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Hash != "concurrent-stop" {
		t.Fatalf("concurrent StopContext lost final flush: %+v", nodes)
	}
}

func TestFlushWorker_StopBeforeStartPerformsFinalFlush(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	nodeStore := map[string]*model.NodeStatic{
		"n1": {Hash: "n1", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nodeStore[h] },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	w := NewCacheFlushWorker(
		engine,
		readers,
		func() int { return 10000 },
		func() time.Duration { return time.Hour },
		50*time.Millisecond,
	)
	engine.MarkNodeStatic("n1")

	// Stop promises a final flush even if the worker has not been started.
	w.Stop()

	if dc := engine.DirtyCount(); dc != 0 {
		t.Fatalf("expected 0 dirty after stop-before-start, got %d", dc)
	}
	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Hash != "n1" {
		t.Fatalf("final stop-before-start flush lost node: %+v", nodes)
	}
}

func TestFlushWorker_StopRejectsLateDirtyWrite(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	readers := CacheReaders{
		ReadNodeStatic:       func(string) *model.NodeStatic { return nil },
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 10000 }, func() time.Duration { return time.Hour }, time.Hour)
	w.Stop()

	if engine.MarkNodeStatic("late-node") {
		t.Fatal("late static dirty write was admitted")
	}
	if engine.MarkNodeDynamic("late-node") {
		t.Fatal("late dynamic dirty write was admitted")
	}
	if engine.MarkNodeLatency("late-node", "example.com") {
		t.Fatal("late latency dirty write was admitted")
	}
	if engine.MarkLease("late-platform", "late-account") {
		t.Fatal("late lease dirty write was admitted")
	}
	if engine.MarkSubscriptionNode("late-subscription", "late-node") {
		t.Fatal("late subscription-node dirty write was admitted")
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("late dirty write polluted closed engine: dirty=%d", dirty)
	}
}

func TestFlushWorker_StopWaitsForAdmittedDirtyWrite(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	nodeStore := map[string]*model.NodeStatic{
		"inflight-node": {Hash: "inflight-node", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(hash string) *model.NodeStatic { return nodeStore[hash] },
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 10000 }, func() time.Duration { return time.Hour }, time.Hour)
	entered := make(chan struct{})
	release := make(chan struct{})
	engine.beforeDirtyWriteHook = func() {
		close(entered)
		<-release
	}
	markDone := make(chan bool, 1)
	go func() { markDone <- engine.MarkNodeStatic("inflight-node") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dirty writer did not pass admission")
	}

	stopDone := make(chan struct{})
	go func() {
		w.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("Stop returned before the admitted dirty write completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case accepted := <-markDone:
		if !accepted {
			t.Fatal("admitted dirty write was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("dirty writer did not finish")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after dirty writer release")
	}
	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Hash != "inflight-node" {
		t.Fatalf("admitted dirty write was not in final flush: %+v", nodes)
	}
}

func TestFlushWorker_StopContextHonorsDeadlineDuringAdmittedDirtyWrite(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	readers := CacheReaders{
		ReadNodeStatic:       func(string) *model.NodeStatic { return nil },
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	w := NewCacheFlushWorker(engine, readers, func() int { return 10000 }, func() time.Duration { return time.Hour }, time.Hour)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	engine.beforeDirtyWriteHook = func() {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}
	markDone := make(chan bool, 1)
	go func() { markDone <- engine.MarkNodeStatic("blocked-dirty-write") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dirty writer did not pass admission")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- w.StopContext(ctx) }()
	<-ctx.Done()
	select {
	case err := <-stopDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext error = %v, want context deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StopContext waited past its deadline for an admitted dirty writer")
	}

	close(release)
	select {
	case accepted := <-markDone:
		if !accepted {
			t.Fatal("admitted dirty write was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("admitted dirty writer did not finish")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- w.StopContext(context.Background()) }()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second StopContext error = %v, want completed owner", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second StopContext did not observe the completed stop owner")
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("canceled stop with admitted dirty write left %d entries, want 0", dirty)
	}
}

func TestFlushWorker_DynamicConfigPulled(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	nodeStore := map[string]*model.NodeStatic{
		"n1": {Hash: "n1", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 1},
	}
	readers := CacheReaders{
		ReadNodeStatic:       func(h string) *model.NodeStatic { return nodeStore[h] },
		ReadNodeDynamic:      func(h string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(k NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(k LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(k SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}

	var threshold atomic.Int64
	threshold.Store(10000)

	w := NewCacheFlushWorker(
		engine,
		readers,
		func() int { return int(threshold.Load()) },
		func() time.Duration { return time.Hour },
		20*time.Millisecond,
	)
	w.Start()
	defer w.Stop()

	engine.MarkNodeStatic("n1")
	time.Sleep(120 * time.Millisecond)
	if dc := engine.DirtyCount(); dc != 1 {
		t.Fatalf("expected dirty count 1 before threshold change, got %d", dc)
	}

	threshold.Store(1)
	time.Sleep(180 * time.Millisecond)
	if dc := engine.DirtyCount(); dc != 0 {
		t.Fatalf("expected dirty count 0 after threshold change, got %d", dc)
	}
}
