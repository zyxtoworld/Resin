package service

import (
	"encoding/json"
	"net/netip"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

func TestSubscriptionReadsDoNotMixConfigAndManagedNodeGenerations(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}

	subMgr := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 4,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	oldContent := `{"outbounds":[{"type":"shadowsocks","tag":"old","server":"1.1.1.1","server_port":443}]}`
	newContent := `{"outbounds":[{"type":"shadowsocks","tag":"new-a","server":"2.2.2.2","server_port":443},{"type":"vmess","tag":"new-b","server":"3.3.3.3","server_port":443}]}`
	sub := subscription.NewSubscription("sub-response-generation", "generation", "", true, false)
	sub.SetSourceType(subscription.SourceTypeLocal)
	sub.SetContent(oldContent)
	sub.SetFetchConfig("", time.Minute.Nanoseconds())
	subMgr.Register(sub)
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               sub.ID,
		Name:             sub.Name(),
		SourceType:       sub.SourceType(),
		Content:          oldContent,
		UpdateIntervalNs: sub.UpdateIntervalNs(),
		Enabled:          sub.Enabled(),
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		_ = closer.Close()
		t.Fatalf("seed subscription: %v", err)
	}

	updated := make(chan struct{}, 4)
	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		OnSubUpdated: func(*subscription.Subscription) {
			updated <- struct{}{}
		},
	})
	releaseRead := make(chan struct{})
	var releaseReadOnce sync.Once
	t.Cleanup(func() {
		releaseReadOnce.Do(func() { close(releaseRead) })
		scheduler.Stop()
		_ = closer.Close()
	})

	if !scheduler.UpdateSubscription(sub) {
		t.Fatal("initial subscription refresh was not admitted")
	}
	select {
	case <-updated:
	case <-time.After(2 * time.Second):
		t.Fatal("initial subscription refresh did not publish")
	}
	if got := sub.ManagedNodes().Size(); got != 1 {
		t.Fatalf("initial managed node count = %d, want 1", got)
	}

	cp := &ControlPlaneService{
		Engine:    engine,
		Pool:      pool,
		SubMgr:    subMgr,
		Scheduler: scheduler,
	}
	readEntered := make(chan struct{})
	var readOnce sync.Once
	cp.beforeRuntimeReadLockHook = func() {
		readOnce.Do(func() {
			close(readEntered)
			<-releaseRead
		})
	}

	getDone := make(chan struct{})
	var got *SubscriptionResponse
	var getErr error
	go func() {
		got, getErr = cp.GetSubscription(sub.ID)
		close(getDone)
	}()
	select {
	case <-readEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("GetSubscription did not reach runtime-read admission")
	}

	patchJSON, err := json.Marshal(map[string]string{"content": newContent})
	if err != nil {
		t.Fatalf("marshal content patch: %v", err)
	}
	updateDone := make(chan error, 1)
	go func() {
		_, updateErr := cp.UpdateSubscription(sub.ID, patchJSON)
		updateDone <- updateErr
	}()
	select {
	case <-updated:
	case <-time.After(2 * time.Second):
		t.Fatal("real subscription PATCH did not publish the new managed-node generation")
	}

	select {
	case <-getDone:
		t.Fatal("GetSubscription returned before its runtime-read admission was released")
	default:
	}
	releaseReadOnce.Do(func() { close(releaseRead) })

	select {
	case <-getDone:
	case <-time.After(2 * time.Second):
		t.Fatal("GetSubscription did not finish after runtime-read admission release")
	}
	if getErr != nil {
		t.Fatalf("GetSubscription: %v", getErr)
	}
	if got == nil {
		t.Fatal("GetSubscription returned nil response")
	}
	oldGeneration := got.Content == oldContent && got.NodeCount == 1
	newGeneration := got.Content == newContent && got.NodeCount == 2
	if !oldGeneration && !newGeneration {
		t.Fatalf("mixed subscription generations: content=%q node_count=%d", got.Content, got.NodeCount)
	}

	select {
	case updateErr := <-updateDone:
		if updateErr != nil {
			t.Fatalf("UpdateSubscription: %v", updateErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("UpdateSubscription did not finish after read admission release")
	}

	thirdContent := `{"outbounds":[{"type":"shadowsocks","tag":"third-a","server":"4.4.4.4","server_port":443},{"type":"vmess","tag":"third-b","server":"5.5.5.5","server_port":443},{"type":"trojan","tag":"third-c","server":"6.6.6.6","server_port":443}]}`
	listReadEntered := make(chan struct{})
	listRelease := make(chan struct{})
	var listReadOnce sync.Once
	cp.beforeRuntimeReadLockHook = func() {
		listReadOnce.Do(func() {
			close(listReadEntered)
			<-listRelease
		})
	}
	listDone := make(chan struct{})
	var listed []SubscriptionResponse
	var listErr error
	go func() {
		listed, listErr = cp.ListSubscriptions(nil)
		close(listDone)
	}()
	select {
	case <-listReadEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("ListSubscriptions did not reach runtime-read admission")
	}
	secondPatchJSON, err := json.Marshal(map[string]string{"content": thirdContent})
	if err != nil {
		t.Fatalf("marshal second content patch: %v", err)
	}
	secondUpdateDone := make(chan error, 1)
	go func() {
		_, updateErr := cp.UpdateSubscription(sub.ID, secondPatchJSON)
		secondUpdateDone <- updateErr
	}()
	select {
	case <-updated:
	case <-time.After(2 * time.Second):
		t.Fatal("second subscription PATCH did not publish the new managed-node generation")
	}
	select {
	case <-listDone:
		t.Fatal("ListSubscriptions returned before its runtime-read admission was released")
	default:
	}
	close(listRelease)
	select {
	case <-listDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ListSubscriptions did not finish after runtime-read admission release")
	}
	if listErr != nil {
		t.Fatalf("ListSubscriptions: %v", listErr)
	}
	if len(listed) != 1 {
		t.Fatalf("ListSubscriptions returned %d rows, want 1", len(listed))
	}
	if got := listed[0]; !((got.Content == newContent && got.NodeCount == 2) || (got.Content == thirdContent && got.NodeCount == 3)) {
		t.Fatalf("ListSubscriptions returned mixed subscription generations: content=%q node_count=%d", got.Content, got.NodeCount)
	}
	select {
	case updateErr := <-secondUpdateDone:
		if updateErr != nil {
			t.Fatalf("second UpdateSubscription: %v", updateErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second UpdateSubscription did not finish after read admission release")
	}
}
