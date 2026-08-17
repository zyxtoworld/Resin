package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

type blockingSubscriptionDeleteFixture struct {
	cp         *service.ControlPlaneService
	id         string
	release    func()
	deleteDone <-chan error
	closer     state.PersistenceCloser
}

func newBlockingSubscriptionDeleteFixture(t *testing.T) blockingSubscriptionDeleteFixture {
	t.Helper()
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}

	subMgr := topology.NewSubscriptionManager()
	removed := make(chan struct{})
	allowRemoval := make(chan struct{})
	var releaseOnce sync.Once
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subMgr.Lookup,
		OnSubNodeChanged: func(_ string, _ node.Hash, added bool) {
			if added {
				return
			}
			close(removed)
			<-allowRemoval
		},
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &service.ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		SubMgr: subMgr,
	}

	name := "subscription-read-cancel"
	url := "https://example.com/subscription"
	created, err := cp.CreateSubscription(service.CreateSubscriptionRequest{
		Name:           &name,
		URL:            &url,
		UpdateInterval: func() *string { v := "1h"; return &v }(),
	})
	if err != nil {
		_ = closer.Close()
		t.Fatalf("seed CreateSubscription: %v", err)
	}
	sub := subMgr.Lookup(created.ID)
	if sub == nil {
		_ = closer.Close()
		t.Fatalf("created subscription is not registered")
	}
	raw := []byte(`{"type":"direct","server":"127.0.0.1","server_port":1}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, created.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"read-cancel"}})

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- cp.DeleteSubscription(created.ID)
	}()
	select {
	case <-removed:
	case <-time.After(time.Second):
		close(allowRemoval)
		_ = closer.Close()
		t.Fatal("DeleteSubscription did not reach the blocking removal callback")
	}

	return blockingSubscriptionDeleteFixture{
		cp: cp,
		id: created.ID,
		release: func() {
			releaseOnce.Do(func() { close(allowRemoval) })
		},
		deleteDone: deleteDone,
		closer:     closer,
	}
}

func (f blockingSubscriptionDeleteFixture) finish(t *testing.T) {
	t.Helper()
	f.release()
	select {
	case err := <-f.deleteDone:
		if err != nil {
			t.Fatalf("DeleteSubscription: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("DeleteSubscription did not finish after removal callback release")
	}
	if err := f.closer.Close(); err != nil {
		t.Fatalf("close persistence: %v", err)
	}
}

func waitForCanceledHandlerBeforeRelease(t *testing.T, done <-chan struct{}) bool {
	t.Helper()
	select {
	case <-done:
		return true
	case <-time.After(time.Second):
		return false
	}
}

func waitForHandlerAfterRelease(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("subscription handler did not finish after deletion release")
	}
}

func TestListSubscriptionsHandlerStopsOnCanceledRequestDuringSubscriptionDelete(t *testing.T) {
	fixture := newBlockingSubscriptionDeleteFixture(t)
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions", nil).WithContext(requestCtx)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		HandleListSubscriptions(fixture.cp).ServeHTTP(rec, req)
		close(done)
	}()

	returnedBeforeRelease := waitForCanceledHandlerBeforeRelease(t, done)
	fixture.finish(t)
	waitForHandlerAfterRelease(t, done)
	if !returnedBeforeRelease {
		t.Fatal("canceled list handler remained blocked by subscription deletion")
	}
}

func TestGetSubscriptionHandlerStopsOnCanceledRequestDuringSubscriptionDelete(t *testing.T) {
	fixture := newBlockingSubscriptionDeleteFixture(t)
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions/"+fixture.id, nil).WithContext(requestCtx)
	req.SetPathValue("id", fixture.id)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		HandleGetSubscription(fixture.cp).ServeHTTP(rec, req)
		close(done)
	}()

	returnedBeforeRelease := waitForCanceledHandlerBeforeRelease(t, done)
	fixture.finish(t)
	waitForHandlerAfterRelease(t, done)
	if !returnedBeforeRelease {
		t.Fatal("canceled get handler remained blocked by subscription deletion")
	}
}

func TestUpdateSubscriptionHandlerStopsOnCanceledRequestDuringSQLiteLock(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	cp := &service.ControlPlaneService{
		Engine: engine,
		SubMgr: topology.NewSubscriptionManager(),
	}
	name := "subscription-lock-seed"
	url := "https://example.com/subscription"
	created, err := cp.CreateSubscription(service.CreateSubscriptionRequest{
		Name: &name,
		URL:  &url,
		UpdateInterval: func() *string {
			v := "1h"
			return &v
		}(),
	})
	if err != nil {
		t.Fatalf("seed CreateSubscription: %v", err)
	}

	blocker, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		_ = blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("UPDATE subscriptions SET updated_at_ns = updated_at_ns WHERE id = ?", created.ID); err != nil {
		_ = tx.Rollback()
		_ = blocker.Close()
		t.Fatalf("hold subscriptions write lock: %v", err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_ = tx.Rollback()
		_ = blocker.Close()
	}
	defer release()

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bodyRead := make(chan struct{})
	req := httptest.NewRequest(
		http.MethodPatch,
		"/api/v1/subscriptions/"+created.ID,
		&eofSignalBody{data: []byte(`{"name":"canceled-subscription"}`), done: bodyRead},
	).WithContext(requestCtx)
	req.SetPathValue("id", created.ID)
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		HandleUpdateSubscription(cp).ServeHTTP(rec, req)
		close(handlerDone)
	}()

	select {
	case <-bodyRead:
	case <-time.After(time.Second):
		release()
		t.Fatal("handler did not finish reading request body")
	}
	cancel()

	returnedBeforeRelease := false
	deadline := time.NewTimer(time.Second)
	select {
	case <-handlerDone:
		returnedBeforeRelease = true
	case <-deadline.C:
	}
	if !deadline.Stop() {
		select {
		case <-deadline.C:
		default:
		}
	}

	release()
	select {
	case <-handlerDone:
	case <-time.After(6 * time.Second):
		t.Fatal("handler did not finish after SQLite lock release")
	}
	if !returnedBeforeRelease {
		t.Fatal("canceled HTTP handler remained blocked on SQLite write lock")
	}

	subscriptions, err := engine.ListSubscriptions()
	if err != nil {
		t.Fatalf("ListSubscriptions: %v", err)
	}
	for _, got := range subscriptions {
		if got.ID == created.ID && got.Name != name {
			t.Fatalf("canceled subscription update persisted name %q, want %q", got.Name, name)
		}
	}
}
