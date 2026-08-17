package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

type blockingLeaseReadFixture struct {
	cp         *service.ControlPlaneService
	platformID string
	release    func()
	deleteDone <-chan error
	closer     state.PersistenceCloser
}

func newBlockingLeaseReadFixture(t *testing.T) blockingLeaseReadFixture {
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
	platformID := "11111111-1111-1111-1111-111111111111"
	plat := platform.NewPlatform(platformID, "lease-read", nil, nil)
	if err := pool.RegisterPlatform(plat); err != nil {
		_ = closer.Close()
		t.Fatalf("RegisterPlatform: %v", err)
	}
	router := routing.NewRouter(routing.RouterConfig{Pool: pool})
	cp := &service.ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		SubMgr: subMgr,
		Router: router,
	}

	name := "lease-read-cancel"
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
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"lease-read"}})

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

	return blockingLeaseReadFixture{
		cp:         cp,
		platformID: platformID,
		release: func() {
			releaseOnce.Do(func() { close(allowRemoval) })
		},
		deleteDone: deleteDone,
		closer:     closer,
	}
}

func (f blockingLeaseReadFixture) finish(t *testing.T) {
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

func TestListLeasesHandlerStopsOnCanceledRequestDuringRuntimeMutation(t *testing.T) {
	fixture := newBlockingLeaseReadFixture(t)
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/platforms/"+fixture.platformID+"/leases", nil).WithContext(requestCtx)
	req.SetPathValue("id", fixture.platformID)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		HandleListLeases(fixture.cp).ServeHTTP(rec, req)
		close(done)
	}()

	returnedBeforeRelease := false
	select {
	case <-done:
		returnedBeforeRelease = true
	case <-time.After(time.Second):
	}
	fixture.finish(t)
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("lease list handler did not finish after runtime mutation release")
	}
	if !returnedBeforeRelease {
		t.Fatal("canceled lease list handler remained blocked by runtime mutation")
	}
}

func TestGetLeaseHandlerStopsOnCanceledRequestDuringRuntimeMutation(t *testing.T) {
	fixture := newBlockingLeaseReadFixture(t)
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/platforms/"+fixture.platformID+"/leases/account", nil).WithContext(requestCtx)
	req.SetPathValue("id", fixture.platformID)
	req.SetPathValue("account", "account")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		HandleGetLease(fixture.cp).ServeHTTP(rec, req)
		close(done)
	}()

	returnedBeforeRelease := false
	select {
	case <-done:
		returnedBeforeRelease = true
	case <-time.After(time.Second):
	}
	fixture.finish(t)
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("lease get handler did not finish after runtime mutation release")
	}
	if !returnedBeforeRelease {
		t.Fatal("canceled lease get handler remained blocked by runtime mutation")
	}
}

func TestDeleteLeaseHandler_DoesNotMutateCanceledRequest(t *testing.T) {
	srv, cp, _ := newControlPlaneTestServer(t)
	platformID := mustCreatePlatform(t, srv, "delete-lease-cancel")
	now := time.Now().UnixNano()
	lease := model.Lease{
		PlatformID:     platformID,
		Account:        "delete-cancel-account",
		NodeHash:       node.HashFromRawOptions([]byte(`{"type":"delete-cancel-node"}`)).Hex(),
		EgressIP:       "203.0.113.21",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}
	if err := cp.Router.UpsertLease(lease); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(
		http.MethodDelete,
		"/api/v1/platforms/"+platformID+"/leases/"+lease.Account,
		nil,
	).WithContext(requestCtx)
	req.SetPathValue("id", platformID)
	req.SetPathValue("account", lease.Account)
	rec := httptest.NewRecorder()
	HandleDeleteLease(cp).ServeHTTP(rec, req)

	if rec.Code == http.StatusNoContent {
		t.Fatalf("canceled delete request was reported successful: status=%d", rec.Code)
	}
	if got := cp.Router.ReadLease(model.LeaseKey{PlatformID: platformID, Account: lease.Account}); got == nil {
		t.Fatal("canceled delete request removed the lease")
	}
}
