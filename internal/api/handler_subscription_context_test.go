package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/topology"
)

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
