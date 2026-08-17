package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/topology"
)

func TestCreatePlatformHandlerStopsOnCanceledRequestDuringSQLiteLock(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &service.ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:              time.Hour,
			DefaultPlatformReverseProxyMissAction: "TREAT_AS_EMPTY",
			DefaultPlatformAllocationPolicy:       "BALANCED",
		},
	}

	seedName := "platform-lock-seed"
	if _, err := cp.CreatePlatform(service.CreatePlatformRequest{Name: &seedName}); err != nil {
		t.Fatalf("seed CreatePlatform: %v", err)
	}

	blocker, err := state.OpenDB(filepath.Join(root, "state", "state.db"))
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		_ = blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("UPDATE platforms SET updated_at_ns = updated_at_ns WHERE name = ?", seedName); err != nil {
		_ = tx.Rollback()
		_ = blocker.Close()
		t.Fatalf("hold platforms write lock: %v", err)
	}
	release := func() {
		_ = tx.Rollback()
		_ = blocker.Close()
	}
	defer release()

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bodyRead := make(chan struct{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/platforms", &eofSignalBody{
		data: []byte(`{"name":"canceled-platform-create"}`),
		done: bodyRead,
	}).WithContext(requestCtx)
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		HandleCreatePlatform(cp).ServeHTTP(rec, req)
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

	platforms, err := engine.ListPlatforms()
	if err != nil {
		t.Fatalf("ListPlatforms: %v", err)
	}
	for _, p := range platforms {
		if p.Name == "canceled-platform-create" {
			t.Fatal("canceled platform create left a persisted row")
		}
	}
	if _, ok := pool.GetPlatformByName("canceled-platform-create"); ok {
		t.Fatal("canceled platform create left a runtime platform")
	}
}

func TestRebuildPlatformHandlerStopsOnCanceledRuntimeBatchRead(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	platformID := "00000000-0000-0000-0000-000000000001"
	if err := pool.RegisterPlatform(platform.NewPlatform(platformID, "rebuild-cancel", nil, nil)); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	cp := &service.ControlPlaneService{Pool: pool}

	mutationEntered := make(chan struct{})
	allowMutation := make(chan struct{})
	mutationDone := make(chan struct{})
	var releaseOnce sync.Once
	releaseMutation := func() { releaseOnce.Do(func() { close(allowMutation) }) }
	t.Cleanup(func() {
		releaseMutation()
		select {
		case <-mutationDone:
		case <-time.After(time.Second):
			t.Error("runtime mutation owner did not finish during cleanup")
		}
	})

	go func() {
		pool.WithRuntimeMutation(func() {
			close(mutationEntered)
			<-allowMutation
		})
		close(mutationDone)
	}()
	select {
	case <-mutationEntered:
	case <-time.After(time.Second):
		t.Fatal("runtime mutation did not acquire its write owner")
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/platforms/"+platformID+"/actions/rebuild-routable-view",
		nil,
	).WithContext(requestCtx)
	req.SetPathValue("id", platformID)
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		HandleRebuildPlatform(cp).ServeHTTP(rec, req)
		close(handlerDone)
	}()

	cancel()
	select {
	case <-handlerDone:
		if rec.Code == http.StatusOK {
			t.Fatalf("canceled rebuild returned success")
		}
	case <-time.After(250 * time.Millisecond):
		releaseMutation()
		<-handlerDone
		t.Fatal("canceled rebuild remained blocked on runtime batch read owner")
	}

	releaseMutation()
	select {
	case <-mutationDone:
	case <-time.After(time.Second):
		t.Fatal("runtime mutation did not finish after release")
	}
}
