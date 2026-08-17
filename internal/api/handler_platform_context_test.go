package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
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
