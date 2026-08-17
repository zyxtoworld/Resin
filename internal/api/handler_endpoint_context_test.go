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
)

func TestUpdateEndpointHandlerStopsOnCanceledRequestDuringSQLiteLock(t *testing.T) {
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
		EnvCfg: &config.EnvConfig{ResinPort: 2260},
	}
	created, err := cp.CreateEndpoint(service.CreateEndpointRequest{
		Port:    32501,
		Enabled: func() *bool { v := false; return &v }(),
	})
	if err != nil {
		t.Fatalf("seed CreateEndpoint: %v", err)
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
	if _, err := tx.Exec("UPDATE endpoints SET updated_at_ns = updated_at_ns WHERE id = ?", created.ID); err != nil {
		_ = tx.Rollback()
		_ = blocker.Close()
		t.Fatalf("hold endpoints write lock: %v", err)
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
		"/api/v1/endpoints/"+created.ID,
		&eofSignalBody{data: []byte(`{"port":32502}`), done: bodyRead},
	).WithContext(requestCtx)
	req.SetPathValue("id", created.ID)
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		HandleUpdateEndpoint(cp).ServeHTTP(rec, req)
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

	got, err := engine.GetEndpoint(created.ID)
	if err != nil {
		t.Fatalf("GetEndpoint: %v", err)
	}
	if got.Port != created.Port {
		t.Fatalf("canceled endpoint update persisted port %d, want %d", got.Port, created.Port)
	}
}
