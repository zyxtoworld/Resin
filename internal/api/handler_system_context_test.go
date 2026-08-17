package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
)

type eofSignalBody struct {
	data []byte
	done chan struct{}
}

func (b *eofSignalBody) Read(p []byte) (int, error) {
	if len(b.data) != 0 {
		n := copy(p, b.data)
		b.data = b.data[n:]
		return n, nil
	}
	select {
	case <-b.done:
	default:
		close(b.done)
	}
	return 0, io.EOF
}

func (b *eofSignalBody) Close() error { return nil }

func TestPatchSystemConfigHandlerStopsOnCanceledRequestDuringSQLiteLock(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	runtimeCfg := &atomic.Pointer[config.RuntimeConfig]{}
	runtimeCfg.Store(config.NewDefaultRuntimeConfig())
	cp := &service.ControlPlaneService{Engine: engine, RuntimeCfg: runtimeCfg}
	if _, err := cp.PatchRuntimeConfig([]byte(`{"request_log_enabled":true}`)); err != nil {
		t.Fatalf("seed PatchRuntimeConfig: %v", err)
	}

	blocker, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("UPDATE system_config SET updated_at_ns = updated_at_ns WHERE id = 1"); err != nil {
		tx.Rollback()
		blocker.Close()
		t.Fatalf("hold system_config write lock: %v", err)
	}
	release := func() {
		_ = tx.Rollback()
		_ = blocker.Close()
	}
	defer release()

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bodyRead := make(chan struct{})
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/system/config", &eofSignalBody{
		data: []byte(`{"request_log_enabled":false}`),
		done: bodyRead,
	}).WithContext(requestCtx)
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		HandlePatchSystemConfig(cp).ServeHTTP(rec, req)
		close(handlerDone)
	}()

	select {
	case <-bodyRead:
	case <-time.After(time.Second):
		release()
		t.Fatalf("handler did not finish reading request body")
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
}
