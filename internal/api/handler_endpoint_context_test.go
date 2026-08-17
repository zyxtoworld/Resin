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
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
)

type blockingEndpointRemovalRuntime struct {
	entered   chan struct{}
	allow     chan struct{}
	enterOnce sync.Once
	allowOnce sync.Once
}

func (r *blockingEndpointRemovalRuntime) PrepareEndpoint(model.Endpoint) (service.EndpointRuntimeStage, error) {
	return nil, nil
}

func (r *blockingEndpointRemovalRuntime) RemoveEndpoint(string) {
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.allow
}

func (r *blockingEndpointRemovalRuntime) EndpointStatus(string) service.EndpointRuntimeStatus {
	return service.EndpointRuntimeStatus{State: "inactive"}
}

func (r *blockingEndpointRemovalRuntime) release() {
	r.allowOnce.Do(func() { close(r.allow) })
}

func TestListEndpointsHandlerStopsOnCanceledRequestWhileDeleteOwnsEndpointLock(t *testing.T) {
	_, cp, _ := newControlPlaneTestServer(t)
	created, err := cp.CreateEndpoint(service.CreateEndpointRequest{
		Port:    32601,
		Enabled: func() *bool { value := false; return &value }(),
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	runtime := &blockingEndpointRemovalRuntime{
		entered: make(chan struct{}),
		allow:   make(chan struct{}),
	}
	cp.EndpointRuntime = runtime
	t.Cleanup(runtime.release)

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- cp.DeleteEndpoint(created.ID) }()
	select {
	case <-runtime.entered:
	case <-time.After(time.Second):
		t.Fatal("delete did not reach runtime removal while holding endpoint lock")
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/endpoints", nil).WithContext(requestCtx)
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		HandleListEndpoints(cp).ServeHTTP(rec, req)
		close(handlerDone)
	}()
	cancel()

	returnedBeforeRelease := false
	select {
	case <-handlerDone:
		returnedBeforeRelease = true
	case <-time.After(500 * time.Millisecond):
	}
	runtime.release()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("list handler did not finish after endpoint removal released")
	}
	if !returnedBeforeRelease {
		t.Fatal("canceled list handler remained blocked behind endpoint mutation")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("canceled list handler status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteEndpoint: %v", err)
	}
}

func TestGetEndpointHandlerStopsOnCanceledRequestWhileDeleteOwnsEndpointLock(t *testing.T) {
	_, cp, _ := newControlPlaneTestServer(t)
	created, err := cp.CreateEndpoint(service.CreateEndpointRequest{
		Port:    32602,
		Enabled: func() *bool { value := false; return &value }(),
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	runtime := &blockingEndpointRemovalRuntime{
		entered: make(chan struct{}),
		allow:   make(chan struct{}),
	}
	cp.EndpointRuntime = runtime
	t.Cleanup(runtime.release)

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- cp.DeleteEndpoint(created.ID) }()
	select {
	case <-runtime.entered:
	case <-time.After(time.Second):
		t.Fatal("delete did not reach runtime removal while holding endpoint lock")
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/endpoints/"+created.ID,
		nil,
	).WithContext(requestCtx)
	req.SetPathValue("id", created.ID)
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		HandleGetEndpoint(cp).ServeHTTP(rec, req)
		close(handlerDone)
	}()
	cancel()

	returnedBeforeRelease := false
	select {
	case <-handlerDone:
		returnedBeforeRelease = true
	case <-time.After(500 * time.Millisecond):
	}
	runtime.release()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("get handler did not finish after endpoint removal released")
	}
	if !returnedBeforeRelease {
		t.Fatal("canceled get handler remained blocked behind endpoint mutation")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("canceled get handler status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteEndpoint: %v", err)
	}
}

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
