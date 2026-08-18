package service

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/state"
	moderncsqlite "modernc.org/sqlite"
)

var cancelAfterEndpointUpdate struct {
	sync.Mutex
	cancel context.CancelFunc
}

var registerCancelAfterEndpointUpdate sync.Once

func registerCancelAfterEndpointUpdateFunction() {
	registerCancelAfterEndpointUpdate.Do(func() {
		moderncsqlite.MustRegisterDeterministicScalarFunction(
			"resin_test_cancel_after_endpoint_update",
			0,
			func(_ *moderncsqlite.FunctionContext, _ []driver.Value) (driver.Value, error) {
				cancelAfterEndpointUpdate.Lock()
				cancel := cancelAfterEndpointUpdate.cancel
				cancelAfterEndpointUpdate.cancel = nil
				cancelAfterEndpointUpdate.Unlock()
				if cancel != nil {
					cancel()
				}
				return int64(1), nil
			},
		)
	})
}

type endpointRuntimeStub struct {
	endpoints    map[string]model.Endpoint
	failPort     int
	beginCount   int
	prepareCount int
	commitCount  int
	abortCount   int
}

type endpointRuntimeCloseDBOnApplyFailure struct {
	closeDB   func()
	endpoints map[string]model.Endpoint
}

type endpointRuntimeCloseDBStage struct {
	runtime   *endpointRuntimeCloseDBOnApplyFailure
	endpoint  model.Endpoint
	committed bool
	aborted   bool
}

func (s *endpointRuntimeCloseDBStage) BeginPersist() bool {
	return s != nil && !s.aborted && !s.committed
}

func (s *endpointRuntimeCloseDBStage) Abort() {
	if s == nil || s.committed || s.aborted {
		return
	}
	s.aborted = true
}

func (s *endpointRuntimeCloseDBStage) Commit() {
	if s == nil || s.committed || s.aborted {
		return
	}
	s.committed = true
	s.runtime.endpoints[s.endpoint.ID] = s.endpoint
}

func (r *endpointRuntimeCloseDBOnApplyFailure) PrepareEndpoint(endpoint model.Endpoint) (EndpointRuntimeStage, error) {
	// The old fixture used closeDB from ApplyEndpoint. The staged fixture keeps
	// that callback intentionally unused: DB failure must call Abort and release
	// only the unpublished runtime candidate.
	return &endpointRuntimeCloseDBStage{runtime: r, endpoint: endpoint}, nil
}

func (r *endpointRuntimeCloseDBOnApplyFailure) RemoveEndpoint(id string) {
	delete(r.endpoints, id)
}

func (r *endpointRuntimeCloseDBOnApplyFailure) EndpointStatus(id string) EndpointRuntimeStatus {
	if _, ok := r.endpoints[id]; ok {
		return EndpointRuntimeStatus{State: "active"}
	}
	return EndpointRuntimeStatus{State: "inactive"}
}

type legacyEndpointRuntimeCloseDBOnApplyFailure struct {
	closeDB   func()
	endpoints map[string]model.Endpoint
}

func (r *legacyEndpointRuntimeCloseDBOnApplyFailure) ApplyEndpoint(endpoint model.Endpoint) error {
	if endpoint.ID != DefaultEndpointID {
		r.closeDB()
		return errors.New("listener failed and state database became unavailable")
	}
	r.endpoints[endpoint.ID] = endpoint
	return nil
}

func (r *legacyEndpointRuntimeCloseDBOnApplyFailure) RemoveEndpoint(id string) {
	delete(r.endpoints, id)
}

func newEndpointRuntimeStub() *endpointRuntimeStub {
	return &endpointRuntimeStub{endpoints: make(map[string]model.Endpoint)}
}

type endpointRuntimeStageStub struct {
	runtime   *endpointRuntimeStub
	endpoint  model.Endpoint
	committed bool
	aborted   bool
}

type endpointCommitAdmissionRuntime struct {
	base          *endpointRuntimeStub
	commitEntered chan struct{}
	allowCommit   chan struct{}
	enteredOnce   sync.Once
}

type endpointCommitAdmissionStage struct {
	inner EndpointRuntimeStage
	owner *endpointCommitAdmissionRuntime
}

type endpointPrepareAdmissionRuntime struct {
	base           *endpointRuntimeStub
	prepareEntered chan struct{}
	allowPrepare   chan struct{}
	enteredOnce    sync.Once
}

func (r *endpointCommitAdmissionRuntime) PrepareEndpoint(endpoint model.Endpoint) (EndpointRuntimeStage, error) {
	inner, err := r.base.PrepareEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	return &endpointCommitAdmissionStage{inner: inner, owner: r}, nil
}

func (r *endpointCommitAdmissionRuntime) RemoveEndpoint(id string) {
	r.base.RemoveEndpoint(id)
}

func (r *endpointCommitAdmissionRuntime) EndpointStatus(id string) EndpointRuntimeStatus {
	return r.base.EndpointStatus(id)
}

func (r *endpointPrepareAdmissionRuntime) PrepareEndpoint(endpoint model.Endpoint) (EndpointRuntimeStage, error) {
	inner, err := r.base.PrepareEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	r.enteredOnce.Do(func() { close(r.prepareEntered) })
	<-r.allowPrepare
	return inner, nil
}

func (r *endpointPrepareAdmissionRuntime) RemoveEndpoint(id string) {
	r.base.RemoveEndpoint(id)
}

func (r *endpointPrepareAdmissionRuntime) EndpointStatus(id string) EndpointRuntimeStatus {
	return r.base.EndpointStatus(id)
}

func (s *endpointCommitAdmissionStage) BeginPersist() bool {
	return s.inner.BeginPersist()
}

func (s *endpointCommitAdmissionStage) Abort() {
	s.inner.Abort()
}

func (s *endpointCommitAdmissionStage) Commit() {
	s.owner.enteredOnce.Do(func() { close(s.owner.commitEntered) })
	<-s.owner.allowCommit
	s.inner.Commit()
}

func (s *endpointRuntimeStageStub) BeginPersist() bool {
	if s == nil || s.aborted || s.committed {
		return false
	}
	s.runtime.beginCount++
	return true
}

func (s *endpointRuntimeStageStub) Abort() {
	if s == nil || s.committed || s.aborted {
		return
	}
	s.aborted = true
	s.runtime.abortCount++
}

func (s *endpointRuntimeStageStub) Commit() {
	if s == nil || s.committed || s.aborted {
		return
	}
	s.committed = true
	s.runtime.commitCount++
	s.runtime.endpoints[s.endpoint.ID] = s.endpoint
}

func (r *endpointRuntimeStub) PrepareEndpoint(endpoint model.Endpoint) (EndpointRuntimeStage, error) {
	r.prepareCount++
	if endpoint.Port == r.failPort {
		return nil, errors.New("address already in use")
	}
	return &endpointRuntimeStageStub{runtime: r, endpoint: endpoint}, nil
}

func (r *endpointRuntimeStub) RemoveEndpoint(id string) {
	delete(r.endpoints, id)
}

func (r *endpointRuntimeStub) EndpointStatus(id string) EndpointRuntimeStatus {
	if _, ok := r.endpoints[id]; ok {
		return EndpointRuntimeStatus{State: "active"}
	}
	return EndpointRuntimeStatus{State: "inactive"}
}

func newEndpointTestService(t *testing.T, env *config.EnvConfig) (*ControlPlaneService, *endpointRuntimeStub) {
	t.Helper()
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(root, "state"), filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	runtime := newEndpointRuntimeStub()
	cp := &ControlPlaneService{Engine: engine, EnvCfg: env, EndpointRuntime: runtime}
	stage, err := runtime.PrepareEndpoint(cp.defaultEndpoint())
	if err != nil {
		t.Fatalf("prepare default endpoint: %v", err)
	}
	stage.Commit()
	return cp, runtime
}

func seedEndpointForMutationTest(t *testing.T, engine *state.StateEngine, runtime *endpointRuntimeStub, id string, port int) model.Endpoint {
	t.Helper()
	endpoint := model.Endpoint{
		ID:               id,
		Port:             port,
		Enabled:          true,
		AllowProxy:       true,
		AllowHTTPForward: true,
		AllowHTTPReverse: true,
		AllowSOCKS5:      true,
		CreatedAtNs:      time.Now().UnixNano(),
		UpdatedAtNs:      time.Now().UnixNano(),
	}
	if err := engine.InsertEndpoint(endpoint); err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}
	if runtime != nil {
		runtime.endpoints[endpoint.ID] = endpoint
	}
	return endpoint
}

func holdEndpointSQLiteWrite(t *testing.T, stateDir, endpointID string) func() {
	t.Helper()
	blocker, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		_ = blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("UPDATE endpoints SET updated_at_ns = updated_at_ns WHERE id = ?", endpointID); err != nil {
		_ = tx.Rollback()
		_ = blocker.Close()
		t.Fatalf("hold endpoint SQLite write gate: %v", err)
	}
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			_ = tx.Rollback()
			_ = blocker.Close()
		})
	}
	t.Cleanup(release)
	return release
}

func boolPointer(value bool) *bool { return &value }

func TestNewDefaultEndpoint(t *testing.T) {
	endpoint := NewDefaultEndpoint(0)
	if endpoint.ID != DefaultEndpointID || endpoint.Port != 2260 ||
		!endpoint.Enabled ||
		!endpoint.AllowManagement || !endpoint.AllowProxy ||
		!endpoint.AllowHTTPForward || !endpoint.AllowHTTPReverse || !endpoint.AllowSOCKS5 {
		t.Fatalf("default endpoint = %+v", endpoint)
	}
}

func TestControlPlaneEndpoints_CRUDAndDefaultProtection(t *testing.T) {
	cp, runtime := newEndpointTestService(t, &config.EnvConfig{
		ResinPort:   2260,
		AuthVersion: config.AuthVersionV1,
	})

	items, err := cp.ListEndpoints()
	if err != nil {
		t.Fatalf("ListEndpoints: %v", err)
	}
	if len(items) != 1 || items[0].ID != DefaultEndpointID || !items[0].ReadOnly || !items[0].AllowSOCKS5 {
		t.Fatalf("default endpoint = %+v", items)
	}

	created, err := cp.CreateEndpoint(CreateEndpointRequest{
		Port:                 32020,
		AllowManagement:      boolPointer(true),
		AllowProxy:           boolPointer(true),
		RequireProxyAuthInfo: boolPointer(true),
		AllowHTTPForward:     boolPointer(true),
		AllowHTTPReverse:     boolPointer(false),
		AllowSOCKS5:          boolPointer(false),
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	if created.ReadOnly || !created.Enabled || created.Status != "active" || !created.RequireProxyAuthInfo {
		t.Fatalf("created endpoint = %+v", created)
	}
	if runtime.endpoints[created.ID].Port != 32020 {
		t.Fatalf("runtime endpoint = %+v", runtime.endpoints[created.ID])
	}

	updated, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{
		"port": 32021,
		"allow_management": false,
		"require_proxy_auth_info": false,
		"allow_http_reverse": true
	}`))
	if err != nil {
		t.Fatalf("UpdateEndpoint: %v", err)
	}
	if updated.Port != 32021 || updated.AllowManagement || updated.RequireProxyAuthInfo || !updated.AllowHTTPReverse {
		t.Fatalf("updated endpoint = %+v", updated)
	}

	if _, err := cp.UpdateEndpoint(DefaultEndpointID, json.RawMessage(`{"port": 32022}`)); err == nil {
		t.Fatal("UpdateEndpoint(default) succeeded, want conflict")
	} else {
		assertServiceErrorCode(t, err, "CONFLICT")
	}
	if err := cp.DeleteEndpoint(DefaultEndpointID); err == nil {
		t.Fatal("DeleteEndpoint(default) succeeded, want conflict")
	} else {
		assertServiceErrorCode(t, err, "CONFLICT")
	}

	if err := cp.DeleteEndpoint(created.ID); err != nil {
		t.Fatalf("DeleteEndpoint: %v", err)
	}
	if _, ok := runtime.endpoints[created.ID]; ok {
		t.Fatal("runtime endpoint still exists after delete")
	}
	if _, err := cp.GetEndpoint(created.ID); err == nil {
		t.Fatal("GetEndpoint after delete succeeded")
	} else {
		assertServiceErrorCode(t, err, "NOT_FOUND")
	}
}

func TestCreateEndpoint_StateWriteAdmissionCoversRuntimeCommit(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(root, "state"), filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	runtime := &endpointCommitAdmissionRuntime{
		base:          newEndpointRuntimeStub(),
		commitEntered: make(chan struct{}),
		allowCommit:   make(chan struct{}),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(runtime.allowCommit) }) })
	cp := &ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}

	createDone := make(chan error, 1)
	go func() {
		_, createErr := cp.CreateEndpoint(CreateEndpointRequest{Port: 32070})
		createDone <- createErr
	}()
	select {
	case <-runtime.commitEntered:
	case <-time.After(time.Second):
		t.Fatal("CreateEndpoint did not reach the post-persist Commit boundary")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- engine.CloseStateWriteAdmissionAndWait(closeCtx) }()
	select {
	case err := <-closeDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown crossed the admitted endpoint commit: got %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown wait did not honor its deadline")
	}

	releaseOnce.Do(func() { close(runtime.allowCommit) })
	if err := <-createDone; err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	if err := engine.CloseStateWriteAdmissionAndWait(context.Background()); err != nil {
		t.Fatalf("final CloseStateWriteAdmissionAndWait: %v", err)
	}
}

func TestCreateEndpointContext_CancellationWhileEndpointMutationHeld(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(root, "state"), filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	runtime := &endpointCommitAdmissionRuntime{
		base:          newEndpointRuntimeStub(),
		commitEntered: make(chan struct{}),
		allowCommit:   make(chan struct{}),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(runtime.allowCommit) }) })
	cp := &ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}
	secondBeforeLock := make(chan struct{})
	var beforeLockCalls atomic.Int32
	cp.beforeEndpointLockHook = func() {
		if beforeLockCalls.Add(1) == 2 {
			close(secondBeforeLock)
		}
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := cp.CreateEndpoint(CreateEndpointRequest{Port: 32070})
		firstDone <- err
	}()
	select {
	case <-runtime.commitEntered:
	case <-time.After(time.Second):
		t.Fatal("first endpoint mutation did not reach runtime commit")
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := cp.CreateEndpointContext(secondCtx, CreateEndpointRequest{Port: 32071})
		secondDone <- err
	}()
	select {
	case <-secondBeforeLock:
	case <-time.After(time.Second):
		cancelSecond()
		releaseOnce.Do(func() { close(runtime.allowCommit) })
		<-firstDone
		t.Fatal("second endpoint mutation did not reach the lock boundary")
	}
	cancelSecond()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled endpoint mutation error = %v, want context.Canceled", err)
		}
	case <-time.After(200 * time.Millisecond):
		releaseOnce.Do(func() { close(runtime.allowCommit) })
		<-firstDone
		t.Fatal("canceled endpoint mutation remained blocked by endpoint mutex")
	}

	releaseOnce.Do(func() { close(runtime.allowCommit) })
	if err := <-firstDone; err != nil {
		t.Fatalf("first endpoint mutation: %v", err)
	}

	endpoints, err := engine.ListEndpoints()
	if err != nil {
		t.Fatalf("ListEndpoints: %v", err)
	}
	if len(endpoints) != 1 || endpoints[0].Port != 32070 {
		t.Fatalf("canceled endpoint mutation changed persisted endpoints: %+v", endpoints)
	}
}

func TestCreateEndpointContext_CancellationAfterBeginPersistCommitsDBAndRuntime(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	engine, closer, err := state.PersistenceBootstrap(stateDir, filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	runtime := newEndpointRuntimeStub()
	seedEndpointForMutationTest(t, engine, runtime, "endpoint-create-gate", 32080)
	releaseDB := holdEndpointSQLiteWrite(t, stateDir, "endpoint-create-gate")

	beginEntered := make(chan struct{})
	allowPersist := make(chan struct{})
	var hookOnce sync.Once
	cp := &ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}
	cp.afterEndpointBeginPersistHook = func() {
		hookOnce.Do(func() { close(beginEntered) })
		<-allowPersist
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		response *EndpointResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, err := cp.CreateEndpointContext(requestCtx, CreateEndpointRequest{Port: 32081})
		done <- result{response: response, err: err}
	}()
	select {
	case <-beginEntered:
	case <-time.After(time.Second):
		releaseDB()
		close(allowPersist)
		t.Fatal("create did not cross BeginPersist boundary")
	}
	cancel()
	close(allowPersist)
	releaseDB()

	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("create canceled after BeginPersist: %v", result.err)
		}
		if result.response == nil || !result.response.Enabled {
			t.Fatalf("create response after cancellation = %+v", result.response)
		}
		if _, ok := runtime.endpoints[result.response.ID]; !ok {
			t.Fatalf("runtime missing committed endpoint %q: %+v", result.response.ID, runtime.endpoints)
		}
		if _, err := engine.GetEndpoint(result.response.ID); err != nil {
			t.Fatalf("persisted committed endpoint %q: %v", result.response.ID, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("create did not finish after SQLite gate release")
	}
}

func TestUpdateEndpointContext_CancellationAfterBeginPersistCommitsDBAndRuntime(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	engine, closer, err := state.PersistenceBootstrap(stateDir, filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	runtime := newEndpointRuntimeStub()
	seed := seedEndpointForMutationTest(t, engine, runtime, "endpoint-update-gate", 32082)
	releaseDB := holdEndpointSQLiteWrite(t, stateDir, seed.ID)

	beginEntered := make(chan struct{})
	allowPersist := make(chan struct{})
	var hookOnce sync.Once
	cp := &ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}
	cp.afterEndpointBeginPersistHook = func() {
		hookOnce.Do(func() { close(beginEntered) })
		<-allowPersist
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		response *EndpointResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, err := cp.UpdateEndpointContext(requestCtx, seed.ID, json.RawMessage(`{"port":32083}`))
		done <- result{response: response, err: err}
	}()
	select {
	case <-beginEntered:
	case <-time.After(time.Second):
		releaseDB()
		close(allowPersist)
		t.Fatal("update did not cross BeginPersist boundary")
	}
	cancel()
	close(allowPersist)
	releaseDB()

	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("update canceled after BeginPersist: %v", result.err)
		}
		if result.response == nil || result.response.Port != 32083 {
			t.Fatalf("update response after cancellation = %+v", result.response)
		}
		if got := runtime.endpoints[seed.ID].Port; got != 32083 {
			t.Fatalf("runtime port after cancellation = %d, want 32083", got)
		}
		persisted, err := engine.GetEndpoint(seed.ID)
		if err != nil {
			t.Fatalf("get persisted endpoint after cancellation: %v", err)
		}
		if persisted.Port != 32083 {
			t.Fatalf("persisted port after cancellation = %d, want 32083", persisted.Port)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("update did not finish after SQLite gate release")
	}
}

func TestCreateEndpointContext_CancellationAfterPrepareAbortsBeforePersist(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(root, "state"), filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	base := newEndpointRuntimeStub()
	runtime := &endpointPrepareAdmissionRuntime{
		base:           base,
		prepareEntered: make(chan struct{}),
		allowPrepare:   make(chan struct{}),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(runtime.allowPrepare) }) })
	cp := &ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := cp.CreateEndpointContext(requestCtx, CreateEndpointRequest{Port: 32085})
		done <- err
	}()
	select {
	case <-runtime.prepareEntered:
	case <-time.After(time.Second):
		releaseOnce.Do(func() { close(runtime.allowPrepare) })
		t.Fatal("create did not enter PrepareEndpoint")
	}
	cancel()
	releaseOnce.Do(func() { close(runtime.allowPrepare) })

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("create after canceled PrepareEndpoint = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("create did not stop after canceled PrepareEndpoint")
	}
	if base.beginCount != 0 || base.commitCount != 0 {
		t.Fatalf("canceled create crossed persistence/runtime boundary: begin=%d commit=%d", base.beginCount, base.commitCount)
	}
	if base.abortCount != 1 {
		t.Fatalf("canceled create abort count = %d, want 1", base.abortCount)
	}
	if endpoints, err := engine.ListEndpoints(); err != nil {
		t.Fatalf("ListEndpoints: %v", err)
	} else if len(endpoints) != 0 {
		t.Fatalf("canceled create persisted endpoints = %+v", endpoints)
	}
}

func TestUpdateEndpointContext_CancellationAfterPrepareAbortsBeforePersist(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(root, "state"), filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	base := newEndpointRuntimeStub()
	seed := seedEndpointForMutationTest(t, engine, base, "endpoint-update-prepare-gate", 32086)
	runtime := &endpointPrepareAdmissionRuntime{
		base:           base,
		prepareEntered: make(chan struct{}),
		allowPrepare:   make(chan struct{}),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(runtime.allowPrepare) }) })
	cp := &ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := cp.UpdateEndpointContext(requestCtx, seed.ID, json.RawMessage(`{"port":32087}`))
		done <- err
	}()
	select {
	case <-runtime.prepareEntered:
	case <-time.After(time.Second):
		releaseOnce.Do(func() { close(runtime.allowPrepare) })
		t.Fatal("update did not enter PrepareEndpoint")
	}
	cancel()
	releaseOnce.Do(func() { close(runtime.allowPrepare) })

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("update after canceled PrepareEndpoint = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("update did not stop after canceled PrepareEndpoint")
	}
	if base.beginCount != 0 || base.commitCount != 0 {
		t.Fatalf("canceled update crossed persistence/runtime boundary: begin=%d commit=%d", base.beginCount, base.commitCount)
	}
	if base.abortCount != 1 {
		t.Fatalf("canceled update abort count = %d, want 1", base.abortCount)
	}
	if got := base.endpoints[seed.ID].Port; got != seed.Port {
		t.Fatalf("runtime endpoint after canceled update = %d, want %d", got, seed.Port)
	}
	persisted, err := engine.GetEndpoint(seed.ID)
	if err != nil {
		t.Fatalf("get endpoint after canceled update: %v", err)
	}
	if persisted.Port != seed.Port {
		t.Fatalf("persisted endpoint after canceled update = %d, want %d", persisted.Port, seed.Port)
	}
}

func TestDeleteEndpointContext_CancellationAfterCommitBoundaryRemovesDBAndRuntime(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	engine, closer, err := state.PersistenceBootstrap(stateDir, filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	runtime := newEndpointRuntimeStub()
	seed := seedEndpointForMutationTest(t, engine, runtime, "endpoint-delete-gate", 32084)
	releaseDB := holdEndpointSQLiteWrite(t, stateDir, seed.ID)

	deleteEntered := make(chan struct{})
	allowDelete := make(chan struct{})
	var hookOnce sync.Once
	cp := &ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}
	cp.beforeEndpointDeletePersistHook = func() {
		hookOnce.Do(func() { close(deleteEntered) })
		<-allowDelete
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cp.DeleteEndpointContext(requestCtx, seed.ID) }()
	select {
	case <-deleteEntered:
	case <-time.After(time.Second):
		releaseDB()
		close(allowDelete)
		t.Fatal("delete did not cross its commit boundary")
	}
	cancel()
	close(allowDelete)
	releaseDB()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("delete canceled after commit boundary: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("delete did not finish after SQLite gate release")
	}
	if _, ok := runtime.endpoints[seed.ID]; ok {
		t.Fatalf("runtime endpoint survived committed delete: %+v", runtime.endpoints[seed.ID])
	}
	if _, err := engine.GetEndpoint(seed.ID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("persisted endpoint after committed delete = %v, want ErrNotFound", err)
	}
}

func TestControlPlaneEndpoints_RequireAuthInfoCanBeConfiguredWithProxyToken(t *testing.T) {
	cp, _ := newEndpointTestService(t, &config.EnvConfig{
		ResinPort:   2260,
		ProxyToken:  "secret",
		AuthVersion: config.AuthVersionV1,
	})
	created, err := cp.CreateEndpoint(CreateEndpointRequest{
		Port:                 32030,
		RequireProxyAuthInfo: boolPointer(true),
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	if !created.RequireProxyAuthInfo {
		t.Fatalf("require_proxy_auth_info = false, want true")
	}
	updated, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{
		"port": 32031,
		"require_proxy_auth_info": true
	}`))
	if err != nil {
		t.Fatalf("UpdateEndpoint: %v", err)
	}
	if updated.Port != 32031 || !updated.RequireProxyAuthInfo {
		t.Fatalf("updated endpoint = %+v", updated)
	}
}

func TestControlPlaneEndpoints_ManagementOnlyDefaultsProxyProtocolsOff(t *testing.T) {
	cp, _ := newEndpointTestService(t, &config.EnvConfig{
		ResinPort:   2260,
		AuthVersion: config.AuthVersionV1,
	})
	created, err := cp.CreateEndpoint(CreateEndpointRequest{
		Port:            32032,
		AllowManagement: boolPointer(true),
		AllowProxy:      boolPointer(false),
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	if created.AllowProxy || created.AllowHTTPForward || created.AllowHTTPReverse || created.AllowSOCKS5 {
		t.Fatalf("management-only endpoint has proxy capability enabled: %+v", created)
	}
}

func TestControlPlaneEndpoints_CreateDisabled(t *testing.T) {
	cp, runtime := newEndpointTestService(t, &config.EnvConfig{
		ResinPort:   2260,
		AuthVersion: config.AuthVersionV1,
	})
	created, err := cp.CreateEndpoint(CreateEndpointRequest{
		Port:    32033,
		Enabled: boolPointer(false),
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	if created.Enabled || created.Status != "inactive" {
		t.Fatalf("disabled endpoint = %+v", created)
	}
	if _, ok := runtime.endpoints[created.ID]; ok {
		t.Fatal("disabled endpoint was applied to runtime")
	}
	persisted, err := cp.Engine.GetEndpoint(created.ID)
	if err != nil {
		t.Fatalf("GetEndpoint: %v", err)
	}
	if persisted.Enabled {
		t.Fatal("disabled create state was not persisted")
	}
}

func TestControlPlaneEndpoints_ListenerFailureRollsBackPersistence(t *testing.T) {
	cp, runtime := newEndpointTestService(t, &config.EnvConfig{
		ResinPort:   2260,
		AuthVersion: config.AuthVersionV1,
	})

	runtime.failPort = 32040
	if _, err := cp.CreateEndpoint(CreateEndpointRequest{Port: 32040}); err == nil {
		t.Fatal("CreateEndpoint succeeded despite listener failure")
	} else {
		assertServiceErrorCode(t, err, "CONFLICT")
	}
	items, err := cp.Engine.ListEndpoints()
	if err != nil {
		t.Fatalf("ListEndpoints after failed create: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("persisted endpoints after failed create = %+v", items)
	}

	runtime.failPort = 0
	created, err := cp.CreateEndpoint(CreateEndpointRequest{Port: 32041})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	runtime.failPort = 32042
	if _, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{"port":32042}`)); err == nil {
		t.Fatal("UpdateEndpoint succeeded despite listener failure")
	} else {
		assertServiceErrorCode(t, err, "CONFLICT")
	}
	persisted, err := cp.Engine.GetEndpoint(created.ID)
	if err != nil {
		t.Fatalf("GetEndpoint after failed update: %v", err)
	}
	if persisted.Port != 32041 {
		t.Fatalf("persisted port after failed update = %d, want 32041", persisted.Port)
	}
}

func TestControlPlaneEndpoints_DatabaseFailureAbortsPreparedStage(t *testing.T) {
	cp, runtime := newEndpointTestService(t, &config.EnvConfig{
		ResinPort:   2260,
		AuthVersion: config.AuthVersionV1,
	})

	occupied, err := cp.CreateEndpoint(CreateEndpointRequest{
		Port:    32050,
		Enabled: boolPointer(false),
	})
	if err != nil {
		t.Fatalf("create occupied endpoint: %v", err)
	}
	if occupied.Enabled {
		t.Fatal("occupied endpoint unexpectedly enabled")
	}
	created, err := cp.CreateEndpoint(CreateEndpointRequest{Port: 32051})
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	commitsBefore := runtime.commitCount
	abortsBefore := runtime.abortCount

	if _, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{"port":32050}`)); err == nil {
		t.Fatal("UpdateEndpoint unexpectedly succeeded on a conflicting port")
	} else {
		assertServiceErrorCode(t, err, "CONFLICT")
	}
	if runtime.commitCount != commitsBefore {
		t.Fatalf("DB-failed update committed runtime stage: commits %d -> %d", commitsBefore, runtime.commitCount)
	}
	if runtime.abortCount != abortsBefore+1 {
		t.Fatalf("DB-failed update abort count = %d, want %d", runtime.abortCount, abortsBefore+1)
	}
	if got := runtime.endpoints[created.ID].Port; got != 32051 {
		t.Fatalf("runtime port after DB-failed update = %d, want 32051", got)
	}
	persisted, err := cp.Engine.GetEndpoint(created.ID)
	if err != nil {
		t.Fatalf("get endpoint after DB-failed update: %v", err)
	}
	if persisted.Port != 32051 {
		t.Fatalf("persisted port after DB-failed update = %d, want 32051", persisted.Port)
	}
}

// TestLegacyCreateEndpoint_RollbackFailureLeavesPersistedRuntimeOrphan keeps
// the old persist -> Apply -> rollback algorithm as executable evidence. The
// production path no longer calls this algorithm: a staged runtime aborts
// before any unpublished candidate can be visible to the control plane.
func TestLegacyCreateEndpoint_RollbackFailureLeavesPersistedRuntimeOrphan(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}

	runtime := &legacyEndpointRuntimeCloseDBOnApplyFailure{
		endpoints: make(map[string]model.Endpoint),
		closeDB: func() {
			if err := closer.Close(); err != nil {
				t.Fatalf("close state database from runtime failure: %v", err)
			}
		},
	}
	endpoint := model.Endpoint{
		ID:               "legacy-endpoint",
		Port:             32070,
		Enabled:          true,
		AllowManagement:  false,
		AllowProxy:       true,
		AllowHTTPForward: true,
		AllowHTTPReverse: true,
		AllowSOCKS5:      true,
	}
	if err := legacyCreateEndpoint(engine, runtime, endpoint); err == nil {
		t.Fatal("legacy create unexpectedly succeeded after listener failure")
	}
	if len(runtime.endpoints) != 0 {
		t.Fatalf("failed endpoint remained in legacy runtime: %+v", runtime.endpoints)
	}

	// The rollback path used the same closed database, so reopen the actual
	// state directory to observe whether the failed create left a durable row.
	reopened, reopenedCloser, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("reopen state after failed rollback: %v", err)
	}
	t.Cleanup(func() { _ = reopenedCloser.Close() })
	endpoints, err := reopened.ListEndpoints()
	if err != nil {
		t.Fatalf("ListEndpoints after failed rollback: %v", err)
	}
	if len(endpoints) != 1 || endpoints[0].ID != endpoint.ID {
		t.Fatalf("legacy rollback did not preserve the old orphan evidence: %+v", endpoints)
	}
}

type legacyEndpointRuntime interface {
	ApplyEndpoint(model.Endpoint) error
	RemoveEndpoint(string)
}

func legacyCreateEndpoint(engine *state.StateEngine, runtime legacyEndpointRuntime, endpoint model.Endpoint) error {
	if err := engine.InsertEndpoint(endpoint); err != nil {
		return err
	}
	if err := runtime.ApplyEndpoint(endpoint); err != nil {
		runtime.RemoveEndpoint(endpoint.ID)
		if rollbackErr := engine.DeleteEndpoint(endpoint.ID); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return err
	}
	return nil
}

func TestControlPlaneEndpoints_EnableAndDisableWithPatch(t *testing.T) {
	cp, runtime := newEndpointTestService(t, &config.EnvConfig{
		ResinPort:   2260,
		AuthVersion: config.AuthVersionV1,
	})
	created, err := cp.CreateEndpoint(CreateEndpointRequest{Port: 32060})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}

	disabled, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{"enabled":false}`))
	if err != nil {
		t.Fatalf("disable endpoint: %v", err)
	}
	if disabled.Enabled || disabled.Status != "inactive" {
		t.Fatalf("disabled endpoint = %+v", disabled)
	}
	if _, ok := runtime.endpoints[created.ID]; ok {
		t.Fatal("disabled endpoint still exists in runtime")
	}
	persisted, err := cp.Engine.GetEndpoint(created.ID)
	if err != nil {
		t.Fatalf("GetEndpoint after disable: %v", err)
	}
	if persisted.Enabled {
		t.Fatal("disabled state was not persisted")
	}

	// A disabled endpoint can be reconfigured without trying to bind its port.
	runtime.failPort = 32061
	updated, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{"port":32061}`))
	if err != nil {
		t.Fatalf("UpdateEndpoint while disabled: %v", err)
	}
	if updated.Enabled || updated.Port != 32061 || updated.Status != "inactive" {
		t.Fatalf("updated disabled endpoint = %+v", updated)
	}

	if _, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{"enabled":true}`)); err == nil {
		t.Fatal("enabling endpoint succeeded despite listener failure")
	} else {
		assertServiceErrorCode(t, err, "CONFLICT")
	}
	persisted, err = cp.Engine.GetEndpoint(created.ID)
	if err != nil {
		t.Fatalf("GetEndpoint after failed start: %v", err)
	}
	if persisted.Enabled {
		t.Fatal("failed start should roll enabled state back to false")
	}

	runtime.failPort = 0
	started, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{"enabled":true}`))
	if err != nil {
		t.Fatalf("enable endpoint: %v", err)
	}
	if !started.Enabled || started.Status != "active" || started.Port != 32061 {
		t.Fatalf("started endpoint = %+v", started)
	}
	if _, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{"enabled":true}`)); err != nil {
		t.Fatalf("second enable patch should be idempotent: %v", err)
	}
	if _, err := cp.UpdateEndpoint(created.ID, json.RawMessage(`{"enabled":false}`)); err != nil {
		t.Fatalf("second disable patch: %v", err)
	}
	if disabled, err = cp.UpdateEndpoint(created.ID, json.RawMessage(`{"enabled":false}`)); err != nil {
		t.Fatalf("third disable patch should be idempotent: %v", err)
	} else if disabled.Enabled || disabled.Status != "inactive" {
		t.Fatalf("idempotently disabled endpoint = %+v", disabled)
	}

	if _, err := cp.UpdateEndpoint(DefaultEndpointID, json.RawMessage(`{"enabled":false}`)); err == nil {
		t.Fatal("disabling default endpoint succeeded")
	} else {
		assertServiceErrorCode(t, err, "CONFLICT")
	}
}

func TestUpdateEndpointContext_DisableSurvivesCancellationAfterSQLiteMutation(t *testing.T) {
	registerCancelAfterEndpointUpdateFunction()
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	runtime := newEndpointRuntimeStub()
	cp := &ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}
	created, err := cp.CreateEndpoint(CreateEndpointRequest{Port: 32063})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}

	triggerDB, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB for cancellation trigger: %v", err)
	}
	t.Cleanup(func() { _ = triggerDB.Close() })
	if _, err := triggerDB.Exec(`
		CREATE TRIGGER cancel_after_endpoint_disable
		AFTER UPDATE ON endpoints
		WHEN NEW.id = '` + created.ID + `' AND NEW.enabled = 0
		BEGIN
			SELECT resin_test_cancel_after_endpoint_update();
		END;`); err != nil {
		t.Fatalf("create endpoint cancellation trigger: %v", err)
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelAfterEndpointUpdate.Lock()
	cancelAfterEndpointUpdate.cancel = cancel
	cancelAfterEndpointUpdate.Unlock()

	response, updateErr := cp.UpdateEndpointContext(requestCtx, created.ID, json.RawMessage(`{"enabled":false}`))
	persisted, err := engine.GetEndpoint(created.ID)
	if err != nil {
		t.Fatalf("GetEndpoint after canceled disable: %v", err)
	}
	if updateErr != nil {
		t.Fatalf("disable after SQLite mutation cancellation: err=%v response=%+v persisted=%+v runtimePresent=%v", updateErr, response, persisted, hasEndpoint(runtime, created.ID))
	}
	if _, ok := runtime.endpoints[created.ID]; ok {
		t.Fatalf("runtime endpoint survived committed disable: %+v", runtime.endpoints[created.ID])
	}
	if persisted.Enabled {
		t.Fatalf("persisted endpoint remained enabled after committed disable: %+v", persisted)
	}
}

func hasEndpoint(runtime *endpointRuntimeStub, id string) bool {
	if runtime == nil {
		return false
	}
	_, ok := runtime.endpoints[id]
	return ok
}
