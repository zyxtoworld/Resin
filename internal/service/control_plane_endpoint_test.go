package service

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/state"
)

type endpointRuntimeStub struct {
	endpoints    map[string]model.Endpoint
	failPort     int
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
	return s != nil && !s.aborted && !s.committed
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
