package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
)

type gatedEndpointRuntime struct {
	manager       *endpointRuntimeManager
	commitEntered chan struct{}
	allowCommit   chan struct{}
	commitOnce    sync.Once
}

type gatedEndpointStage struct {
	inner      service.EndpointRuntimeStage
	runtime    *gatedEndpointRuntime
	commitOnce sync.Once
}

func (r *gatedEndpointRuntime) PrepareEndpoint(endpoint model.Endpoint) (service.EndpointRuntimeStage, error) {
	stage, err := r.manager.PrepareEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	return &gatedEndpointStage{inner: stage, runtime: r}, nil
}

func (r *gatedEndpointRuntime) RemoveEndpoint(id string) {
	r.manager.RemoveEndpoint(id)
}

func (r *gatedEndpointRuntime) EndpointStatus(id string) service.EndpointRuntimeStatus {
	return r.manager.EndpointStatus(id)
}

func (s *gatedEndpointStage) Abort() {
	s.inner.Abort()
}

func (s *gatedEndpointStage) BeginPersist() bool {
	return s.inner.BeginPersist()
}

func (s *gatedEndpointStage) Commit() {
	s.commitOnce.Do(func() {
		s.runtime.commitOnce.Do(func() { close(s.runtime.commitEntered) })
		<-s.runtime.allowCommit
	})
	s.inner.Commit()
}

func TestCreateEndpoint_ShutdownDoesNotLoseDatabaseCommittedStage(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	retirementEntered := make(chan struct{})
	manager.beforeRetiredRuntimeStopHook = func() {
		close(retirementEntered)
	}
	runtime := &gatedEndpointRuntime{
		manager:       manager,
		commitEntered: make(chan struct{}),
		allowCommit:   make(chan struct{}),
	}
	var releaseCommit sync.Once
	defer releaseCommit.Do(func() { close(runtime.allowCommit) })
	cp := &service.ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}
	port := reserveTestPorts(t, 1)[0]

	createDone := make(chan struct {
		response *service.EndpointResponse
		err      error
	}, 1)
	go func() {
		response, createErr := cp.CreateEndpoint(service.CreateEndpointRequest{Port: port})
		createDone <- struct {
			response *service.EndpointResponse
			err      error
		}{response, createErr}
	}()

	select {
	case <-runtime.commitEntered:
	case <-time.After(time.Second):
		t.Fatal("CreateEndpoint did not reach the post-persist Commit boundary")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	shutdownErr, continuation := closeEndpointForShutdown(shutdownCtx, manager)
	cancel()
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want context deadline exceeded", shutdownErr)
	}
	if continuation == nil {
		t.Fatal("shutdown dropped the endpoint continuation owner")
	}

	releaseCommit.Do(func() { close(runtime.allowCommit) })
	select {
	case <-retirementEntered:
	case <-time.After(time.Second):
		t.Fatal("database-committed endpoint was not collected by shutdown after late Commit")
	}

	result := <-createDone
	if result.err != nil {
		t.Fatalf("CreateEndpoint: %v", result.err)
	}
	if result.response == nil {
		t.Fatal("CreateEndpoint returned no response")
	}
	if _, err := engine.GetEndpoint(result.response.ID); err != nil {
		t.Fatalf("database-committed endpoint disappeared: %v", err)
	}
	if err := <-continuation; err != nil {
		t.Fatalf("final endpoint shutdown: %v", err)
	}
}
