package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
)

func TestResinAppShutdownClosesDirectProxyIdleConnections(t *testing.T) {
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	defer upstreamListener.Close()

	upstreamIdle := make(chan struct{})
	upstreamClosed := make(chan struct{})
	var idleOnce sync.Once
	var closedOnce sync.Once
	upstream := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}),
		ConnState: func(_ net.Conn, state http.ConnState) {
			switch state {
			case http.StateIdle:
				idleOnce.Do(func() { close(upstreamIdle) })
			case http.StateClosed:
				closedOnce.Do(func() { close(upstreamClosed) })
			}
		},
	}
	go func() { _ = upstream.Serve(upstreamListener) }()
	defer upstream.Shutdown(context.Background())

	port := reserveTestPorts(t, 1)[0]
	forward := proxy.NewForwardProxy(proxy.ForwardProxyConfig{
		ProxyBypassRules: []string{"127.0.0.1"},
		OutboundTransport: proxy.OutboundTransportConfig{
			IdleConnTimeout: time.Hour,
		},
	})
	manager := newEndpointRuntimeManager("127.0.0.1", "", forward, nil, nil, nil, nil, nil)
	endpoint := service.NewDefaultEndpoint(port)
	if err := manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("ApplyEndpoint: %v", err)
	}
	manager.Start()

	proxyURL, err := url.Parse("http://" + formatListenAddress("127.0.0.1", port))
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	clientTransport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	client := &http.Client{Transport: clientTransport}
	defer clientTransport.CloseIdleConnections()
	resp, err := client.Get("http://" + upstreamListener.Addr().String() + "/keep-alive")
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read proxy response: %v", err)
	}
	resp.Body.Close()
	select {
	case <-upstreamIdle:
	case <-time.After(time.Second):
		t.Fatal("upstream connection did not become idle")
	}

	app := &resinApp{
		endpointManager: manager,
		closeProxyTransports: func() {
			forward.CloseIdleConnections()
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	app.shutdown(ctx)
	select {
	case <-upstreamClosed:
	case <-time.After(time.Second):
		t.Fatal("direct proxy transport kept an idle upstream connection after shutdown")
	}
}

func TestResinAppShutdownClosesDirectTransportCreatedAfterHTTPDrainTimeout(t *testing.T) {
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	defer upstreamListener.Close()

	upstreamIdle := make(chan struct{})
	upstreamClosed := make(chan struct{})
	var idleOnce sync.Once
	var closedOnce sync.Once
	upstream := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("late-ok"))
		}),
		ConnState: func(_ net.Conn, state http.ConnState) {
			switch state {
			case http.StateIdle:
				idleOnce.Do(func() { close(upstreamIdle) })
			case http.StateClosed:
				closedOnce.Do(func() { close(upstreamClosed) })
			}
		},
	}
	go func() { _ = upstream.Serve(upstreamListener) }()
	defer upstream.Shutdown(context.Background())

	port := reserveTestPorts(t, 1)[0]
	forward := proxy.NewForwardProxy(proxy.ForwardProxyConfig{
		ProxyBypassRules: []string{"127.0.0.1"},
		OutboundTransport: proxy.OutboundTransportConfig{
			IdleConnTimeout: time.Hour,
		},
	})
	handlerEntered := make(chan struct{})
	allowHandler := make(chan struct{})
	gatedForward := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerEntered)
		<-allowHandler
		// The endpoint has already force-closed the client connection after the
		// shutdown deadline. Keep this admitted handler alive to exercise the
		// late bypass owner, rather than making request cancellation hide it.
		forward.ServeHTTP(w, r.WithContext(context.Background()))
	})
	manager := newEndpointRuntimeManager("127.0.0.1", "", gatedForward, nil, nil, nil, nil, nil)
	endpoint := service.NewDefaultEndpoint(port)
	if err := manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("ApplyEndpoint: %v", err)
	}
	manager.Start()

	proxyURL, err := url.Parse("http://" + formatListenAddress("127.0.0.1", port))
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	clientTransport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	client := &http.Client{Transport: clientTransport}
	defer clientTransport.CloseIdleConnections()
	requestDone := make(chan error, 1)
	go func() {
		resp, requestErr := client.Get("http://" + upstreamListener.Addr().String() + "/late")
		if requestErr == nil {
			_, requestErr = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-handlerEntered:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not enter gate")
	}

	app := &resinApp{
		endpointManager: manager,
		closeProxyTransports: func() {
			forward.CloseIdleConnections()
		},
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(shutdownCtx) }()
	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after HTTP drain deadline")
	}

	close(allowHandler)
	select {
	case <-upstreamIdle:
	case <-time.After(time.Second):
		t.Fatal("late handler did not create an idle upstream connection")
	}
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuation: %v", err)
	}
	select {
	case <-upstreamClosed:
	case <-time.After(time.Second):
		t.Fatal("late direct transport was not closed by the shutdown owner")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("late handler did not finish")
	}
}

func TestEndpointRuntimeManager_RemoveReleasesPortBeforeReturn(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve test port: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatalf("release test port: %v", err)
	}

	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	endpoint := model.Endpoint{ID: "custom", Port: port, Enabled: true}
	if err := manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("first ApplyEndpoint: %v", err)
	}
	manager.RemoveEndpoint(endpoint.ID)
	if err := manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("ApplyEndpoint immediately after RemoveEndpoint: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestEndpointRuntimeManager_RemovedRuntimeLeavesNoPendingRetirement(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	endpoint := model.Endpoint{ID: "retirement-reclaimed", Port: port, Enabled: true}
	if err := manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("ApplyEndpoint: %v", err)
	}
	runtime := manager.runtimes[endpoint.ID]
	if runtime == nil {
		t.Fatal("runtime was not published")
	}

	stopEntered := make(chan struct{})
	allowStop := make(chan struct{})
	manager.beforeRetiredRuntimeStopHook = func() {
		close(stopEntered)
		<-allowStop
	}
	manager.RemoveEndpoint(endpoint.ID)
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("retirement stop did not start")
	}

	manager.mu.Lock()
	if len(manager.retirements) != 1 {
		manager.mu.Unlock()
		t.Fatalf("pending retirements before stop release = %d, want 1", len(manager.retirements))
	}
	var retirement *endpointRuntimeRetirement
	for _, candidate := range manager.retirements {
		retirement = candidate
		break
	}
	manager.mu.Unlock()
	if retirement == nil {
		t.Fatal("retirement record was not retained while stop was gated")
	}

	close(allowStop)
	select {
	case <-retirement.done:
	case <-time.After(time.Second):
		t.Fatal("retirement did not complete")
	}
	manager.mu.Lock()
	pending := len(manager.retirements)
	manager.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending retirements after completed stop = %d, want 0", pending)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// A Serve goroutine may report a late fatal result after the retirement
	// record has left the pending map and shutdown has closed admission. The
	// runtime-owned record must still identify this as the same retirement.
	manager.handleRuntimeError(runtime, errors.New("late fatal after shutdown"))
	manager.mu.Lock()
	pending = len(manager.retirements)
	manager.mu.Unlock()
	if pending != 0 {
		t.Fatalf("late fatal created a new pending retirement = %d", pending)
	}
}

func TestEndpointRuntimeManager_ShutdownSnapshotsRetirementDuringCompletion(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	endpoint := model.Endpoint{ID: "retirement-shutdown-race", Port: port, Enabled: true}
	if err := manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("ApplyEndpoint: %v", err)
	}

	stopEntered := make(chan struct{})
	allowStop := make(chan struct{})
	manager.beforeRetiredRuntimeStopHook = func() {
		close(stopEntered)
		<-allowStop
	}
	manager.RemoveEndpoint(endpoint.ID)
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("retirement stop did not start")
	}

	shutdownSnapshotReached := make(chan struct{})
	allowSnapshot := make(chan struct{})
	manager.shutdownHook = func(stage endpointRuntimeShutdownStage) {
		if stage != endpointRuntimeShutdownAfterSnapshot {
			return
		}
		close(shutdownSnapshotReached)
		<-allowSnapshot
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.Shutdown(context.Background()) }()
	select {
	case <-shutdownSnapshotReached:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not snapshot pending retirement")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned while retirement stop was gated: %v", err)
	default:
	}

	close(allowSnapshot)
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before retirement completion: %v", err)
	default:
	}
	close(allowStop)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after retirement completion")
	}
}

func TestEndpointRuntimeManager_LateKnownRetirementUsesClosedAdmissionOwner(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	endpoint := model.Endpoint{ID: "retirement-late-error", Port: port, Enabled: true}
	if err := manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("ApplyEndpoint: %v", err)
	}
	runtime := manager.runtimes[endpoint.ID]
	if runtime == nil {
		t.Fatal("runtime was not published")
	}

	stopEntered := make(chan struct{})
	allowStop := make(chan struct{})
	var allowStopOnce sync.Once
	manager.beforeRetiredRuntimeStopHook = func() {
		close(stopEntered)
		<-allowStop
	}

	shutdownSnapshotReached := make(chan struct{})
	allowSnapshot := make(chan struct{})
	var allowSnapshotOnce sync.Once
	manager.shutdownHook = func(stage endpointRuntimeShutdownStage) {
		if stage != endpointRuntimeShutdownAfterSnapshot {
			return
		}
		close(shutdownSnapshotReached)
		<-allowSnapshot
	}
	release := func() {
		allowSnapshotOnce.Do(func() { close(allowSnapshot) })
		allowStopOnce.Do(func() { close(allowStop) })
	}
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.Shutdown(context.Background()) }()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("shutdown retirement stop did not start")
	}
	select {
	case <-shutdownSnapshotReached:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not close retirement admission")
	}

	// The pending map is now owned by Shutdown, but a late Serve error still
	// carries the exact runtime pointer. It must reuse that owner rather than
	// creating an untracked retirement or panicking on closed admission.
	manager.handleRuntimeError(runtime, errors.New("late runtime error"))
	manager.RemoveEndpoint(endpoint.ID)
	manager.mu.Lock()
	pending := len(manager.retirements)
	admissionClosed := manager.retirementAdmissionClosed
	manager.mu.Unlock()
	if !admissionClosed {
		t.Fatal("retirement admission was not closed at shutdown snapshot")
	}
	if pending != 1 {
		t.Fatalf("late error changed shutdown retirement set: %d, want 1", pending)
	}

	release()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after releasing known retirement")
	}
}

func TestRestorePersistedEndpoints_SkipsDisabledListeners(t *testing.T) {
	ports := reserveTestPorts(t, 2)
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(root, "state"), filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	for _, endpoint := range []model.Endpoint{
		{ID: "enabled", Port: ports[0], Enabled: true},
		{ID: "disabled", Port: ports[1], Enabled: false},
	} {
		if err := engine.InsertEndpoint(endpoint); err != nil {
			t.Fatalf("InsertEndpoint(%s): %v", endpoint.ID, err)
		}
	}

	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	if err := restorePersistedEndpoints(engine, manager); err != nil {
		t.Fatalf("restorePersistedEndpoints: %v", err)
	}
	if status := manager.EndpointStatus("enabled"); status.State != "starting" {
		t.Fatalf("enabled endpoint status = %+v, want starting", status)
	}
	if status := manager.EndpointStatus("disabled"); status.State != "inactive" {
		t.Fatalf("disabled endpoint status = %+v, want inactive", status)
	}

	disabledListener, err := net.Listen("tcp", formatListenAddress("127.0.0.1", ports[1]))
	if err != nil {
		t.Fatalf("disabled endpoint port should remain available: %v", err)
	}
	_ = disabledListener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestEndpointRuntimeManager_PrepareBindFailureLeavesRuntimeStateUntouched(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve test port: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	endpoint := model.Endpoint{ID: "not-persisted", Port: port, Enabled: true}

	if _, err := manager.PrepareEndpoint(endpoint); err == nil {
		t.Fatal("PrepareEndpoint unexpectedly succeeded on an occupied port")
	}
	if len(manager.runtimes) != 0 {
		t.Fatalf("runtime map after failed prepare = %+v", manager.runtimes)
	}
	if _, ok := manager.statuses[endpoint.ID]; ok {
		t.Fatalf("failed prepare published unreachable status: %+v", manager.statuses[endpoint.ID])
	}
	if err := probe.Close(); err != nil {
		t.Fatalf("release test port: %v", err)
	}

	stage, err := manager.PrepareEndpoint(endpoint)
	if err != nil {
		t.Fatalf("PrepareEndpoint after releasing port: %v", err)
	}
	stage.Abort()
	stage.Abort()
	stage.Commit()
	stage.Commit()
	if len(manager.runtimes) != 0 {
		t.Fatalf("runtime map after aborted stage = %+v", manager.runtimes)
	}
	rebound, err := net.Listen("tcp", formatListenAddress("127.0.0.1", port))
	if err != nil {
		t.Fatalf("aborted stage did not release port: %v", err)
	}
	_ = rebound.Close()
}

func TestEndpointRuntimeManager_ShutdownIncludesCommitBeforeSnapshot(t *testing.T) {
	ports := reserveTestPorts(t, 2)
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	endpoint := model.Endpoint{ID: "staged", Port: ports[0], Enabled: true}
	stage, err := manager.PrepareEndpoint(endpoint)
	if err != nil {
		t.Fatalf("PrepareEndpoint: %v", err)
	}

	shutdownEntered := make(chan struct{})
	shutdownRelease := make(chan struct{})
	shutdownFinished := make(chan error, 1)
	manager.shutdownHook = func(stage endpointRuntimeShutdownStage) {
		if stage != endpointRuntimeShutdownAfterStopping {
			return
		}
		select {
		case <-shutdownEntered:
		default:
			close(shutdownEntered)
		}
		<-shutdownRelease
	}
	go func() {
		shutdownFinished <- manager.Shutdown(context.Background())
	}()
	<-shutdownEntered

	select {
	case <-shutdownFinished:
		t.Fatal("Shutdown completed before the prepared stage was resolved")
	default:
	}
	stage.Commit()
	close(shutdownRelease)
	if err := <-shutdownFinished; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if manager.stopping != true {
		t.Fatal("Shutdown did not enter stopping state")
	}
	if len(manager.runtimes) != 0 {
		t.Fatalf("runtime map after shutdown = %+v", manager.runtimes)
	}
	stage.Abort()

	rebound, err := net.Listen("tcp", formatListenAddress("127.0.0.1", ports[0]))
	if err != nil {
		t.Fatalf("shutdown did not release committed port: %v", err)
	}
	_ = rebound.Close()
}

func TestEndpointRuntimeManager_CanceledShutdownAbortsPreparedStage(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	stage, err := manager.PrepareEndpoint(model.Endpoint{ID: "canceled", Port: port, Enabled: true})
	if err != nil {
		t.Fatalf("PrepareEndpoint: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.Shutdown(ctx) }()
	var shutdownErr error
	select {
	case shutdownErr = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("canceled Shutdown did not return while a stage was pending")
	}
	if !errors.Is(shutdownErr, context.Canceled) {
		t.Fatalf("canceled Shutdown error = %v, want context.Canceled", shutdownErr)
	}
	if manager.activeStage != nil {
		t.Fatal("canceled Shutdown left active staged ownership")
	}
	if len(manager.runtimes) != 0 {
		t.Fatalf("runtime map after canceled Shutdown = %+v", manager.runtimes)
	}
	stage.Commit()
	stage.Abort()
	rebound, err := net.Listen("tcp", formatListenAddress("127.0.0.1", port))
	if err != nil {
		t.Fatalf("canceled Shutdown did not release candidate port: %v", err)
	}
	_ = rebound.Close()
}

func TestEndpointRuntimeManager_CanceledShutdownWinsBeforeStageAbort(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	stage, err := manager.PrepareEndpoint(model.Endpoint{ID: "cancel-race", Port: port, Enabled: true})
	if err != nil {
		t.Fatalf("PrepareEndpoint: %v", err)
	}

	abortEntered := make(chan struct{})
	allowAbort := make(chan struct{})
	manager.beforeStageAbortHook = func() {
		close(abortEntered)
		<-allowAbort
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.Shutdown(ctx) }()
	select {
	case <-abortEntered:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not reach the cancellation-to-abort boundary")
	}

	// The context is already canceled and the shutdown goroutine is paused
	// immediately before abortStage. A late Commit must observe that canceled
	// stage and remain unpublished.
	stage.Commit()
	manager.mu.Lock()
	runtimePublished := len(manager.runtimes) != 0
	manager.mu.Unlock()
	if runtimePublished {
		t.Fatal("late Commit published a runtime after shutdown cancellation")
	}

	close(allowAbort)
	select {
	case shutdownErr := <-shutdownDone:
		if !errors.Is(shutdownErr, context.Canceled) {
			t.Fatalf("Shutdown error = %v, want context.Canceled", shutdownErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after releasing the abort boundary")
	}

	rebound, err := net.Listen("tcp", formatListenAddress("127.0.0.1", port))
	if err != nil {
		t.Fatalf("canceled late stage did not release candidate port: %v", err)
	}
	_ = rebound.Close()
}

func TestEndpointRuntimeManager_FatalOldRuntimeReleasesPortBeforePendingCommit(t *testing.T) {
	ports := reserveTestPorts(t, 2)
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	oldEndpoint := model.Endpoint{ID: "fatal-port-change", Port: ports[0], Enabled: true}
	oldStage, err := manager.PrepareEndpoint(oldEndpoint)
	if err != nil {
		t.Fatalf("prepare old endpoint: %v", err)
	}
	oldStage.Commit()
	old := manager.runtimes[oldEndpoint.ID]
	if old == nil {
		t.Fatal("old endpoint was not published")
	}

	newEndpoint := oldEndpoint
	newEndpoint.Port = ports[1]
	stage, err := manager.PrepareEndpoint(newEndpoint)
	if err != nil {
		t.Fatalf("prepare pending port change: %v", err)
	}
	t.Cleanup(func() {
		stage.Abort()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})

	manager.handleRuntimeError(old, errors.New("fatal accept error"))
	stage.Commit()

	current := manager.runtimes[oldEndpoint.ID]
	if current == nil || current.current().Port != ports[1] {
		t.Fatalf("committed runtime after old fatal = %+v, want port %d", current, ports[1])
	}
	oldPort, err := net.Listen("tcp", formatListenAddress("127.0.0.1", ports[0]))
	if err != nil {
		t.Fatalf("fatal old runtime leaked port %d: %v", ports[0], err)
	}
	_ = oldPort.Close()
	newPort, err := net.Listen("tcp", formatListenAddress("127.0.0.1", ports[1]))
	if err == nil {
		_ = newPort.Close()
		t.Fatalf("committed candidate port %d was not published", ports[1])
	}
}

func TestEndpointRuntimeManager_FatalRuntimeWithConfigStageClosesActiveConnections(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	endpoint := model.Endpoint{ID: "fatal-config", Port: port, Enabled: true}
	initial, err := manager.PrepareEndpoint(endpoint)
	if err != nil {
		t.Fatalf("prepare endpoint: %v", err)
	}
	initial.Commit()
	old := manager.runtimes[endpoint.ID]
	if old == nil {
		t.Fatal("endpoint was not published")
	}
	configStage, err := manager.PrepareEndpoint(endpoint)
	if err != nil {
		t.Fatalf("prepare same-port config stage: %v", err)
	}
	clientConn, serverConn := net.Pipe()
	old.server.trackActiveConn(serverConn)
	stopEntered := make(chan struct{})
	allowStop := make(chan struct{})
	var allowStopOnce sync.Once
	manager.beforeRetiredRuntimeStopHook = func() {
		close(stopEntered)
		<-allowStop
	}
	t.Cleanup(func() {
		configStage.Abort()
		allowStopOnce.Do(func() { close(allowStop) })
		_ = clientConn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})

	manager.handleRuntimeError(old, errors.New("fatal config runtime error"))
	configStage.Commit()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("fatal runtime retirement did not start")
	}
	manager.mu.Lock()
	retirement := manager.retirements[old]
	manager.mu.Unlock()
	if retirement == nil {
		t.Fatal("fatal runtime retirement was not registered")
	}
	allowStopOnce.Do(func() { close(allowStop) })
	select {
	case <-retirement.done:
	case <-time.After(time.Second):
		t.Fatal("fatal runtime retirement did not complete")
	}

	if _, ok := manager.runtimes[endpoint.ID]; ok {
		t.Fatalf("fatal same-port runtime remained published: %+v", manager.runtimes[endpoint.ID])
	}
	old.server.mu.Lock()
	activeCount := len(old.server.activeConns)
	old.server.mu.Unlock()
	if activeCount != 0 {
		t.Fatalf("fatal same-port runtime retained %d active connections", activeCount)
	}
	rebound, err := net.Listen("tcp", formatListenAddress("127.0.0.1", port))
	if err != nil {
		t.Fatalf("fatal same-port runtime leaked port %d: %v", port, err)
	}
	_ = rebound.Close()
}

type gatedCloseListener struct {
	net.Listener
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	closeOnce   sync.Once
	releaseOnce sync.Once
}

func (l *gatedCloseListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		l.enteredOnce.Do(func() { close(l.entered) })
		<-l.release
		err = l.Listener.Close()
	})
	return err
}

func TestEndpointRuntimeStageCommitWaitsForOldListenerClose(t *testing.T) {
	ports := reserveTestPorts(t, 2)
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	oldEndpoint := model.Endpoint{ID: "gated-close", Port: ports[0], Enabled: true}
	initial, err := manager.PrepareEndpoint(oldEndpoint)
	if err != nil {
		t.Fatalf("prepare old endpoint: %v", err)
	}
	initial.Commit()
	old := manager.runtimes[oldEndpoint.ID]
	if old == nil {
		t.Fatal("old endpoint was not published")
	}

	gated := &gatedCloseListener{
		Listener: old.listener,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	old.listener = gated
	newEndpoint := oldEndpoint
	newEndpoint.Port = ports[1]
	stage, err := manager.PrepareEndpoint(newEndpoint)
	if err != nil {
		t.Fatalf("prepare port change: %v", err)
	}
	release := func() { gated.releaseOnce.Do(func() { close(gated.release) }) }
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})

	commitDone := make(chan struct{})
	go func() {
		stage.Commit()
		close(commitDone)
	}()
	select {
	case <-gated.entered:
	case <-time.After(time.Second):
		t.Fatal("Commit did not synchronously begin closing the old listener")
	}
	select {
	case <-commitDone:
		t.Fatal("Commit returned before the old listener was actually closed")
	default:
	}

	release()
	select {
	case <-commitDone:
	case <-time.After(time.Second):
		t.Fatal("Commit did not return after releasing the old listener")
	}

	rebound, err := net.Listen("tcp", formatListenAddress("127.0.0.1", ports[0]))
	if err != nil {
		t.Fatalf("old port was not reusable after Commit: %v", err)
	}
	_ = rebound.Close()
}

func TestEndpointRuntimeManager_ShutdownWaitsForRetiredRuntime(t *testing.T) {
	ports := reserveTestPorts(t, 2)
	handlerStarted := make(chan struct{})
	handlerRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(handlerRelease) }) }

	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	oldEndpoint := model.Endpoint{ID: "retired-shutdown", Port: ports[0], Enabled: true}
	initial, err := manager.PrepareEndpoint(oldEndpoint)
	if err != nil {
		t.Fatalf("prepare old endpoint: %v", err)
	}
	initial.Commit()
	old := manager.runtimes[oldEndpoint.ID]
	if old == nil || old.server == nil || old.server.httpServer == nil {
		t.Fatal("old runtime was not fully built")
	}
	old.server.httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(handlerStarted)
		<-handlerRelease
		w.WriteHeader(http.StatusNoContent)
	})
	manager.Start()

	client, err := net.Dial("tcp", formatListenAddress("127.0.0.1", ports[0]))
	if err != nil {
		releaseHandler()
		t.Fatalf("dial old endpoint: %v", err)
	}
	t.Cleanup(func() {
		releaseHandler()
		_ = client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})
	if _, err := io.WriteString(client, "GET /retired HTTP/1.1\r\nHost: retired\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatalf("write old request: %v", err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("old runtime handler did not start")
	}

	retiredStopStarted := make(chan struct{})
	manager.beforeRetiredRuntimeStopHook = func() {
		select {
		case <-retiredStopStarted:
		default:
			close(retiredStopStarted)
		}
	}
	newEndpoint := oldEndpoint
	newEndpoint.Port = ports[1]
	stage, err := manager.PrepareEndpoint(newEndpoint)
	if err != nil {
		t.Fatalf("prepare replacement endpoint: %v", err)
	}
	stage.Commit()
	select {
	case <-retiredStopStarted:
	case <-time.After(time.Second):
		t.Fatal("retired runtime stop did not start")
	}

	shutdownSnapshotReached := make(chan struct{})
	allowShutdownSnapshot := make(chan struct{})
	manager.shutdownHook = func(stage endpointRuntimeShutdownStage) {
		if stage != endpointRuntimeShutdownAfterSnapshot {
			return
		}
		close(shutdownSnapshotReached)
		<-allowShutdownSnapshot
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.Shutdown(context.Background()) }()
	select {
	case <-shutdownSnapshotReached:
	case <-time.After(time.Second):
		releaseHandler()
		t.Fatal("Shutdown did not reach the post-snapshot boundary")
	}
	close(allowShutdownSnapshot)

	// The old runtime is still serving the blocked request. Shutdown must not
	// return until that retired runtime's graceful stop has completed.
	select {
	case err := <-shutdownDone:
		releaseHandler()
		t.Fatalf("Shutdown returned before retired runtime stopped: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseHandler()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after retired handler release")
	}
}

func TestEndpointRuntimeManager_ConcurrentShutdownWaitsForOwner(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	endpoint := model.Endpoint{ID: "concurrent-shutdown", Port: port, Enabled: true}
	initial, err := manager.PrepareEndpoint(endpoint)
	if err != nil {
		t.Fatalf("PrepareEndpoint: %v", err)
	}
	initial.Commit()

	stopEntered := make(chan struct{})
	allowStop := make(chan struct{})
	manager.beforeRetiredRuntimeStopHook = func() {
		close(stopEntered)
		<-allowStop
	}
	manager.RemoveEndpoint(endpoint.ID)
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("retirement stop did not start")
	}

	shutdownSnapshotReached := make(chan struct{})
	manager.shutdownHook = func(stage endpointRuntimeShutdownStage) {
		if stage == endpointRuntimeShutdownAfterSnapshot {
			close(shutdownSnapshotReached)
		}
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- manager.Shutdown(context.Background()) }()
	select {
	case <-shutdownSnapshotReached:
	case <-time.After(time.Second):
		t.Fatal("first Shutdown did not snapshot pending retirement")
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondDone <- manager.Shutdown(context.Background())
	}()
	<-secondStarted
	select {
	case err := <-secondDone:
		t.Fatalf("second Shutdown returned before the first owner completed: %v", err)
	default:
	}

	close(allowStop)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Shutdown did not finish after retirement release")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second Shutdown did not join the first owner")
	}
}

func TestEndpointRuntimeManager_ConcurrentShutdownHonorsCallerContext(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	endpoint := model.Endpoint{ID: "concurrent-shutdown-context", Port: port, Enabled: true}
	initial, err := manager.PrepareEndpoint(endpoint)
	if err != nil {
		t.Fatalf("PrepareEndpoint: %v", err)
	}
	initial.Commit()

	stopEntered := make(chan struct{})
	allowStop := make(chan struct{})
	var allowStopOnce sync.Once
	manager.beforeRetiredRuntimeStopHook = func() {
		close(stopEntered)
		<-allowStop
	}
	manager.RemoveEndpoint(endpoint.ID)
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("retirement stop did not start")
	}
	t.Cleanup(func() {
		allowStopOnce.Do(func() { close(allowStop) })
	})

	shutdownSnapshotReached := make(chan struct{})
	manager.shutdownHook = func(stage endpointRuntimeShutdownStage) {
		if stage == endpointRuntimeShutdownAfterSnapshot {
			close(shutdownSnapshotReached)
		}
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- manager.Shutdown(context.Background()) }()
	select {
	case <-shutdownSnapshotReached:
	case <-time.After(time.Second):
		t.Fatal("first Shutdown did not snapshot pending retirement")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	secondDone := make(chan error, 1)
	go func() { secondDone <- manager.Shutdown(ctx) }()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("second Shutdown error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second Shutdown ignored its canceled context while owner was pending")
	}

	allowStopOnce.Do(func() { close(allowStop) })
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Shutdown did not finish after retirement release")
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatalf("third Shutdown: %v", err)
	}
}

func TestControlPlaneUpdateEndpointImmediatelyReusesOldPort(t *testing.T) {
	ports := reserveTestPorts(t, 2)
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(root, "state"), filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})
	cp := &service.ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: manager,
	}

	created, err := cp.CreateEndpoint(service.CreateEndpointRequest{Port: ports[0]})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	updated, err := cp.UpdateEndpoint(created.ID, []byte(fmt.Sprintf(`{"port":%d}`, ports[1])))
	if err != nil {
		t.Fatalf("UpdateEndpoint: %v", err)
	}
	if updated.Port != ports[1] {
		t.Fatalf("updated endpoint port = %d, want %d", updated.Port, ports[1])
	}

	reused, err := cp.CreateEndpoint(service.CreateEndpointRequest{Port: ports[0]})
	if err != nil {
		t.Fatalf("CreateEndpoint immediately after port change: %v", err)
	}
	if reused.Port != ports[0] {
		t.Fatalf("reused endpoint port = %d, want %d", reused.Port, ports[0])
	}
}

type endpointRuntimePrepareGate struct {
	manager  *endpointRuntimeManager
	prepared chan struct{}
	release  chan struct{}
}

func (g *endpointRuntimePrepareGate) PrepareEndpoint(endpoint model.Endpoint) (service.EndpointRuntimeStage, error) {
	stage, err := g.manager.PrepareEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	close(g.prepared)
	<-g.release
	return stage, nil
}

func (g *endpointRuntimePrepareGate) RemoveEndpoint(id string) {
	g.manager.RemoveEndpoint(id)
}

func (g *endpointRuntimePrepareGate) EndpointStatus(id string) service.EndpointRuntimeStatus {
	return g.manager.EndpointStatus(id)
}

func TestControlPlaneCreateEndpoint_StageOwnerSerializesShutdownAndCommit(t *testing.T) {
	port := reserveTestPorts(t, 1)[0]
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(root, "state"), filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil)
	runtime := &endpointRuntimePrepareGate{
		manager:  manager,
		prepared: make(chan struct{}),
		release:  make(chan struct{}),
	}
	cp := &service.ControlPlaneService{
		Engine:          engine,
		EnvCfg:          &config.EnvConfig{ResinPort: 2260, AuthVersion: config.AuthVersionV1},
		EndpointRuntime: runtime,
	}

	type createResult struct {
		response *service.EndpointResponse
		err      error
	}
	createDone := make(chan createResult, 1)
	go func() {
		response, createErr := cp.CreateEndpoint(service.CreateEndpointRequest{Port: port})
		createDone <- createResult{response: response, err: createErr}
	}()
	<-runtime.prepared

	shutdownEntered := make(chan struct{})
	manager.shutdownHook = func(stage endpointRuntimeShutdownStage) {
		if stage == endpointRuntimeShutdownAfterStopping {
			close(shutdownEntered)
		}
	}
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- manager.Shutdown(ctx)
	}()
	<-shutdownEntered

	if !manager.stopping {
		t.Fatal("Shutdown did not stop new prepares before waiting for the stage")
	}
	close(runtime.release)
	created := <-createDone
	if created.err != nil {
		t.Fatalf("CreateEndpoint: %v", created.err)
	}
	if created.response == nil || created.response.Port != port {
		t.Fatalf("created endpoint = %+v", created.response)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if len(manager.runtimes) != 0 {
		t.Fatalf("runtime map after shutdown = %+v", manager.runtimes)
	}
	rebound, err := net.Listen("tcp", formatListenAddress("127.0.0.1", port))
	if err != nil {
		t.Fatalf("shutdown did not collect committed runtime: %v", err)
	}
	_ = rebound.Close()
	if _, err := engine.GetEndpoint(created.response.ID); err != nil {
		t.Fatalf("database did not retain successful create: %v", err)
	}
}

func TestEndpointRuntimeManager_PrepareStageLifecycleMethodsAreVoid(t *testing.T) {
	var _ service.EndpointRuntimeStage = (*endpointRuntimeStage)(nil)
}

func reserveTestPorts(t *testing.T, count int) []int {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	for range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve test port: %v", err)
		}
		listeners = append(listeners, listener)
	}
	ports := make([]int, 0, count)
	for _, listener := range listeners {
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
		if err := listener.Close(); err != nil {
			t.Fatalf("release test port: %v", err)
		}
	}
	return ports
}
