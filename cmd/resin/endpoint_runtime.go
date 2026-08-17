package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/service"
)

type managedEndpointRuntime struct {
	config       atomic.Pointer[model.Endpoint]
	server       *inboundDemuxServer
	listener     net.Listener
	retirement   *endpointRuntimeRetirement
	started      bool
	listenerOnce sync.Once
	stopOnce     sync.Once
	stopErr      error
}

type endpointRuntimeStageState uint8

const (
	endpointRuntimeStagePrepared endpointRuntimeStageState = iota
	endpointRuntimeStageCommitted
	endpointRuntimeStageAborted
)

type endpointRuntimeShutdownStage uint8

const (
	endpointRuntimeShutdownAfterStopping endpointRuntimeShutdownStage = iota
	endpointRuntimeShutdownAfterSnapshot
)

type endpointRuntimeRetirement struct {
	done chan struct{}
	err  error
}

type endpointRuntimeStage struct {
	manager    *endpointRuntimeManager
	endpoint   model.Endpoint
	current    *managedEndpointRuntime
	candidate  *managedEndpointRuntime
	state      endpointRuntimeStageState
	persisting bool
	// abortRequested is published before Shutdown tries to acquire the
	// manager lock for the cancellation path. Commit checks it while holding
	// that lock, so a canceled shutdown cannot lose to a late commit.
	abortRequested atomic.Bool
	done           chan struct{}
}

func (r *managedEndpointRuntime) current() model.Endpoint {
	if r == nil {
		return model.Endpoint{}
	}
	endpoint := r.config.Load()
	if endpoint == nil {
		return model.Endpoint{}
	}
	return *endpoint
}

type endpointRuntimeManager struct {
	mu                        sync.Mutex
	listenAddress             string
	proxyToken                string
	forward                   http.Handler
	reverse                   http.Handler
	apiHandler                http.Handler
	tokenAPI                  http.Handler
	socks5                    inboundConnHandler
	metricsSink               proxy.MetricsEventSink
	runtimes                  map[string]*managedEndpointRuntime
	statuses                  map[string]service.EndpointRuntimeStatus
	retirements               map[*managedEndpointRuntime]*endpointRuntimeRetirement
	started                   bool
	stopping                  bool
	retirementAdmissionClosed bool
	serverErrCh               chan error
	activeStage               *endpointRuntimeStage
	shutdownDone              chan struct{}
	shutdownErr               error
	shutdownHook              func(endpointRuntimeShutdownStage)
	// beforeStageAbortHook is a package-private test seam for the exact
	// cancellation-to-abort interleaving. Production leaves it nil.
	beforeStageAbortHook func()
	// beforeRetiredRuntimeStopHook is a package-private test seam for the
	// retirement/shutdown ordering boundary. Production leaves it nil.
	beforeRetiredRuntimeStopHook func()
}

func newEndpointRuntimeManager(
	listenAddress string,
	proxyToken string,
	forward, reverse, apiHandler, tokenAPI http.Handler,
	socks5 inboundConnHandler,
	metricsSink proxy.MetricsEventSink,
) *endpointRuntimeManager {
	return &endpointRuntimeManager{
		listenAddress: listenAddress,
		proxyToken:    proxyToken,
		forward:       forward,
		reverse:       reverse,
		apiHandler:    apiHandler,
		tokenAPI:      tokenAPI,
		socks5:        socks5,
		metricsSink:   metricsSink,
		runtimes:      make(map[string]*managedEndpointRuntime),
		statuses:      make(map[string]service.EndpointRuntimeStatus),
		retirements:   make(map[*managedEndpointRuntime]*endpointRuntimeRetirement),
		serverErrCh:   make(chan error, 1),
		shutdownDone:  make(chan struct{}),
	}
}

// registerRuntimeRetirementLocked records exactly one stop operation for a
// runtime. The record is created while manager.mu is held, but the actual stop
// and all waiting happen after the lock is released. This gives Shutdown a
// stable owner set without putting runtime shutdown callbacks under mu.
func (m *endpointRuntimeManager) registerRuntimeRetirementLocked(
	runtime *managedEndpointRuntime,
	stop func() error,
) *endpointRuntimeRetirement {
	if runtime == nil || stop == nil {
		return nil
	}
	if m.retirements == nil {
		m.retirements = make(map[*managedEndpointRuntime]*endpointRuntimeRetirement)
	}
	if retirement, ok := m.retirements[runtime]; ok {
		return retirement
	}
	if runtime.retirement != nil {
		return runtime.retirement
	}
	if m.retirementAdmissionClosed {
		panic("endpoint runtime retirement admission closed")
	}
	retirement := &endpointRuntimeRetirement{done: make(chan struct{})}
	runtime.retirement = retirement
	m.retirements[runtime] = retirement
	go func() {
		if hook := m.beforeRetiredRuntimeStopHook; hook != nil {
			hook()
		}
		retirement.err = stop()
		close(retirement.done)

		// A bounded graceful stop can finish after force-closing the listener
		// while an HTTP handler still ignores request cancellation. Keep the
		// runtime owner discoverable until that handler admission drains; the
		// application calls WaitForHTTPHandlers after Shutdown and must not lose
		// this retired runtime merely because its listener stop already ended.
		if runtime.server != nil {
			_ = runtime.server.waitForHTTPHandlers(context.Background())
		}

		m.mu.Lock()
		if !m.retirementAdmissionClosed {
			if current, ok := m.retirements[runtime]; ok && current == retirement {
				delete(m.retirements, runtime)
			}
		}
		m.mu.Unlock()
	}()
	return retirement
}

func asyncRuntimeRetirement(runtime *managedEndpointRuntime) func() error {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return runtime.stop(ctx)
	}
}

func (m *endpointRuntimeManager) PrepareEndpoint(endpoint model.Endpoint) (service.EndpointRuntimeStage, error) {
	if m == nil {
		return nil, fmt.Errorf("endpoint runtime manager is nil")
	}
	for {
		m.mu.Lock()
		if m.stopping {
			m.mu.Unlock()
			return nil, net.ErrClosed
		}
		if m.activeStage != nil {
			done := m.activeStage.done
			m.mu.Unlock()
			<-done
			continue
		}

		stage := &endpointRuntimeStage{
			manager:  m,
			endpoint: endpoint,
			state:    endpointRuntimeStagePrepared,
			done:     make(chan struct{}),
		}
		m.activeStage = stage
		current := m.runtimes[endpoint.ID]
		if current != nil && current.current().Port == endpoint.Port {
			stage.current = current
			m.mu.Unlock()
			return stage, nil
		}

		listener, err := net.Listen("tcp", formatListenAddress(m.listenAddress, endpoint.Port))
		if err != nil {
			// Prepare has not published anything. In particular, a not-yet-
			// persisted endpoint must not acquire an unreachable status entry.
			stage.state = endpointRuntimeStageAborted
			m.releaseStageLocked(stage)
			m.mu.Unlock()
			return nil, err
		}
		listener = proxy.NewCountingListener(listener, m.metricsSink)
		stage.current = current
		stage.candidate = m.buildRuntimeLocked(endpoint, listener)
		m.mu.Unlock()
		return stage, nil
	}
}

// ApplyEndpoint is used by bootstrap, where there is no database mutation to
// coordinate. Control-plane mutations use PrepareEndpoint directly.
func (m *endpointRuntimeManager) ApplyEndpoint(endpoint model.Endpoint) error {
	stage, err := m.PrepareEndpoint(endpoint)
	if err != nil {
		return err
	}
	if !stage.BeginPersist() {
		stage.Abort()
		return net.ErrClosed
	}
	stage.Commit()
	return nil
}

func (m *endpointRuntimeManager) buildRuntimeLocked(endpoint model.Endpoint, listener net.Listener) *managedEndpointRuntime {
	runtime := &managedEndpointRuntime{listener: listener}
	copy := endpoint
	runtime.config.Store(&copy)
	currentConfig := func() model.Endpoint { return runtime.current() }
	httpHandler := newEndpointInboundMux(
		currentConfig,
		m.proxyToken,
		m.forward,
		m.reverse,
		m.apiHandler,
		m.tokenAPI,
	)
	runtime.server = newInboundDemuxServer(
		&http.Server{Handler: httpHandler},
		&endpointSocksGate{current: currentConfig, next: m.socks5},
	)
	return runtime
}

func (s *endpointRuntimeStage) BeginPersist() bool {
	if s == nil || s.manager == nil {
		return false
	}
	m := s.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.state != endpointRuntimeStagePrepared || m.activeStage != s || s.abortRequested.Load() {
		return false
	}
	s.persisting = true
	return true
}

func (s *endpointRuntimeStage) Commit() {
	if s == nil || s.manager == nil {
		return
	}
	m := s.manager
	m.mu.Lock()
	if s.state != endpointRuntimeStagePrepared || m.activeStage != s {
		m.mu.Unlock()
		return
	}
	if s.abortRequested.Load() {
		s.state = endpointRuntimeStageAborted
		candidate := s.candidate
		m.releaseStageLocked(s)
		m.mu.Unlock()
		if candidate != nil {
			candidate.closeListener()
		}
		return
	}
	s.state = endpointRuntimeStageCommitted
	if s.candidate == nil {
		if current := m.runtimes[s.endpoint.ID]; current == s.current {
			copy := s.endpoint
			current.config.Store(&copy)
			if current.started {
				m.statuses[s.endpoint.ID] = service.EndpointRuntimeStatus{State: "active"}
			} else {
				m.statuses[s.endpoint.ID] = service.EndpointRuntimeStatus{State: "starting"}
			}
		}
		m.releaseStageLocked(s)
		m.mu.Unlock()
		return
	}

	old := m.runtimes[s.endpoint.ID]
	if old != nil {
		m.registerRuntimeRetirementLocked(old, asyncRuntimeRetirement(old))
	}
	m.runtimes[s.endpoint.ID] = s.candidate
	m.statuses[s.endpoint.ID] = service.EndpointRuntimeStatus{State: "starting"}
	if m.started && !m.stopping {
		m.startRuntimeLocked(s.candidate)
	}
	m.releaseStageLocked(s)
	m.mu.Unlock()

	if old != nil {
		old.closeListener()
	}
}

func (s *endpointRuntimeStage) Abort() {
	if s == nil || s.manager == nil {
		return
	}
	m := s.manager
	m.mu.Lock()
	if s.state != endpointRuntimeStagePrepared || m.activeStage != s {
		m.mu.Unlock()
		return
	}
	s.state = endpointRuntimeStageAborted
	candidate := s.candidate
	m.releaseStageLocked(s)
	m.mu.Unlock()
	if candidate != nil {
		_ = candidate.listener.Close()
	}
}

func (m *endpointRuntimeManager) releaseStageLocked(stage *endpointRuntimeStage) {
	if m.activeStage != stage {
		return
	}
	m.activeStage = nil
	close(stage.done)
}

func (m *endpointRuntimeManager) abortStage(stage *endpointRuntimeStage) *managedEndpointRuntime {
	if stage == nil {
		return nil
	}
	m.mu.Lock()
	if stage.state != endpointRuntimeStagePrepared || m.activeStage != stage {
		m.mu.Unlock()
		return nil
	}
	stage.state = endpointRuntimeStageAborted
	candidate := stage.candidate
	m.releaseStageLocked(stage)
	m.mu.Unlock()
	return candidate
}

func (m *endpointRuntimeManager) RemoveEndpoint(id string) {
	if m == nil {
		return
	}
	for {
		m.mu.Lock()
		if m.stopping {
			m.mu.Unlock()
			return
		}
		if m.activeStage != nil {
			done := m.activeStage.done
			m.mu.Unlock()
			<-done
			continue
		}
		runtime := m.runtimes[id]
		if runtime != nil {
			m.registerRuntimeRetirementLocked(runtime, asyncRuntimeRetirement(runtime))
		}
		delete(m.runtimes, id)
		delete(m.statuses, id)
		m.mu.Unlock()
		if runtime != nil {
			// Release the port before returning so a following start can rebind it.
			runtime.closeListener()
		}
		return
	}
}

func (m *endpointRuntimeManager) EndpointStatus(id string) service.EndpointRuntimeStatus {
	if m == nil {
		return service.EndpointRuntimeStatus{State: "inactive"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	status := m.statuses[id]
	if status.State == "" {
		status.State = "inactive"
	}
	return status
}

func (m *endpointRuntimeManager) RecordEndpointError(endpoint model.Endpoint, err error) {
	if m == nil || err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statuses[endpoint.ID] = service.EndpointRuntimeStatus{State: "error", LastError: err.Error()}
}

func (m *endpointRuntimeManager) Start() <-chan error {
	m.mu.Lock()
	if !m.started && !m.stopping {
		m.started = true
		for _, runtime := range m.runtimes {
			m.startRuntimeLocked(runtime)
		}
	}
	m.mu.Unlock()
	return m.serverErrCh
}

func (m *endpointRuntimeManager) startRuntimeLocked(runtime *managedEndpointRuntime) {
	if runtime == nil || runtime.started {
		return
	}
	runtime.started = true
	endpoint := runtime.current()
	m.statuses[endpoint.ID] = service.EndpointRuntimeStatus{State: "active"}
	log.Printf("Endpoint %s starting on %s", endpoint.ID, formatListenAddress(m.listenAddress, endpoint.Port))
	go func() {
		err := runtime.server.Serve(runtime.listener)
		if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return
		}
		m.handleRuntimeError(runtime, err)
	}()
}

func (m *endpointRuntimeManager) handleRuntimeError(runtime *managedEndpointRuntime, err error) {
	if runtime == nil {
		return
	}
	endpoint := runtime.current()
	m.mu.Lock()
	m.registerRuntimeRetirementLocked(runtime, asyncRuntimeRetirement(runtime))
	if m.runtimes[endpoint.ID] == runtime {
		delete(m.runtimes, endpoint.ID)
		m.statuses[endpoint.ID] = service.EndpointRuntimeStatus{State: "error", LastError: err.Error()}
	}
	stopping := m.stopping
	m.mu.Unlock()
	// The retirement owner handles graceful connection shutdown asynchronously,
	// but the outer listener is the endpoint's port ownership boundary. Release
	// it synchronously after leaving manager.mu so a fatal runtime cannot keep
	// the old port occupied while its worker drains.
	runtime.closeListener()
	if !stopping && endpoint.ID == service.DefaultEndpointID {
		select {
		case m.serverErrCh <- fmt.Errorf("default endpoint: %w", err):
		default:
		}
	}
}

func (m *endpointRuntimeManager) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return m.shutdown(ctx, nil)
}

func (m *endpointRuntimeManager) shutdownOwnerInProgress() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	done := m.shutdownDone
	m.mu.Unlock()
	select {
	case <-done:
		return false
	default:
		return true
	}
}

// WaitForHTTPHandlers drains requests that were admitted before endpoint
// shutdown. Shutdown(ctx) intentionally keeps its caller deadline; this
// separate barrier is used before closing the sinks those handlers can mark.
func (m *endpointRuntimeManager) WaitForHTTPHandlers(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	runtimes := make(map[*managedEndpointRuntime]struct{}, len(m.runtimes)+len(m.retirements)+1)
	for _, runtime := range m.runtimes {
		runtimes[runtime] = struct{}{}
	}
	for runtime := range m.retirements {
		runtimes[runtime] = struct{}{}
	}
	if m.activeStage != nil {
		runtimes[m.activeStage.current] = struct{}{}
		runtimes[m.activeStage.candidate] = struct{}{}
	}
	m.mu.Unlock()

	for runtime := range runtimes {
		if runtime == nil || runtime.server == nil {
			continue
		}
		if err := runtime.server.waitForHTTPHandlers(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *endpointRuntimeManager) shutdown(ctx context.Context, snapshotHook func([]*managedEndpointRuntime)) (result error) {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if m.stopping {
		done := m.shutdownDone
		m.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		m.mu.Lock()
		result = m.shutdownErr
		m.mu.Unlock()
		return result
	}
	m.stopping = true
	stage := m.activeStage
	m.mu.Unlock()
	if m.shutdownHook != nil {
		m.shutdownHook(endpointRuntimeShutdownAfterStopping)
	}

	var waitErr error
	if stage != nil {
		select {
		case <-stage.done:
		case <-ctx.Done():
			if m.preservePersistingStageOnShutdown(stage) {
				// The control-plane mutation has already entered its database
				// commit owner. Let a background shutdown owner wait for its
				// Commit/Abort, then collect the resulting runtime. The caller's
				// deadline still governs this waiter's return.
				go func() {
					m.finishShutdown(context.Background(), snapshotHook, nil)
				}()
				return ctx.Err()
			}
			m.abortPreparedStage(stage)
			waitErr = ctx.Err()
		}
	} else if err := ctx.Err(); err != nil {
		waitErr = err
	}
	return m.finishShutdown(ctx, snapshotHook, waitErr)
}

// preservePersistingStageOnShutdown atomically decides whether a stage that
// has entered the control-plane persistence owner may still be canceled. A
// stage before BeginPersist may be aborted on a shutdown deadline; once
// persistence has started, abandoning it would create a database/runtime
// split, so the background shutdown owner must wait for the mutation.
func (m *endpointRuntimeManager) preservePersistingStageOnShutdown(stage *endpointRuntimeStage) bool {
	if m == nil || stage == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeStage == stage && stage.state == endpointRuntimeStagePrepared && stage.persisting
}

func (m *endpointRuntimeManager) abortPreparedStage(stage *endpointRuntimeStage) {
	if m == nil || stage == nil {
		return
	}
	// Publish the cancellation decision before taking the stage lock. A
	// concurrent Commit that wins the mutex after this point must observe the
	// abort and leave its unpublished candidate closed.
	stage.abortRequested.Store(true)
	if hook := m.beforeStageAbortHook; hook != nil {
		hook()
	}
	candidate := m.abortStage(stage)
	if candidate != nil {
		_ = candidate.listener.Close()
	}
}

func (m *endpointRuntimeManager) finishShutdown(
	ctx context.Context,
	snapshotHook func([]*managedEndpointRuntime),
	waitErr error,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// A persisted stage is deliberately allowed to finish after the first
	// caller's deadline. This function is then owned by the background
	// continuation and must not use that expired context for runtime stops.
	m.mu.Lock()
	stage := m.activeStage
	m.mu.Unlock()
	if stage != nil {
		<-stage.done
	}

	m.mu.Lock()
	runtimes := make([]*managedEndpointRuntime, 0, len(m.runtimes))
	for _, runtime := range m.runtimes {
		runtimes = append(runtimes, runtime)
	}
	for _, runtime := range runtimes {
		current := runtime
		m.registerRuntimeRetirementLocked(current, func() error {
			return current.stop(ctx)
		})
	}
	m.runtimes = make(map[string]*managedEndpointRuntime)
	// Every runtime that can still be removed is now either in runtimes above
	// or already present in retirements. RemoveEndpoint rejects new removal
	// after stopping, and an active stage was resolved before this boundary.
	m.retirementAdmissionClosed = true
	retirements := make([]*endpointRuntimeRetirement, 0, len(m.retirements))
	for _, retirement := range m.retirements {
		retirements = append(retirements, retirement)
	}
	m.mu.Unlock()
	if snapshotHook != nil {
		snapshotHook(runtimes)
	}
	if m.shutdownHook != nil {
		m.shutdownHook(endpointRuntimeShutdownAfterSnapshot)
	}

	var shutdownErrs []error
	for _, retirement := range retirements {
		<-retirement.done
		if retirement.err != nil {
			shutdownErrs = append(shutdownErrs, retirement.err)
		}
	}
	result := errors.Join(append([]error{waitErr}, shutdownErrs...)...)
	m.mu.Lock()
	m.shutdownErr = result
	close(m.shutdownDone)
	m.mu.Unlock()
	return result
}

func (r *managedEndpointRuntime) stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.stopOnce.Do(func() {
		// Release the outer listener first. Remove, Commit retirement, and fatal
		// error handling all promise that the old port is reusable immediately;
		// graceful connection shutdown may continue behind that boundary.
		r.closeListener()
		if r.server != nil {
			r.stopErr = r.server.Shutdown(ctx)
		}
	})
	return r.stopErr
}

func (r *managedEndpointRuntime) closeListener() {
	if r == nil || r.listener == nil {
		return
	}
	r.listenerOnce.Do(func() {
		_ = r.listener.Close()
	})
}

type endpointSocksGate struct {
	current func() model.Endpoint
	next    inboundConnHandler
}

func (g *endpointSocksGate) ServeConnContext(ctx context.Context, conn net.Conn) {
	endpoint := model.Endpoint{}
	if g != nil && g.current != nil {
		endpoint = g.current()
	}
	if !endpoint.AllowProxy || !endpoint.AllowSOCKS5 || g == nil || g.next == nil {
		if conn != nil {
			_, _ = conn.Write([]byte{0x05, 0xFF})
			_ = conn.Close()
		}
		return
	}
	ctx = proxy.ContextWithInboundPolicy(ctx, proxy.InboundPolicy{
		RequireProxyAuthInfo: endpoint.RequireProxyAuthInfo,
	})
	g.next.ServeConnContext(ctx, conn)
}
