package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Resinat/Resin/internal/api"
	"github.com/Resinat/Resin/internal/buildinfo"
	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/metrics"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/requestlog"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
	"github.com/joho/godotenv"
)

type resinApp struct {
	envCfg               *config.EnvConfig
	runtimeCfg           *atomic.Pointer[config.RuntimeConfig]
	stateEngine          *state.StateEngine
	accountMatcher       *proxy.AccountMatcherRuntime
	geoSvc               *geoip.Service
	topoRuntime          *topologyRuntime
	flushWorker          *state.CacheFlushWorker
	metricsDB            *metrics.MetricsRepo
	metricsManager       *metrics.Manager
	requestlogRepo       *requestlog.Repo
	requestlogSvc        *requestlog.Service
	endpointManager      *endpointRuntimeManager
	transportPool        *proxy.OutboundTransportPool
	healthWriteOwner     *proxy.HealthWriteOwner
	closeProxyTransports func()

	// Package-private shutdown seam for deterministic ordering tests. Production
	// leaves it nil.
	beforeTopologyEventSourcesStopHook func()
	afterTopologySchedulerStopHook     func()
	beforeTransportPoolShutdownHook    func()
	afterStateWriteAdmissionCloseHook  func()
	afterStateWriteTimeoutHook         func()
	beforeOutboundShutdownHook         func()
	afterOutboundShutdownHook          func()
}

func run() error {
	if err := loadDotenvFile(".env"); err != nil {
		return err
	}

	envCfg, err := config.LoadEnvConfig()
	if err != nil {
		return err
	}

	engine, dbCloser, err := state.PersistenceBootstrap(envCfg.StateDir, envCfg.CacheDir)
	if err != nil {
		return fmt.Errorf("persistence bootstrap: %w", err)
	}
	log.Println("Persistence bootstrap complete")

	app, err := newResinApp(envCfg, engine)
	if err != nil {
		_ = dbCloser.Close()
		return err
	}

	serverErrCh := app.startServers()
	runtimeErr := waitForShutdown(serverErrCh)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	continuations := app.shutdown(ctx)
	if err := closePersistenceAfterShutdown(continuations, dbCloser); err != nil {
		log.Printf("Persistence close error: %v", err)
	}
	if runtimeErr != nil {
		return fmt.Errorf("runtime server error: %w", runtimeErr)
	}
	return nil
}

func loadDotenvFile(path string) error {
	if err := godotenv.Load(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("load %s: %w", path, err)
	}
	return nil
}

func newResinApp(envCfg *config.EnvConfig, engine *state.StateEngine) (*resinApp, error) {
	app := &resinApp{
		envCfg:      envCfg,
		runtimeCfg:  &atomic.Pointer[config.RuntimeConfig]{},
		stateEngine: engine,
	}
	startupComplete := false
	defer func() {
		if !startupComplete {
			app.closeUnstartedResources()
		}
	}()
	app.runtimeCfg.Store(loadRuntimeConfig(engine))
	if err := ensureDefaultAccountHeaderRule(engine); err != nil {
		return nil, err
	}
	app.accountMatcher = buildAccountMatcher(engine)

	retryDL, err := app.initTopologyRuntime(engine)
	if err != nil {
		return nil, err
	}
	app.wireRetryDownloader(retryDL)

	if err := app.bootstrapFromPersistence(engine); err != nil {
		return nil, err
	}
	if err := app.initObservability(); err != nil {
		return nil, err
	}
	if err := app.buildNetworkServers(engine); err != nil {
		return nil, err
	}

	app.startBackgroundServices()
	startupComplete = true
	return app, nil
}

// closeUnstartedResources releases resources created before startup completed.
// newResinApp does not expose a partially initialized app to its caller, so it
// owns rollback for every constructor that succeeded before a later one
// failed. None of these workers has been started yet at this point.
func (a *resinApp) closeUnstartedResources() {
	if a == nil {
		return
	}
	if a.endpointManager != nil {
		_ = a.endpointManager.Shutdown(context.Background())
		a.endpointManager = nil
	}
	if a.closeProxyTransports != nil {
		a.closeProxyTransports()
		a.closeProxyTransports = nil
	}
	if a.flushWorker != nil {
		if err := a.flushWorker.StopContext(context.Background()); err != nil {
			log.Printf("startup rollback cache flush: %v", err)
		}
		a.flushWorker = nil
	}
	if a.geoSvc != nil {
		_ = a.geoSvc.StopContext(context.Background())
		a.geoSvc = nil
	}
	if a.healthWriteOwner != nil {
		a.healthWriteOwner.CloseAndWait()
		a.healthWriteOwner = nil
	}
	if a.transportPool != nil {
		a.transportPool.Shutdown()
		a.transportPool = nil
	}
	if a.requestlogSvc != nil {
		_ = a.requestlogSvc.CloseContext(context.Background())
		a.requestlogSvc = nil
		a.requestlogRepo = nil
	}
	if a.requestlogRepo != nil {
		_ = a.requestlogRepo.Close()
		a.requestlogRepo = nil
	}
	if a.metricsManager != nil {
		_ = a.metricsManager.CloseContext(context.Background())
		a.metricsManager = nil
		a.metricsDB = nil
	}
	if a.metricsDB != nil {
		_ = a.metricsDB.Close()
		a.metricsDB = nil
	}
	if a.topoRuntime != nil && a.topoRuntime.outboundMgr != nil {
		a.topoRuntime.outboundMgr.RetireAllOutboundsAndWait()
		a.topoRuntime.outboundMgr = nil
	}
	if a.topoRuntime != nil && a.topoRuntime.singboxBuilder != nil {
		_ = a.topoRuntime.singboxBuilder.Close()
		a.topoRuntime.singboxBuilder = nil
	}
}

func (a *resinApp) initTopologyRuntime(engine *state.StateEngine) (*netutil.RetryDownloader, error) {
	// Phase 1: Create DirectDownloader and RetryDownloader shell.
	// NodePicker/ProxyFetch are nil initially; set after Pool + OutboundManager creation.
	direct := newDirectDownloader(a.envCfg)
	retryDL := &netutil.RetryDownloader{Direct: direct}

	// Phase 2: Construct GeoIP service (start after retry downloader wiring).
	a.geoSvc = newGeoIPService(a.envCfg.CacheDir, a.envCfg.GeoIPUpdateSchedule, retryDL)

	// Phase 3: Topology (pool, probe, scheduler).
	topoRuntime, err := newTopologyRuntime(
		engine,
		a.envCfg,
		a.runtimeCfg,
		a.geoSvc,
		retryDL,
		a.onProbeConnectionLifecycle,
		func(hash node.Hash) {
			if a.transportPool != nil {
				a.transportPool.Evict(hash)
			}
		},
	)
	if err != nil {
		return nil, fmt.Errorf("topology runtime: %w", err)
	}
	a.topoRuntime = topoRuntime

	// Phase 4: OutboundManager and Router (now that pool exists).
	log.Println("OutboundManager initialized with lifecycle callbacks")
	a.topoRuntime.router = routing.NewRouter(routing.RouterConfig{
		Pool: a.topoRuntime.pool,
		Authorities: func() []string {
			return runtimeConfigSnapshot(a.runtimeCfg).LatencyAuthorities
		},
		P2CWindow: func() time.Duration {
			return time.Duration(runtimeConfigSnapshot(a.runtimeCfg).P2CLatencyWindow)
		},
		NodeTagResolver: a.topoRuntime.pool.ResolveNodeDisplayTagForEntry,
		// Lease events are delivered synchronously after the routing lifecycle
		// lock is released. This callback persists dirty-set state and metrics;
		// it must not call Router mutation APIs that emit another lease event.
		OnLeaseEvent: func(e routing.LeaseEvent) {
			switch e.Type {
			case routing.LeaseCreate, routing.LeaseTouch, routing.LeaseReplace:
				engine.MarkLease(e.PlatformID, e.Account)
			case routing.LeaseRemove, routing.LeaseExpire:
				engine.MarkLeaseDelete(e.PlatformID, e.Account)
			}
			a.onLeaseEventForMetrics(e)
		},
	})
	a.topoRuntime.leaseCleaner = routing.NewLeaseCleaner(a.topoRuntime.router)
	log.Println("Router and LeaseCleaner initialized")
	return retryDL, nil
}

func (a *resinApp) onProbeConnectionLifecycle(op netutil.ConnLifecycleOp) {
	if a == nil || a.metricsManager == nil {
		return
	}
	switch op {
	case netutil.ConnLifecycleOpen:
		a.metricsManager.OnConnectionLifecycle(proxy.ConnectionOutbound, proxy.ConnectionOpen)
	case netutil.ConnLifecycleClose:
		a.metricsManager.OnConnectionLifecycle(proxy.ConnectionOutbound, proxy.ConnectionClose)
	}
}

func (a *resinApp) onLeaseEventForMetrics(e routing.LeaseEvent) {
	if a == nil || a.metricsManager == nil {
		return
	}

	op := metrics.LeaseOpTouch
	switch e.Type {
	case routing.LeaseCreate:
		op = metrics.LeaseOpCreate
	case routing.LeaseReplace:
		op = metrics.LeaseOpReplace
	case routing.LeaseRemove:
		op = metrics.LeaseOpRemove
	case routing.LeaseExpire:
		op = metrics.LeaseOpExpire
	}

	lifetimeNs := int64(0)
	if e.CreatedAtNs > 0 && op.HasLifetimeSample() {
		lifetimeNs = time.Now().UnixNano() - e.CreatedAtNs
	}

	a.metricsManager.OnLeaseEvent(metrics.LeaseMetricEvent{
		PlatformID: e.PlatformID,
		Op:         op,
		LifetimeNs: lifetimeNs,
	})
}

func (a *resinApp) wireRetryDownloader(retryDL *netutil.RetryDownloader) {
	// Phase 5: Complete RetryDownloader wiring (now that Pool + OutboundManager exist).
	// Resource downloads have no account or sticky-lease semantics. Pick from
	// the published Default platform view without entering Router's lifecycle
	// lock or creating routing state.
	retryDL.NodePicker = func(ctx context.Context, _ string) (netutil.NodeSelection, error) {
		hash, entry, err := a.topoRuntime.pool.PickDefaultPlatformOutbound(ctx)
		if err != nil {
			return netutil.NodeSelection{}, err
		}
		return netutil.NodeSelection{Hash: hash, Entry: entry}, nil
	}
	retryDL.ProxyFetch = func(ctx context.Context, selection netutil.NodeSelection, url string) ([]byte, error) {
		body, _, err := a.topoRuntime.outboundMgr.FetchWithExpectedEntry(
			ctx,
			selection.Hash,
			selection.Entry,
			url,
			currentDownloadUserAgent(),
		)
		return body, err
	}
	log.Println("RetryDownloader wiring complete")
}

func (a *resinApp) bootstrapFromPersistence(engine *state.StateEngine) error {
	// Phase 6: Bootstrap topology data from persistence.
	if err := bootstrapTopology(engine, a.topoRuntime.subManager, a.topoRuntime.pool, a.envCfg); err != nil {
		return err
	}

	// Phase 6.1: Bootstrap nodes (steps 3-6: static, subscription_nodes, dynamic, latency).
	if err := bootstrapNodes(
		engine,
		a.topoRuntime.pool,
		a.topoRuntime.subManager,
		a.topoRuntime.outboundMgr,
		a.envCfg,
		runtimeConfigSnapshot(a.runtimeCfg).LatencyAuthorities,
	); err != nil {
		return err
	}

	// GeoIP moved to step 8 batch 1 (after lease restore, per DESIGN.md).

	// Phase 8.1: Rebuild platform views BEFORE lease restore.
	// DESIGN.md requires step 6 (rebuild) before step 7 (leases).
	a.topoRuntime.pool.RebuildAllPlatforms()
	log.Println("Platform rebuild complete")

	// Phase 9: Restore leases (AFTER rebuild so platform views are populated).
	leases, err := engine.LoadAllLeases()
	if err != nil {
		log.Printf("Warning: load leases: %v", err)
	} else if len(leases) > 0 {
		a.topoRuntime.router.RestoreLeases(leases)
		log.Printf("Restored %d leases from cache.db", len(leases))
	}

	flushReaders := newFlushReaders(a.topoRuntime.pool, a.topoRuntime.subManager, a.topoRuntime.router)
	a.flushWorker = state.NewCacheFlushWorker(
		engine,
		flushReaders,
		func() int { return runtimeConfigSnapshot(a.runtimeCfg).CacheFlushDirtyThreshold },
		func() time.Duration { return time.Duration(runtimeConfigSnapshot(a.runtimeCfg).CacheFlushInterval) },
		5*time.Second, // check tick
	)
	return nil
}

func (a *resinApp) initObservability() error {
	// Phase 10: Initialize observability services.
	requestLogCfg, err := deriveRequestLogRuntimeSettings(a.envCfg)
	if err != nil {
		return fmt.Errorf("request log config: %w", err)
	}
	metricsCfg := deriveMetricsManagerSettings(a.envCfg)

	metricsDB, err := metrics.NewMetricsRepo(filepath.Join(a.envCfg.LogDir, "metrics.db"))
	if err != nil {
		return fmt.Errorf("metrics DB: %w", err)
	}
	a.metricsDB = metricsDB

	a.metricsManager, err = metrics.NewManager(metrics.ManagerConfig{
		Repo:                    a.metricsDB,
		LatencyBinMs:            metricsCfg.LatencyBinMs,
		LatencyOverflowMs:       metricsCfg.LatencyOverflowMs,
		BucketSeconds:           metricsCfg.BucketSeconds,
		ThroughputIntervalSec:   metricsCfg.ThroughputIntervalSec,
		ThroughputRetentionSec:  metricsCfg.ThroughputRetentionSec,
		ConnectionsIntervalSec:  metricsCfg.ConnectionsIntervalSec,
		ConnectionsRetentionSec: metricsCfg.ConnectionsRetentionSec,
		LeasesIntervalSec:       metricsCfg.LeasesIntervalSec,
		LeasesRetentionSec:      metricsCfg.LeasesRetentionSec,
		RuntimeStats: &runtimeStatsAdapter{
			pool:   a.topoRuntime.pool,
			router: a.topoRuntime.router,
			authorities: func() []string {
				return runtimeConfigSnapshot(a.runtimeCfg).LatencyAuthorities
			},
		},
	})
	if err != nil {
		_ = metricsDB.Close()
		a.metricsDB = nil
		return fmt.Errorf("metrics manager: %w", err)
	}

	a.requestlogRepo = requestlog.NewRepo(
		a.envCfg.LogDir,
		requestLogCfg.DBMaxBytes,
		requestLogCfg.DBRetainCount,
	)
	if err := a.requestlogRepo.Open(); err != nil {
		// Observability initialization is staged: requestlog is opened after
		// metrics. Roll back both owners before exposing the startup error so a
		// later bootstrap failure cannot inherit a half-initialized app.
		_ = a.requestlogRepo.Close()
		a.requestlogRepo = nil
		a.requestlogSvc = nil
		_ = a.metricsManager.CloseContext(context.Background())
		a.metricsManager = nil
		a.metricsDB = nil
		return fmt.Errorf("requestlog repo open: %w", err)
	}
	a.requestlogSvc = requestlog.NewService(requestlog.ServiceConfig{
		Repo:          a.requestlogRepo,
		QueueSize:     requestLogCfg.QueueSize,
		FlushBatch:    requestLogCfg.FlushBatch,
		FlushInterval: requestLogCfg.FlushInterval,
	})
	return nil
}

func (a *resinApp) startBackgroundServices() {
	// --- Step 8 Batch 1: CacheFlushWorker + GeoIP + MetricsManager ---
	a.flushWorker.Start()
	log.Println("Cache flush worker started")

	startGeoIPService(a.geoSvc)
	log.Println("GeoIP service started (batch 1)")

	a.metricsManager.Start()
	log.Println("Metrics manager started (batch 1)")

	// --- Step 8 Batch 2: ProbeManager, RequestLog, LeaseCleaner, EphemeralCleaner ---
	a.topoRuntime.probeMgr.SetOnProbeEvent(func(kind string) {
		a.metricsManager.OnProbeEvent(metrics.ProbeEvent{Kind: metrics.ProbeKind(kind)})
	})

	a.topoRuntime.probeMgr.Start()
	log.Println("Probe manager started (batch 2)")

	a.requestlogSvc.Start()
	log.Println("Request log service started (batch 2)")

	a.topoRuntime.leaseCleaner.Start()
	log.Println("Lease cleaner started (batch 2)")

	a.topoRuntime.ephemeralCleaner.Start()
	log.Println("Ephemeral cleaner started (batch 2)")

	// --- Step 8 Batch 3: Subscription scheduler (force full refresh on start) ---
	a.topoRuntime.scheduler.Start()
	a.topoRuntime.scheduler.ForceRefreshAllAsync()
	log.Println("Subscription scheduler started; forced full refresh running in background (batch 3)")
}

func (a *resinApp) buildNetworkServers(engine *state.StateEngine) error {
	startedAt := time.Now().UTC()
	systemInfo := service.SystemInfo{
		Version:   buildinfo.Version,
		GitCommit: buildinfo.GitCommit,
		BuildTime: buildinfo.BuildTime,
		StartedAt: startedAt,
	}

	cpService := &service.ControlPlaneService{
		RuntimeCfg:     a.runtimeCfg,
		EnvCfg:         a.envCfg,
		Engine:         engine,
		Pool:           a.topoRuntime.pool,
		SubMgr:         a.topoRuntime.subManager,
		Scheduler:      a.topoRuntime.scheduler,
		Router:         a.topoRuntime.router,
		ProbeMgr:       a.topoRuntime.probeMgr,
		GeoIP:          a.geoSvc,
		MatcherRuntime: a.accountMatcher,
	}

	apiSrv := api.NewServerWithAddress(
		a.envCfg.ListenAddress,
		a.envCfg.ResinPort,
		a.envCfg.AdminToken,
		systemInfo,
		a.runtimeCfg,
		a.envCfg,
		cpService,
		int64(a.envCfg.APIMaxBodyBytes),
		a.requestlogRepo,
		a.metricsManager,
	)
	tokenActionHandler := api.NewTokenActionHandler(
		a.envCfg.ProxyToken,
		cpService,
		int64(a.envCfg.APIMaxBodyBytes),
	)

	proxyEvents := a.buildProxyEvents()
	outboundTransportCfg := proxy.OutboundTransportConfig{
		MaxIdleConns:        a.envCfg.ProxyTransportMaxIdleConns,
		MaxIdleConnsPerHost: a.envCfg.ProxyTransportMaxIdleConnsPerHost,
		IdleConnTimeout:     a.envCfg.ProxyTransportIdleConnTimeout,
	}
	if a.transportPool == nil {
		a.transportPool = proxy.NewOutboundTransportPool(outboundTransportCfg)
	}
	if a.healthWriteOwner == nil {
		a.healthWriteOwner = proxy.NewHealthWriteOwner(a.topoRuntime.pool)
	}

	forwardProxy := proxy.NewForwardProxy(proxy.ForwardProxyConfig{
		ProxyToken:        a.envCfg.ProxyToken,
		Router:            a.topoRuntime.router,
		Pool:              a.topoRuntime.pool,
		Health:            a.healthWriteOwner,
		Events:            proxyEvents,
		MetricsSink:       a.metricsManager,
		OutboundTransport: outboundTransportCfg,
		TransportPool:     a.transportPool,
		ProxyBypassRules:  a.envCfg.ProxyBypassRules,
	})

	reverseProxy := proxy.NewReverseProxy(proxy.ReverseProxyConfig{
		ProxyToken:        a.envCfg.ProxyToken,
		Router:            a.topoRuntime.router,
		Pool:              a.topoRuntime.pool,
		PlatformLookup:    a.topoRuntime.pool,
		Health:            a.healthWriteOwner,
		Matcher:           a.accountMatcher,
		Events:            proxyEvents,
		MetricsSink:       a.metricsManager,
		OutboundTransport: outboundTransportCfg,
		TransportPool:     a.transportPool,
		ProxyBypassRules:  a.envCfg.ProxyBypassRules,
	})
	a.closeProxyTransports = func() {
		forwardProxy.CloseIdleConnections()
		reverseProxy.CloseIdleConnections()
	}
	socks5Inbound := proxy.NewSocks5Inbound(proxy.Socks5InboundConfig{
		ProxyToken:       a.envCfg.ProxyToken,
		Router:           a.topoRuntime.router,
		Pool:             a.topoRuntime.pool,
		Health:           a.healthWriteOwner,
		Events:           proxyEvents,
		MetricsSink:      a.metricsManager,
		ProxyBypassRules: a.envCfg.ProxyBypassRules,
	})

	endpointManager := newEndpointRuntimeManager(
		a.envCfg.ListenAddress,
		a.envCfg.ProxyToken,
		forwardProxy,
		reverseProxy,
		apiSrv.Handler(),
		tokenActionHandler,
		socks5Inbound,
		a.metricsManager,
	)
	a.endpointManager = endpointManager
	cpService.EndpointRuntime = endpointManager
	defaultEndpoint := service.NewDefaultEndpoint(a.envCfg.ResinPort)
	if err := endpointManager.ApplyEndpoint(defaultEndpoint); err != nil {
		return fmt.Errorf("default endpoint listen: %w", err)
	}
	if err := restorePersistedEndpoints(engine, endpointManager); err != nil {
		endpointManager.RemoveEndpoint(service.DefaultEndpointID)
		return fmt.Errorf("load endpoints: %w", err)
	}
	return nil
}

func restorePersistedEndpoints(engine *state.StateEngine, endpointManager *endpointRuntimeManager) error {
	customEndpoints, err := engine.ListEndpoints()
	if err != nil {
		return err
	}
	for _, endpoint := range customEndpoints {
		if !endpoint.Enabled {
			continue
		}
		if err := endpointManager.ApplyEndpoint(endpoint); err != nil {
			endpointManager.RecordEndpointError(endpoint, err)
			log.Printf("Endpoint %s on port %d unavailable: %v", endpoint.ID, endpoint.Port, err)
		}
	}
	return nil
}

func (a *resinApp) buildProxyEvents() proxy.ConfigAwareEventEmitter {
	// Composite emitter: requestlog handles EmitRequestLog, metricsManager handles EmitRequestFinished.
	composite := compositeEmitter{logSvc: a.requestlogSvc, metricsMgr: a.metricsManager}
	return proxy.ConfigAwareEventEmitter{
		Base: composite,
		RequestLogConfigProvider: func() proxy.RequestLogRuntimeConfig {
			cfg := runtimeConfigSnapshot(a.runtimeCfg)
			return proxy.RequestLogRuntimeConfig{
				Enabled:             cfg.RequestLogEnabled,
				DetailEnabled:       cfg.ReverseProxyLogDetailEnabled,
				ReqHeadersMaxBytes:  cfg.ReverseProxyLogReqHeadersMaxBytes,
				ReqBodyMaxBytes:     cfg.ReverseProxyLogReqBodyMaxBytes,
				RespHeadersMaxBytes: cfg.ReverseProxyLogRespHeadersMaxBytes,
				RespBodyMaxBytes:    cfg.ReverseProxyLogRespBodyMaxBytes,
			}
		},
	}
}

func (a *resinApp) startServers() <-chan error {
	log.Printf(
		"Resin default endpoint starting on %s (%s)",
		formatListenAddress(a.envCfg.ListenAddress, a.envCfg.ResinPort),
		"HTTP + SOCKS5",
	)
	return a.endpointManager.Start()
}

func waitForShutdown(serverErrCh <-chan error) error {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)

	select {
	case sig := <-quit:
		log.Printf("Received signal %s, shutting down...", sig)
		return nil
	case err := <-serverErrCh:
		log.Printf("Received server runtime error (%v), shutting down...", err)
		return err
	}
}

func formatListenAddress(listenAddress string, port int) string {
	return net.JoinHostPort(listenAddress, strconv.Itoa(port))
}

func formatListenURL(listenAddress string, port int) string {
	return "http://" + formatListenAddress(listenAddress, port)
}

func (a *resinApp) shutdown(ctx context.Context) shutdownContinuations {
	var continuations shutdownContinuations
	var topologyReady <-chan struct{}
	if err, continuation := closeEndpointForShutdown(ctx, a.endpointManager); err != nil {
		log.Printf("Server shutdown error: %v", err)
		if continuation != nil {
			continuations.endpoint = continuation
			log.Println("Endpoint shutdown continues in shutdown owner")
		}
	}
	log.Println("Resin server stopped")
	// Transport retirement can complete health writebacks through the shared
	// owner, so close both transport reuse and health-write admission before
	// closing the dirty/state sinks.
	if a.transportPool != nil {
		if hook := a.beforeTransportPoolShutdownHook; hook != nil {
			hook()
		}
		a.transportPool.Shutdown()
		log.Println("Outbound transport pool closed")
	}
	if a.healthWriteOwner != nil {
		a.healthWriteOwner.CloseAndWait()
		log.Println("Proxy health writes stopped")
	}

	// Stop internal event sources before the normal sink shutdown. Callbacks that
	// finish within the caller deadline can still publish their final state. If
	// the scheduler owner outlives that deadline, its production runtime
	// preparation callback is fail-closed by the outbound/probe owners below;
	// the topology continuation is still joined before shared persistence closes.
	if err, continuation := a.stopTopologyEventSources(ctx); err != nil {
		log.Printf("Topology event-source shutdown error: %v", err)
		if continuation != nil {
			continuations.topology, topologyReady = trackShutdownContinuation(continuation)
			log.Println("Topology event sources continue in shutdown owner")
		}
	}
	if a.geoSvc != nil {
		err, continuation := closeGeoIPForShutdown(ctx, a.geoSvc)
		if err != nil {
			log.Printf("GeoIP shutdown error: %v", err)
		}
		if continuation != nil {
			continuations.geoIP = continuation
			log.Println("GeoIP shutdown continues in shutdown owner")
		}
		log.Println("GeoIP service stopped")
	}

	// endpoint Shutdown preserves its caller deadline and may return while an
	// already-admitted HTTP handler is still unwinding. Drain those handlers
	// before closing any sink or persistence handle they can touch; otherwise a
	// late Mark* or strong state mutation can happen after final flush. The same
	// shutdown context bounds this drain; once it expires, both write owners
	// close admission.
	// A topology owner can still be inside a synchronous lease/health callback.
	// Keep dirty admission open until that owner has delivered its callback; the
	// cache stop owner is the later barrier that rejects genuinely late writes.
	var dirtyAdmission dirtyWriteAdmissionWaiter
	if topologyReady == nil {
		dirtyAdmission = a.flushWorker
	}
	httpDrainErr := drainHTTPHandlersBeforePersistence(ctx, a.endpointManager, dirtyAdmission, a.stateEngine)
	if httpDrainErr != nil {
		log.Printf("HTTP handler drain error: %v", httpDrainErr)
	}
	closeProxyTransports := a.closeProxyTransports
	a.closeProxyTransports = nil
	if closeProxyTransports != nil {
		if httpDrainErr != nil && a.endpointManager != nil {
			manager := a.endpointManager
			continuation := make(chan error, 1)
			continuations.directProxy = continuation
			go func() {
				// Join the endpoint manager's own shutdown owner first. A persisted
				// stage can publish a runtime after the bounded caller returned; the
				// handler snapshot must be taken only after that owner has settled.
				_ = manager.Shutdown(context.Background())
				waitErr := manager.WaitForHTTPHandlers(context.Background())
				closeProxyTransports()
				continuation <- waitErr
			}()
			log.Println("Direct proxy transports wait for late HTTP handlers")
		} else {
			closeProxyTransports()
			log.Println("Direct proxy transports closed")
		}
	}
	stateWritesDrained := a.stateEngine == nil
	if a.stateEngine != nil {
		if err := a.stateEngine.CloseStateWriteAdmissionAndWait(ctx); err != nil {
			stateWritesDrained = false
			log.Printf("State write shutdown error: %v", err)
		}
		if hook := a.afterStateWriteAdmissionCloseHook; hook != nil {
			hook()
		}
		if !stateWritesDrained {
			if hook := a.afterStateWriteTimeoutHook; hook != nil {
				hook()
			}
		}
	}

	shutdownOutbound := func(shutdownCtx context.Context) (error, <-chan error) {
		var closeBuilder func() error
		if a.topoRuntime.singboxBuilder != nil {
			closeBuilder = a.topoRuntime.singboxBuilder.Close
		}
		if hook := a.beforeOutboundShutdownHook; hook != nil {
			hook()
		}
		err, continuation := closeOutboundForShutdown(
			shutdownCtx,
			a.topoRuntime.outboundMgr.ShutdownContext,
			closeBuilder,
		)
		if hook := a.afterOutboundShutdownHook; hook != nil {
			hook()
		}
		return err, continuation
	}

	if a.topoRuntime != nil && a.topoRuntime.outboundMgr != nil {
		// All new network admission is closed by the endpoint/source barriers
		// above. Close outbound construction admission and retire the exact node
		// outbounds now; active leased operations keep their adapter until release,
		// while late callers fail closed.
		waitForDirtyWrites := httpDrainErr != nil && a.flushWorker != nil
		if !stateWritesDrained || waitForDirtyWrites {
			continuation := make(chan error, 1)
			go func() {
				if topologyReady != nil {
					<-topologyReady
				}
				if !stateWritesDrained {
					a.stateEngine.WaitForStateWrites()
				}
				if waitForDirtyWrites {
					a.flushWorker.CloseDirtyWriteAdmissionAndWait()
				}
				err, ownerContinuation := shutdownOutbound(context.Background())
				if ownerContinuation != nil {
					err = errors.Join(err, <-ownerContinuation)
				}
				continuation <- err
			}()
			continuations.outbound = continuation
			log.Println("Node outbound shutdown waits for state/dirty-write owner")
		} else {
			err, continuation := shutdownOutbound(ctx)
			if err != nil {
				log.Printf("Node outbound shutdown error: %v", err)
			}
			if continuation != nil {
				continuations.outbound = continuation
				log.Println("Node outbound retirement continues in shutdown owner")
			} else {
				log.Println("Node outbounds retired")
			}
		}
	}
	// Shutdown dependency order is transport/health and event sources first,
	// then HTTP/write admission barriers, sinks, and persistence. On the normal
	// path event sources finish before those sinks close. A scheduler timeout is
	// tracked as a topology continuation; its late runtime preparation can only
	// hit the subordinate fail-closed owners and is joined before DB close.
	// 2. Stop observability sinks (flush remaining data).
	if a.requestlogSvc != nil {
		err, continuation := closeRequestLogForShutdown(ctx, a.requestlogSvc)
		if err != nil {
			log.Printf("Request log shutdown/close error: %v", err)
		}
		if continuation != nil {
			continuations.requestLog = continuation
		}
		log.Println("Request log service and repo stopped")
	} else if a.requestlogRepo != nil {
		if err := a.requestlogRepo.CloseContext(ctx); err != nil {
			log.Printf("Request log repo close error: %v", err)
		}
		log.Println("Request log repo closed")
	}

	if a.metricsManager != nil {
		err, continuation := closeMetricsManagerForShutdown(ctx, a.metricsManager)
		if err != nil {
			log.Printf("Metrics shutdown/close error: %v", err)
		}
		if continuation != nil {
			continuations.metrics = continuation
		}
		log.Println("Metrics manager and DB stopped")
	} else if a.metricsDB != nil {
		if err := a.metricsDB.Close(); err != nil {
			log.Printf("Metrics DB close error: %v", err)
		}
		log.Println("Metrics DB closed")
	}

	// 3. Stop infrastructure. The outbound helper closes SingboxBuilder
	// immediately on the normal path, or after its continuation owner retires
	// admitted builds/leases on a caller timeout.

	if topologyReady != nil {
		continuation := make(chan error, 1)
		continuations.cacheFlush = continuation
		go func() {
			<-topologyReady
			cacheErr, ownerContinuation := closeCacheFlushWorkerForShutdown(context.Background(), a.flushWorker)
			if ownerContinuation != nil {
				cacheErr = errors.Join(cacheErr, <-ownerContinuation)
			}
			continuation <- cacheErr
		}()
	} else {
		err, continuation := closeCacheFlushWorkerForShutdown(ctx, a.flushWorker)
		if err != nil {
			log.Printf("Cache flush shutdown error: %v", err)
		}
		if continuation != nil {
			continuations.cacheFlush = continuation
		}
	}
	log.Println("Server stopped")
	return continuations
}

func (a *resinApp) stopTopologyEventSources(contexts ...context.Context) (error, <-chan error) {
	if a == nil {
		return nil, nil
	}
	ctx := context.Background()
	if len(contexts) != 0 && contexts[0] != nil {
		ctx = contexts[0]
	}
	if hook := a.beforeTopologyEventSourcesStopHook; hook != nil {
		hook()
	}
	if a.topoRuntime == nil {
		return nil, nil
	}
	var stopErrs []error
	var pendingContinuations []<-chan error
	if a.topoRuntime.leaseCleaner != nil {
		leaseErr := a.topoRuntime.leaseCleaner.StopContext(ctx)
		if leaseErr != nil {
			stopErrs = append(stopErrs, leaseErr)
			continuation := make(chan error, 1)
			pendingContinuations = append(pendingContinuations, continuation)
			cleaner := a.topoRuntime.leaseCleaner
			router := a.topoRuntime.router
			go func() {
				ownerErr := cleaner.StopContext(context.Background())
				if router != nil {
					router.Stop()
				}
				continuation <- ownerErr
			}()
			log.Println("Lease cleaner stop continues in shutdown owner")
		} else {
			log.Println("Lease cleaner stopped")
			if a.topoRuntime.router != nil {
				a.topoRuntime.router.Stop()
				log.Println("Router stopped")
			}
		}
	} else if a.topoRuntime.router != nil {
		a.topoRuntime.router.Stop()
		log.Println("Router stopped")
	}
	if a.topoRuntime.ephemeralCleaner != nil {
		ephemeralErr := a.topoRuntime.ephemeralCleaner.StopContext(ctx)
		if ephemeralErr != nil {
			stopErrs = append(stopErrs, ephemeralErr)
			continuation := make(chan error, 1)
			pendingContinuations = append(pendingContinuations, continuation)
			cleaner := a.topoRuntime.ephemeralCleaner
			go func() {
				continuation <- cleaner.StopContext(context.Background())
			}()
			log.Println("Ephemeral cleaner stop continues in shutdown owner")
		} else {
			log.Println("Ephemeral cleaner stopped")
		}
	}
	if a.topoRuntime.scheduler != nil {
		schedulerErr := a.topoRuntime.scheduler.StopContext(ctx)
		if hook := a.afterTopologySchedulerStopHook; hook != nil {
			hook()
		}
		if schedulerErr != nil {
			stopErrs = append(stopErrs, schedulerErr)
			continuation := make(chan error, 1)
			pendingContinuations = append(pendingContinuations, continuation)
			scheduler := a.topoRuntime.scheduler
			go func() {
				continuation <- scheduler.StopContext(context.Background())
			}()
			log.Println("Subscription scheduler stop continues in shutdown owner")
		} else {
			log.Println("Subscription scheduler stopped")
		}
	}
	if a.topoRuntime.probeMgr != nil {
		a.topoRuntime.probeMgr.Stop()
		log.Println("Probe manager stopped")
	}
	if len(pendingContinuations) == 0 {
		return errors.Join(stopErrs...), nil
	}
	joined := make(chan error, 1)
	go func() {
		var continuationErrs []error
		for _, continuation := range pendingContinuations {
			if err := <-continuation; err != nil {
				continuationErrs = append(continuationErrs, err)
			}
		}
		joined <- errors.Join(continuationErrs...)
	}()
	return errors.Join(stopErrs...), joined
}

type shutdownContinuations struct {
	endpoint    <-chan error
	directProxy <-chan error
	geoIP       <-chan error
	topology    <-chan error
	outbound    <-chan error
	cacheFlush  <-chan error
	requestLog  <-chan error
	metrics     <-chan error
}

// wait joins all shutdown owners in dependency order. Cache persistence must
// finish before the shared cache DB is closed; the other sink owners are also
// joined so the process cannot return and kill tracked goroutines.
func (c shutdownContinuations) wait() error {
	var errs []error
	for _, continuation := range []<-chan error{c.endpoint, c.directProxy, c.geoIP, c.topology, c.outbound, c.cacheFlush, c.requestLog, c.metrics} {
		if continuation == nil {
			continue
		}
		if err := <-continuation; err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// closeEndpointForShutdown joins the endpoint manager's single shutdown owner
// when a persisted control-plane stage outlives the caller's deadline. The
// manager owns the background continuation; this helper only provides the
// process-level join before shared persistence is closed.
func closeEndpointForShutdown(
	ctx context.Context,
	manager *endpointRuntimeManager,
) (error, <-chan error) {
	if manager == nil {
		return nil, nil
	}
	err := manager.Shutdown(ctx)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err, nil
	}
	if !manager.shutdownOwnerInProgress() {
		return err, nil
	}
	continuation := make(chan error, 1)
	go func() {
		continuation <- manager.Shutdown(context.Background())
	}()
	return err, continuation
}

// closeGeoIPForShutdown keeps the GeoIP service's single StopContext owner
// alive after a caller deadline. The service itself owns reader/update
// completion; this continuation only joins that owner before process exit.
func closeGeoIPForShutdown(ctx context.Context, service *geoip.Service) (error, <-chan error) {
	if service == nil {
		return nil, nil
	}
	err := service.StopContext(ctx)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err, nil
	}
	continuation := make(chan error, 1)
	go func() {
		continuation <- service.StopContext(context.Background())
	}()
	return err, continuation
}

// closeOutboundForShutdown keeps builder ownership with the outbound
// shutdown owner. A caller timeout may return while an admitted Build or
// leased outbound is still unwinding; closing the builder before that owner
// finishes would race its sing-box service graph. The continuation performs
// the same owner wait with a fresh context and closes the builder exactly once.
func closeOutboundForShutdown(
	ctx context.Context,
	shutdown func(context.Context) error,
	closeBuilder func() error,
) (error, <-chan error) {
	if shutdown == nil {
		if closeBuilder == nil {
			return nil, nil
		}
		return closeBuilder(), nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := shutdown(ctx)
	if err == nil {
		if closeBuilder == nil {
			return nil, nil
		}
		return closeBuilder(), nil
	}
	continuation := make(chan error, 1)
	go func() {
		ownerErr := shutdown(context.Background())
		if closeBuilder != nil {
			ownerErr = errors.Join(ownerErr, closeBuilder())
		}
		continuation <- ownerErr
	}()
	return err, continuation
}

// closePersistenceAfterShutdown is the process-level shutdown owner. It
// joins every tracked continuation before closing shared state/cache DBs and
// deliberately uses a fresh context for that final close.
func closePersistenceAfterShutdown(continuations shutdownContinuations, closer state.PersistenceCloser) error {
	waitErr := continuations.wait()
	if closer == nil {
		return waitErr
	}
	return errors.Join(waitErr, closer.CloseContext(context.Background()))
}

// closeMetricsManagerForShutdown keeps the application shutdown call site
// testable. If the bounded caller wait expires, the same manager owner is
// continued in the background; no raw repository close can race that owner.
func closeMetricsManagerForShutdown(ctx context.Context, manager *metrics.Manager) (error, <-chan error) {
	if manager == nil {
		return nil, nil
	}
	err := manager.CloseContext(ctx)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err, nil
	}
	continuation := make(chan error, 1)
	go func() {
		continuation <- manager.CloseContext(context.Background())
	}()
	return err, continuation
}

// closeRequestLogForShutdown keeps the request-log shutdown owner in one
// production/test seam. If the bounded caller wait expires, the same service
// owner is continued in the background; no raw repo close can race it.
func closeRequestLogForShutdown(ctx context.Context, service *requestlog.Service) (error, <-chan error) {
	if service == nil {
		return nil, nil
	}
	err := service.CloseContext(ctx)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err, nil
	}
	continuation := make(chan error, 1)
	go func() {
		continuation <- service.CloseContext(context.Background())
	}()
	return err, continuation
}

// closeCacheFlushWorkerForShutdown keeps the cache flush stop owner tracked.
// If the bounded caller wait expires, the same owner is continued and joined
// by the process coordinator before shared persistence is closed.
func closeCacheFlushWorkerForShutdown(ctx context.Context, worker *state.CacheFlushWorker) (error, <-chan error) {
	if worker == nil {
		return nil, nil
	}
	err := worker.StopContext(ctx)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err, nil
	}
	continuation := make(chan error, 1)
	go func() {
		continuation <- worker.StopContext(context.Background())
	}()
	return err, continuation
}

// trackShutdownContinuation exposes one completion signal to dependent
// shutdown owners while preserving the error result for the process join.
// A plain one-shot result channel cannot safely serve both consumers.
func trackShutdownContinuation(continuation <-chan error) (<-chan error, <-chan struct{}) {
	result := make(chan error, 1)
	ready := make(chan struct{})
	go func() {
		err := <-continuation
		close(ready)
		result <- err
	}()
	return result, ready
}

type dirtyWriteAdmissionWaiter interface {
	CloseDirtyWriteAdmission()
}

func drainHTTPHandlersBeforeSinks(
	ctx context.Context,
	manager *endpointRuntimeManager,
	flushWorker dirtyWriteAdmissionWaiter,
) error {
	if manager == nil {
		return nil
	}
	err := manager.WaitForHTTPHandlers(ctx)
	if err != nil && flushWorker != nil {
		// The bounded drain has expired. Close the dirty-write admission before
		// continuing shutdown so a handler that unwinds later cannot re-dirty
		// cache state after the final flush. The stop owner waits for marks that
		// passed admission before it performs that final flush.
		flushWorker.CloseDirtyWriteAdmission()
	}
	return err
}

// drainHTTPHandlersBeforePersistence is the production shutdown barrier for
// handlers that can write either cache dirty sets or strong state.db rows.
// When the bounded drain expires, strong-write admission closes first. Dirty
// admission closes immediately only if all admitted strong mutations have
// already returned; otherwise the cache stop owner closes it after waiting for
// that same state owner, immediately before its final flush.
func drainHTTPHandlersBeforePersistence(
	ctx context.Context,
	manager *endpointRuntimeManager,
	flushWorker dirtyWriteAdmissionWaiter,
	engine *state.StateEngine,
) error {
	if manager == nil {
		return nil
	}
	err := manager.WaitForHTTPHandlers(ctx)
	if err == nil {
		return nil
	}
	// Close the strong-write admission first. A control-plane mutation can
	// persist state and then emit dirty cache marks; closing dirty admission
	// first would allow that mutation to delete state while its cache delete is
	// rejected in the gap between the two barriers. If an admitted strong
	// mutation outlives this bounded wait, the cache stop owner waits for that
	// same mutation before closing dirty admission and doing its final flush.
	stateWritesDrained := engine == nil
	if engine != nil {
		if closeErr := engine.CloseStateWriteAdmissionAndWait(ctx); closeErr != nil {
			log.Printf("State write admission close error: %v", closeErr)
		} else {
			stateWritesDrained = true
		}
	}
	if flushWorker != nil && stateWritesDrained {
		flushWorker.CloseDirtyWriteAdmission()
	}
	return err
}
