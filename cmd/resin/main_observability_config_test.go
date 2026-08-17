package main

import (
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/topology"
)

func TestDeriveRequestLogRuntimeSettings_FromEnv(t *testing.T) {
	envCfg := &config.EnvConfig{
		RequestLogQueueSize:           1234,
		RequestLogQueueFlushBatchSize: 321,
		RequestLogQueueFlushInterval:  7 * time.Second,
		RequestLogDBMaxMB:             64,
		RequestLogDBRetainCount:       9,
	}

	got, err := deriveRequestLogRuntimeSettings(envCfg)
	if err != nil {
		t.Fatalf("deriveRequestLogRuntimeSettings: %v", err)
	}
	if got.QueueSize != 1234 {
		t.Fatalf("QueueSize: got %d, want %d", got.QueueSize, 1234)
	}
	if got.FlushBatch != 321 {
		t.Fatalf("FlushBatch: got %d, want %d", got.FlushBatch, 321)
	}
	if got.FlushInterval != 7*time.Second {
		t.Fatalf("FlushInterval: got %v, want %v", got.FlushInterval, 7*time.Second)
	}
	if got.DBMaxBytes != int64(64)*1024*1024 {
		t.Fatalf("DBMaxBytes: got %d, want %d", got.DBMaxBytes, int64(64)*1024*1024)
	}
	if got.DBRetainCount != 9 {
		t.Fatalf("DBRetainCount: got %d, want %d", got.DBRetainCount, 9)
	}
}

func TestDeriveRequestLogRuntimeSettings_RejectsDBMaxBytesOverflow(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("int cannot represent a value that overflows int64 byte conversion")
	}

	overflowMB := int64(math.MaxInt64/(1024*1024) + 1)
	maxMB, err := strconv.Atoi(strconv.FormatInt(overflowMB, 10))
	if err != nil {
		t.Fatalf("parse overflow MB value: %v", err)
	}
	envCfg := &config.EnvConfig{
		RequestLogDBMaxMB: maxMB,
	}
	if _, err := deriveRequestLogRuntimeSettings(envCfg); err == nil {
		t.Fatal("expected DBMaxBytes overflow to be rejected")
	}
}

func TestInitObservability_RejectsRequestLogQueueOverBudget(t *testing.T) {
	app := &resinApp{
		envCfg: &config.EnvConfig{
			LogDir:                        t.TempDir(),
			RequestLogQueueSize:           config.MaxRequestLogQueueSize + 1,
			RequestLogQueueFlushBatchSize: 1,
		},
	}

	err := app.initObservability()
	if err == nil {
		t.Fatal("expected request-log queue budget error")
	}
	if !strings.Contains(err.Error(), "request log config") || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("error = %q, want request-log budget diagnostic", err)
	}
	if app.metricsDB != nil {
		t.Fatal("metrics DB must not be opened after request-log config rejection")
	}
}

func TestInitObservability_RejectsMetricsDurationOverflow(t *testing.T) {
	app := &resinApp{
		topoRuntime: &topologyRuntime{
			pool: topology.NewGlobalNodePool(topology.PoolConfig{
				MaxConsecutiveFailures: func() int { return 3 },
			}),
			router: routing.NewRouter(routing.RouterConfig{}),
		},
		envCfg: &config.EnvConfig{
			LogDir:                            t.TempDir(),
			MetricBucketSeconds:               300,
			MetricThroughputIntervalSeconds:   9223372037,
			MetricThroughputRetentionSeconds:  1,
			MetricConnectionsIntervalSeconds:  1,
			MetricConnectionsRetentionSeconds: 1,
			MetricLeasesIntervalSeconds:       1,
			MetricLeasesRetentionSeconds:      1,
			MetricLatencyBinWidthMS:           50,
			MetricLatencyBinOverflowMS:        5000,
		},
	}

	err := app.initObservability()
	if err == nil {
		t.Fatal("expected startup metrics validation error")
	}
	if !strings.Contains(err.Error(), "metrics manager") || !strings.Contains(err.Error(), "throughput interval seconds") {
		t.Fatalf("error = %q, want diagnostic startup path", err)
	}
	if app.metricsDB != nil {
		t.Fatal("metrics DB should be closed after manager construction fails")
	}
}

func TestInitObservability_RequestLogOpenFailureRollsBackEarlierResources(t *testing.T) {
	logDir := t.TempDir()
	badShard := filepath.Join(logDir, "request_logs-0000000000001.db")
	if err := os.WriteFile(badShard, []byte("not a sqlite database"), 0o600); err != nil {
		t.Fatalf("write malformed request-log shard: %v", err)
	}

	app := &resinApp{
		topoRuntime: &topologyRuntime{
			pool: topology.NewGlobalNodePool(topology.PoolConfig{
				MaxConsecutiveFailures: func() int { return 3 },
			}),
			router: routing.NewRouter(routing.RouterConfig{}),
		},
		envCfg: &config.EnvConfig{
			LogDir:                            logDir,
			MetricBucketSeconds:               300,
			MetricThroughputIntervalSeconds:   1,
			MetricThroughputRetentionSeconds:  1,
			MetricConnectionsIntervalSeconds:  1,
			MetricConnectionsRetentionSeconds: 1,
			MetricLeasesIntervalSeconds:       1,
			MetricLeasesRetentionSeconds:      1,
			MetricLatencyBinWidthMS:           50,
			MetricLatencyBinOverflowMS:        5000,
		},
	}

	err := app.initObservability()
	if err == nil {
		t.Fatal("expected request-log open failure")
	}
	if app.metricsDB != nil || app.metricsManager != nil {
		t.Fatalf("metrics resources remain after request-log failure: db=%p manager=%p", app.metricsDB, app.metricsManager)
	}
	if app.requestlogRepo != nil || app.requestlogSvc != nil {
		t.Fatalf("request-log resources remain after request-log failure: repo=%p service=%p", app.requestlogRepo, app.requestlogSvc)
	}

	if err := os.Remove(filepath.Join(logDir, "metrics.db")); err != nil {
		t.Fatalf("metrics.db remains open after rollback: %v", err)
	}
}

func TestNewResinApp_BuildNetworkFailureRollsBackEarlierResources(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")
	logDir := filepath.Join(root, "log")
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer func() { _ = closer.Close() }()

	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen port blocker: %v", err)
	}
	port := blocker.Addr().(*net.TCPAddr).Port
	defer blocker.Close()

	envCfg := newDefaultPlatformEnvConfig()
	envCfg.StateDir = stateDir
	envCfg.CacheDir = cacheDir
	envCfg.LogDir = logDir
	envCfg.ListenAddress = "127.0.0.1"
	envCfg.ResinPort = port
	envCfg.APIMaxBodyBytes = 1 << 20
	envCfg.MaxLatencyTableEntries = 12
	envCfg.ProbeConcurrency = 1
	envCfg.GeoIPUpdateSchedule = "0 7 * * *"
	envCfg.ProbeTimeout = 15 * time.Second
	envCfg.ResourceFetchTimeout = 30 * time.Second
	envCfg.NodeDNSUpstreams = config.DefaultNodeDNSUpstreams()
	envCfg.ProxyTransportMaxIdleConns = 8
	envCfg.ProxyTransportMaxIdleConnsPerHost = 4
	envCfg.ProxyTransportIdleConnTimeout = 90 * time.Second
	envCfg.RequestLogQueueSize = 8
	envCfg.RequestLogQueueFlushBatchSize = 4
	envCfg.RequestLogQueueFlushInterval = time.Minute
	envCfg.RequestLogDBMaxMB = 8
	envCfg.RequestLogDBRetainCount = 1
	envCfg.AuthVersion = config.AuthVersionV1
	envCfg.AdminToken = "admin"
	envCfg.ProxyToken = "proxy"
	envCfg.MetricThroughputIntervalSeconds = 1
	envCfg.MetricThroughputRetentionSeconds = 1
	envCfg.MetricBucketSeconds = 300
	envCfg.MetricConnectionsIntervalSeconds = 1
	envCfg.MetricConnectionsRetentionSeconds = 1
	envCfg.MetricLeasesIntervalSeconds = 1
	envCfg.MetricLeasesRetentionSeconds = 1
	envCfg.MetricLatencyBinWidthMS = 50
	envCfg.MetricLatencyBinOverflowMS = 5000

	_, err = newResinApp(envCfg, engine)
	if err == nil {
		t.Fatal("expected default endpoint listen failure")
	}
	if !strings.Contains(err.Error(), "default endpoint listen") {
		t.Fatalf("error = %q, want default endpoint listen failure", err)
	}

	if err := os.Remove(filepath.Join(logDir, "metrics.db")); err != nil {
		t.Fatalf("metrics.db remains owned after startup rollback: %v", err)
	}
	requestLogFiles, err := filepath.Glob(filepath.Join(logDir, "request_logs-*.db"))
	if err != nil {
		t.Fatalf("glob request-log files: %v", err)
	}
	for _, path := range requestLogFiles {
		if err := os.Remove(path); err != nil {
			t.Fatalf("request-log file %s remains owned after startup rollback: %v", path, err)
		}
	}
}

func TestDeriveMetricsManagerSettings_FromEnv(t *testing.T) {
	envCfg := &config.EnvConfig{
		MetricThroughputIntervalSeconds:   2,
		MetricThroughputRetentionSeconds:  100,
		MetricConnectionsIntervalSeconds:  5,
		MetricConnectionsRetentionSeconds: 18000,
		MetricLeasesIntervalSeconds:       10,
		MetricLeasesRetentionSeconds:      12000,
		MetricBucketSeconds:               3600,
		MetricLatencyBinWidthMS:           80,
		MetricLatencyBinOverflowMS:        2500,
	}

	got := deriveMetricsManagerSettings(envCfg)
	if got.ThroughputIntervalSec != 2 {
		t.Fatalf("ThroughputIntervalSec: got %d, want %d", got.ThroughputIntervalSec, 2)
	}
	if got.ConnectionsIntervalSec != 5 {
		t.Fatalf("ConnectionsIntervalSec: got %d, want %d", got.ConnectionsIntervalSec, 5)
	}
	if got.LeasesIntervalSec != 10 {
		t.Fatalf("LeasesIntervalSec: got %d, want %d", got.LeasesIntervalSec, 10)
	}
	if got.BucketSeconds != 3600 {
		t.Fatalf("BucketSeconds: got %d, want %d", got.BucketSeconds, 3600)
	}
	if got.LatencyBinMs != 80 {
		t.Fatalf("LatencyBinMs: got %d, want %d", got.LatencyBinMs, 80)
	}
	if got.LatencyOverflowMs != 2500 {
		t.Fatalf("LatencyOverflowMs: got %d, want %d", got.LatencyOverflowMs, 2500)
	}
}
