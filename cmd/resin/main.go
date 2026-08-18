package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/netip"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/metrics"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/requestlog"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

type topologyRuntime struct {
	subManager       *topology.SubscriptionManager
	pool             *topology.GlobalNodePool
	probeMgr         *probe.ProbeManager
	scheduler        *topology.SubscriptionScheduler
	ephemeralCleaner *topology.EphemeralCleaner
	router           *routing.Router
	leaseCleaner     *routing.LeaseCleaner
	outboundMgr      *outbound.OutboundManager
	singboxBuilder   *outbound.SingboxBuilder // for Close on shutdown
}

const downloadUserAgent = "clash.meta"

func main() {
	if err := run(); err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	if supportsANSIColorOnStderr() {
		fmt.Fprintf(os.Stderr, "\x1b[31mfatal:\x1b[0m %s\n", message)
	} else {
		fmt.Fprintf(os.Stderr, "fatal: %s\n", message)
	}
	os.Exit(1)
}

func supportsANSIColorOnStderr() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	term := os.Getenv("TERM")
	if term == "" || term == "dumb" {
		return false
	}
	stat, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

func loadRuntimeConfig(engine *state.StateEngine) *config.RuntimeConfig {
	runtimeCfg, ver, err := engine.GetSystemConfig()
	if err != nil {
		fatalf("load system config: %v", err)
	}
	if runtimeCfg == nil {
		log.Println("No persisted runtime config found, using defaults")
		return config.NewDefaultRuntimeConfig()
	}
	if err := validateLoadedRuntimeConfig(runtimeCfg); err != nil {
		fatalf("load system config: invalid persisted runtime config: %v", err)
	}
	log.Printf("Loaded persisted runtime config (version %d)", ver)
	return runtimeCfg
}

func validateLoadedRuntimeConfig(runtimeCfg *config.RuntimeConfig) error {
	if runtimeCfg == nil {
		return nil
	}
	return service.ValidateRuntimeConfig(runtimeCfg)
}

func newDirectDownloader(
	envCfg *config.EnvConfig,
) *netutil.DirectDownloader {
	return netutil.NewDirectDownloader(
		func() time.Duration {
			return envCfg.ResourceFetchTimeout
		},
		func() string {
			return currentDownloadUserAgent()
		},
	)
}

func currentDownloadUserAgent() string {
	return downloadUserAgent
}

func runtimeConfigSnapshot(runtimeCfg *atomic.Pointer[config.RuntimeConfig]) *config.RuntimeConfig {
	if runtimeCfg == nil {
		return config.NewDefaultRuntimeConfig()
	}
	cfg := runtimeCfg.Load()
	if cfg == nil {
		return config.NewDefaultRuntimeConfig()
	}
	return cfg
}

type requestLogRuntimeSettings struct {
	DBMaxBytes    int64
	DBRetainCount int
	QueueSize     int
	FlushBatch    int
	FlushInterval time.Duration
}

func deriveRequestLogRuntimeSettings(envCfg *config.EnvConfig) (requestLogRuntimeSettings, error) {
	dbMaxBytes, err := config.RequestLogDBMaxBytes(envCfg.RequestLogDBMaxMB)
	if err != nil {
		return requestLogRuntimeSettings{}, err
	}
	queueSize := envCfg.RequestLogQueueSize
	if queueSize <= 0 {
		queueSize = 8192
	}
	batchSize := envCfg.RequestLogQueueFlushBatchSize
	if batchSize <= 0 {
		batchSize = 4096
	}
	if err := config.ValidateRequestLogQueueConfig(queueSize, batchSize); err != nil {
		return requestLogRuntimeSettings{}, err
	}
	return requestLogRuntimeSettings{
		DBMaxBytes:    dbMaxBytes,
		DBRetainCount: envCfg.RequestLogDBRetainCount,
		QueueSize:     queueSize,
		FlushBatch:    batchSize,
		FlushInterval: envCfg.RequestLogQueueFlushInterval,
	}, nil
}

type metricsManagerSettings struct {
	LatencyBinMs            int
	LatencyOverflowMs       int
	BucketSeconds           int
	ThroughputIntervalSec   int
	ThroughputRetentionSec  int
	ConnectionsIntervalSec  int
	ConnectionsRetentionSec int
	LeasesIntervalSec       int
	LeasesRetentionSec      int
}

func deriveMetricsManagerSettings(envCfg *config.EnvConfig) metricsManagerSettings {
	return metricsManagerSettings{
		LatencyBinMs:            envCfg.MetricLatencyBinWidthMS,
		LatencyOverflowMs:       envCfg.MetricLatencyBinOverflowMS,
		BucketSeconds:           envCfg.MetricBucketSeconds,
		ThroughputIntervalSec:   envCfg.MetricThroughputIntervalSeconds,
		ThroughputRetentionSec:  envCfg.MetricThroughputRetentionSeconds,
		ConnectionsIntervalSec:  envCfg.MetricConnectionsIntervalSeconds,
		ConnectionsRetentionSec: envCfg.MetricConnectionsRetentionSeconds,
		LeasesIntervalSec:       envCfg.MetricLeasesIntervalSeconds,
		LeasesRetentionSec:      envCfg.MetricLeasesRetentionSeconds,
	}
}

func newGeoIPService(
	cacheDir string,
	updateSchedule string,
	downloader netutil.Downloader,
) *geoip.Service {
	geoSvc := geoip.NewService(geoip.ServiceConfig{
		CacheDir:       cacheDir,
		UpdateSchedule: updateSchedule,
		Downloader:     downloader,
		OpenDB:         geoip.MMDBOpen,
	})
	return geoSvc
}

func startGeoIPService(geoSvc *geoip.Service) {
	if err := geoSvc.Start(); err != nil {
		log.Printf("GeoIP service start (non-fatal): %v", err)
	}
	log.Println("GeoIP service initialized")
}

func newTopologyRuntime(
	engine *state.StateEngine,
	envCfg *config.EnvConfig,
	runtimeCfg *atomic.Pointer[config.RuntimeConfig],
	geoSvc *geoip.Service,
	downloader netutil.Downloader,
	onProbeConnLifecycle func(netutil.ConnLifecycleOp),
	onNodeRemoved func(node.Hash),
) (*topologyRuntime, error) {
	subManager := topology.NewSubscriptionManager()

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subManager.Lookup,
		GeoLookup: geoSvc.Lookup,
		OnSubNodeChanged: func(subID string, hash node.Hash, added bool) {
			if added {
				engine.MarkSubscriptionNode(subID, hash.Hex())
			} else {
				engine.MarkSubscriptionNodeDelete(subID, hash.Hex())
			}
		},
		OnNodeAddedWithPersistence: func(hash node.Hash, admission topology.PersistenceAdmission) {
			admission.MarkNodeStatic(hash.Hex())
		},
		OnSubNodeChangedWithPersistence: func(subID string, hash node.Hash, added bool, admission topology.PersistenceAdmission) {
			if added {
				admission.MarkSubscriptionNode(subID, hash.Hex())
			} else {
				admission.MarkSubscriptionNodeDelete(subID, hash.Hex())
			}
		},
		OnFinalNodeRemoved: func(subID string, hash node.Hash, entry *node.NodeEntry) {
			markFinalNodeRemovedDirty(engine, subID, hash, entry)
		},
		OnFinalNodeRemovedWithPersistence: func(subID string, hash node.Hash, entry *node.NodeEntry, admission topology.PersistenceAdmission) {
			admission.MarkSubscriptionNodeDelete(subID, hash.Hex())
			markNodeRemovedDirtyWithAdmission(admission, hash, entry)
		},
		OnNodeDynamicChanged: func(hash node.Hash) {
			engine.MarkNodeDynamic(hash.Hex())
		},
		OnNodeLatencyChanged: func(hash node.Hash, domain string) {
			engine.MarkNodeLatency(hash.Hex(), domain)
		},
		MaxLatencyTableEntries: envCfg.MaxLatencyTableEntries,
		MaxConsecutiveFailures: func() int {
			return runtimeConfigSnapshot(runtimeCfg).MaxConsecutiveFailures
		},
		LatencyDecayWindow: func() time.Duration {
			return time.Duration(runtimeConfigSnapshot(runtimeCfg).LatencyDecayWindow)
		},
		LatencyAuthorities: func() []string {
			return runtimeConfigSnapshot(runtimeCfg).LatencyAuthorities
		},
	})
	log.Println("Topology: GlobalNodePool initialized")

	singboxBuilder, err := outbound.NewSingboxBuilderWithConfig(outbound.SingboxBuilderConfig{
		DNSUpstreams: envCfg.NodeDNSUpstreams,
	})
	if err != nil {
		return nil, fmt.Errorf("singbox builder: %w", err)
	}
	outboundMgr := outbound.NewOutboundManager(pool, singboxBuilder)

	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool:        pool,
		Concurrency: envCfg.ProbeConcurrency,
		Fetcher: func(ctx context.Context, entry *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			ctx, cancel := context.WithTimeout(ctx, envCfg.ProbeTimeout)
			defer cancel()
			if entry == nil {
				return nil, 0, fmt.Errorf("node not found")
			}
			nodeOutbound, release, ready := entry.AcquireOutbound()
			if !ready {
				return nil, 0, outbound.ErrOutboundNotReady
			}
			defer release()
			return netutil.HTTPGetViaOutbound(ctx, nodeOutbound, url, netutil.OutboundHTTPOptions{
				RequireStatusOK: false,
				OnConnLifecycle: func(op netutil.ConnLifecycleOp) {
					if onProbeConnLifecycle != nil {
						onProbeConnLifecycle(op)
					}
				},
			})
		},
		MaxEgressTestInterval: func() time.Duration {
			return time.Duration(runtimeConfigSnapshot(runtimeCfg).MaxEgressTestInterval)
		},
		MaxLatencyTestInterval: func() time.Duration {
			return time.Duration(runtimeConfigSnapshot(runtimeCfg).MaxLatencyTestInterval)
		},
		MaxAuthorityLatencyTestInterval: func() time.Duration {
			return time.Duration(runtimeConfigSnapshot(runtimeCfg).MaxAuthorityLatencyTestInterval)
		},
		LatencyTestURL: func() string {
			return runtimeConfigSnapshot(runtimeCfg).LatencyTestURL
		},
		LatencyAuthorities: func() []string {
			return runtimeConfigSnapshot(runtimeCfg).LatencyAuthorities
		},
	})

	pool.SetOnNodeAdded(func(hash node.Hash) {
		engine.MarkNodeStatic(hash.Hex())
	})
	pool.SetOnNodeAddedRuntime(func(hash node.Hash, expected *node.NodeEntry) {
		outboundMgr.EnsureNodeOutboundForEntry(hash, expected)
		// No NotifyNodeDirty here — AddNodeFromSub already notifies all platforms.
		probeMgr.TriggerImmediateEgressProbeForEntry(hash, expected)
	})
	pool.SetOnNodeRemoved(func(hash node.Hash, entry *node.NodeEntry) {
		outboundMgr.RemoveNodeOutbound(entry)
		if entry != nil && entry.LatencyTable != nil {
			entry.LatencyTable.Close()
		}
		if onNodeRemoved != nil {
			onNodeRemoved(hash)
		}
	})
	log.Println("ProbeManager initialized")

	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: subManager,
		Pool:       pool,
		Downloader: downloader,
		RunRefreshMutation: func(fn func(topology.PersistenceAdmission)) bool {
			return engine.WithDirtyWriteAdmission(func(admission *state.DirtyWriteAdmission) {
				fn(admission)
			})
		},
		OnSubReenabledNode: func(hash node.Hash, expected *node.NodeEntry) {
			outboundMgr.EnsureNodeOutboundForEntry(hash, expected)
			probeMgr.TriggerImmediateEgressProbeForEntry(hash, expected)
			probeMgr.TriggerImmediateLatencyProbeForEntry(hash, expected)
		},
	})
	ephemeralCleaner := topology.NewEphemeralCleaner(
		subManager,
		pool,
	)
	ephemeralCleaner.SetPersistenceMutationRunner(func(fn func(topology.PersistenceAdmission)) bool {
		return engine.WithDirtyWriteAdmission(func(admission *state.DirtyWriteAdmission) {
			fn(admission)
		})
	})

	return &topologyRuntime{
		subManager:       subManager,
		pool:             pool,
		probeMgr:         probeMgr,
		scheduler:        scheduler,
		ephemeralCleaner: ephemeralCleaner,
		outboundMgr:      outboundMgr,
		singboxBuilder:   singboxBuilder,
	}, nil
}

// beforeNodeRemovedDirtyMarkHook is a package-test seam for the compound
// node-removal dirty-write boundary. Production leaves it nil.
var beforeNodeRemovedDirtyMarkHook func(index int)

// beforeFlushNodeDynamicReadHook is a package-test seam immediately before
// the production cache reader loads one node's dynamic value. Production
// leaves it nil; tests use it to place a runtime mutation between the static
// and dynamic reads of one flush.
var beforeFlushNodeDynamicReadHook func(string)

func markNodeRemovedDirty(engine *state.StateEngine, hash node.Hash, entry *node.NodeEntry) {
	engine.WithDirtyWriteAdmission(func(admission *state.DirtyWriteAdmission) {
		markNodeRemovedDirtyWithAdmission(admission, hash, entry)
	})
}

func markFinalNodeRemovedDirty(
	engine *state.StateEngine,
	subID string,
	hash node.Hash,
	entry *node.NodeEntry,
) {
	engine.WithDirtyWriteAdmission(func(admission *state.DirtyWriteAdmission) {
		admission.MarkSubscriptionNodeDelete(subID, hash.Hex())
		markNodeRemovedDirtyWithAdmission(admission, hash, entry)
	})
}

func markNodeRemovedDirtyWithAdmission(
	admission topology.PersistenceAdmission,
	hash node.Hash,
	entry *node.NodeEntry,
) {
	if admission == nil {
		return
	}
	hashHex := hash.Hex()
	admission.MarkNodeStaticDelete(hashHex)
	if hook := beforeNodeRemovedDirtyMarkHook; hook != nil {
		hook(1)
	}
	admission.MarkNodeDynamicDelete(hashHex)

	if entry == nil || entry.LatencyTable == nil {
		return
	}
	entry.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
		admission.MarkNodeLatencyDelete(hashHex, domain)
		return true
	})
}

func bootstrapTopology(
	engine *state.StateEngine,
	subManager *topology.SubscriptionManager,
	pool *topology.GlobalNodePool,
	envCfg *config.EnvConfig,
) error {
	dbSubs, err := engine.ListSubscriptions()
	if err != nil {
		return fmt.Errorf("load subscriptions: %w", err)
	}
	for _, ms := range dbSubs {
		sub := subscription.NewSubscription(ms.ID, ms.Name, ms.URL, ms.Enabled, ms.Ephemeral)
		sub.SetFetchConfig(ms.URL, ms.UpdateIntervalNs)
		sub.SetSourceType(ms.SourceType)
		sub.SetContent(ms.Content)
		sub.SetIncrementalAliveNodes(ms.IncrementalAliveNodes)
		sub.SetEphemeralNodeEvictDelayNs(ms.EphemeralNodeEvictDelayNs)
		sub.CreatedAtNs = ms.CreatedAtNs
		sub.UpdatedAtNs = ms.UpdatedAtNs
		subManager.Register(sub)
	}
	log.Printf("Loaded %d subscriptions from state.db", len(dbSubs))

	dbPlats, err := engine.ListPlatforms()
	if err != nil {
		return fmt.Errorf("load platforms: %w", err)
	}
	if err := validatePersistedPlatformNamesForV1(dbPlats); err != nil {
		return fmt.Errorf("validate platform names for V1: %w", err)
	}
	if err := ensureDefaultPlatform(engine, envCfg, dbPlats); err != nil {
		return fmt.Errorf("ensure default platform: %w", err)
	}
	dbPlats, err = engine.ListPlatforms()
	if err != nil {
		return fmt.Errorf("reload platforms: %w", err)
	}
	for _, mp := range dbPlats {
		plat, err := platform.BuildFromModel(mp)
		if err != nil {
			return err
		}
		if err := pool.RegisterPlatform(plat); err != nil {
			if errors.Is(err, topology.ErrPlatformAlreadyRegistered) {
				if err := pool.ReplacePlatform(plat); err != nil {
					return fmt.Errorf("replace bootstrapped platform %s: %w", mp.ID, err)
				}
				continue
			}
			return fmt.Errorf("register bootstrapped platform %s: %w", mp.ID, err)
		}
	}
	log.Printf("Loaded %d platforms from state.db", len(dbPlats))
	return nil
}

func validatePersistedPlatformNamesForV1(platformsInDB []model.Platform) error {
	var invalidPlatformNames []string
	for _, p := range platformsInDB {
		if err := platform.ValidatePlatformName(p.Name); err != nil {
			invalidPlatformNames = append(invalidPlatformNames, fmt.Sprintf("%q", p.Name))
		}
	}

	if len(invalidPlatformNames) > 0 {
		return fmt.Errorf(
			"%d platform(s) are incompatible with V1: %s. Platform name rules: must be non-empty; must not be reserved name; must not contain any of \".:|/\\\\@?#%%~\"; must not contain spaces, tabs, newlines, or carriage returns. Use a Resin release that supports LEGACY_V0 to rename these platforms before upgrading. Migration guide: https://github.com/Resinat/Resin/blob/master/doc/v1.0.0-migration-guide.md",
			len(invalidPlatformNames),
			strings.Join(invalidPlatformNames, ", "),
		)
	}
	return nil
}

func ensureDefaultPlatform(
	engine *state.StateEngine,
	envCfg *config.EnvConfig,
	platformsInDB []model.Platform,
) error {
	hasDefaultID := false
	for _, p := range platformsInDB {
		if p.ID == platform.DefaultPlatformID {
			hasDefaultID = true
		}
	}
	if hasDefaultID {
		return nil
	}

	defaultPlatform := model.Platform{
		ID:                               platform.DefaultPlatformID,
		Name:                             platform.DefaultPlatformName,
		StickyTTLNs:                      int64(envCfg.DefaultPlatformStickyTTL),
		RegexFilters:                     append([]string(nil), envCfg.DefaultPlatformRegexFilters...),
		RegionFilters:                    append([]string(nil), envCfg.DefaultPlatformRegionFilters...),
		ReverseProxyMissAction:           envCfg.DefaultPlatformReverseProxyMissAction,
		ReverseProxyEmptyAccountBehavior: envCfg.DefaultPlatformReverseProxyEmptyAccountBehavior,
		ReverseProxyFixedAccountHeader:   envCfg.DefaultPlatformReverseProxyFixedAccountHeader,
		AllocationPolicy:                 envCfg.DefaultPlatformAllocationPolicy,
		UpdatedAtNs:                      time.Now().UnixNano(),
	}
	if err := engine.UpsertPlatform(defaultPlatform); err != nil {
		return err
	}
	log.Println("Created built-in Default platform")
	return nil
}

var defaultFallbackAccountHeaders = []string{"Authorization", "x-api-key"}

func ensureDefaultAccountHeaderRule(engine *state.StateEngine) error {
	created, err := engine.EnsureAccountHeaderRule(model.AccountHeaderRule{
		URLPrefix:   "*",
		Headers:     append([]string(nil), defaultFallbackAccountHeaders...),
		UpdatedAtNs: time.Now().UnixNano(),
	})
	if err != nil {
		return fmt.Errorf("ensure default account header fallback rule: %w", err)
	}
	if created {
		log.Printf("Created built-in account header fallback rule %q", "*")
	}
	return nil
}

func newFlushReaders(
	pool *topology.GlobalNodePool,
	subManager *topology.SubscriptionManager,
	router *routing.Router,
) state.CacheReaders {
	return state.CacheReaders{
		WithNodeSnapshot: func(fn func()) {
			if pool == nil {
				fn()
				return
			}
			pool.WithRuntimeRead(fn)
		},
		ReadNodeStatic: func(hash string) *model.NodeStatic {
			h, err := node.ParseHex(hash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok {
				return nil
			}
			return &model.NodeStatic{
				Hash:        hash,
				RawOptions:  append(json.RawMessage(nil), entry.RawOptions...),
				CreatedAtNs: entry.CreatedAt.UnixNano(),
			}
		},
		ReadNodeDynamic: func(hash string) *model.NodeDynamic {
			if hook := beforeFlushNodeDynamicReadHook; hook != nil {
				hook(hash)
			}
			h, err := node.ParseHex(hash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok {
				return nil
			}
			egressIP := entry.GetEgressIP()
			egressStr := ""
			if egressIP.IsValid() {
				egressStr = egressIP.String()
			}
			return &model.NodeDynamic{
				Hash:                               hash,
				FailureCount:                       int(entry.FailureCount.Load()),
				CircuitOpenSince:                   entry.CircuitOpenSince.Load(),
				EgressIP:                           egressStr,
				EgressRegion:                       entry.GetEgressRegion(),
				EgressUpdatedAtNs:                  entry.LastEgressUpdate.Load(),
				LastLatencyProbeAttemptNs:          entry.LastLatencyProbeAttempt.Load(),
				LastAuthorityLatencyProbeAttemptNs: entry.LastAuthorityLatencyProbeAttempt.Load(),
				LastEgressUpdateAttemptNs:          entry.LastEgressUpdateAttempt.Load(),
			}
		},
		ReadNodeLatency: func(key model.NodeLatencyKey) *model.NodeLatency {
			h, err := node.ParseHex(key.NodeHash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok || entry.LatencyTable == nil {
				return nil
			}
			stats, ok := entry.LatencyTable.GetDomainStats(key.Domain)
			if !ok {
				return nil
			}
			return &model.NodeLatency{
				NodeHash:      key.NodeHash,
				Domain:        key.Domain,
				EwmaNs:        int64(stats.Ewma),
				LastUpdatedNs: stats.LastUpdated.UnixNano(),
			}
		},
		ReadLease: func(key model.LeaseKey) *model.Lease {
			return router.ReadLeaseForPersistence(key)
		},
		ReadSubscriptionNode: func(key model.SubscriptionNodeKey) *model.SubscriptionNode {
			h, err := node.ParseHex(key.NodeHash)
			if err != nil {
				return nil
			}
			sub := subManager.Lookup(key.SubscriptionID)
			if sub == nil {
				return nil
			}
			managed, ok := sub.ManagedNodes().LoadNode(h)
			if !ok {
				return nil
			}
			return &model.SubscriptionNode{
				SubscriptionID: key.SubscriptionID,
				NodeHash:       key.NodeHash,
				Tags:           append([]string(nil), managed.Tags...),
				Evicted:        managed.Evicted,
			}
		},
	}
}

func buildAccountMatcher(engine *state.StateEngine) *proxy.AccountMatcherRuntime {
	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		log.Printf("Warning: load account header rules: %v", err)
		return proxy.NewAccountMatcherRuntime(proxy.BuildAccountMatcher(nil))
	}
	if len(rules) > 0 {
		log.Printf("Loaded %d account header rules", len(rules))
	}
	return proxy.NewAccountMatcherRuntime(proxy.BuildAccountMatcher(rules))
}

// --- Metrics runtime stats adapter ---

// runtimeStatsAdapter implements metrics.RuntimeStatsProvider using
// GlobalNodePool + Router.
type runtimeStatsAdapter struct {
	pool        *topology.GlobalNodePool
	router      *routing.Router
	authorities func() []string

	// beforePlatformStatsViewSnapshotHook is a package-private deterministic
	// test seam after capturing a platform view and before reading pool entries.
	// Production leaves it nil.
	beforePlatformStatsViewSnapshotHook func()
}

func (a *runtimeStatsAdapter) TotalNodes() (count int) {
	a.pool.WithRuntimeRead(func() {
		count = a.pool.Size()
	})
	return count
}

func (a *runtimeStatsAdapter) HealthyNodes() int {
	count := 0
	a.pool.WithRuntimeRead(func() {
		isHealthyAndEnabled := a.pool.MakeHealthyAndEnabledEvaluator()
		a.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
			if isHealthyAndEnabled(entry) {
				count++
			}
			return true
		})
	})
	return count
}

func (a *runtimeStatsAdapter) EgressIPCount() int {
	seen := make(map[netip.Addr]struct{})
	a.pool.WithRuntimeRead(func() {
		a.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
			if ip := entry.GetEgressIP(); ip.IsValid() {
				seen[ip] = struct{}{}
			}
			return true
		})
	})
	return len(seen)
}

func (a *runtimeStatsAdapter) UniqueHealthyEgressIPCount() int {
	seen := make(map[netip.Addr]struct{})
	a.pool.WithRuntimeRead(func() {
		isHealthyAndEnabled := a.pool.MakeHealthyAndEnabledEvaluator()
		a.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
			if !isHealthyAndEnabled(entry) {
				return true
			}
			if ip := entry.GetEgressIP(); ip.IsValid() {
				seen[ip] = struct{}{}
			}
			return true
		})
	})
	return len(seen)
}

func (a *runtimeStatsAdapter) LeaseCountsByPlatform() map[string]int {
	result := make(map[string]int)
	a.pool.WithRuntimeRead(func() {
		a.pool.RangePlatforms(func(plat *platform.Platform) bool {
			count := 0
			a.router.RangeLeases(plat.ID, func(_ string, _ routing.Lease) bool {
				count++
				return true
			})
			if count > 0 {
				result[plat.ID] = count
			}
			return true
		})
	})
	return result
}

func (a *runtimeStatsAdapter) RoutableNodeCount(platformID string) (int, bool) {
	count := 0
	ok := false
	a.pool.WithRuntimeRead(func() {
		entries, found := a.pool.SnapshotPlatformViewEntries(platformID)
		if !found {
			return
		}
		ok = true
		for _, viewEntry := range entries {
			entry, found := a.pool.GetEntry(viewEntry.Hash)
			if found && entry == viewEntry.Entry {
				count++
			}
		}
	})
	return count, ok
}

func (a *runtimeStatsAdapter) PlatformEgressIPCount(platformID string) (int, bool) {
	seen := make(map[netip.Addr]struct{})
	ok := false
	a.pool.WithRuntimeRead(func() {
		viewEntries, found := a.pool.SnapshotPlatformViewEntries(platformID)
		if !found {
			return
		}
		ok = true
		if hook := a.beforePlatformStatsViewSnapshotHook; hook != nil {
			hook()
		}
		for _, viewEntry := range viewEntries {
			entry, found := a.pool.GetEntry(viewEntry.Hash)
			if !found || entry != viewEntry.Entry {
				continue
			}
			if ip := entry.GetEgressIP(); ip.IsValid() {
				seen[ip] = struct{}{}
			}
		}
	})
	return len(seen), ok
}

func (a *runtimeStatsAdapter) CollectNodeEWMAs(platformID string) []float64 {
	authorities := a.authorities()
	var ewmas []float64

	a.pool.WithRuntimeRead(func() {
		if platformID == "" {
			// Global: iterate all nodes.
			a.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
				if avg, ok := node.AverageEWMAForDomainsMs(entry, authorities); ok {
					ewmas = append(ewmas, avg)
				}
				return true
			})
			return
		}
		// Platform-scoped: iterate only nodes routable by this platform.
		viewEntries, ok := a.pool.SnapshotPlatformViewEntries(platformID)
		if !ok {
			return
		}
		if hook := a.beforePlatformStatsViewSnapshotHook; hook != nil {
			hook()
		}
		for _, viewEntry := range viewEntries {
			entry, ok := a.pool.GetEntry(viewEntry.Hash)
			if !ok || entry != viewEntry.Entry {
				continue
			}
			if avg, ok := node.AverageEWMAForDomainsMs(entry, authorities); ok {
				ewmas = append(ewmas, avg)
			}
		}
	})
	return ewmas
}

// compositeEmitter dispatches proxy events to both requestlog and metrics.
type compositeEmitter struct {
	logSvc     *requestlog.Service
	metricsMgr *metrics.Manager
}

func (c compositeEmitter) EmitRequestFinished(ev proxy.RequestFinishedEvent) {
	c.metricsMgr.OnRequestFinished(ev)
}

func (c compositeEmitter) EmitRequestLog(ev proxy.RequestLogEntry) {
	c.logSvc.EmitRequestLog(ev)
}

func loadBootstrapNodeStatics(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	envCfg *config.EnvConfig,
) ([]node.Hash, error) {
	statics, err := engine.LoadAllNodesStatic()
	if err != nil {
		return nil, fmt.Errorf("load nodes_static: %w", err)
	}

	hashes := make([]node.Hash, 0, len(statics))
	bootstrapNowNs := time.Now().UnixNano()
	for _, ns := range statics {
		hash, err := node.ParseHex(ns.Hash)
		if err != nil {
			log.Printf("[bootstrap] skip node %s: %v", ns.Hash, err)
			continue
		}
		entry := &node.NodeEntry{
			Hash:       hash,
			RawOptions: append(json.RawMessage(nil), ns.RawOptions...),
			CreatedAt:  time.Unix(0, ns.CreatedAtNs),
		}
		// Bootstrap default: treat nodes as circuit-open unless a persisted
		// nodes_dynamic row later overrides this state.
		entry.CircuitOpenSince.Store(bootstrapNowNs)
		entry.LatencyTable = node.NewLatencyTable(envCfg.MaxLatencyTableEntries)
		pool.LoadNodeFromBootstrap(entry)
		hashes = append(hashes, hash)
	}
	log.Printf("Loaded %d static nodes from cache.db", len(statics))
	return hashes, nil
}

func warmupBootstrapOutbounds(
	hashes []node.Hash,
	pool *topology.GlobalNodePool,
	outboundMgr *outbound.OutboundManager,
) error {
	if len(hashes) == 0 {
		return nil
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	type warmupJob struct {
		index int
		hash  node.Hash
	}
	jobCh := make(chan warmupJob, len(hashes))
	results := make([]error, len(hashes))
	for index, h := range hashes {
		jobCh <- warmupJob{index: index, hash: h}
	}
	close(jobCh)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				outboundMgr.EnsureNodeOutbound(job.hash)
				entry, ok := pool.GetEntry(job.hash)
				if !ok {
					results[job.index] = fmt.Errorf("bootstrap outbound %s: node disappeared", job.hash.Hex())
					continue
				}
				if !entry.HasOutbound() {
					results[job.index] = fmt.Errorf("bootstrap outbound %s: %s", job.hash.Hex(), entry.GetLastError())
				}
			}
		}()
	}
	wg.Wait()
	for _, err := range results {
		if err != nil {
			return err
		}
	}
	log.Printf("Parallel outbound init complete (%d workers)", workers)
	return nil
}

func restoreBootstrapSubscriptionBindings(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	subManager *topology.SubscriptionManager,
) error {
	subNodes, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		return fmt.Errorf("load subscription_nodes: %w", err)
	}

	// Group by subscription ID for batch processing.
	subNodeMap := make(map[string][]model.SubscriptionNode)
	for _, sn := range subNodes {
		subNodeMap[sn.SubscriptionID] = append(subNodeMap[sn.SubscriptionID], sn)
	}
	for subID, nodes := range subNodeMap {
		sub, ok := subManager.Get(subID)
		if !ok {
			log.Printf("[bootstrap] subscription %s not found, skipping %d node bindings", subID, len(nodes))
			continue
		}
		managed := subscription.NewManagedNodes()
		for _, sn := range nodes {
			hash, err := node.ParseHex(sn.NodeHash)
			if err != nil {
				continue
			}
			managed.StoreNode(hash, subscription.ManagedNode{
				Tags:    append([]string(nil), sn.Tags...),
				Evicted: sn.Evicted,
			})
			// Restore runtime hold references only for non-evicted rows.
			if !sn.Evicted {
				if entry, ok := pool.GetEntry(hash); ok {
					entry.AddSubscriptionID(subID)
				}
			}
		}
		sub.SwapManagedNodes(managed)
	}
	log.Printf("Loaded %d subscription-node bindings from cache.db", len(subNodes))
	return nil
}

func restoreBootstrapNodeDynamics(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
) error {
	dynamics, err := engine.LoadAllNodesDynamic()
	if err != nil {
		return fmt.Errorf("load nodes_dynamic: %w", err)
	}

	for _, nd := range dynamics {
		hash, err := node.ParseHex(nd.Hash)
		if err != nil {
			continue
		}
		entry, ok := pool.GetEntry(hash)
		if !ok {
			continue
		}
		entry.FailureCount.Store(int32(nd.FailureCount))
		entry.CircuitOpenSince.Store(nd.CircuitOpenSince)
		entry.LastLatencyProbeAttempt.Store(nd.LastLatencyProbeAttemptNs)
		entry.LastAuthorityLatencyProbeAttempt.Store(nd.LastAuthorityLatencyProbeAttemptNs)
		entry.LastEgressUpdateAttempt.Store(nd.LastEgressUpdateAttemptNs)
		if nd.EgressIP != "" {
			if ip, err := netip.ParseAddr(nd.EgressIP); err == nil {
				entry.SetEgressIP(ip)
			}
		}
		entry.SetEgressRegion(nd.EgressRegion)
		entry.LastEgressUpdate.Store(nd.EgressUpdatedAtNs)
	}
	log.Printf("Loaded %d node dynamic states from cache.db", len(dynamics))
	return nil
}

func restoreBootstrapNodeLatencies(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	maxRegularEntries int,
	latencyAuthorities []string,
) error {
	latencies, err := engine.LoadAllNodeLatency()
	if err != nil {
		return fmt.Errorf("load node_latency: %w", err)
	}

	if maxRegularEntries <= 0 {
		maxRegularEntries = 1
	}
	authoritySet := make(map[string]struct{}, len(latencyAuthorities))
	for _, authority := range latencyAuthorities {
		authority = strings.ToLower(strings.TrimSpace(authority))
		if authority == "" {
			continue
		}
		authoritySet[authority] = struct{}{}
	}
	isAuthority := func(domain string) bool {
		_, ok := authoritySet[strings.ToLower(strings.TrimSpace(domain))]
		return ok
	}

	byNode := make(map[string][]model.NodeLatency)
	for _, nl := range latencies {
		byNode[nl.NodeHash] = append(byNode[nl.NodeHash], nl)
	}

	loadedCount := 0
	trimmedCount := 0
	for nodeHash, rows := range byNode {
		hash, err := node.ParseHex(nodeHash)
		if err != nil {
			continue
		}
		entry, ok := pool.GetEntry(hash)
		if !ok || entry.LatencyTable == nil {
			continue
		}

		authorities := make([]model.NodeLatency, 0, len(rows))
		regular := make([]model.NodeLatency, 0, len(rows))
		for _, row := range rows {
			if isAuthority(row.Domain) {
				authorities = append(authorities, row)
			} else {
				regular = append(regular, row)
			}
		}
		sort.SliceStable(authorities, func(i, j int) bool {
			if authorities[i].LastUpdatedNs == authorities[j].LastUpdatedNs {
				return authorities[i].Domain < authorities[j].Domain
			}
			return authorities[i].LastUpdatedNs > authorities[j].LastUpdatedNs
		})
		if len(authorities) > node.MaxLatencyAuthorityEntries {
			for _, dropped := range authorities[node.MaxLatencyAuthorityEntries:] {
				if !engine.MarkNodeLatencyDelete(dropped.NodeHash, dropped.Domain) {
					return fmt.Errorf("mark trimmed node latency %s/%s", dropped.NodeHash, dropped.Domain)
				}
				trimmedCount++
			}
			authorities = authorities[:node.MaxLatencyAuthorityEntries]
		}
		sort.SliceStable(regular, func(i, j int) bool {
			if regular[i].LastUpdatedNs == regular[j].LastUpdatedNs {
				return regular[i].Domain < regular[j].Domain
			}
			return regular[i].LastUpdatedNs > regular[j].LastUpdatedNs
		})
		if len(regular) > maxRegularEntries {
			for _, dropped := range regular[maxRegularEntries:] {
				engine.MarkNodeLatencyDelete(dropped.NodeHash, dropped.Domain)
				trimmedCount++
			}
			regular = regular[:maxRegularEntries]
		}

		for _, row := range authorities {
			evictedDomain, evicted := entry.LatencyTable.LoadEntryClassified(row.Domain, node.DomainLatencyStats{
				Ewma:        time.Duration(row.EwmaNs),
				LastUpdated: time.Unix(0, row.LastUpdatedNs),
			}, true)
			loadedCount++
			if evicted {
				if !engine.MarkNodeLatencyDelete(nodeHash, evictedDomain) {
					return fmt.Errorf("mark evicted node latency %s/%s", nodeHash, evictedDomain)
				}
				trimmedCount++
			}
		}
		// regular is sorted by LastUpdated desc (newest -> oldest).
		// Load in reverse so in-memory LRU order stays oldest -> newest.
		for i := len(regular) - 1; i >= 0; i-- {
			row := regular[i]
			entry.LatencyTable.LoadEntryClassified(row.Domain, node.DomainLatencyStats{
				Ewma:        time.Duration(row.EwmaNs),
				LastUpdated: time.Unix(0, row.LastUpdatedNs),
			}, false)
			loadedCount++
		}
	}
	log.Printf("Loaded %d latency entries from cache.db (trimmed=%d)", loadedCount, trimmedCount)
	return nil
}

// bootstrapNodes loads cached node data from persistence for bootstrap recovery.
// Steps: static nodes → subscription bindings → dynamic state → latency tables.
func bootstrapNodes(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	subManager *topology.SubscriptionManager,
	outboundMgr *outbound.OutboundManager,
	envCfg *config.EnvConfig,
	latencyAuthorities []string,
) error {
	bootstrapSucceeded := false
	defer func() {
		if !bootstrapSucceeded {
			outboundMgr.RetireAllOutboundsAndWait()
		}
	}()

	hashes, err := loadBootstrapNodeStatics(engine, pool, envCfg)
	if err != nil {
		return err
	}

	if err := warmupBootstrapOutbounds(hashes, pool, outboundMgr); err != nil {
		return err
	}

	if err := restoreBootstrapSubscriptionBindings(engine, pool, subManager); err != nil {
		return err
	}
	if err := restoreBootstrapNodeDynamics(engine, pool); err != nil {
		return err
	}
	if err := restoreBootstrapNodeLatencies(
		engine,
		pool,
		envCfg.MaxLatencyTableEntries,
		latencyAuthorities,
	); err != nil {
		return err
	}
	bootstrapSucceeded = true
	return nil
}
