package main

import (
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func unsetEnvForDotenvTest(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		oldValue, hadOldValue := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("Unsetenv(%s): %v", key, err)
		}
		key := key
		t.Cleanup(func() {
			if hadOldValue {
				_ = os.Setenv(key, oldValue)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
}

func newBootstrapTestRuntime(runtimeCfg *config.RuntimeConfig) (*topology.SubscriptionManager, *topology.GlobalNodePool) {
	subManager := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return runtimeCfg.MaxConsecutiveFailures },
		LatencyDecayWindow: func() time.Duration {
			return time.Duration(runtimeCfg.LatencyDecayWindow)
		},
	})
	return subManager, pool
}

func newDefaultPlatformEnvConfig() *config.EnvConfig {
	return &config.EnvConfig{
		AuthVersion:                                     config.AuthVersionV1,
		DefaultPlatformStickyTTL:                        7 * 24 * time.Hour,
		DefaultPlatformRegexFilters:                     []string{},
		DefaultPlatformRegionFilters:                    []string{},
		DefaultPlatformReverseProxyMissAction:           "TREAT_AS_EMPTY",
		DefaultPlatformReverseProxyEmptyAccountBehavior: "ACCOUNT_HEADER_RULE",
		DefaultPlatformReverseProxyFixedAccountHeader:   "Authorization",
		DefaultPlatformAllocationPolicy:                 "BALANCED",
		NodeDNSUpstreams:                                config.DefaultNodeDNSUpstreams(),
	}
}

func TestLoadDotenvFile_LoadsEmptyProxyToken(t *testing.T) {
	unsetEnvForDotenvTest(t, "RESIN_AUTH_VERSION", "RESIN_ADMIN_TOKEN", "RESIN_PROXY_TOKEN")

	path := filepath.Join(t.TempDir(), ".env")
	content := strings.Join([]string{
		"RESIN_AUTH_VERSION=V1",
		"RESIN_ADMIN_TOKEN=admin-secret",
		"RESIN_PROXY_TOKEN=",
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile(.env): %v", err)
	}

	if err := loadDotenvFile(path); err != nil {
		t.Fatalf("loadDotenvFile: %v", err)
	}

	proxyToken, ok := os.LookupEnv("RESIN_PROXY_TOKEN")
	if !ok {
		t.Fatal("expected RESIN_PROXY_TOKEN to be defined from .env")
	}
	if proxyToken != "" {
		t.Fatalf("RESIN_PROXY_TOKEN = %q, want empty", proxyToken)
	}

	cfg, err := config.LoadEnvConfig()
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}
	if cfg.ProxyToken != "" {
		t.Fatalf("cfg.ProxyToken = %q, want empty", cfg.ProxyToken)
	}
}

func TestLoadDotenvFile_DoesNotOverrideExistingEnv(t *testing.T) {
	unsetEnvForDotenvTest(t, "RESIN_AUTH_VERSION", "RESIN_ADMIN_TOKEN", "RESIN_PROXY_TOKEN")
	if err := os.Setenv("RESIN_PROXY_TOKEN", "real-env-token"); err != nil {
		t.Fatalf("Setenv(RESIN_PROXY_TOKEN): %v", err)
	}

	path := filepath.Join(t.TempDir(), ".env")
	content := strings.Join([]string{
		"RESIN_AUTH_VERSION=V1",
		"RESIN_ADMIN_TOKEN=admin-secret",
		"RESIN_PROXY_TOKEN=dotenv-token",
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile(.env): %v", err)
	}

	if err := loadDotenvFile(path); err != nil {
		t.Fatalf("loadDotenvFile: %v", err)
	}

	if got := os.Getenv("RESIN_PROXY_TOKEN"); got != "real-env-token" {
		t.Fatalf("RESIN_PROXY_TOKEN = %q, want real-env-token", got)
	}
	if got := os.Getenv("RESIN_AUTH_VERSION"); got != "V1" {
		t.Fatalf("RESIN_AUTH_VERSION = %q, want V1", got)
	}
}

func TestLoadDotenvFile_IgnoresMissingFile(t *testing.T) {
	if err := loadDotenvFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("loadDotenvFile missing file: %v", err)
	}
}

func TestBootstrapTopology_CreatesDefaultPlatformWhenMissing(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.DefaultPlatformStickyTTL = 2 * time.Hour
	envCfg.DefaultPlatformRegexFilters = []string{`^Provider/.*`}
	envCfg.DefaultPlatformRegionFilters = []string{"us", "hk"}
	envCfg.DefaultPlatformReverseProxyMissAction = "REJECT"
	envCfg.DefaultPlatformReverseProxyEmptyAccountBehavior = "FIXED_HEADER"
	envCfg.DefaultPlatformReverseProxyFixedAccountHeader = "X-Account-Id"
	envCfg.DefaultPlatformAllocationPolicy = "PREFER_LOW_LATENCY"

	subManager, pool := newBootstrapTestRuntime(runtimeCfg)
	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	platforms, err := engine.ListPlatforms()
	if err != nil {
		t.Fatalf("ListPlatforms: %v", err)
	}
	if len(platforms) != 1 {
		t.Fatalf("expected 1 platform, got %d", len(platforms))
	}

	defaultPlat := platforms[0]
	if defaultPlat.ID != platform.DefaultPlatformID {
		t.Fatalf("default id: got %q, want %q", defaultPlat.ID, platform.DefaultPlatformID)
	}
	if defaultPlat.Name != platform.DefaultPlatformName {
		t.Fatalf("default name: got %q, want %q", defaultPlat.Name, platform.DefaultPlatformName)
	}
	if defaultPlat.StickyTTLNs != int64(2*time.Hour) {
		t.Fatalf("sticky_ttl_ns: got %d, want %d", defaultPlat.StickyTTLNs, int64(2*time.Hour))
	}
	if defaultPlat.ReverseProxyMissAction != "REJECT" {
		t.Fatalf("reverse_proxy_miss_action: got %q, want %q", defaultPlat.ReverseProxyMissAction, "REJECT")
	}
	if defaultPlat.ReverseProxyEmptyAccountBehavior != "FIXED_HEADER" {
		t.Fatalf(
			"reverse_proxy_empty_account_behavior: got %q, want %q",
			defaultPlat.ReverseProxyEmptyAccountBehavior,
			"FIXED_HEADER",
		)
	}
	if defaultPlat.ReverseProxyFixedAccountHeader != "X-Account-Id" {
		t.Fatalf(
			"reverse_proxy_fixed_account_header: got %q, want %q",
			defaultPlat.ReverseProxyFixedAccountHeader,
			"X-Account-Id",
		)
	}
	if defaultPlat.AllocationPolicy != "PREFER_LOW_LATENCY" {
		t.Fatalf("allocation_policy: got %q, want %q", defaultPlat.AllocationPolicy, "PREFER_LOW_LATENCY")
	}
	if defaultPlat.PassiveCircuitBreakerDisabled {
		t.Fatal("passive_circuit_breaker_disabled: got true, want false")
	}

	if !reflect.DeepEqual(defaultPlat.RegexFilters, []string{`^Provider/.*`}) {
		t.Fatalf("regex_filters: got %v", defaultPlat.RegexFilters)
	}
	if !reflect.DeepEqual(defaultPlat.RegionFilters, []string{"us", "hk"}) {
		t.Fatalf("region_filters: got %v", defaultPlat.RegionFilters)
	}

	if _, ok := pool.GetPlatform(platform.DefaultPlatformID); !ok {
		t.Fatal("default platform should be registered in pool by ID")
	}
	if _, ok := pool.GetPlatformByName(platform.DefaultPlatformName); !ok {
		t.Fatal("default platform should be registered in pool by name")
	}
}

func TestBootstrapTopology_DefaultPlatformCreationIsIdempotent(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("first bootstrapTopology: %v", err)
	}
	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("second bootstrapTopology: %v", err)
	}

	platforms, err := engine.ListPlatforms()
	if err != nil {
		t.Fatalf("ListPlatforms: %v", err)
	}
	if len(platforms) != 1 {
		t.Fatalf("expected exactly 1 platform after repeated bootstrap, got %d", len(platforms))
	}
	if platforms[0].ID != platform.DefaultPlatformID {
		t.Fatalf("unexpected platform id after repeated bootstrap: %q", platforms[0].ID)
	}
}

func TestEnsureDefaultAccountHeaderRule_CreatesFallbackWhenMissing(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	if err := ensureDefaultAccountHeaderRule(engine); err != nil {
		t.Fatalf("ensureDefaultAccountHeaderRule: %v", err)
	}
	if err := ensureDefaultAccountHeaderRule(engine); err != nil {
		t.Fatalf("ensureDefaultAccountHeaderRule second call: %v", err)
	}

	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("ListAccountHeaderRules: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 fallback rule, got %d", len(rules))
	}
	if rules[0].URLPrefix != "*" {
		t.Fatalf("fallback url_prefix = %q, want %q", rules[0].URLPrefix, "*")
	}
	if !reflect.DeepEqual(rules[0].Headers, []string{"Authorization", "x-api-key"}) {
		t.Fatalf("fallback headers = %v, want %v", rules[0].Headers, []string{"Authorization", "x-api-key"})
	}
}

func TestEnsureDefaultAccountHeaderRule_DoesNotOverwriteExistingFallback(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	custom := model.AccountHeaderRule{
		URLPrefix:   "*",
		Headers:     []string{"X-Custom-Account"},
		UpdatedAtNs: time.Now().UnixNano(),
	}
	if _, err := engine.UpsertAccountHeaderRuleWithCreated(custom); err != nil {
		t.Fatalf("seed fallback rule: %v", err)
	}

	if err := ensureDefaultAccountHeaderRule(engine); err != nil {
		t.Fatalf("ensureDefaultAccountHeaderRule: %v", err)
	}

	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("ListAccountHeaderRules: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 fallback rule, got %d", len(rules))
	}
	if !reflect.DeepEqual(rules[0].Headers, custom.Headers) {
		t.Fatalf("fallback headers should stay custom, got %v, want %v", rules[0].Headers, custom.Headers)
	}
}

func TestBootstrapTopology_DefaultPlatformByNameDoesNotSatisfyDefaultID(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "legacy-default-id",
		Name:                   platform.DefaultPlatformName,
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{},
		RegionFilters:          []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("seed legacy default-by-name platform: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	err = bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig())
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when default ID is missing but default name is occupied")
	}
	if !strings.Contains(err.Error(), "ensure default platform") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "platform name already exists") {
		t.Fatalf("unexpected error detail: %v", err)
	}
}

func TestBootstrapTopology_FailsFastOnCorruptPlatformFilters(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "plat-1",
		Name:                   "BrokenOnRead",
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{`^ok$`},
		RegionFilters:          []string{"us"},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("UpsertPlatform: %v", err)
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(
		`UPDATE platforms SET regex_filters_json = ? WHERE id = ?`,
		`["(broken"]`,
		"plat-1",
	); err != nil {
		t.Fatalf("corrupt platform row: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	err = bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig())
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail on corrupt platform filters")
	}
	if !strings.Contains(err.Error(), "regex_filters") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBootstrapTopology_V1RejectsPersistedInvalidPlatformName(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "plat-1",
		Name:                   "StoredPlatform",
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{},
		RegionFilters:          []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("UpsertPlatform: %v", err)
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, "invalid:bad", "plat-1"); err != nil {
		t.Fatalf("corrupt platform name row: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	err = bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig())
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when V1 detects invalid persisted platform name")
	}
	if !strings.Contains(err.Error(), "incompatible with V1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBootstrapTopology_V1RejectsPersistedPlatformNameWithLeadingSpace(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "plat-1",
		Name:                   "StoredPlatform",
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{},
		RegionFilters:          []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("UpsertPlatform: %v", err)
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, " legacy-space", "plat-1"); err != nil {
		t.Fatalf("corrupt platform name row: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	err = bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig())
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when V1 detects leading space in persisted platform name")
	}
	if !strings.Contains(err.Error(), "incompatible with V1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBootstrapTopology_V1RejectsPersistedReservedPlatformNameAPI(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "plat-1",
		Name:                   "StoredPlatform",
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{},
		RegionFilters:          []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("UpsertPlatform: %v", err)
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, "api", "plat-1"); err != nil {
		t.Fatalf("corrupt platform name row: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	err = bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig())
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when V1 detects reserved platform name api")
	}
	if !strings.Contains(err.Error(), "incompatible with V1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBootstrapTopology_V1RejectsAllPersistedInvalidPlatformNames(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	for _, p := range []model.Platform{
		{
			ID:                     "plat-1",
			Name:                   "StoredPlatformOne",
			StickyTTLNs:            int64(time.Hour),
			RegexFilters:           []string{},
			RegionFilters:          []string{},
			ReverseProxyMissAction: "TREAT_AS_EMPTY",
			AllocationPolicy:       "BALANCED",
			UpdatedAtNs:            now,
		},
		{
			ID:                     "plat-2",
			Name:                   "StoredPlatformTwo",
			StickyTTLNs:            int64(time.Hour),
			RegexFilters:           []string{},
			RegionFilters:          []string{},
			ReverseProxyMissAction: "TREAT_AS_EMPTY",
			AllocationPolicy:       "BALANCED",
			UpdatedAtNs:            now,
		},
	} {
		if err := engine.UpsertPlatform(p); err != nil {
			t.Fatalf("UpsertPlatform(%s): %v", p.ID, err)
		}
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, "invalid:bad-one", "plat-1"); err != nil {
		t.Fatalf("corrupt platform name row (plat-1): %v", err)
	}
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, "api", "plat-2"); err != nil {
		t.Fatalf("corrupt platform name row (plat-2): %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	err = bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig())
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when V1 detects multiple invalid persisted platform names")
	}
	if !strings.Contains(err.Error(), "2 platform(s) are incompatible with V1") {
		t.Fatalf("unexpected error summary: %v", err)
	}
	if !strings.Contains(err.Error(), "\"invalid:bad-one\"") || !strings.Contains(err.Error(), "\"api\"") {
		t.Fatalf("expected all invalid platform names in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "Platform name rules:") {
		t.Fatalf("expected platform-name rules in error, got: %v", err)
	}
	if strings.Contains(err.Error(), "\"invalid:bad-one\":") || strings.Contains(err.Error(), "\"api\":") {
		t.Fatalf("error should list invalid platform names without per-platform reason details, got: %v", err)
	}
}

func TestBootstrapNodes_MissingDynamicDefaultsCircuitOpen(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-bootstrap-missing-dynamic"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapSub",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		Ephemeral:        false,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.77","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hash.Hex(),
		RawOptions:  raw,
		CreatedAtNs: now - int64(time.Hour),
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hash.Hex(),
		Tags:           []string{"bootstrap-tag"},
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	outboundMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing after bootstrapNodes", hash.Hex())
	}
	if !entry.IsCircuitOpen() {
		t.Fatal("node without dynamic record should default to circuit-open on bootstrap")
	}
	if entry.CircuitOpenSince.Load() <= 0 {
		t.Fatalf("CircuitOpenSince should be set, got %d", entry.CircuitOpenSince.Load())
	}
}

func TestBootstrapNodes_DynamicRecordOverridesDefaultCircuitOpen(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-bootstrap-with-dynamic"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapSub",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		Ephemeral:        false,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.88","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hash.Hex(),
		RawOptions:  raw,
		CreatedAtNs: now - int64(time.Hour),
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hash.Hex(),
		Tags:           []string{"bootstrap-tag"},
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := engine.BulkUpsertNodesDynamic([]model.NodeDynamic{{
		Hash:             hash.Hex(),
		FailureCount:     0,
		CircuitOpenSince: 0,
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	outboundMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing after bootstrapNodes", hash.Hex())
	}
	if entry.IsCircuitOpen() {
		t.Fatal("persisted nodes_dynamic should override bootstrap default circuit-open")
	}
}

func TestBootstrapNodes_RestoreEvictedSubscriptionNodeWithoutPoolRef(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-bootstrap-evicted"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapSub",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		Ephemeral:        false,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := []byte(`{"type":"stub","server":"198.51.100.199","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hash.Hex(),
		Tags:           []string{"evicted-tag"},
		Evicted:        true,
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	outboundMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}

	sub, ok := subManager.Get(subID)
	if !ok {
		t.Fatalf("subscription %q missing after bootstrap", subID)
	}
	managed, ok := sub.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatalf("evicted subscription node %s missing from managed view", hash.Hex())
	}
	if !managed.Evicted {
		t.Fatal("restored subscription node should keep Evicted=true")
	}
	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("evicted subscription node should not restore subscription hold in pool")
	}
}

func TestBootstrapNodes_TrimRegularLatencyKeepsAuthorities(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-bootstrap-trim-latency"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapSub",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		Ephemeral:        false,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.120","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	hashHex := hash.Hex()
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hashHex,
		RawOptions:  raw,
		CreatedAtNs: now - int64(time.Hour),
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hashHex,
		Tags:           []string{"bootstrap-tag"},
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}

	// 2 authority domains + 3 regular domains (capacity=2, one regular should be trimmed).
	if err := engine.BulkUpsertNodeLatency([]model.NodeLatency{
		{NodeHash: hashHex, Domain: "gstatic.com", EwmaNs: int64(10 * time.Millisecond), LastUpdatedNs: now - int64(1*time.Second)},
		{NodeHash: hashHex, Domain: "github.com", EwmaNs: int64(20 * time.Millisecond), LastUpdatedNs: now - int64(2*time.Second)},
		{NodeHash: hashHex, Domain: "recent-a.com", EwmaNs: int64(30 * time.Millisecond), LastUpdatedNs: now - int64(3*time.Second)},
		{NodeHash: hashHex, Domain: "recent-b.com", EwmaNs: int64(40 * time.Millisecond), LastUpdatedNs: now - int64(4*time.Second)},
		{NodeHash: hashHex, Domain: "old-c.com", EwmaNs: int64(50 * time.Millisecond), LastUpdatedNs: now - int64(5*time.Second)},
	}); err != nil {
		t.Fatalf("BulkUpsertNodeLatency: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	runtimeCfg.LatencyAuthorities = []string{"gstatic.com", "github.com"}
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 2
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	outboundMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}

	entry, ok := pool.GetEntry(hash)
	if !ok || entry.LatencyTable == nil {
		t.Fatalf("node %s missing or no latency table after bootstrap", hashHex)
	}
	restored := make(map[string]bool)
	entry.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
		restored[domain] = true
		return true
	})
	for _, domain := range []string{"gstatic.com", "github.com", "recent-a.com", "recent-b.com"} {
		if !restored[domain] {
			t.Fatalf("expected domain %q to be restored", domain)
		}
	}
	if restored["old-c.com"] {
		t.Fatal("old regular domain should be trimmed at bootstrap")
	}
	// Post-bootstrap first regular insert should evict the oldest kept regular
	// entry (recent-b.com), not the newest one (recent-a.com).
	entry.LatencyTable.Update("fresh-d.com", 60*time.Millisecond, 30*time.Second)
	if _, ok := entry.LatencyTable.GetDomainStats("recent-a.com"); !ok {
		t.Fatal("recent-a.com should remain as the newer regular entry")
	}
	if _, ok := entry.LatencyTable.GetDomainStats("recent-b.com"); ok {
		t.Fatal("recent-b.com should be evicted as the oldest regular entry")
	}
	if _, ok := entry.LatencyTable.GetDomainStats("fresh-d.com"); !ok {
		t.Fatal("fresh-d.com should be inserted into regular LRU")
	}

	if err := engine.FlushDirtySets(newFlushReaders(pool, subManager, nil)); err != nil {
		t.Fatalf("FlushDirtySets: %v", err)
	}
	latencies, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency: %v", err)
	}
	domains := make(map[string]bool)
	for _, row := range latencies {
		if row.NodeHash == hashHex {
			domains[row.Domain] = true
		}
	}
	for _, domain := range []string{"gstatic.com", "github.com", "recent-a.com", "recent-b.com"} {
		if !domains[domain] {
			t.Fatalf("expected persisted domain %q after trim flush", domain)
		}
	}
	if domains["old-c.com"] {
		t.Fatal("trimmed regular domain should be deleted from persistence")
	}
}

func TestRestoreBootstrapLatencies_TrimsAuthorityTieAcrossRestart(t *testing.T) {
	cacheDir := t.TempDir()
	stateDir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(cacheDir, stateDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-bootstrap-authority-trim"
	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.122","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	hashHex := hash.Hex()
	now := time.Unix(123, 0).UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapAuthorityTrim",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hashHex,
		RawOptions:  raw,
		CreatedAtNs: now,
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hashHex,
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}

	authorities := make([]string, node.MaxLatencyAuthorityEntries+1)
	latencies := make([]model.NodeLatency, len(authorities))
	for i := range authorities {
		number := strconv.FormatInt(int64(i), 10)
		if i < 10 {
			number = "0" + number
		}
		authorities[i] = "authority-" + number + ".com"
		latencies[i] = model.NodeLatency{
			NodeHash:      hashHex,
			Domain:        authorities[i],
			EwmaNs:        int64(time.Duration(i+1) * time.Millisecond),
			LastUpdatedNs: now,
		}
	}
	if err := engine.BulkUpsertNodeLatency(latencies); err != nil {
		t.Fatalf("BulkUpsertNodeLatency: %v", err)
	}

	subManager := topology.NewSubscriptionManager()
	subManager.Register(subscription.NewSubscription(subID, "BootstrapAuthorityTrim", "https://example.com/sub", true, false))
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 1,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyAuthorities:     func() []string { return authorities },
	})
	pool.AddNodeFromSub(hash, raw, subID)
	if err := restoreBootstrapNodeLatencies(engine, pool, 1, authorities); err != nil {
		t.Fatalf("restoreBootstrapNodeLatencies: %v", err)
	}

	entry, ok := pool.GetEntry(hash)
	if !ok || entry.LatencyTable == nil {
		t.Fatal("bootstrap node missing latency table")
	}
	restored := make(map[string]bool)
	entry.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
		restored[domain] = true
		return true
	})
	if len(restored) != node.MaxLatencyAuthorityEntries {
		t.Fatalf("bootstrap authority resident count: got %d, want %d", len(restored), node.MaxLatencyAuthorityEntries)
	}
	for i := 0; i < node.MaxLatencyAuthorityEntries; i++ {
		if !restored[authorities[i]] {
			t.Fatalf("newest/tie-ordered authority %q was not restored", authorities[i])
		}
	}
	if restored[authorities[node.MaxLatencyAuthorityEntries]] {
		t.Fatalf("authority beyond bootstrap capacity was restored: %q", authorities[node.MaxLatencyAuthorityEntries])
	}

	if err := engine.FlushDirtySets(newFlushReaders(pool, subManager, nil)); err != nil {
		t.Fatalf("FlushDirtySets: %v", err)
	}
	rows, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency: %v", err)
	}
	persisted := make(map[string]bool)
	for _, row := range rows {
		if row.NodeHash == hashHex {
			persisted[row.Domain] = true
		}
	}
	if len(persisted) != node.MaxLatencyAuthorityEntries || persisted[authorities[node.MaxLatencyAuthorityEntries]] {
		t.Fatalf("bootstrap authority trim was not persisted: %v", persisted)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("close first persistence owner: %v", err)
	}

	engine2, closer2, err := state.PersistenceBootstrap(cacheDir, stateDir)
	if err != nil {
		t.Fatalf("reopen persistence: %v", err)
	}
	defer func() { _ = closer2.Close() }()
	subManager2 := topology.NewSubscriptionManager()
	subManager2.Register(subscription.NewSubscription(subID, "BootstrapAuthorityTrim", "https://example.com/sub", true, false))
	pool2 := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager2.Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 1,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyAuthorities:     func() []string { return authorities },
	})
	pool2.AddNodeFromSub(hash, raw, subID)
	if err := restoreBootstrapNodeLatencies(engine2, pool2, 1, authorities); err != nil {
		t.Fatalf("restoreBootstrapNodeLatencies after restart: %v", err)
	}
	entry2, ok := pool2.GetEntry(hash)
	if !ok || entry2.LatencyTable == nil {
		t.Fatal("restarted bootstrap node missing latency table")
	}
	restoredAfterRestart := make(map[string]bool)
	entry2.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
		restoredAfterRestart[domain] = true
		return true
	})
	if len(restoredAfterRestart) != node.MaxLatencyAuthorityEntries || restoredAfterRestart[authorities[node.MaxLatencyAuthorityEntries]] {
		t.Fatalf("trimmed authority reappeared after restart: %v", restoredAfterRestart)
	}
}

func TestAuthorityLatencyRotationFlushesEvictionAcrossRestart(t *testing.T) {
	cacheDir := t.TempDir()
	stateDir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(cacheDir, stateDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-authority-rotation"
	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.121","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	hashHex := hash.Hex()
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "AuthorityRotation",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		_ = closer.Close()
		t.Fatalf("UpsertSubscription: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hashHex,
		Tags:           []string{"authority-rotation"},
	}}); err != nil {
		_ = closer.Close()
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hashHex,
		RawOptions:  raw,
		CreatedAtNs: now,
	}}); err != nil {
		_ = closer.Close()
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}

	subManager := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(subID, "AuthorityRotation", "https://example.com/sub", true, false)
	subManager.Register(sub)
	authorities := make([]string, node.MaxLatencyAuthorityEntries)
	for i := range authorities {
		authorities[i] = "old-" + strconv.Itoa(i) + ".com"
	}
	persistLatency := func(hash node.Hash, domain string) {
		if !engine.MarkNodeLatency(hash.Hex(), domain) {
			t.Fatalf("MarkNodeLatency(%s) was rejected", domain)
		}
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 1,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyAuthorities:     func() []string { return authorities },
		OnNodeLatencyChanged:   persistLatency,
	})
	pool.AddNodeFromSub(hash, raw, subID)
	entry, ok := pool.GetEntry(hash)
	if !ok || entry.LatencyTable == nil {
		_ = closer.Close()
		t.Fatal("expected pool node with latency table")
	}

	for i, domain := range authorities {
		latency := time.Duration(i+1) * time.Millisecond
		if !pool.RecordLatencyForEntry(hash, entry, domain, &latency) {
			_ = closer.Close()
			t.Fatalf("initial RecordLatencyForEntry(%s) rejected", domain)
		}
	}
	if err := engine.FlushDirtySets(newFlushReaders(pool, subManager, nil)); err != nil {
		_ = closer.Close()
		t.Fatalf("initial FlushDirtySets: %v", err)
	}

	authorities = []string{"new.com"}
	latency := 2 * time.Second
	if !pool.RecordLatencyForEntry(hash, entry, "new.com", &latency) {
		_ = closer.Close()
		t.Fatal("rotated RecordLatencyForEntry rejected")
	}
	var evictedDomain string
	for i := 0; i < node.MaxLatencyAuthorityEntries; i++ {
		domain := "old-" + strconv.Itoa(i) + ".com"
		if _, ok := entry.LatencyTable.GetDomainStats(domain); !ok {
			if evictedDomain != "" {
				t.Fatalf("multiple old authorities disappeared: %q and %q", evictedDomain, domain)
			}
			evictedDomain = domain
		}
	}
	if evictedDomain == "" {
		t.Fatal("authority rotation did not evict an old authority")
	}
	if err := engine.FlushDirtySets(newFlushReaders(pool, subManager, nil)); err != nil {
		_ = closer.Close()
		t.Fatalf("rotation FlushDirtySets: %v", err)
	}

	rows, err := engine.LoadAllNodeLatency()
	if err != nil {
		_ = closer.Close()
		t.Fatalf("LoadAllNodeLatency after rotation: %v", err)
	}
	for _, row := range rows {
		if row.NodeHash == hashHex && row.Domain == evictedDomain {
			t.Fatal("evicted authority remained in cache after rotation flush")
		}
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("close first persistence owner: %v", err)
	}

	engine2, closer2, err := state.PersistenceBootstrap(cacheDir, stateDir)
	if err != nil {
		t.Fatalf("reopen persistence: %v", err)
	}
	defer func() { _ = closer2.Close() }()

	subManager2 := topology.NewSubscriptionManager()
	subManager2.Register(subscription.NewSubscription(subID, "AuthorityRotation", "https://example.com/sub", true, false))
	pool2 := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager2.Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 1,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyAuthorities:     func() []string { return []string{"new.com"} },
	})
	pool2.AddNodeFromSub(hash, raw, subID)
	if err := restoreBootstrapNodeLatencies(engine2, pool2, 1, []string{"new.com"}); err != nil {
		t.Fatalf("restoreBootstrapNodeLatencies: %v", err)
	}
	entry2, ok := pool2.GetEntry(hash)
	if !ok || entry2.LatencyTable == nil {
		t.Fatal("restarted pool node missing latency table")
	}
	if _, ok := entry2.LatencyTable.GetDomainStats("new.com"); !ok {
		t.Fatal("rotated authority did not survive restart")
	}
	if _, ok := entry2.LatencyTable.GetDomainStats(evictedDomain); ok {
		t.Fatal("evicted authority reappeared after restart")
	}
}

func TestValidateLoadedRuntimeConfigRejectsTooManyAuthorities(t *testing.T) {
	cfg := config.NewDefaultRuntimeConfig()
	cfg.LatencyAuthorities = make([]string, node.MaxLatencyAuthorityEntries+1)
	if err := validateLoadedRuntimeConfig(cfg); err == nil {
		t.Fatal("expected oversized persisted latency authorities to be rejected")
	}
}

func TestValidateLoadedRuntimeConfigRejectsOversizedPayloadCapture(t *testing.T) {
	cfg := config.NewDefaultRuntimeConfig()
	cfg.ReverseProxyLogReqBodyMaxBytes = config.MaxReverseProxyLogCaptureBytes + 1
	if err := validateLoadedRuntimeConfig(cfg); err == nil {
		t.Fatal("expected oversized persisted payload capture limit to be rejected")
	}
}

func TestValidateLoadedRuntimeConfigRejectsInvalidRuntimeValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config.RuntimeConfig)
	}{
		{
			name: "negative consecutive failures",
			mutate: func(cfg *config.RuntimeConfig) {
				cfg.MaxConsecutiveFailures = -1
			},
		},
		{
			name: "short latency probe interval",
			mutate: func(cfg *config.RuntimeConfig) {
				cfg.MaxLatencyTestInterval = config.Duration(29 * time.Second)
			},
		},
		{
			name: "negative p2c window",
			mutate: func(cfg *config.RuntimeConfig) {
				cfg.P2CLatencyWindow = -1
			},
		},
		{
			name: "short cache flush interval",
			mutate: func(cfg *config.RuntimeConfig) {
				cfg.CacheFlushInterval = config.Duration(4 * time.Second)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewDefaultRuntimeConfig()
			tt.mutate(cfg)
			if err := validateLoadedRuntimeConfig(cfg); err == nil {
				t.Fatal("expected invalid persisted runtime config to be rejected")
			}
		})
	}
}

func TestMarkNodeRemovedDirtyKeepsCompoundDirtyAdmission(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	raw := json.RawMessage(`{"type":"compound-delete"}`)
	hash := node.HashFromRawOptions(raw)
	hashHex := hash.Hex()
	entry := node.NewNodeEntry(hash, raw, time.Now(), 4)
	entry.LatencyTable.Update("example.com", 50*time.Millisecond, time.Minute)
	entry.LatencyTable.Update("cloudflare.com", 60*time.Millisecond, time.Minute)

	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hashHex,
		RawOptions:  raw,
		CreatedAtNs: entry.CreatedAt.UnixNano(),
	}}); err != nil {
		t.Fatalf("seed static row: %v", err)
	}
	if err := engine.BulkUpsertNodesDynamic([]model.NodeDynamic{{
		Hash:             hashHex,
		FailureCount:     1,
		CircuitOpenSince: entry.CircuitOpenSince.Load(),
	}}); err != nil {
		t.Fatalf("seed dynamic row: %v", err)
	}
	if err := engine.BulkUpsertNodeLatency([]model.NodeLatency{
		{NodeHash: hashHex, Domain: "example.com", EwmaNs: int64(50 * time.Millisecond), LastUpdatedNs: time.Now().UnixNano()},
		{NodeHash: hashHex, Domain: "cloudflare.com", EwmaNs: int64(60 * time.Millisecond), LastUpdatedNs: time.Now().UnixNano()},
	}); err != nil {
		t.Fatalf("seed latency rows: %v", err)
	}

	firstMarkEntered := make(chan struct{})
	allowRemainingMarks := make(chan struct{})
	var releaseOnce sync.Once
	beforeNodeRemovedDirtyMarkHook = func(index int) {
		if index != 1 {
			return
		}
		close(firstMarkEntered)
		<-allowRemainingMarks
	}
	t.Cleanup(func() {
		beforeNodeRemovedDirtyMarkHook = nil
		releaseOnce.Do(func() { close(allowRemainingMarks) })
	})

	removed := make(chan struct{})
	go func() {
		markNodeRemovedDirty(engine, hash, entry)
		close(removed)
	}()
	select {
	case <-firstMarkEntered:
	case <-time.After(time.Second):
		t.Fatal("node removal did not reach the controlled dirty-mark boundary")
	}

	// Close admission while the first delete has completed. The whole
	// node-removal callback must already own one compound admission, so all
	// remaining deletes still belong to the same mutation.
	engine.CloseDirtyWriteAdmission()
	releaseOnce.Do(func() { close(allowRemainingMarks) })
	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("node removal dirty callback did not finish")
	}

	if err := engine.FlushDirtySets(state.CacheReaders{}); err != nil {
		t.Fatalf("flush delete marks: %v", err)
	}
	staticRows, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("load static rows: %v", err)
	}
	dynamicRows, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("load dynamic rows: %v", err)
	}
	latencyRows, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("load latency rows: %v", err)
	}
	if len(staticRows) != 0 || len(dynamicRows) != 0 || len(latencyRows) != 0 {
		t.Fatalf("node removal dirty callback was split by admission close: static=%+v dynamic=%+v latency=%+v", staticRows, dynamicRows, latencyRows)
	}
}

func TestFinalNodeRemovalKeepsSubscriptionAndNodeDeletesInOneDirtyAdmission(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"compound-node-delete",
		"compound-node-delete",
		"https://example.com",
		true,
		false,
	)
	subMgr.Register(sub)

	raw := json.RawMessage(`{"type":"compound-node-delete"}`)
	hash := node.HashFromRawOptions(raw)
	hashHex := hash.Hex()
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"node"}})
	entry := node.NewNodeEntry(hash, raw, time.Now(), 4)
	entry.AddSubscriptionID(sub.ID)
	entry.LatencyTable.Update("example.com", 50*time.Millisecond, time.Minute)

	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: sub.ID,
		NodeHash:       hashHex,
		Tags:           []string{"node"},
	}}); err != nil {
		t.Fatalf("seed subscription node: %v", err)
	}
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hashHex,
		RawOptions:  raw,
		CreatedAtNs: entry.CreatedAt.UnixNano(),
	}}); err != nil {
		t.Fatalf("seed static node: %v", err)
	}
	if err := engine.BulkUpsertNodesDynamic([]model.NodeDynamic{{
		Hash:             hashHex,
		FailureCount:     1,
		CircuitOpenSince: entry.CircuitOpenSince.Load(),
	}}); err != nil {
		t.Fatalf("seed dynamic node: %v", err)
	}
	if err := engine.BulkUpsertNodeLatency([]model.NodeLatency{{
		NodeHash:      hashHex,
		Domain:        "example.com",
		EwmaNs:        int64(50 * time.Millisecond),
		LastUpdatedNs: time.Now().UnixNano(),
	}}); err != nil {
		t.Fatalf("seed node latency: %v", err)
	}

	firstNodeDeleteEntered := make(chan struct{})
	allowNodeDelete := make(chan struct{})
	var firstNodeDeleteOnce sync.Once
	var allowDeleteOnce sync.Once
	t.Cleanup(func() {
		beforeNodeRemovedDirtyMarkHook = nil
		allowDeleteOnce.Do(func() { close(allowNodeDelete) })
	})
	beforeNodeRemovedDirtyMarkHook = func(index int) {
		if index != 1 {
			return
		}
		firstNodeDeleteOnce.Do(func() { close(firstNodeDeleteEntered) })
		<-allowNodeDelete
	}

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subMgr.Lookup,
		OnSubNodeChanged: func(subID string, changedHash node.Hash, added bool) {
			if !added && !engine.MarkSubscriptionNodeDelete(subID, changedHash.Hex()) {
				t.Errorf("legacy subscription-node delete callback was rejected")
			}
		},
		OnFinalNodeRemoved: func(subID string, changedHash node.Hash, removed *node.NodeEntry) {
			markFinalNodeRemovedDirty(engine, subID, changedHash, removed)
		},
		MaxConsecutiveFailures: func() int { return 3 },
	})
	pool.LoadNodeFromBootstrap(entry)

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(hash, sub.ID)
		close(removeDone)
	}()
	select {
	case <-firstNodeDeleteEntered:
	case <-time.After(time.Second):
		t.Fatal("final node removal did not reach the compound dirty callback")
	}

	// The node-level delete has already entered the same admission as the
	// subscription-node delete. Closing admission now must not interrupt the
	// remaining marks in this already-admitted compound mutation.
	engine.CloseDirtyWriteAdmission()
	flushDone := make(chan error, 1)
	go func() { flushDone <- engine.FlushDirtySets(state.CacheReaders{}) }()
	select {
	case err := <-flushDone:
		t.Fatalf("flush crossed the admitted final-node removal: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	allowDeleteOnce.Do(func() { close(allowNodeDelete) })
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("final node removal did not finish after releasing callback")
	}
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("flush final node deletion: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("flush did not finish after final node removal")
	}
	subscriptionNodes, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("load subscription nodes: %v", err)
	}
	staticNodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("load static nodes: %v", err)
	}
	dynamicNodes, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("load dynamic nodes: %v", err)
	}
	latencyNodes, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("load node latency: %v", err)
	}
	if len(subscriptionNodes) != 0 || len(staticNodes) != 0 || len(dynamicNodes) != 0 || len(latencyNodes) != 0 {
		t.Fatalf("final node deletion was split by dirty admission close: subscription=%+v static=%+v dynamic=%+v latency=%+v", subscriptionNodes, staticNodes, dynamicNodes, latencyNodes)
	}
}

func TestMarkNodeRemovedDirty_DeletesStaticDynamicAndLatency(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.42","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	hashHex := hash.Hex()

	entry := node.NewNodeEntry(hash, raw, time.Now(), 16)
	entry.FailureCount.Store(2)
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Minute).UnixNano())
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.50"))
	entry.LastEgressUpdate.Store(time.Now().UnixNano())
	entry.LastEgressUpdateAttempt.Store(time.Now().UnixNano())
	entry.LastLatencyProbeAttempt.Store(time.Now().UnixNano())
	entry.LastAuthorityLatencyProbeAttempt.Store(time.Now().UnixNano())
	entry.LatencyTable.Update("example.com", 55*time.Millisecond, 5*time.Minute)
	entry.LatencyTable.Update("cloudflare.com", 65*time.Millisecond, 5*time.Minute)

	readers := state.CacheReaders{
		ReadNodeStatic: func(h string) *model.NodeStatic {
			if h != hashHex {
				return nil
			}
			return &model.NodeStatic{
				Hash:        hashHex,
				RawOptions:  append(json.RawMessage(nil), raw...),
				CreatedAtNs: entry.CreatedAt.UnixNano(),
			}
		},
		ReadNodeDynamic: func(h string) *model.NodeDynamic {
			if h != hashHex {
				return nil
			}
			return &model.NodeDynamic{
				Hash:                               hashHex,
				FailureCount:                       int(entry.FailureCount.Load()),
				CircuitOpenSince:                   entry.CircuitOpenSince.Load(),
				EgressIP:                           entry.GetEgressIP().String(),
				EgressUpdatedAtNs:                  entry.LastEgressUpdate.Load(),
				LastLatencyProbeAttemptNs:          entry.LastLatencyProbeAttempt.Load(),
				LastAuthorityLatencyProbeAttemptNs: entry.LastAuthorityLatencyProbeAttempt.Load(),
				LastEgressUpdateAttemptNs:          entry.LastEgressUpdateAttempt.Load(),
			}
		},
		ReadNodeLatency: func(key model.NodeLatencyKey) *model.NodeLatency {
			if key.NodeHash != hashHex {
				return nil
			}
			stats, ok := entry.LatencyTable.GetDomainStats(key.Domain)
			if !ok {
				return nil
			}
			return &model.NodeLatency{
				NodeHash:      hashHex,
				Domain:        key.Domain,
				EwmaNs:        int64(stats.Ewma),
				LastUpdatedNs: stats.LastUpdated.UnixNano(),
			}
		},
	}

	// Seed cache rows for this node.
	engine.MarkNodeStatic(hashHex)
	engine.MarkNodeDynamic(hashHex)
	engine.MarkNodeLatency(hashHex, "example.com")
	engine.MarkNodeLatency(hashHex, "cloudflare.com")
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("seed FlushDirtySets: %v", err)
	}

	// Simulate node removed callback and flush deletes.
	markNodeRemovedDirty(engine, hash, entry)
	if err := engine.FlushDirtySets(state.CacheReaders{}); err != nil {
		t.Fatalf("delete FlushDirtySets: %v", err)
	}

	nodesStatic, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodesStatic) != 0 {
		t.Fatalf("nodes_static not deleted: %+v", nodesStatic)
	}

	nodesDynamic, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	if len(nodesDynamic) != 0 {
		t.Fatalf("nodes_dynamic not deleted: %+v", nodesDynamic)
	}

	latencies, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency: %v", err)
	}
	if len(latencies) != 0 {
		t.Fatalf("node_latency not deleted: %+v", latencies)
	}
}
