package proxy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/topology"
	M "github.com/sagernet/sing/common/metadata"
)

type blockingAccountRuleMatcher struct {
	header      string
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (m *blockingAccountRuleMatcher) Match(string, string) []string {
	m.once.Do(func() { close(m.entered) })
	<-m.release
	return []string{m.header}
}

func (m *blockingAccountRuleMatcher) allow() {
	m.releaseOnce.Do(func() { close(m.release) })
}

type gatedPlatformGenerationLookup struct {
	pool      *topology.GlobalNodePool
	readHeld  chan struct{}
	allowRead chan struct{}
	readOnce  sync.Once
}

func (l *gatedPlatformGenerationLookup) GetPlatform(id string) (*platform.Platform, bool) {
	return l.pool.GetPlatform(id)
}

func (l *gatedPlatformGenerationLookup) GetPlatformByName(name string) (*platform.Platform, bool) {
	return l.pool.GetPlatformByName(name)
}

func (l *gatedPlatformGenerationLookup) WithPlatformReadByName(name string, fn func(*platform.Platform)) bool {
	return l.pool.WithPlatformReadByName(name, func(plat *platform.Platform) {
		l.readOnce.Do(func() { close(l.readHeld) })
		<-l.allowRead
		fn(plat)
	})
}

func TestReverseProxyDoesNotMixAccountPolicyAcrossPlatformReplacement(t *testing.T) {
	env := newProxyE2EEnv(t)
	oldPlatform, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("initial platform not found")
	}
	oldPlatform.ReverseProxyMissAction = string(platform.ReverseProxyMissActionReject)
	oldPlatform.ReverseProxyEmptyAccountBehavior = string(platform.ReverseProxyEmptyAccountBehaviorAccountHeaderRule)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("mixed-generation request reached upstream"))
	}))
	defer upstream.Close()
	setProxyE2EOutboundDialFunc(t, env, func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, upstream.Listener.Addr().String())
	})

	matcher := &blockingAccountRuleMatcher{
		header:  "Authorization",
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer matcher.allow()
	lookup := &gatedPlatformGenerationLookup{
		pool:      env.pool,
		readHeld:  make(chan struct{}),
		allowRead: make(chan struct{}),
	}
	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: lookup,
		Matcher:        matcher,
	})

	request := httptest.NewRequest(http.MethodGet, "/tok/plat/http/example.com/mixed", nil)
	request.Header.Set("Authorization", "old-account")
	response := httptest.NewRecorder()
	requestDone := make(chan struct{})
	go func() {
		rp.ServeHTTP(response, request)
		close(requestDone)
	}()
	snapshotHeld := false
	select {
	case <-lookup.readHeld:
		// The fixed implementation enters the real platform read owner before
		// invoking the matcher. Keep that owner held while replacement starts.
		snapshotHeld = true
	case <-matcher.entered:
		// The old implementation never used the generation owner. Its
		// replacement can commit while the matcher is blocked, which is the
		// deterministic red state below.
	}

	replaceDone := make(chan error, 1)
	go func() {
		replaceDone <- func() error {
			nextPlatform := platform.NewPlatform("plat-id", "plat", nil, nil)
			nextPlatform.ReverseProxyMissAction = string(platform.ReverseProxyMissActionReject)
			nextPlatform.ReverseProxyEmptyAccountBehavior = string(platform.ReverseProxyEmptyAccountBehaviorFixedHeader)
			nextPlatform.ReverseProxyFixedAccountHeader = "X-New-Account"
			return env.pool.ReplacePlatform(nextPlatform)
		}()
	}()
	if !snapshotHeld {
		if err := <-replaceDone; err != nil {
			t.Fatalf("replace platform: %v", err)
		}
		t.Fatalf("platform replacement committed while the old-generation matcher was blocked")
	}
	close(lookup.allowRead)
	select {
	case <-matcher.entered:
	case <-requestDone:
		t.Fatal("request completed before account matcher was released")
	}
	matcher.allow()

	<-requestDone
	if err := <-replaceDone; err != nil {
		t.Fatalf("replace platform: %v", err)
	}
	if response.Code != http.StatusOK || response.Body.String() != "mixed-generation request reached upstream" {
		t.Fatalf("request did not use one old platform snapshot: status=%d error=%q body=%q", response.Code, response.Header().Get("X-Resin-Error"), response.Body.String())
	}

	secondRequest := httptest.NewRequest(http.MethodGet, "/tok/plat/http/example.com/after-replace", nil)
	secondResponse := httptest.NewRecorder()
	rp.ServeHTTP(secondResponse, secondRequest)
	if secondResponse.Code != http.StatusForbidden || secondResponse.Header().Get("X-Resin-Error") != "ACCOUNT_REJECTED" {
		t.Fatalf("replacement policy was not used by the next request: status=%d error=%q body=%q", secondResponse.Code, secondResponse.Header().Get("X-Resin-Error"), secondResponse.Body.String())
	}
}
