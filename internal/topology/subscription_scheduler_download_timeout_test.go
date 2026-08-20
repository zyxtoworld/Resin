package topology

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/subscription"
)

type slowSubscriptionRoundTripper func(*http.Request) (*http.Response, error)

func (f slowSubscriptionRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestScheduler_UpdateSubscription_PerAttemptBudgetLeavesRetryHeadroom(t *testing.T) {
	t.Setenv("RESIN_AUTH_VERSION", "V1")
	t.Setenv("RESIN_ADMIN_TOKEN", "test-admin")
	t.Setenv("RESIN_PROXY_TOKEN", "test-proxy")
	t.Setenv("RESIN_RESOURCE_FETCH_TIMEOUT", "")

	envCfg, err := config.LoadEnvConfig()
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}

	direct := netutil.NewDirectDownloader(
		func() time.Duration { return envCfg.ResourceFetchTimeout },
		func() string { return "" },
	)
	direct.Client = &http.Client{
		Transport: slowSubscriptionRoundTripper(func(req *http.Request) (*http.Response, error) {
			deadline, ok := req.Context().Deadline()
			remaining := time.Until(deadline)
			if !ok || remaining <= 0 || remaining >= envCfg.ResourceFetchTimeout {
				return nil, errors.New("subscription direct attempt did not receive a shorter request slice")
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(string(makeSubscriptionJSON(`{"type":"shadowsocks","tag":"slow","server":"1.1.1.1","server_port":443}`)))),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("slow-fetch", "Slow fetch", "https://example.com/sub", true, false)
	subMgr.Register(sub)
	pool := newTestPool(subMgr)
	scheduler := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Downloader: &netutil.RetryDownloader{Direct: direct},
	})

	if !scheduler.UpdateSubscription(sub) {
		t.Fatal("refresh was not admitted")
	}
	if got := sub.GetLastError(); got != "" {
		t.Fatalf("slow subscription refresh failed: %q", got)
	}
	if got := pool.Size(); got != 1 {
		t.Fatalf("slow subscription refresh published %d nodes, want 1", got)
	}
}
