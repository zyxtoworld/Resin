package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/service"
)

type geoIPDownloaderFunc func(context.Context, string) ([]byte, error)

func (f geoIPDownloaderFunc) Download(ctx context.Context, url string) ([]byte, error) {
	return f(ctx, url)
}

func TestHandleGeoIPUpdateHonorsRequestCancellation(t *testing.T) {
	started := make(chan struct{})
	geoService := geoip.NewService(geoip.ServiceConfig{
		CacheDir: t.TempDir(),
		OpenDB:   geoip.NoOpOpen,
		Downloader: geoIPDownloaderFunc(func(ctx context.Context, _ string) ([]byte, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}),
	})
	defer geoService.Stop()

	cp := &service.ControlPlaneService{GeoIP: geoService}
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/geoip/actions/update-now", nil).WithContext(requestCtx)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		HandleGeoIPUpdate(cp).ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("GeoIP handler did not enter the real downloader")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		// Bound the old implementation so this red test does not leak a
		// downloader goroutine after reporting the missing request context.
		_ = geoService.StopContext(context.Background())
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("GeoIP handler remained blocked after service shutdown")
		}
		t.Fatal("GeoIP handler ignored the canceled request context")
	}
}

var _ netutil.Downloader = geoIPDownloaderFunc(nil)
