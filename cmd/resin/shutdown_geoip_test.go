package main

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/geoip"
)

type shutdownBlockingGeoReader struct {
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
	closes    atomic.Int32
}

func (r *shutdownBlockingGeoReader) Lookup(netip.Addr) string { return "" }

func (r *shutdownBlockingGeoReader) Close() error {
	r.closes.Add(1)
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.release
	return nil
}

func TestShutdownTracksGeoIPReaderCloseContinuation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "country.mmdb")
	if err := os.WriteFile(path, []byte("placeholder"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	reader := &shutdownBlockingGeoReader{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	service := geoip.NewService(geoip.ServiceConfig{
		CacheDir:       dir,
		DBFilename:     "country.mmdb",
		UpdateSchedule: "@every 24h",
		OpenDB: func(string) (geoip.GeoReader, error) {
			return reader, nil
		},
	})
	if err := service.Start(); err != nil {
		t.Fatalf("GeoIP Start: %v", err)
	}

	app := &resinApp{geoSvc: service}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()

	select {
	case <-reader.entered:
	case <-time.After(time.Second):
		t.Fatal("GeoIP reader Close did not start")
	}

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("app shutdown did not return after its deadline")
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- continuations.wait() }()
	select {
	case err := <-waitDone:
		t.Fatalf("shutdown continuations completed while GeoIP reader Close was blocked: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(reader.release)
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("shutdown continuations: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown continuation did not finish after GeoIP reader Close release")
	}
	if got := reader.closes.Load(); got != 1 {
		t.Fatalf("GeoIP reader Close count = %d, want 1", got)
	}
}
