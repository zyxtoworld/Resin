package geoip

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestUpdateNowContextCancellationInterruptsSerializedUpdateWait(t *testing.T) {
	downloader := &blockingDownloader{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	service := NewService(ServiceConfig{
		CacheDir:   t.TempDir(),
		OpenDB:     NoOpOpen,
		Downloader: downloader,
	})
	var releaseOnce sync.Once
	releaseDownloader := func() { releaseOnce.Do(func() { close(downloader.release) }) }
	t.Cleanup(releaseDownloader)

	firstDone := make(chan error, 1)
	go func() { firstDone <- service.UpdateNowContext(context.Background()) }()
	select {
	case <-downloader.started:
	case <-time.After(time.Second):
		t.Fatal("first GeoIP update did not acquire the serialized update owner")
	}

	secondStarted := make(chan struct{})
	requestCtx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondDone <- service.UpdateNowContext(requestCtx)
	}()
	<-secondStarted
	cancel()

	select {
	case updateErr := <-secondDone:
		if !errors.Is(updateErr, context.Canceled) {
			t.Fatalf("second GeoIP update error = %v, want context canceled", updateErr)
		}
	case <-time.After(500 * time.Millisecond):
		releaseDownloader()
		<-firstDone
		t.Fatal("canceled GeoIP update remained blocked behind the active update owner")
	}

	releaseDownloader()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first GeoIP update did not finish after downloader release")
	}
}
