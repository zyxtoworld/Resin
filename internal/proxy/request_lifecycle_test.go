package proxy

import (
	"errors"
	"net/http"
	"sync"
	"testing"
)

func TestRequestLifecycleConcurrentFinishAndUpdatesEmitsOneStableSnapshot(t *testing.T) {
	lifecycle := newRequestLifecycleFromMetadata(newMockEventEmitter(), "127.0.0.1:1", http.MethodPost, ProxyTypeReverse, false)
	emitter := lifecycle.events.(*mockEventEmitter)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lifecycle.setHTTPStatus(http.StatusBadGateway)
			lifecycle.setUpstreamError("reverse_roundtrip", errLifecycleTest)
			lifecycle.addIngressBytes(int64(i + 1))
			lifecycle.addEgressBytes(int64(i + 1))
			lifecycle.setNetOK(false)
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lifecycle.finish()
		}()
	}
	wg.Wait()

	select {
	case <-emitter.finishedCh:
	default:
		t.Fatal("lifecycle did not emit finished event")
	}
	select {
	case <-emitter.finishedCh:
		t.Fatal("lifecycle emitted more than one finished event")
	default:
	}
	select {
	case <-emitter.logCh:
	default:
		t.Fatal("lifecycle did not emit log event")
	}
	select {
	case <-emitter.logCh:
		t.Fatal("lifecycle emitted more than one log event")
	default:
	}
}

var errLifecycleTest = errors.New("lifecycle test upstream timeout")
