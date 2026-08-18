package topology

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
)

func TestScheduler_RequestCancellationWhileWaitingForRuntimeMutation(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("runtime-cancel", "Runtime cancel", "", true, false)
	sub.SetSourceType(subscription.SourceTypeLocal)
	raw := `{"type":"shadowsocks","tag":"runtime-cancel","server":"1.1.1.1","server_port":443}`
	sub.SetContent(string(makeSubscriptionJSON(raw)))
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	pool.runtimeBatchMu.writeLock()
	defer pool.runtimeBatchMu.writeUnlock()
	queued := make(chan struct{})
	pool.runtimeBatchMu.afterWriterQueued = func() {
		select {
		case <-queued:
		default:
			close(queued)
		}
	}
	defer func() { pool.runtimeBatchMu.afterWriterQueued = nil }()

	sched := newTestScheduler(subMgr, pool, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		admitted bool
		err      error
	}
	done := make(chan result, 1)
	go func() {
		admitted, err := sched.UpdateSubscriptionContextResult(ctx, sub)
		done <- result{admitted: admitted, err: err}
	}()

	select {
	case <-queued:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not enter the runtime mutation writer")
	}
	cancel()

	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("canceled refresh error = %v, want context.Canceled", got.err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("canceled refresh remained blocked behind the runtime writer")
	}

	if _, ok := pool.GetEntry(node.HashFromRawOptions([]byte(raw))); ok {
		t.Fatal("canceled refresh published a node")
	}
}
