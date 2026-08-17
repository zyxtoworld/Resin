package requestlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
)

func ptrInt(v int) *int { return &v }

func TestRepo_InsertBatchContext_DiscardsConnectionWhenBusyTimeoutRestoreFails(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	repo.activeDB.SetMaxOpenConns(1)
	repo.beforeContextConnResetHook = func(conn *sql.Conn) {
		if err := conn.Raw(func(driverConn any) error {
			closer, ok := driverConn.(interface{ Close() error })
			if !ok {
				return fmt.Errorf("driver connection %T does not expose Close", driverConn)
			}
			return closer.Close()
		}); err != nil {
			t.Fatalf("close raw driver connection: %v", err)
		}
	}

	entry := proxy.RequestLogEntry{
		ID:          "restore-failure-1",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
	}
	if n, err := repo.InsertBatchContext(context.Background(), []proxy.RequestLogEntry{entry}); err != nil || n != 1 {
		t.Fatalf("InsertBatchContext: n=%d err=%v, want n=1 and no error", n, err)
	}

	entry.ID = "restore-failure-2"
	if n, err := repo.InsertBatch([]proxy.RequestLogEntry{entry}); err != nil || n != 1 {
		t.Fatalf("ordinary InsertBatch after failed restore: n=%d err=%v, want n=1 and no error", n, err)
	}
	if row, err := repo.GetByID(entry.ID); err != nil || row == nil {
		t.Fatalf("GetByID after failed restore: row=%v err=%v", row, err)
	}
}

func TestRepo_InsertBatchContext_CanceledBeforeRotationHasNoSideEffects(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	// Set the threshold after Open so the test forces the real rotation check;
	// NewRepo intentionally normalizes non-positive configuration values.
	repo.maxBytes = 0
	beforeDB := repo.activeDB
	beforePath := repo.activePath
	beforeFiles, err := repo.listDBFiles()
	if err != nil {
		t.Fatalf("list files before canceled insert: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repo.InsertBatchContext(ctx, []proxy.RequestLogEntry{{ID: "must-not-rotate"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("InsertBatchContext error = %v, want context.Canceled", err)
	}

	afterFiles, err := repo.listDBFiles()
	if err != nil {
		t.Fatalf("list files after canceled insert: %v", err)
	}
	if repo.activeDB != beforeDB || repo.activePath != beforePath {
		t.Fatalf("canceled insert changed active owner: before=(%p,%q), after=(%p,%q)", beforeDB, beforePath, repo.activeDB, repo.activePath)
	}
	if !reflect.DeepEqual(afterFiles, beforeFiles) {
		t.Fatalf("canceled insert changed request-log files: before=%v, after=%v", beforeFiles, afterFiles)
	}
	row, err := repo.GetByID("must-not-rotate")
	if err != nil {
		t.Fatalf("GetByID after canceled insert: %v", err)
	}
	if row != nil {
		t.Fatalf("canceled insert wrote request-log row: %+v", row)
	}
}

func TestRepo_InsertBatchContext_CanceledBeforeRotationCommitDiscardsPreparedDB(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	repo.maxBytes = 0

	beforeDB := repo.activeDB
	beforePath := repo.activePath
	beforeFiles, err := repo.listDBFiles()
	if err != nil {
		t.Fatalf("list files before staged rotation: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stagedDB *sql.DB
	var stagedPath string
	repo.beforeContextRotationCommitHook = func(db *sql.DB, path string) {
		stagedDB = db
		stagedPath = path
		cancel()
	}

	if _, err := repo.InsertBatchContext(ctx, []proxy.RequestLogEntry{{ID: "must-not-commit-rotation"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("InsertBatchContext error = %v, want context.Canceled", err)
	}
	if stagedDB == nil || stagedPath == "" {
		t.Fatal("rotation did not reach the prepared-db commit seam")
	}
	if err := stagedDB.Ping(); err == nil {
		t.Fatal("canceled rotation left the staged database open")
	}
	if repo.activeDB != beforeDB || repo.activePath != beforePath {
		t.Fatalf("canceled rotation changed active owner: before=(%p,%q), after=(%p,%q)", beforeDB, beforePath, repo.activeDB, repo.activePath)
	}
	afterFiles, err := repo.listDBFiles()
	if err != nil {
		t.Fatalf("list files after canceled staged rotation: %v", err)
	}
	if !reflect.DeepEqual(afterFiles, beforeFiles) {
		t.Fatalf("canceled rotation changed request-log files: before=%v, after=%v", beforeFiles, afterFiles)
	}
	if _, err := os.Stat(stagedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staged request-log artifact still exists: path=%q err=%v", stagedPath, err)
	}
	row, err := repo.GetByID("must-not-commit-rotation")
	if err != nil {
		t.Fatalf("GetByID after canceled staged rotation: %v", err)
	}
	if row != nil {
		t.Fatalf("canceled rotation wrote request-log row: %+v", row)
	}
}

func TestService_StopContextWaitsForDBLockReleasedBeforeDeadline(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	defer repo.Close()

	blocker, err := state.OpenDB(repo.activePath)
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO request_logs (id, ts_ns, proxy_type) VALUES (?, ?, ?)", "held-until-release", 1, 1); err != nil {
		tx.Rollback()
		blocker.Close()
		t.Fatalf("blocker lock write: %v", err)
	}

	var releaseOnce sync.Once
	released := make(chan struct{})
	release := func() {
		releaseOnce.Do(func() {
			_ = tx.Rollback()
			_ = blocker.Close()
			close(released)
		})
	}
	defer release()

	beginReady := make(chan struct{})
	allowBegin := make(chan struct{})
	var beginOnce sync.Once
	gateBegin := func() {
		beginOnce.Do(func() {
			close(beginReady)
			<-allowBegin
		})
	}
	repo.beforeContextTxBeginHook = gateBegin
	repo.beforeTxBeginHook = gateBegin

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	svc.Start()
	svc.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "waited-request-log",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
		HTTPMethod:  "GET",
		HTTPStatus:  200,
		NetOK:       true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- svc.StopContext(ctx) }()
	select {
	case <-beginReady:
	case <-time.After(time.Second):
		t.Fatal("requestlog final drain did not reach transaction begin gate")
	}
	close(allowBegin)
	timer := time.AfterFunc(250*time.Millisecond, release)
	defer timer.Stop()

	select {
	case err := <-done:
		t.Fatalf("requestlog StopContext returned before the held lock was released: %v", err)
	case <-released:
	}
	if err := <-done; err != nil {
		t.Fatalf("StopContext after lock release: %v", err)
	}
	row, err := repo.GetByID("waited-request-log")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if row == nil {
		t.Fatal("requestlog final drain lost the row after the lock was released")
	}
}

func TestRepo_InsertListGetPayloads(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	ts := time.Now().Add(-time.Minute).UnixNano()
	rows := []proxy.RequestLogEntry{
		{
			ID:                  "log-a",
			StartedAtNs:         ts,
			ProxyType:           proxy.ProxyTypeForward,
			ClientIP:            "10.0.0.1",
			PlatformID:          "plat-1",
			PlatformName:        "Platform One",
			Account:             "acct-a",
			TargetHost:          "example.com",
			TargetURL:           "https://example.com/a",
			DurationNs:          int64(12 * time.Millisecond),
			FirstByteDurationNs: int64(7 * time.Millisecond),
			NetOK:               true,
			HTTPMethod:          "GET",
			HTTPStatus:          200,
			ResinError:          "",
			UpstreamStage:       "",
			UpstreamErrKind:     "",
			UpstreamErrno:       "",
			UpstreamErrMsg:      "",
			IngressBytes:        1234,
			EgressBytes:         567,
			ReqHeadersLen:       8,
			ReqBodyLen:          7,
			RespHeadersLen:      6,
			RespBodyLen:         5,
			ReqHeaders:          []byte("req-h-a"),
			ReqBody:             []byte("req-b-a"),
			RespHeaders:         []byte("resp-h-a"),
			RespBody:            []byte("resp-b-a"),
			ReqBodyTruncated:    true,
			RespBodyTruncated:   true,
		},
		{
			ID:                  "log-b",
			StartedAtNs:         ts,
			ProxyType:           proxy.ProxyTypeReverse,
			ClientIP:            "10.0.0.2",
			PlatformID:          "plat-2",
			PlatformName:        "Platform Two",
			Account:             "acct-b",
			TargetHost:          "example.org",
			TargetURL:           "https://example.org/b",
			DurationNs:          int64(20 * time.Millisecond),
			FirstByteDurationNs: int64(15 * time.Millisecond),
			NetOK:               false,
			HTTPMethod:          "POST",
			HTTPStatus:          502,
			ResinError:          "UPSTREAM_REQUEST_FAILED",
			UpstreamStage:       "reverse_roundtrip",
			UpstreamErrKind:     "connection_refused",
			UpstreamErrno:       "ECONNREFUSED",
			UpstreamErrMsg:      "dial tcp 203.0.113.1:443: connect: connection refused",
			IngressBytes:        2222,
			EgressBytes:         1111,
			ReqBodyLen:          10,
			RespBodyLen:         11,
		},
	}
	inserted, err := repo.InsertBatch(rows)
	if err != nil {
		t.Fatalf("repo.InsertBatch: %v", err)
	}
	if inserted != 2 {
		t.Fatalf("inserted: got %d, want %d", inserted, 2)
	}

	list, hasMore, nextCursor, err := repo.List(ListFilter{Limit: 10})
	if err != nil {
		t.Fatalf("repo.List: %v", err)
	}
	if hasMore {
		t.Fatalf("hasMore: got true, want false")
	}
	if nextCursor != nil {
		t.Fatalf("nextCursor: got %+v, want nil", nextCursor)
	}
	if len(list) != 2 {
		t.Fatalf("list len: got %d, want %d", len(list), 2)
	}
	if list[0].FirstByteDurationNs != int64(7*time.Millisecond) || list[1].FirstByteDurationNs != int64(15*time.Millisecond) {
		t.Fatalf("first byte durations: got [%d, %d]", list[0].FirstByteDurationNs, list[1].FirstByteDurationNs)
	}
	if list[0].ID != "log-a" || list[1].ID != "log-b" {
		t.Fatalf("list order (ts desc, id asc tie-break): got [%s, %s]", list[0].ID, list[1].ID)
	}

	filtered, hasMore, nextCursor, err := repo.List(ListFilter{PlatformID: "plat-1", Limit: 10})
	if err != nil {
		t.Fatalf("repo.List filtered: %v", err)
	}
	if hasMore {
		t.Fatalf("filtered hasMore: got true, want false")
	}
	if nextCursor != nil {
		t.Fatalf("filtered nextCursor: got %+v, want nil", nextCursor)
	}
	if len(filtered) != 1 || filtered[0].ID != "log-a" {
		t.Fatalf("filtered list: got %+v", filtered)
	}

	filteredByName, hasMore, nextCursor, err := repo.List(ListFilter{PlatformName: "Platform One", Limit: 10})
	if err != nil {
		t.Fatalf("repo.List filtered by platform_name: %v", err)
	}
	if hasMore {
		t.Fatalf("filtered by platform_name hasMore: got true, want false")
	}
	if nextCursor != nil {
		t.Fatalf("filtered by platform_name nextCursor: got %+v, want nil", nextCursor)
	}
	if len(filteredByName) != 1 || filteredByName[0].ID != "log-a" {
		t.Fatalf("filtered by platform_name list: got %+v", filteredByName)
	}

	filteredByProxyType, hasMore, nextCursor, err := repo.List(ListFilter{
		ProxyType: ptrInt(int(proxy.ProxyTypeReverse)),
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("repo.List filtered by proxy_type: %v", err)
	}
	if hasMore {
		t.Fatalf("filtered by proxy_type hasMore: got true, want false")
	}
	if nextCursor != nil {
		t.Fatalf("filtered by proxy_type nextCursor: got %+v, want nil", nextCursor)
	}
	if len(filteredByProxyType) != 1 || filteredByProxyType[0].ID != "log-b" {
		t.Fatalf("filtered by proxy_type list: got %+v", filteredByProxyType)
	}

	fuzzyFiltered, hasMore, nextCursor, err := repo.List(ListFilter{
		PlatformID:   "LAT-1",
		PlatformName: "FORM o",
		Account:      "CT-A",
		TargetHost:   "AMPLE.C",
		Fuzzy:        true,
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("repo.List fuzzy filtered: %v", err)
	}
	if hasMore {
		t.Fatalf("fuzzy filtered hasMore: got true, want false")
	}
	if nextCursor != nil {
		t.Fatalf("fuzzy filtered nextCursor: got %+v, want nil", nextCursor)
	}
	if len(fuzzyFiltered) != 1 || fuzzyFiltered[0].ID != "log-a" {
		t.Fatalf("fuzzy filtered list: got %+v", fuzzyFiltered)
	}

	strictPartial, hasMore, nextCursor, err := repo.List(ListFilter{
		PlatformName: "form O",
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("repo.List strict partial: %v", err)
	}
	if hasMore {
		t.Fatalf("strict partial hasMore: got true, want false")
	}
	if nextCursor != nil {
		t.Fatalf("strict partial nextCursor: got %+v, want nil", nextCursor)
	}
	if len(strictPartial) != 0 {
		t.Fatalf("strict partial list len: got %d, want 0", len(strictPartial))
	}

	row, err := repo.GetByID("log-a")
	if err != nil {
		t.Fatalf("repo.GetByID: %v", err)
	}
	if row == nil || !row.PayloadPresent {
		t.Fatalf("expected payload-present log row, got %+v", row)
	}
	if row.IngressBytes != 1234 || row.EgressBytes != 567 {
		t.Fatalf("traffic bytes not persisted: ingress=%d egress=%d", row.IngressBytes, row.EgressBytes)
	}
	if !row.ReqBodyTruncated || !row.RespBodyTruncated {
		t.Fatalf("truncated flags not persisted: %+v", row)
	}

	rowB, err := repo.GetByID("log-b")
	if err != nil {
		t.Fatalf("repo.GetByID(log-b): %v", err)
	}
	if rowB == nil {
		t.Fatal("expected log-b row")
	}
	if rowB.ResinError != "UPSTREAM_REQUEST_FAILED" ||
		rowB.UpstreamStage != "reverse_roundtrip" ||
		rowB.UpstreamErrKind != "connection_refused" ||
		rowB.UpstreamErrno != "ECONNREFUSED" {
		t.Fatalf("upstream error fields not persisted: %+v", rowB)
	}
	if rowB.UpstreamErrMsg == "" {
		t.Fatal("expected upstream error message")
	}

	payload, err := repo.GetPayloads("log-a")
	if err != nil {
		t.Fatalf("repo.GetPayloads: %v", err)
	}
	if payload == nil {
		t.Fatal("expected payload row for log-a")
	}
	if string(payload.ReqHeaders) != "req-h-a" || string(payload.RespBody) != "resp-b-a" {
		t.Fatalf("payload mismatch: %+v", payload)
	}

	none, err := repo.GetPayloads("log-b")
	if err != nil {
		t.Fatalf("repo.GetPayloads(log-b): %v", err)
	}
	if none != nil {
		t.Fatalf("expected nil payload for log-b, got %+v", none)
	}
}

func TestRepo_ListDoesNotLoseSnapshottedShardDuringRotation(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 1)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	if _, err := repo.InsertBatch([]proxy.RequestLogEntry{{
		ID:          "must-survive-list-rotation",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
	}}); err != nil {
		t.Fatalf("seed request log: %v", err)
	}

	// Force the next production InsertBatch path to rotate before inserting.
	repo.maxBytes = 1
	readSnapshotted := make(chan struct{})
	allowReadOpen := make(chan struct{})
	rotationReached := make(chan struct{})
	writeBlocked := make(chan struct{})
	allowInsert := make(chan struct{})
	var releaseReadOnce, releaseInsertOnce sync.Once
	releaseRead := func() { releaseReadOnce.Do(func() { close(allowReadOpen) }) }
	releaseInsert := func() { releaseInsertOnce.Do(func() { close(allowInsert) }) }
	t.Cleanup(func() {
		releaseRead()
		releaseInsert()
	})
	repo.beforeReadShardOpenHook = func(files []string) {
		if len(files) == 0 {
			t.Errorf("read snapshot unexpectedly empty")
		}
		close(readSnapshotted)
		<-allowReadOpen
	}
	repo.beforeTxBeginHook = func() {
		close(rotationReached)
		<-allowInsert
	}
	repo.beforeWriteShardWaitHook = func() {
		close(writeBlocked)
	}

	type listResult struct {
		rows []LogSummary
		err  error
	}
	listDone := make(chan listResult, 1)
	go func() {
		rows, _, _, err := repo.List(ListFilter{Limit: 10})
		listDone <- listResult{rows: rows, err: err}
	}()
	<-readSnapshotted

	insertDone := make(chan error, 1)
	go func() {
		_, err := repo.InsertBatch([]proxy.RequestLogEntry{{
			ID:          "rotation-trigger",
			StartedAtNs: time.Now().UnixNano(),
			ProxyType:   proxy.ProxyTypeForward,
		}})
		insertDone <- err
	}()
	// The writer must wait for the read lease before it can prepare/commit a
	// rotation. The pre-fix path signaled rotationReached here, which is the
	// deterministic old-code red state captured separately.
	select {
	case <-writeBlocked:
	case <-rotationReached:
		t.Fatal("rotation started while a reader still held its shard snapshot")
	case <-time.After(2 * time.Second):
		t.Fatal("rotation writer did not reach the shard wait gate")
	}

	releaseRead()
	result := <-listDone
	if result.err != nil {
		t.Fatalf("repo.List: %v", result.err)
	}
	found := false
	for _, row := range result.rows {
		if row.ID == "must-survive-list-rotation" {
			found = true
			break
		}
	}
	if !found {
		releaseInsert()
		t.Fatalf("list lost a row from its shard snapshot during concurrent rotation: %+v", result.rows)
	}

	releaseInsert()
	if err := <-insertDone; err != nil {
		t.Fatalf("rotation-trigger insert: %v", err)
	}
}

func TestRepo_CloseContextDoesNotWaitPastReadLeaseDeadline(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 2)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}

	readSnapshotted := make(chan struct{})
	allowReadOpen := make(chan struct{})
	writeBlocked := make(chan struct{})
	var releaseReadOnce sync.Once
	releaseRead := func() { releaseReadOnce.Do(func() { close(allowReadOpen) }) }
	t.Cleanup(func() {
		releaseRead()
		_ = repo.Close()
	})
	repo.beforeReadShardOpenHook = func(files []string) {
		if len(files) == 0 {
			t.Errorf("read snapshot unexpectedly empty")
		}
		close(readSnapshotted)
		<-allowReadOpen
	}
	repo.beforeWriteShardWaitHook = func() {
		close(writeBlocked)
	}

	listDone := make(chan error, 1)
	go func() {
		_, _, _, err := repo.List(ListFilter{Limit: 10})
		listDone <- err
	}()
	<-readSnapshotted

	stopCtx, cancel := context.WithCancel(context.Background())
	closeDone := make(chan error, 1)
	go func() { closeDone <- repo.CloseContext(stopCtx) }()
	<-writeBlocked
	cancel()
	if err := <-closeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("CloseContext error = %v, want context.Canceled", err)
	}
	if repo.activeDB == nil {
		t.Fatal("CloseContext closed the active DB after its deadline")
	}

	releaseRead()
	if err := <-listDone; err != nil {
		t.Fatalf("repo.List after canceled CloseContext: %v", err)
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("repo.Close after read lease release: %v", err)
	}
}

func TestRepo_ListContextCancellationWhileWriterPending(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 2)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}

	readEntered := make(chan struct{})
	allowRead := make(chan struct{})
	writerWaiting := make(chan struct{})
	readWaiting := make(chan struct{})
	var readOnce, writerOnce, waitOnce, releaseOnce sync.Once
	releaseRead := func() { releaseOnce.Do(func() { close(allowRead) }) }
	t.Cleanup(func() {
		releaseRead()
		_ = repo.Close()
	})
	repo.beforeReadShardOpenHook = func(files []string) {
		if len(files) == 0 {
			t.Error("read snapshot unexpectedly empty")
		}
		readOnce.Do(func() { close(readEntered) })
		<-allowRead
	}
	repo.beforeWriteShardWaitHook = func() {
		writerOnce.Do(func() { close(writerWaiting) })
	}
	repo.beforeReadShardWaitHook = func() {
		waitOnce.Do(func() { close(readWaiting) })
	}

	firstDone := make(chan error, 1)
	go func() {
		_, _, _, err := repo.List(ListFilter{Limit: 10})
		firstDone <- err
	}()
	select {
	case <-readEntered:
	case <-time.After(time.Second):
		t.Fatal("first request-log reader did not acquire its read snapshot")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- repo.CloseContext(context.Background()) }()
	select {
	case <-writerWaiting:
	case <-time.After(time.Second):
		t.Fatal("request-log close did not become a queued writer")
	}

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, _, _, err := repo.ListContext(ctx, ListFilter{Limit: 10})
		secondDone <- err
	}()
	select {
	case <-readWaiting:
	case <-time.After(time.Second):
		t.Fatal("canceled request-log reader did not reach the writer-priority gate")
	}
	cancel()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ListContext error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		releaseRead()
		t.Fatal("ListContext remained blocked after its request context was canceled")
	}

	releaseRead()
	if err := <-firstDone; err != nil {
		t.Fatalf("first List: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
}

func TestService_FlushesByBatchSize(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    2,
		FlushInterval: time.Hour,
	})
	svc.Start()
	t.Cleanup(svc.Stop)

	baseTs := time.Now().UnixNano()
	svc.EmitRequestLog(proxy.RequestLogEntry{
		StartedAtNs: baseTs,
		ProxyType:   proxy.ProxyTypeForward,
		ClientIP:    "127.0.0.1",
		PlatformID:  "plat-1",
		Account:     "acct-1",
		TargetHost:  "example.com",
		TargetURL:   "https://example.com/1",
		HTTPMethod:  "GET",
		HTTPStatus:  200,
		NetOK:       true,
	})
	svc.EmitRequestLog(proxy.RequestLogEntry{
		StartedAtNs: baseTs + 1,
		ProxyType:   proxy.ProxyTypeReverse,
		ClientIP:    "127.0.0.2",
		PlatformID:  "plat-1",
		Account:     "acct-2",
		TargetHost:  "example.com",
		TargetURL:   "https://example.com/2",
		HTTPMethod:  "POST",
		HTTPStatus:  502,
		NetOK:       false,
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rows, _, _, err := repo.List(ListFilter{PlatformID: "plat-1", Limit: 10})
		if err != nil {
			t.Fatalf("repo.List: %v", err)
		}
		if len(rows) == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for service flush")
}

func TestService_StopRejectsLateEmit(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	defer repo.Close()

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	svc.Start()
	svc.Stop()

	svc.EmitRequestLog(proxy.RequestLogEntry{ID: "late-log"})
	if got := len(svc.queue); got != 0 {
		t.Fatalf("late request log entered stopped queue: %d", got)
	}
}

func TestService_StopBeforeStartFlushesQueuedEntries(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	defer repo.Close()

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	svc.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "before-start",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
		HTTPMethod:  "GET",
		HTTPStatus:  200,
		NetOK:       true,
	})

	svc.Stop()

	row, err := repo.GetByID("before-start")
	if err != nil {
		t.Fatalf("repo.GetByID: %v", err)
	}
	if row == nil {
		t.Fatal("Stop before Start dropped a queued request log")
	}
}

func TestService_StopContextTimeoutOwnerDrainsQueuedEntry(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	defer repo.Close()

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	stopFinalDrainEntered := make(chan struct{})
	allowStopFinalDrain := make(chan struct{})
	svc.beforeStopFinalDrainHook = func() {
		close(stopFinalDrainEntered)
		<-allowStopFinalDrain
	}
	svc.Start()
	svc.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "timeout-owner-drain",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
		HTTPMethod:  "GET",
		HTTPStatus:  200,
		NetOK:       true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- svc.StopContext(ctx) }()
	select {
	case <-stopFinalDrainEntered:
	case <-time.After(time.Second):
		t.Fatal("requestlog stop did not reach final drain gate")
	}
	<-ctx.Done()
	var firstErr error
	select {
	case firstErr = <-firstDone:
		if !errors.Is(firstErr, context.DeadlineExceeded) {
			t.Fatalf("first StopContext error = %v, want deadline exceeded", firstErr)
		}
	case <-time.After(250 * time.Millisecond):
		close(allowStopFinalDrain)
		<-firstDone
		t.Fatal("first StopContext did not honor its caller deadline")
	}
	close(allowStopFinalDrain)

	if err := svc.StopContext(context.Background()); err != nil {
		t.Fatalf("owner StopContext after waiter timeout: %v", err)
	}
	row, err := repo.GetByID("timeout-owner-drain")
	if err != nil {
		t.Fatalf("GetByID after owner drain: %v", err)
	}
	if row == nil {
		t.Fatal("requestlog owner dropped queued entry after waiter timeout")
	}
}

func TestService_StopContextCanceledBeforeStartStillDrainsQueue(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	defer repo.Close()

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	svc.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "canceled-pre-start",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := svc.StopContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("first StopContext error = %v, want context canceled", err)
	}
	if err := svc.StopContext(context.Background()); err != nil {
		t.Fatalf("owner StopContext after canceled waiter: %v", err)
	}
	row, err := repo.GetByID("canceled-pre-start")
	if err != nil {
		t.Fatalf("GetByID after canceled waiter: %v", err)
	}
	if row == nil {
		t.Fatal("pre-start queue entry was dropped after canceled waiter")
	}
}

func TestService_StopContextHonorsDeadlineDuringDBFlush(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	defer repo.Close()

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	svc.Start()
	svc.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "blocked-request-log",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
		HTTPMethod:  "GET",
		HTTPStatus:  200,
		NetOK:       true,
	})

	blocker, err := state.OpenDB(repo.activePath)
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO request_logs (id, ts_ns, proxy_type) VALUES (?, ?, ?)", "held-request-lock", 1, 1); err != nil {
		tx.Rollback()
		blocker.Close()
		t.Fatalf("blocker lock write: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
		_ = blocker.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- svc.StopContext(ctx) }()

	select {
	case err := <-stopDone:
		if err == nil {
			t.Fatal("StopContext succeeded while requestlog DB lock was held")
		}
	case <-time.After(250 * time.Millisecond):
		_ = tx.Rollback()
		_ = blocker.Close()
		select {
		case <-stopDone:
		case <-time.After(7 * time.Second):
		}
		t.Fatal("StopContext ignored shutdown deadline during requestlog DB flush")
	}
}

func TestService_StopContextCancelsReadBarrierFlush(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	defer repo.Close()

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	svc.Start()

	svc.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "barrier-cancel-request",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
		HTTPMethod:  "GET",
		HTTPStatus:  200,
		NetOK:       true,
	})

	blocker, err := state.OpenDB(repo.activePath)
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO request_logs (id, ts_ns, proxy_type) VALUES (?, ?, ?)", "barrier-held-lock", 1, 1); err != nil {
		tx.Rollback()
		blocker.Close()
		t.Fatalf("blocker lock write: %v", err)
	}

	beginEntered := make(chan struct{})
	var beginOnce sync.Once
	repo.beforeTxBeginHook = func() { beginOnce.Do(func() { close(beginEntered) }) }
	connReleased := make(chan struct{})
	var connReleaseOnce sync.Once
	repo.beforeContextConnResetHook = func(*sql.Conn) {
		connReleaseOnce.Do(func() { close(connReleased) })
	}
	flushDone := make(chan struct{})
	go func() {
		svc.FlushNow()
		close(flushDone)
	}()
	select {
	case <-beginEntered:
	case <-time.After(time.Second):
		_ = tx.Rollback()
		_ = blocker.Close()
		t.Fatal("FlushNow did not reach the SQLite transaction begin")
	}

	stopWaiting := make(chan struct{})
	svc.beforeStopWorkerWaitHook = func() { close(stopWaiting) }
	finalDrainReady := make(chan struct{})
	allowFinalDrain := make(chan struct{})
	svc.beforeStopFinalDrainHook = func() {
		close(finalDrainReady)
		<-allowFinalDrain
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- svc.StopContext(ctx) }()
	select {
	case <-stopWaiting:
	case <-time.After(time.Second):
		_ = tx.Rollback()
		_ = blocker.Close()
		t.Fatal("StopContext did not reach worker wait")
	}
	cancel()

	select {
	case <-connReleased:
		// The fixed implementation must cancel the barrier transaction and
		// reach connection cleanup without waiting for the held SQLite lock.
	case <-time.After(2 * time.Second):
		_ = tx.Rollback()
		_ = blocker.Close()
		select {
		case <-stopDone:
		case <-time.After(7 * time.Second):
		}
		t.Fatal("canceled read barrier did not release its SQLite connection")
	}
	select {
	case <-finalDrainReady:
		// The barrier has returned and the worker is at the final-drain
		// boundary. The waiter context is already canceled; release the final
		// gate and the SQLite lock so the independent owner can finish.
		close(allowFinalDrain)
		_ = tx.Rollback()
		_ = blocker.Close()
	case <-time.After(2 * time.Second):
		_ = tx.Rollback()
		_ = blocker.Close()
		select {
		case <-stopDone:
		case <-time.After(7 * time.Second):
		}
		t.Fatal("worker did not reach the final-drain boundary")
	}
	select {
	case <-stopDone:
		// Stop must finish after the canceled worker has released its
		// connection, without waiting for the held writer lock.
	case <-time.After(2 * time.Second):
		_ = tx.Rollback()
		_ = blocker.Close()
		select {
		case <-stopDone:
		case <-time.After(7 * time.Second):
		}
		t.Fatal("StopContext waited for an uninterruptible read barrier flush")
	}

	select {
	case <-flushDone:
	case <-time.After(time.Second):
		t.Fatal("FlushNow did not finish after StopContext canceled the worker")
	}
}

func TestService_StopContextRetriesAbortedReadBarrierBatch(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	defer repo.Close()

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	svc.Start()

	if _, err := repo.activeDB.Exec("PRAGMA busy_timeout=5000"); err != nil {
		t.Fatalf("set requestlog busy timeout: %v", err)
	}
	blocker, err := state.OpenDB(repo.activePath)
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		_ = blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
		_ = blocker.Close()
	}()
	if _, err := tx.Exec("INSERT INTO request_logs (id, ts_ns, proxy_type) VALUES (?, ?, ?)", "barrier-retry-held-lock", 1, 1); err != nil {
		t.Fatalf("blocker lock write: %v", err)
	}

	svc.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "barrier-retry-request",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
		HTTPMethod:  "GET",
		HTTPStatus:  200,
		NetOK:       true,
	})

	beginEntered := make(chan struct{})
	var beginOnce sync.Once
	repo.beforeTxBeginHook = func() { beginOnce.Do(func() { close(beginEntered) }) }
	flushDone := make(chan struct{})
	go func() {
		svc.FlushNow()
		close(flushDone)
	}()
	select {
	case <-beginEntered:
	case <-time.After(time.Second):
		t.Fatal("FlushNow did not reach the SQLite transaction begin")
	}

	stopWaiting := make(chan struct{})
	svc.beforeStopWorkerWaitHook = func() { close(stopWaiting) }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- svc.StopContext(ctx) }()
	select {
	case <-stopWaiting:
	case <-ctx.Done():
		t.Fatal("StopContext did not reach worker wait before its deadline")
	}

	// Stop has canceled the worker's barrier context and is now waiting for
	// it. Releasing the lock must let the retained batch go through the final
	// drain using the still-live shutdown context.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("release blocker transaction: %v", err)
	}
	if err := blocker.Close(); err != nil {
		t.Fatalf("release blocker DB: %v", err)
	}

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("StopContext: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("StopContext did not finish within its live shutdown budget")
	}
	select {
	case <-flushDone:
	case <-time.After(time.Second):
		t.Fatal("FlushNow did not return after StopContext completed")
	}

	var count int
	if err := repo.activeDB.QueryRow("SELECT COUNT(*) FROM request_logs WHERE id = ?", "barrier-retry-request").Scan(&count); err != nil {
		t.Fatalf("count retried request log: %v", err)
	}
	if count != 1 {
		t.Fatalf("retried request log count = %d, want exactly one", count)
	}
}

func TestService_FlushNowRetriesAfterShortAttemptWhileContextLives(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	svc.Start()

	var blocker *sql.DB
	var tx *sql.Tx
	var err error
	var released bool
	allowFirstAttemptReset := make(chan struct{})
	var allowResetOnce sync.Once
	allowReset := func() {
		allowResetOnce.Do(func() { close(allowFirstAttemptReset) })
	}
	release := func() {
		if released {
			return
		}
		released = true
		if tx != nil {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				t.Errorf("release blocker transaction: %v", err)
			}
		}
		if blocker != nil {
			if err := blocker.Close(); err != nil {
				t.Errorf("release blocker DB: %v", err)
			}
		}
	}
	defer func() {
		release()
		allowReset()
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := svc.StopContext(stopCtx); err != nil {
			t.Errorf("StopContext cleanup: %v", err)
		}
		_ = repo.Close()
	}()

	blocker, err = state.OpenDB(repo.activePath)
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err = blocker.Begin()
	if err != nil {
		_ = blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO request_logs (id, ts_ns, proxy_type) VALUES (?, ?, ?)", "short-attempt-held-lock", 1, 1); err != nil {
		_ = tx.Rollback()
		_ = blocker.Close()
		t.Fatalf("blocker lock write: %v", err)
	}

	svc.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "short-attempt-retry-request",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
		HTTPMethod:  "GET",
		HTTPStatus:  200,
		NetOK:       true,
	})

	firstBegin := make(chan struct{})
	var beginOnce sync.Once
	repo.beforeContextTxBeginHook = func() {
		beginOnce.Do(func() { close(firstBegin) })
	}
	firstAttemptReleased := make(chan struct{})
	var releaseOnce sync.Once
	repo.beforeContextConnResetHook = func(*sql.Conn) {
		releaseOnce.Do(func() { close(firstAttemptReleased) })
		<-allowFirstAttemptReset
	}

	flushDone := make(chan struct{})
	go func() {
		svc.FlushNow()
		close(flushDone)
	}()
	select {
	case <-firstBegin:
	case <-time.After(time.Second):
		t.Fatal("FlushNow did not enter the first context transaction")
	}
	select {
	case <-firstAttemptReleased:
		// The first 100ms attempt has ended while the write lock is still held.
	case <-time.After(time.Second):
		t.Fatal("first short SQLite attempt did not release its connection")
	}
	release()
	allowReset()

	select {
	case <-flushDone:
	case <-time.After(2 * time.Second):
		t.Fatal("FlushNow did not complete after the SQLite lock was released")
	}

	var count int
	if err := repo.activeDB.QueryRow("SELECT COUNT(*) FROM request_logs WHERE id = ?", "short-attempt-retry-request").Scan(&count); err != nil {
		t.Fatalf("count retried request log: %v", err)
	}
	if count != 1 {
		t.Fatalf("retried request log count = %d, want exactly one", count)
	}
}

func TestService_ConcurrentStopWaitsForAdmittedEmit(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	defer repo.Close()

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	svc.Start()
	entered := make(chan struct{})
	release := make(chan struct{})
	svc.beforeEmitHook = func() {
		close(entered)
		<-release
	}
	stopAdmissionClosed := make(chan struct{})
	svc.beforeEmitDrainHook = func() { close(stopAdmissionClosed) }

	emitDone := make(chan struct{})
	go func() {
		svc.EmitRequestLog(proxy.RequestLogEntry{
			ID:          "admitted-log",
			StartedAtNs: time.Now().UnixNano(),
			ProxyType:   proxy.ProxyTypeForward,
			HTTPMethod:  "GET",
			HTTPStatus:  200,
			NetOK:       true,
		})
		close(emitDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("EmitRequestLog did not enter admission")
	}

	stop1Done := make(chan struct{})
	stop2Done := make(chan struct{})
	go func() {
		svc.Stop()
		close(stop1Done)
	}()
	select {
	case <-stopAdmissionClosed:
	case <-time.After(time.Second):
		t.Fatal("Stop did not close emit admission")
	}
	go func() {
		svc.Stop()
		close(stop2Done)
	}()
	select {
	case <-stop1Done:
		t.Fatal("first Stop returned before admitted emit completed")
	case <-stop2Done:
		t.Fatal("second Stop returned before the first Stop completed")
	default:
	}

	close(release)
	select {
	case <-emitDone:
	case <-time.After(time.Second):
		t.Fatal("admitted EmitRequestLog did not finish")
	}
	select {
	case <-stop1Done:
	case <-time.After(time.Second):
		t.Fatal("first Stop did not finish")
	}
	select {
	case <-stop2Done:
	case <-time.After(time.Second):
		t.Fatal("second Stop did not finish")
	}

	row, err := repo.GetByID("admitted-log")
	if err != nil {
		t.Fatalf("repo.GetByID: %v", err)
	}
	if row == nil {
		t.Fatal("final Stop flush lost admitted request log")
	}
}

func TestService_RepoReadFlushesQueuedLogs(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,      // keep below batch threshold
		FlushInterval: time.Hour, // avoid timer-driven flush in test
	})
	svc.Start()
	t.Cleanup(svc.Stop)

	baseTs := time.Now().UnixNano()
	svc.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "barrier-log-1",
		StartedAtNs: baseTs,
		ProxyType:   proxy.ProxyTypeForward,
		PlatformID:  "plat-1",
		TargetHost:  "example.com",
		TargetURL:   "https://example.com/barrier",
		HTTPMethod:  "GET",
		HTTPStatus:  200,
		NetOK:       true,
	})

	rows, _, _, err := repo.List(ListFilter{PlatformID: "plat-1", Limit: 10})
	if err != nil {
		t.Fatalf("repo.List: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows len: got %d, want 1", len(rows))
	}
	if rows[0].ID != "barrier-log-1" {
		t.Fatalf("row id: got %q, want %q", rows[0].ID, "barrier-log-1")
	}
}

func TestRepo_OpenCreatesLogDir(t *testing.T) {
	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	repo := NewRepo(logDir, 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
}

func TestRepo_OpenCreatesOptimizedIndexes(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 2)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	assertRequestLogIndexes(t, repo.activeDB)
	assertQueryPlanUsesIndex(t, repo.activeDB, "idx_request_logs_proxy_type_ts_id", `
		SELECT id, ts_ns FROM request_logs
		WHERE proxy_type = ? ORDER BY ts_ns DESC, id ASC LIMIT 101
	`, int(proxy.ProxyTypeReverse))
	assertQueryPlanUsesIndex(t, repo.activeDB, "idx_request_logs_account_ts_id", `
		SELECT id, ts_ns FROM request_logs
		WHERE account = ? AND account <> '' ORDER BY ts_ns DESC, id ASC LIMIT 101
	`, "acct-a")
}

func TestRepo_OpenMigratesIndexesInHistoricalDBs(t *testing.T) {
	logDir := t.TempDir()
	repo := NewRepo(logDir, 1<<20, 2)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	historicalPath := repo.activePath

	legacyIndexesDDL := `
		DROP INDEX IF EXISTS idx_request_logs_ts_id;
		DROP INDEX IF EXISTS idx_request_logs_proxy_type_ts_id;
		DROP INDEX IF EXISTS idx_request_logs_account_ts_id;
		CREATE INDEX idx_request_logs_ts_ns ON request_logs(ts_ns);
		CREATE INDEX idx_request_logs_proxy_type ON request_logs(proxy_type);
		CREATE INDEX idx_request_logs_platform_id ON request_logs(platform_id);
	`
	if _, err := repo.activeDB.Exec(legacyIndexesDDL); err != nil {
		t.Fatalf("prepare legacy indexes: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	if err := repo.rotateDB(); err != nil {
		t.Fatalf("repo.rotateDB: %v", err)
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("repo.Close: %v", err)
	}

	reopened := NewRepo(logDir, 1<<20, 2)
	if err := reopened.Open(); err != nil {
		t.Fatalf("reopened.Open: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	historicalDB, err := reopened.openReadOnly(historicalPath)
	if err != nil {
		t.Fatalf("open historical DB: %v", err)
	}
	t.Cleanup(func() { _ = historicalDB.Close() })
	assertRequestLogIndexes(t, historicalDB)
}

func TestRepo_CleanupRetainsConfiguredFileCount(t *testing.T) {
	logDir := t.TempDir()
	for i := 1; i <= 5; i++ {
		path := filepath.Join(logDir, fmt.Sprintf("request_logs-%013d.db", i))
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("create shard %d: %v", i, err)
		}
	}

	repo := NewRepo(logDir, 1<<20, 2)
	if err := repo.cleanup(); err != nil {
		t.Fatalf("repo.cleanup: %v", err)
	}
	files, err := repo.listDBFiles()
	if err != nil {
		t.Fatalf("repo.listDBFiles: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("retained files: got %d, want 2", len(files))
	}
	if got := filepath.Base(files[0]); got != "request_logs-0000000000004.db" {
		t.Fatalf("oldest retained file: got %q", got)
	}
}

func TestNewRepo_DefaultsToTwoHistoricalDBs(t *testing.T) {
	repo := NewRepo(t.TempDir(), 0, 0)
	if repo.retainCount != 2 {
		t.Fatalf("retainCount: got %d, want 2", repo.retainCount)
	}
}

func assertRequestLogIndexes(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'request_logs'`)
	if err != nil {
		t.Fatalf("list request-log indexes: %v", err)
	}
	defer rows.Close()

	indexes := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan request-log index: %v", err)
		}
		indexes[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate request-log indexes: %v", err)
	}

	for _, name := range []string{
		"idx_request_logs_ts_id",
		"idx_request_logs_proxy_type_ts_id",
		"idx_request_logs_account_ts_id",
		"idx_request_logs_platform_name",
		"idx_request_logs_plat_acct",
		"idx_request_logs_target_host",
		"idx_request_logs_egress_ip",
	} {
		if !indexes[name] {
			t.Errorf("expected index %q", name)
		}
	}
	for _, name := range []string{
		"idx_request_logs_ts_ns",
		"idx_request_logs_proxy_type",
		"idx_request_logs_platform_id",
	} {
		if indexes[name] {
			t.Errorf("obsolete index %q still exists", name)
		}
	}
}

func assertQueryPlanUsesIndex(t *testing.T, db *sql.DB, indexName, query string, args ...any) {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("explain query plan: %v", err)
	}
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		plan.WriteString(detail)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate query plan: %v", err)
	}
	if !strings.Contains(plan.String(), indexName) {
		t.Fatalf("query plan does not use %s:\n%s", indexName, plan.String())
	}
	if strings.Contains(plan.String(), "USE TEMP B-TREE FOR ORDER BY") {
		t.Fatalf("query plan still sorts with a temporary B-tree:\n%s", plan.String())
	}
}

func TestRepo_ListAcrossDBsUsesGlobalTsOrdering(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	// Insert a newer timestamp into the first DB file.
	if _, err := repo.InsertBatch([]proxy.RequestLogEntry{{
		ID:          "old-file-new-ts",
		StartedAtNs: 200,
		ProxyType:   proxy.ProxyTypeForward,
	}}); err != nil {
		t.Fatalf("insert first db row: %v", err)
	}

	// Rotate and insert an older timestamp into the newer DB file.
	if err := repo.rotateDB(); err != nil {
		t.Fatalf("rotateDB: %v", err)
	}
	if _, err := repo.InsertBatch([]proxy.RequestLogEntry{{
		ID:          "new-file-old-ts",
		StartedAtNs: 100,
		ProxyType:   proxy.ProxyTypeForward,
	}}); err != nil {
		t.Fatalf("insert second db row: %v", err)
	}

	rows, hasMore, nextCursor, err := repo.List(ListFilter{Limit: 1})
	if err != nil {
		t.Fatalf("repo.List: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows len: got %d, want 1", len(rows))
	}
	if !hasMore {
		t.Fatalf("hasMore: got false, want true")
	}
	if nextCursor == nil {
		t.Fatal("nextCursor: got nil, want non-nil")
	}
	if rows[0].ID != "old-file-new-ts" {
		t.Fatalf("top row id: got %q, want %q", rows[0].ID, "old-file-new-ts")
	}
}

func TestRepo_ListCursorPagination(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	// Same ts to verify id ASC tie-break within ts.
	rows := []proxy.RequestLogEntry{
		{ID: "a", StartedAtNs: 300, ProxyType: proxy.ProxyTypeForward},
		{ID: "b", StartedAtNs: 300, ProxyType: proxy.ProxyTypeForward},
		{ID: "c", StartedAtNs: 200, ProxyType: proxy.ProxyTypeForward},
	}
	if _, err := repo.InsertBatch(rows); err != nil {
		t.Fatalf("repo.InsertBatch: %v", err)
	}

	page1, hasMore1, next1, err := repo.List(ListFilter{Limit: 2})
	if err != nil {
		t.Fatalf("repo.List page1: %v", err)
	}
	if len(page1) != 2 || page1[0].ID != "a" || page1[1].ID != "b" {
		t.Fatalf("page1 rows: got %+v", page1)
	}
	if !hasMore1 || next1 == nil {
		t.Fatalf("page1 pagination: hasMore=%v next=%+v", hasMore1, next1)
	}

	page2, hasMore2, next2, err := repo.List(ListFilter{
		Limit:  2,
		Cursor: next1,
	})
	if err != nil {
		t.Fatalf("repo.List page2: %v", err)
	}
	if len(page2) != 1 || page2[0].ID != "c" {
		t.Fatalf("page2 rows: got %+v", page2)
	}
	if hasMore2 {
		t.Fatalf("page2 hasMore: got true, want false")
	}
	if next2 != nil {
		t.Fatalf("page2 next: got %+v, want nil", next2)
	}
}

func TestRepo_MaybeRotateCountsWalAndShmSize(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1024, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	// Make base DB tiny but WAL large enough to cross threshold.
	if err := os.WriteFile(repo.activePath+"-wal", make([]byte, 1500), 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}

	before := repo.activePath
	if err := repo.maybeRotate(); err != nil {
		t.Fatalf("repo.maybeRotate: %v", err)
	}
	if repo.activePath == before {
		t.Fatal("expected rotation when wal size exceeds threshold")
	}
}

func TestRepo_InsertBatchRecoversAfterActiveDBLost(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	if repo.activeDB == nil || repo.activePath == "" {
		t.Fatalf("repo should have active db after open")
	}

	// Simulate a failed rotation aftermath:
	// old DB handle is gone, but activePath still points to the old DB file.
	if err := repo.activeDB.Close(); err != nil {
		t.Fatalf("close active db: %v", err)
	}
	repo.activeDB = nil

	inserted, err := repo.InsertBatch([]proxy.RequestLogEntry{{
		ID:          "recovered-insert",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
	}})
	if err != nil {
		t.Fatalf("repo.InsertBatch recover path: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("inserted: got %d, want 1", inserted)
	}

	row, err := repo.GetByID("recovered-insert")
	if err != nil {
		t.Fatalf("repo.GetByID: %v", err)
	}
	if row == nil {
		t.Fatal("expected inserted row after recovery")
	}
}

func TestRepo_InsertBatchWithoutOpenReturnsNoActiveDB(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	_, err := repo.InsertBatch([]proxy.RequestLogEntry{{
		ID:          "without-open",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
	}})
	if err == nil {
		t.Fatal("expected error when InsertBatch is called before Open")
	}
	if !strings.Contains(err.Error(), "no active db") {
		t.Fatalf("unexpected error: %v", err)
	}
}
