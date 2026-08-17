package service

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
)

func TestUpsertAccountHeaderRule_PublishesCandidateWhenReloadListFailsAfterPersist(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	var closeOnce sync.Once
	closeDB := func() {
		closeOnce.Do(func() { _ = closer.Close() })
	}
	t.Cleanup(closeDB)

	oldRule := model.AccountHeaderRule{
		URLPrefix:   "old.example.com",
		Headers:     []string{"X-Old"},
		UpdatedAtNs: time.Now().UnixNano(),
	}
	if _, err := engine.UpsertAccountHeaderRuleWithCreated(oldRule); err != nil {
		t.Fatalf("seed old rule: %v", err)
	}
	loaded, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("load old rules: %v", err)
	}
	runtime := proxy.NewAccountMatcherRuntime(proxy.BuildAccountMatcher(loaded))
	cp := &ControlPlaneService{Engine: engine, MatcherRuntime: runtime}
	var persisted atomic.Bool
	closeDone := make(chan struct{})
	cp.ruleMutationHook = func(stage ruleMutationStage) {
		if stage == ruleMutationAfterPersist {
			persisted.Store(true)
			go func() {
				closeDB()
				close(closeDone)
			}()
		}
	}

	createdRule, created, err := cp.UpsertAccountHeaderRule("new.example.com", []string{"X-New"})
	if err != nil {
		t.Fatalf("UpsertAccountHeaderRule: %v", err)
	}
	if !created || createdRule == nil || createdRule.URLPrefix != "new.example.com" {
		t.Fatalf("unexpected upsert response: created=%v rule=%+v", created, createdRule)
	}
	if !persisted.Load() {
		t.Fatal("test did not reach the post-persist failure injection")
	}
	<-closeDone

	prefix, headers := runtime.MatchWithPrefix("new.example.com", "/")
	if prefix != "new.example.com" || len(headers) != 1 || headers[0] != "X-New" {
		t.Fatalf("runtime matcher did not publish the persisted candidate: prefix=%q headers=%v", prefix, headers)
	}
}

func TestUpsertAccountHeaderRule_StateWriteAdmissionCoversMatcherPublish(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	runtime := proxy.NewAccountMatcherRuntime(nil)
	cp := &ControlPlaneService{Engine: engine, MatcherRuntime: runtime}
	persisted := make(chan struct{})
	allowPublish := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(allowPublish) }) })
	cp.ruleMutationHook = func(stage ruleMutationStage) {
		if stage == ruleMutationAfterPersist {
			close(persisted)
			<-allowPublish
		}
	}

	upsertDone := make(chan error, 1)
	go func() {
		_, _, upsertErr := cp.UpsertAccountHeaderRule("shutdown.example.com", []string{"X-Shutdown"})
		upsertDone <- upsertErr
	}()
	select {
	case <-persisted:
	case <-time.After(time.Second):
		t.Fatal("rule upsert did not reach the post-persist boundary")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- engine.CloseStateWriteAdmissionAndWait(closeCtx) }()
	select {
	case err := <-closeDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown crossed the admitted rule publish: got %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown wait did not honor its deadline")
	}
	if prefix, headers := runtime.MatchWithPrefix("shutdown.example.com", "/"); prefix != "" || headers != nil {
		t.Fatalf("matcher published before the post-persist gate was released: prefix=%q headers=%v", prefix, headers)
	}

	releaseOnce.Do(func() { close(allowPublish) })
	if err := <-upsertDone; err != nil {
		t.Fatalf("UpsertAccountHeaderRule: %v", err)
	}
	if err := engine.CloseStateWriteAdmissionAndWait(context.Background()); err != nil {
		t.Fatalf("final CloseStateWriteAdmissionAndWait: %v", err)
	}
	if prefix, headers := runtime.MatchWithPrefix("shutdown.example.com", "/"); prefix != "shutdown.example.com" || len(headers) != 1 || headers[0] != "X-Shutdown" {
		t.Fatalf("matcher did not publish after the admitted mutation completed: prefix=%q headers=%v", prefix, headers)
	}
}

func TestDeleteAccountHeaderRule_PublishesCandidateWhenReloadListFailsAfterPersist(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	var closeOnce sync.Once
	closeDB := func() {
		closeOnce.Do(func() { _ = closer.Close() })
	}
	t.Cleanup(closeDB)

	deletedRule := model.AccountHeaderRule{
		URLPrefix:   "delete.example.com",
		Headers:     []string{"X-Delete"},
		UpdatedAtNs: time.Now().UnixNano(),
	}
	keptRule := model.AccountHeaderRule{
		URLPrefix:   "keep.example.com",
		Headers:     []string{"X-Keep"},
		UpdatedAtNs: time.Now().UnixNano(),
	}
	for _, rule := range []model.AccountHeaderRule{deletedRule, keptRule} {
		if _, err := engine.UpsertAccountHeaderRuleWithCreated(rule); err != nil {
			t.Fatalf("seed rule %q: %v", rule.URLPrefix, err)
		}
	}
	loaded, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("load rules: %v", err)
	}
	runtime := proxy.NewAccountMatcherRuntime(proxy.BuildAccountMatcher(loaded))
	cp := &ControlPlaneService{Engine: engine, MatcherRuntime: runtime}
	var persisted atomic.Bool
	closeDone := make(chan struct{})
	cp.ruleMutationHook = func(stage ruleMutationStage) {
		if stage == ruleMutationAfterPersist {
			persisted.Store(true)
			go func() {
				closeDB()
				close(closeDone)
			}()
		}
	}

	if err := cp.DeleteAccountHeaderRule(deletedRule.URLPrefix); err != nil {
		t.Fatalf("DeleteAccountHeaderRule: %v", err)
	}
	if !persisted.Load() {
		t.Fatal("test did not reach the post-persist failure injection")
	}
	<-closeDone

	prefix, headers := runtime.MatchWithPrefix(deletedRule.URLPrefix, "/")
	if prefix != "" || headers != nil {
		t.Fatalf("runtime matcher retained the deleted rule: prefix=%q headers=%v", prefix, headers)
	}
	prefix, headers = runtime.MatchWithPrefix(keptRule.URLPrefix, "/")
	if prefix != keptRule.URLPrefix || len(headers) != 1 || headers[0] != keptRule.Headers[0] {
		t.Fatalf("runtime matcher lost the retained rule: prefix=%q headers=%v", prefix, headers)
	}
}

func TestUpsertAccountHeaderRule_PreservesFallbackCandidate(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	fallback := model.AccountHeaderRule{
		URLPrefix:   "*",
		Headers:     []string{"Authorization", "x-api-key"},
		UpdatedAtNs: time.Now().UnixNano(),
	}
	if _, err := engine.UpsertAccountHeaderRuleWithCreated(fallback); err != nil {
		t.Fatalf("seed fallback rule: %v", err)
	}
	loaded, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("load fallback rule: %v", err)
	}
	runtime := proxy.NewAccountMatcherRuntime(proxy.BuildAccountMatcher(loaded))
	cp := &ControlPlaneService{Engine: engine, MatcherRuntime: runtime}
	if _, _, err := cp.UpsertAccountHeaderRule("api.example.com", []string{"X-Account"}); err != nil {
		t.Fatalf("upsert alongside fallback: %v", err)
	}

	prefix, headers := runtime.MatchWithPrefix("unknown.example.com", "/")
	if prefix != fallback.URLPrefix || len(headers) != len(fallback.Headers) || headers[0] != "Authorization" || headers[1] != "x-api-key" {
		t.Fatalf("fallback was lost after upsert: prefix=%q headers=%v", prefix, headers)
	}
}

func TestAccountHeaderRuleMutationsSerializeSnapshotAndPublish(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	seedRules := []model.AccountHeaderRule{
		{URLPrefix: "first.example.com", Headers: []string{"X-Old-First"}, UpdatedAtNs: time.Now().UnixNano()},
		{URLPrefix: "second.example.com", Headers: []string{"X-Second"}, UpdatedAtNs: time.Now().UnixNano()},
	}
	for _, rule := range seedRules {
		if _, err := engine.UpsertAccountHeaderRuleWithCreated(rule); err != nil {
			t.Fatalf("seed rule %q: %v", rule.URLPrefix, err)
		}
	}
	loaded, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("load seeded rules: %v", err)
	}
	runtime := proxy.NewAccountMatcherRuntime(proxy.BuildAccountMatcher(loaded))
	cp := &ControlPlaneService{Engine: engine, MatcherRuntime: runtime}
	firstSnapshot := make(chan struct{})
	secondBeforeLock := make(chan struct{})
	secondSnapshot := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	var beforeLockCalls atomic.Int32
	var snapshotCalls atomic.Int32
	cp.ruleMutationHook = func(stage ruleMutationStage) {
		switch stage {
		case ruleMutationBeforeLock:
			if beforeLockCalls.Add(1) == 2 {
				close(secondBeforeLock)
			}
		case ruleMutationAfterSnapshot:
			switch snapshotCalls.Add(1) {
			case 1:
				close(firstSnapshot)
				<-releaseFirst
			case 2:
				close(secondSnapshot)
				<-releaseSecond
			}
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	startMutation := func(mutation func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- mutation()
		}()
	}

	startMutation(func() error {
		_, _, err := cp.UpsertAccountHeaderRule("first.example.com", []string{"X-First"})
		return err
	})
	select {
	case <-firstSnapshot:
	case <-time.After(time.Second):
		t.Fatal("first rule mutation did not reach its snapshot")
	}
	startMutation(func() error {
		return cp.DeleteAccountHeaderRule("second.example.com")
	})
	select {
	case <-secondBeforeLock:
	case <-time.After(time.Second):
		t.Fatal("second rule mutation did not reach beforeLock")
	}

	reachedSecondSnapshotBeforeFirstRelease := false
	select {
	case <-secondSnapshot:
		reachedSecondSnapshotBeforeFirstRelease = true
	case <-time.After(time.Second):
	}

	// Release the second snapshot first, then the first. This order makes the
	// old reload-after-write path publish its older first snapshot last.
	close(releaseSecond)
	close(releaseFirst)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("UpsertAccountHeaderRule: %v", err)
		}
	}
	if reachedSecondSnapshotBeforeFirstRelease {
		t.Fatal("second rule mutation reached its snapshot while the first mutation was held")
	}

	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("ListAccountHeaderRules: %v", err)
	}
	if len(rules) != 1 || rules[0].URLPrefix != "first.example.com" || len(rules[0].Headers) != 1 || rules[0].Headers[0] != "X-First" {
		t.Fatalf("persisted rules after upsert/delete = %+v, want only updated first rule", rules)
	}
	prefix, headers := runtime.MatchWithPrefix("first.example.com", "/")
	if prefix != "first.example.com" || len(headers) != 1 || headers[0] != "X-First" {
		t.Fatalf("runtime updated first rule = prefix=%q headers=%v, want updated first rule", prefix, headers)
	}
	prefix, headers = runtime.MatchWithPrefix("second.example.com", "/")
	if prefix != "" || headers != nil {
		t.Fatalf("runtime retained deleted second rule: prefix=%q headers=%v", prefix, headers)
	}
}

func TestAccountHeaderRuleMutationContextCancellationWhileRuleMutexHeld(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	runtime := proxy.NewAccountMatcherRuntime(nil)
	cp := &ControlPlaneService{Engine: engine, MatcherRuntime: runtime}
	firstSnapshot := make(chan struct{})
	secondBeforeLock := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	var beforeLockCalls atomic.Int32
	var snapshotCalls atomic.Int32
	cp.ruleMutationHook = func(stage ruleMutationStage) {
		switch stage {
		case ruleMutationBeforeLock:
			if beforeLockCalls.Add(1) == 2 {
				close(secondBeforeLock)
			}
		case ruleMutationAfterSnapshot:
			if snapshotCalls.Add(1) == 1 {
				close(firstSnapshot)
				<-releaseFirst
			}
		}
	}

	firstDone := make(chan error, 1)
	go func() {
		_, _, err := cp.UpsertAccountHeaderRule("first.example.com", []string{"X-First"})
		firstDone <- err
	}()
	select {
	case <-firstSnapshot:
	case <-time.After(time.Second):
		t.Fatal("first rule mutation did not hold rule mutex at snapshot")
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, _, err := cp.UpsertAccountHeaderRuleContext(secondCtx, "second.example.com", []string{"X-Second"})
		secondDone <- err
	}()
	select {
	case <-secondBeforeLock:
	case <-time.After(time.Second):
		cancelSecond()
		releaseOnce.Do(func() { close(releaseFirst) })
		<-firstDone
		t.Fatal("second rule mutation did not reach the lock boundary")
	}
	cancelSecond()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled rule mutation error = %v, want context.Canceled", err)
		}
	case <-time.After(200 * time.Millisecond):
		releaseOnce.Do(func() { close(releaseFirst) })
		<-firstDone
		t.Fatal("canceled rule mutation remained blocked by rule mutex")
	}

	releaseOnce.Do(func() { close(releaseFirst) })
	if err := <-firstDone; err != nil {
		t.Fatalf("first rule mutation: %v", err)
	}

	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("ListAccountHeaderRules: %v", err)
	}
	if len(rules) != 1 || rules[0].URLPrefix != "first.example.com" {
		t.Fatalf("canceled rule mutation changed persisted rules: %+v", rules)
	}
	if prefix, headers := runtime.MatchWithPrefix("first.example.com", "/"); prefix != "first.example.com" || len(headers) != 1 || headers[0] != "X-First" {
		t.Fatalf("first rule was not published: prefix=%q headers=%v", prefix, headers)
	}
	if prefix, headers := runtime.MatchWithPrefix("second.example.com", "/"); prefix != "" || headers != nil {
		t.Fatalf("canceled rule was published: prefix=%q headers=%v", prefix, headers)
	}
}

func TestUpsertAccountHeaderRuleContextCancellationInterruptsSnapshotRead(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	engine, closer, err := state.PersistenceBootstrap(stateDir, filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	blocker, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_, _ = blocker.Exec("ROLLBACK")
		_ = blocker.Close()
	}
	t.Cleanup(release)
	if _, err := blocker.Exec("BEGIN EXCLUSIVE"); err != nil {
		t.Fatalf("BEGIN EXCLUSIVE: %v", err)
	}

	cp := &ControlPlaneService{
		Engine:         engine,
		MatcherRuntime: proxy.NewAccountMatcherRuntime(nil),
	}
	snapshotStarted := make(chan struct{})
	cp.ruleMutationHook = func(stage ruleMutationStage) {
		if stage == ruleMutationBeforeSnapshot {
			close(snapshotStarted)
		}
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := cp.UpsertAccountHeaderRuleContext(requestCtx, "snapshot-cancel.example.com", []string{"X-Snapshot"})
		done <- err
	}()
	select {
	case <-snapshotStarted:
	case <-time.After(time.Second):
		t.Fatal("rule mutation did not acquire ruleMu before snapshot read")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("snapshot read cancellation error = %v, want context.Canceled", err)
		}
	case <-time.After(200 * time.Millisecond):
		release()
		<-done
		t.Fatal("canceled rule mutation remained blocked in the snapshot read")
	}
}

func TestAccountHeaderRuleMutationCommitsAfterRequestCancellationOnceRuleLocked(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	engine, closer, err := state.PersistenceBootstrap(stateDir, filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	oldRule := model.AccountHeaderRule{
		URLPrefix:   "old.example.com",
		Headers:     []string{"X-Old"},
		UpdatedAtNs: time.Now().UnixNano(),
	}
	if _, err := engine.UpsertAccountHeaderRuleWithCreated(oldRule); err != nil {
		t.Fatalf("seed old rule: %v", err)
	}

	blocker, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB blocker: %v", err)
	}
	tx, err := blocker.Begin()
	if err != nil {
		_ = blocker.Close()
		t.Fatalf("blocker Begin: %v", err)
	}
	if _, err := tx.Exec("UPDATE account_header_rules SET updated_at_ns = updated_at_ns WHERE url_prefix = ?", oldRule.URLPrefix); err != nil {
		_ = tx.Rollback()
		_ = blocker.Close()
		t.Fatalf("hold account-header SQLite gate: %v", err)
	}
	releasedDB := false
	releaseDB := func() {
		if releasedDB {
			return
		}
		releasedDB = true
		_ = tx.Rollback()
		_ = blocker.Close()
	}
	defer releaseDB()

	runtime := proxy.NewAccountMatcherRuntime(proxy.BuildAccountMatcher([]model.AccountHeaderRule{oldRule}))
	cp := &ControlPlaneService{Engine: engine, MatcherRuntime: runtime}
	atSnapshot := make(chan struct{})
	allowPersist := make(chan struct{})
	var allowOnce sync.Once
	t.Cleanup(func() { allowOnce.Do(func() { close(allowPersist) }) })
	cp.ruleMutationHook = func(stage ruleMutationStage) {
		if stage == ruleMutationAfterSnapshot {
			close(atSnapshot)
			<-allowPersist
		}
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		response *RuleResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, _, err := cp.UpsertAccountHeaderRuleContext(requestCtx, "new.example.com", []string{"X-New"})
		done <- result{response: response, err: err}
	}()
	select {
	case <-atSnapshot:
	case <-time.After(time.Second):
		releaseDB()
		allowOnce.Do(func() { close(allowPersist) })
		t.Fatal("rule mutation did not reach the post-snapshot commit boundary")
	}

	cancel()
	allowOnce.Do(func() { close(allowPersist) })
	releaseDB()

	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("rule mutation was canceled after lock acquisition: %v", result.err)
		}
		if result.response == nil || result.response.URLPrefix != "new.example.com" {
			t.Fatalf("committed rule response = %+v", result.response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rule mutation did not finish after SQLite gate release")
	}

	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("ListAccountHeaderRules: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("persisted rules after request cancellation = %+v, want two committed rules", rules)
	}
	if prefix, headers := runtime.MatchWithPrefix("new.example.com", "/"); prefix != "new.example.com" || len(headers) != 1 || headers[0] != "X-New" {
		t.Fatalf("matcher did not publish committed rule: prefix=%q headers=%v", prefix, headers)
	}
}
