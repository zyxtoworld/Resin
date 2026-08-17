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
