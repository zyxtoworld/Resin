package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
)

type cancelBeforeEOFBody struct {
	data       []byte
	beforeEOF  chan struct{}
	allowEOF   <-chan struct{}
	beforeOnce sync.Once
}

func (b *cancelBeforeEOFBody) Read(p []byte) (int, error) {
	if len(b.data) != 0 {
		n := copy(p, b.data)
		b.data = b.data[n:]
		return n, nil
	}
	b.beforeOnce.Do(func() { close(b.beforeEOF) })
	<-b.allowEOF
	return 0, io.EOF
}

func (b *cancelBeforeEOFBody) Close() error { return nil }

func TestAccountHeaderRuleHandlerStopsOnCanceledRequestBeforeRuleAdmission(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	cp := &service.ControlPlaneService{Engine: engine}
	if _, _, err := cp.UpsertAccountHeaderRule("api.example.com/v1", []string{"old-header"}); err != nil {
		t.Fatalf("seed account header rule: %v", err)
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
	if _, err := tx.Exec("UPDATE account_header_rules SET updated_at_ns = updated_at_ns WHERE url_prefix = ?", "api.example.com/v1"); err != nil {
		_ = tx.Rollback()
		_ = blocker.Close()
		t.Fatalf("hold account-header write lock: %v", err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_ = tx.Rollback()
		_ = blocker.Close()
	}
	defer release()

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	beforeEOF := make(chan struct{})
	allowEOF := make(chan struct{})
	req := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/account-header-rules/api.example.com%2Fv1",
		&cancelBeforeEOFBody{
			data:      []byte(`{"headers":["new-header"]}`),
			beforeEOF: beforeEOF,
			allowEOF:  allowEOF,
		},
	).WithContext(requestCtx)
	req.SetPathValue("prefix", "api.example.com/v1")
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		HandleUpsertRule(cp).ServeHTTP(rec, req)
		close(handlerDone)
	}()

	select {
	case <-beforeEOF:
	case <-time.After(time.Second):
		release()
		t.Fatal("handler did not finish reading request body")
	}
	cancel()
	close(allowEOF)

	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		release()
		<-handlerDone
		t.Fatal("cancelled account-header handler remained blocked on SQLite write lock")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("cancelled account-header handler status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	release()
	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("ListAccountHeaderRules: %v", err)
	}
	if len(rules) != 1 || len(rules[0].Headers) != 1 || rules[0].Headers[0] != "old-header" {
		t.Fatalf("cancelled handler changed persisted rule: %+v", rules)
	}
}
