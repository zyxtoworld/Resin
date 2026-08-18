package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/api"
	"github.com/Resinat/Resin/internal/topology"
)

func TestHealthzDoesNotWaitForRuntimeWriter(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	app := &resinApp{topoRuntime: &topologyRuntime{pool: pool}}

	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		pool.WithRuntimeMutation(func() {
			close(writerEntered)
			<-releaseWriter
		})
		close(writerDone)
	}()
	<-writerEntered

	handler := api.HandleHealthzWithStatus(app.healthzStatus)
	handlerStarted := make(chan struct{})
	healthzDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		close(handlerStarted)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		healthzDone <- rec
	}()
	<-handlerStarted

	var rec *httptest.ResponseRecorder
	select {
	case rec = <-healthzDone:
	case <-time.After(300 * time.Millisecond):
		close(releaseWriter)
		<-writerDone
		t.Fatal("healthz waited for the runtime writer")
	}

	close(releaseWriter)
	<-writerDone
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want %d", rec.Code, http.StatusOK)
	}
	var status api.HealthzStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode healthz response: %v", err)
	}
	if status.Status != "updating" {
		t.Fatalf("healthz status = %q, want updating while runtime writer is busy", status.Status)
	}
}
