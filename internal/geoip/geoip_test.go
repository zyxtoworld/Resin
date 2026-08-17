package geoip

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockReader is a test GeoReader that returns a fixed country.
type mockReader struct {
	country string
	closed  bool
	mu      sync.Mutex
}

func (m *mockReader) Lookup(_ netip.Addr) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.country
}

func (m *mockReader) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockReader) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

type blockingCloseReader struct {
	closeEntered chan struct{}
	allowClose   <-chan struct{}

	mu         sync.Mutex
	closeCount int
	closeOnce  sync.Once
}

func (r *blockingCloseReader) Lookup(_ netip.Addr) string { return "us" }

func (r *blockingCloseReader) Close() error {
	r.mu.Lock()
	r.closeCount++
	r.mu.Unlock()
	r.closeOnce.Do(func() { close(r.closeEntered) })
	<-r.allowClose
	return nil
}

func (r *blockingCloseReader) closes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeCount
}

type trackingReader struct {
	inner  GeoReader
	mu     sync.Mutex
	closed bool
}

func (r *trackingReader) Lookup(ip netip.Addr) string { return r.inner.Lookup(ip) }

func (r *trackingReader) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return r.inner.Close()
}

func (r *trackingReader) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func minimalMMDB(databaseType string) []byte {
	appendString := func(dst []byte, value string) []byte {
		dst = append(dst, byte(0x40|len(value)))
		return append(dst, value...)
	}
	appendUint := func(dst []byte, value byte) []byte {
		return append(dst, 0xc1, value)
	}
	metadata := []byte{0xe4}
	metadata = appendString(metadata, "node_count")
	metadata = appendUint(metadata, 0)
	metadata = appendString(metadata, "record_size")
	metadata = appendUint(metadata, 24)
	metadata = appendString(metadata, "ip_version")
	metadata = appendUint(metadata, 4)
	metadata = appendString(metadata, "database_type")
	metadata = appendString(metadata, databaseType)
	database := make([]byte, 16)
	database = append(database, 0xab, 0xcd, 0xef)
	database = append(database, []byte("MaxMind.com")...)
	return append(database, metadata...)
}

// --- Existing tests ---

func TestGeoIP_Lookup_NilReader(t *testing.T) {
	s := &Service{}
	if got := s.Lookup(netip.MustParseAddr("1.2.3.4")); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestNewService_Defaults(t *testing.T) {
	s := NewService(ServiceConfig{
		CacheDir: t.TempDir(),
		OpenDB:   NoOpOpen,
	})
	defer s.Stop()

	if s.dbFilename != "country.mmdb" {
		t.Fatalf("dbFilename = %q, want %q", s.dbFilename, "country.mmdb")
	}

	entry := s.cron.Entry(s.cronEntryID)
	if entry.ID == 0 || entry.Schedule == nil {
		t.Fatal("default cron entry is not configured")
	}

	base := time.Date(2026, 1, 2, 6, 30, 0, 0, time.Local)
	next := entry.Schedule.Next(base)
	want := time.Date(2026, 1, 2, 7, 0, 0, 0, time.Local)
	if !next.Equal(want) {
		t.Fatalf("next schedule = %v, want %v", next, want)
	}
}

func TestGeoIP_ReloadReader(t *testing.T) {
	old := &mockReader{country: "us"}
	s := &Service{reader: old}

	newReader := &mockReader{country: "jp"}
	s.openDB = func(path string) (GeoReader, error) { return newReader, nil }

	if err := s.reloadReader("/fake/path"); err != nil {
		t.Fatal(err)
	}

	if got := s.Lookup(netip.Addr{}); got != "jp" {
		t.Fatalf("expected jp, got %q", got)
	}
	if !old.isClosed() {
		t.Fatal("old reader should be closed")
	}
}

func TestGeoIP_Stop_ClosesReader(t *testing.T) {
	r := &mockReader{country: "cn"}
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	s := &Service{
		reader:     r,
		cron:       nil, // no cron for this test
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
	}
	s.Stop()

	if !r.isClosed() {
		t.Fatal("reader should be closed after stop")
	}
	if got := s.Lookup(netip.Addr{}); got != "" {
		t.Fatalf("expected empty after stop, got %q", got)
	}
}

func TestGeoIPStopContextHonorsDeadlineDuringReaderClose(t *testing.T) {
	allowClose := make(chan struct{})
	r := &blockingCloseReader{
		closeEntered: make(chan struct{}),
		allowClose:   allowClose,
	}
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	s := &Service{
		reader:     r,
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- s.StopContext(stopCtx) }()

	select {
	case <-r.closeEntered:
	case <-time.After(time.Second):
		close(allowClose)
		t.Fatal("StopContext did not enter reader Close")
	}

	select {
	case err := <-stopDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext error = %v, want context deadline exceeded", err)
		}
	case <-time.After(time.Second):
		close(allowClose)
		t.Fatal("StopContext waited past its deadline for reader Close")
	}
	if got := s.Lookup(netip.MustParseAddr("1.2.3.4")); got != "" {
		t.Fatalf("reader remained visible after Stop admission: %q", got)
	}

	close(allowClose)
	secondStopDone := make(chan error, 1)
	go func() { secondStopDone <- s.StopContext(context.Background()) }()
	select {
	case err := <-secondStopDone:
		if err != nil {
			t.Fatalf("second StopContext: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second StopContext did not complete after reader Close was released")
	}
	if got := r.closes(); got != 1 {
		t.Fatalf("reader Close count = %d, want 1", got)
	}
}

func TestGeoIP_StartAfterStopIsRejected(t *testing.T) {
	s := NewService(ServiceConfig{
		CacheDir: t.TempDir(),
		OpenDB:   NoOpOpen,
	})

	s.Stop()
	err := s.Start()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Start after Stop error = %v, want context.Canceled", err)
	}
	if next := s.NextScheduledUpdate(); !next.IsZero() {
		t.Fatalf("Start after Stop restarted the cron scheduler, next=%v", next)
	}
}

func TestGeoIP_StopWaitsForConcurrentStartInitialization(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "geoip.db"), []byte("existing"), 0644); err != nil {
		t.Fatalf("write db: %v", err)
	}

	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	loaded := &mockReader{country: "us"}
	s := NewService(ServiceConfig{
		CacheDir:   dir,
		DBFilename: "geoip.db",
		OpenDB: func(string) (GeoReader, error) {
			close(openStarted)
			<-releaseOpen
			return loaded, nil
		},
	})

	startDone := make(chan error, 1)
	go func() { startDone <- s.Start() }()
	select {
	case <-openStarted:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach initial reader load")
	}

	stopDone := make(chan struct{})
	go func() {
		s.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("Stop returned before concurrent Start initialization completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseOpen)
	if err := <-startDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want context.Canceled after Stop", err)
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after Start initialization was released")
	}
	if !loaded.isClosed() {
		t.Fatal("reader loaded by canceled Start was not closed by Stop")
	}
}

func TestGeoIP_StopContextCancelsConcurrentStartInitialization(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "geoip.db"), []byte("existing"), 0644); err != nil {
		t.Fatalf("write db: %v", err)
	}

	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	loaded := &mockReader{country: "us"}
	s := NewService(ServiceConfig{
		CacheDir:   dir,
		DBFilename: "geoip.db",
		OpenDB: func(string) (GeoReader, error) {
			close(openStarted)
			<-releaseOpen
			return loaded, nil
		},
	})

	startDone := make(chan error, 1)
	go func() { startDone <- s.Start() }()
	select {
	case <-openStarted:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach initial reader load")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- s.StopContext(stopCtx) }()
	select {
	case err := <-stopDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext error = %v, want context deadline exceeded", err)
		}
	case <-time.After(time.Second):
		close(releaseOpen)
		<-startDone
		t.Fatal("StopContext did not honor its deadline while Start was blocked")
	}

	close(releaseOpen)
	if err := <-startDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want context.Canceled", err)
	}
	if !loaded.isClosed() {
		t.Fatal("reader prepared by canceled Start was not closed")
	}
	if got := s.Lookup(netip.MustParseAddr("1.2.3.4")); got != "" {
		t.Fatalf("canceled Start published reader country %q", got)
	}
	if next := s.NextScheduledUpdate(); !next.IsZero() {
		t.Fatalf("canceled Start started cron, next=%v", next)
	}
}

func TestGeoIP_ConcurrentLookupDuringReload(t *testing.T) {
	initial := &mockReader{country: "us"}
	s := &Service{reader: initial}
	s.openDB = func(path string) (GeoReader, error) {
		return &mockReader{country: "jp"}, nil
	}

	var wg sync.WaitGroup
	// Concurrent lookups.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := s.Lookup(netip.MustParseAddr("1.2.3.4"))
			if got != "us" && got != "jp" {
				t.Errorf("unexpected country: %q", got)
			}
		}()
	}

	// Concurrent reload.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.reloadReader("/fake")
	}()

	wg.Wait()
}

func TestMMDBOpenKeepsLiveFileReplaceable(t *testing.T) {
	dir := t.TempDir()
	livePath := filepath.Join(dir, "geoip.db")
	if err := os.WriteFile(livePath, minimalMMDB("old"), 0644); err != nil {
		t.Fatalf("write old database: %v", err)
	}
	oldReader, err := MMDBOpen(livePath)
	if err != nil {
		t.Fatalf("MMDBOpen old database: %v", err)
	}
	defer oldReader.Close()
	newPath := filepath.Join(dir, "geoip.db.new")
	if err := os.WriteFile(newPath, minimalMMDB("new"), 0644); err != nil {
		t.Fatalf("write new database: %v", err)
	}
	if err := os.Rename(newPath, livePath); err != nil {
		t.Fatalf("replace live database while reader is active: %v", err)
	}
	newReader, err := MMDBOpen(livePath)
	if err != nil {
		t.Fatalf("MMDBOpen new database: %v", err)
	}
	defer newReader.Close()
}

func TestMMDBOpenRejectsOversizedDatabase(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "geoip.db")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create oversized database: %v", err)
	}
	if err := file.Truncate(int64(maxGeoIPDatabaseBytes) + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate oversized database: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close oversized database: %v", err)
	}
	if _, err := MMDBOpen(path); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("MMDBOpen oversized database error = %v, want size error", err)
	}
}

func TestUpdateNow_MMDBOpenKeepsOldReaderUntilSwap(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "geoip.db")
	oldBytes := minimalMMDB("old")
	newBytes := minimalMMDB("new")
	if err := os.WriteFile(dbPath, oldBytes, 0644); err != nil {
		t.Fatalf("write old database: %v", err)
	}
	oldInner, err := MMDBOpen(dbPath)
	if err != nil {
		t.Fatalf("MMDBOpen old database: %v", err)
	}
	oldReader := &trackingReader{inner: oldInner}

	hash := sha256.Sum256(newBytes)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	releaseJSON, err := json.Marshal(releaseInfo{
		TagName: "v-mmdb-memory-reader",
		Assets: []releaseAsset{{
			Name:               "geoip.db",
			Digest:             &digest,
			BrowserDownloadURL: "https://example.com/geoip.db",
		}},
	})
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}
	dl := &mockDownloader{responses: map[string][]byte{
		ReleaseAPIURL:                  releaseJSON,
		"https://example.com/geoip.db": newBytes,
	}}
	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		openDB:     MMDBOpen,
		reader:     oldReader,
	}
	commitEntered := make(chan struct{})
	allowCommit := make(chan struct{})
	s.beforeGeoIPCommitHook = func() {
		if oldReader.isClosed() {
			t.Error("old reader closed before atomic swap")
		}
		close(commitEntered)
		<-allowCommit
	}
	updateDone := make(chan error, 1)
	go func() { updateDone <- s.UpdateNow() }()
	select {
	case <-commitEntered:
	case <-time.After(time.Second):
		t.Fatal("UpdateNow did not reach commit gate")
	}
	if oldReader.isClosed() {
		t.Fatal("old reader closed while update was waiting to commit")
	}
	close(allowCommit)
	if err := <-updateDone; err != nil {
		t.Fatalf("UpdateNow: %v", err)
	}
	if !oldReader.isClosed() {
		t.Fatal("old reader was not closed after successful swap")
	}
	if got, err := os.ReadFile(dbPath); err != nil {
		t.Fatalf("read published database: %v", err)
	} else if string(got) != string(newBytes) {
		t.Fatalf("published database = %q, want new generation", got)
	}
}

func TestVerifySHA256_Success(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")
	data := []byte("hello world")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	// SHA256("hello world") = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
	if err := VerifySHA256(path, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVerifySHA256_Failure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")
	if err := os.WriteFile(path, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := VerifySHA256(path, "0000000000000000000000000000000000000000000000000000000000000000"); err == nil {
		t.Fatal("expected SHA256 mismatch error")
	}
}

// --- New download chain tests ---

// mockDownloader records downloads and serves canned responses.
type mockDownloader struct {
	mu        sync.Mutex
	responses map[string][]byte
	calls     []string
}

func (d *mockDownloader) Download(_ context.Context, url string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, url)
	body, ok := d.responses[url]
	if !ok {
		return nil, fmt.Errorf("mock: not found: %s", url)
	}
	return body, nil
}

func TestGeoIPStopContextHonorsDeadlineForBlockedUpdate(t *testing.T) {
	downloader := &blockingDownloader{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	s := NewService(ServiceConfig{
		CacheDir:   t.TempDir(),
		OpenDB:     NoOpOpen,
		Downloader: downloader,
	})

	updateDone := make(chan error, 1)
	go func() { updateDone <- s.UpdateNow() }()
	select {
	case <-downloader.started:
	case <-time.After(time.Second):
		t.Fatal("UpdateNow did not enter the blocking downloader")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- s.StopContext(stopCtx) }()

	var stopErr error
	select {
	case stopErr = <-stopDone:
	case <-time.After(time.Second):
		close(downloader.release)
		<-updateDone
		t.Fatal("StopContext did not honor its deadline")
	}
	if !errors.Is(stopErr, context.DeadlineExceeded) {
		t.Fatalf("StopContext error = %v, want context deadline exceeded", stopErr)
	}

	close(downloader.release)
	if err := <-updateDone; err == nil {
		t.Fatal("UpdateNow after StopContext unexpectedly succeeded")
	}
}

func TestUpdateNow_DownloadVerifyReload(t *testing.T) {
	dir := t.TempDir()

	// Prepare fake database content.
	dbContent := []byte("fake-geoip-database-content")
	hash := sha256.Sum256(dbContent)
	hashHex := hex.EncodeToString(hash[:])
	digest := "sha256:" + hashHex

	// Build mock release JSON.
	release := releaseInfo{
		TagName: "v20240101",
		Assets: []releaseAsset{
			{Name: "geoip.db", Digest: &digest, BrowserDownloadURL: "https://example.com/geoip.db"},
		},
	}
	releaseJSON, _ := json.Marshal(release)

	dl := &mockDownloader{
		responses: map[string][]byte{
			ReleaseAPIURL:                  releaseJSON,
			"https://example.com/geoip.db": dbContent,
		},
	}

	var reloaded bool
	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		openDB: func(path string) (GeoReader, error) {
			reloaded = true
			return &mockReader{country: "us"}, nil
		},
	}

	if err := s.UpdateNow(); err != nil {
		t.Fatalf("UpdateNow: %v", err)
	}

	// Verify the file was written.
	dbPath := filepath.Join(dir, "geoip.db")
	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db: %v", err)
	}
	if string(data) != string(dbContent) {
		t.Fatal("database content mismatch")
	}

	// Verify reader was reloaded.
	if !reloaded {
		t.Fatal("reader was not reloaded after download")
	}

	// Verify lookup works.
	if got := s.Lookup(netip.MustParseAddr("1.2.3.4")); got != "us" {
		t.Fatalf("expected 'us', got %q", got)
	}
}

func TestGeoIPStopWinsBeforeAtomicReplace(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "geoip.db")
	original := []byte("original-db")
	if err := os.WriteFile(dbPath, original, 0o644); err != nil {
		t.Fatalf("write original db: %v", err)
	}

	newContent := []byte("new-db-content")
	hash := sha256.Sum256(newContent)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	releaseJSON, err := json.Marshal(releaseInfo{
		TagName: "v20240105",
		Assets: []releaseAsset{{
			Name:               "geoip.db",
			Digest:             &digest,
			BrowserDownloadURL: "https://example.com/geoip.db",
		}},
	})
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}

	dl := &mockDownloader{responses: map[string][]byte{
		ReleaseAPIURL:                  releaseJSON,
		"https://example.com/geoip.db": newContent,
	}}
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	oldReader := &mockReader{country: "us"}
	newReader := &mockReader{country: "jp"}
	commitEntered := make(chan struct{})
	allowCommit := make(chan struct{})
	stopAdmitted := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(allowCommit) }) }
	t.Cleanup(release)

	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		openDB: func(string) (GeoReader, error) {
			return newReader, nil
		},
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
		reader:     oldReader,
	}
	s.beforeGeoIPCommitHook = func() {
		close(commitEntered)
		<-allowCommit
	}
	s.afterGeoIPStopAdmissionHook = func() { close(stopAdmitted) }

	updateDone := make(chan error, 1)
	go func() { updateDone <- s.UpdateNow() }()
	select {
	case <-commitEntered:
	case <-time.After(time.Second):
		t.Fatal("UpdateNow did not reach the pre-commit gate")
	}

	stopDone := make(chan struct{})
	go func() {
		s.Stop()
		close(stopDone)
	}()
	select {
	case <-stopAdmitted:
	case <-time.After(time.Second):
		t.Fatal("Stop did not close GeoIP update admission")
	}

	release()
	if err := <-updateDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateNow after Stop admission = %v, want context.Canceled", err)
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after the in-flight update exited")
	}

	got, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read database after canceled update: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("Stop-admitted update replaced database: got %q want %q", got, original)
	}
	if !oldReader.isClosed() {
		t.Fatal("old reader was not closed by Stop")
	}
	if !newReader.isClosed() {
		t.Fatal("rejected candidate reader was not closed")
	}
}

func TestUpdateNow_OpenDBNilFailsBeforeDownload(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "geoip.db")
	original := []byte("original-db")
	if err := os.WriteFile(dbPath, original, 0644); err != nil {
		t.Fatalf("write original db: %v", err)
	}

	newContent := []byte("new-db-content")
	hash := sha256.Sum256(newContent)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	releaseJSON, err := json.Marshal(releaseInfo{
		TagName: "v20240106",
		Assets: []releaseAsset{{
			Name:               "geoip.db",
			Digest:             &digest,
			BrowserDownloadURL: "https://example.com/geoip.db",
		}},
	})
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}
	dl := &mockDownloader{responses: map[string][]byte{
		ReleaseAPIURL:                  releaseJSON,
		"https://example.com/geoip.db": newContent,
	}}
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
	}

	var updateErr error
	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		updateErr = s.UpdateNow()
	}()
	if panicked {
		t.Fatal("UpdateNow panicked with nil OpenDB")
	}
	if updateErr == nil || !strings.Contains(updateErr.Error(), "no OpenDB function configured") {
		t.Fatalf("UpdateNow error = %v, want missing OpenDB error", updateErr)
	}

	dl.mu.Lock()
	downloadCount := len(dl.calls)
	dl.mu.Unlock()
	if downloadCount != 0 {
		t.Fatalf("nil OpenDB started %d downloads before failing", downloadCount)
	}
	got, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read original db: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("nil OpenDB changed database: got %q want %q", got, original)
	}
	lifeCancel()
}

func TestGeoIPStopAdmissionIsNotBlockedByOldReaderClose(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "geoip.db")
	if err := os.WriteFile(dbPath, []byte("original-db"), 0644); err != nil {
		t.Fatalf("write original db: %v", err)
	}

	newContent := []byte("new-db-content")
	hash := sha256.Sum256(newContent)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	releaseJSON, err := json.Marshal(releaseInfo{
		TagName: "v20240107",
		Assets: []releaseAsset{{
			Name:               "geoip.db",
			Digest:             &digest,
			BrowserDownloadURL: "https://example.com/geoip.db",
		}},
	})
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}
	dl := &mockDownloader{responses: map[string][]byte{
		ReleaseAPIURL:                  releaseJSON,
		"https://example.com/geoip.db": newContent,
	}}
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	allowClose := make(chan struct{})
	oldReader := &blockingCloseReader{
		closeEntered: make(chan struct{}),
		allowClose:   allowClose,
	}
	newReader := &mockReader{country: "jp"}
	stopAdmitted := make(chan struct{})
	var stopAdmissionOnce sync.Once
	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
		reader:     oldReader,
		openDB: func(string) (GeoReader, error) {
			return newReader, nil
		},
	}
	s.afterGeoIPStopAdmissionHook = func() {
		stopAdmissionOnce.Do(func() { close(stopAdmitted) })
	}

	updateDone := make(chan error, 1)
	go func() { updateDone <- s.UpdateNow() }()
	select {
	case <-oldReader.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("UpdateNow did not reach old reader Close")
	}

	stopDone := make(chan struct{})
	go func() {
		s.Stop()
		close(stopDone)
	}()
	stopAdmissionObserved := false
	select {
	case <-stopAdmitted:
		stopAdmissionObserved = true
	case <-time.After(time.Second):
	}
	if got := s.Lookup(netip.MustParseAddr("1.2.3.4")); got != "" {
		t.Fatalf("reader remained visible after Stop admission while old Close was blocked: %q", got)
	}

	close(allowClose)
	if err := <-updateDone; err != nil {
		t.Fatalf("UpdateNow: %v", err)
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after old reader Close was released")
	}
	if !stopAdmissionObserved {
		t.Fatal("Stop admission was blocked by old reader Close")
	}
	if got := oldReader.closes(); got != 1 {
		t.Fatalf("old reader Close count = %d, want 1", got)
	}
	if !newReader.isClosed() {
		t.Fatal("published reader was not closed by Stop")
	}
}

func TestUpdateNow_OpenNewReaderFailurePreservesExistingDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "geoip.db")
	original := []byte("original-db")
	if err := os.WriteFile(dbPath, original, 0644); err != nil {
		t.Fatalf("write original db: %v", err)
	}

	newContent := []byte("new-db-content")
	hash := sha256.Sum256(newContent)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	releaseJSON, err := json.Marshal(releaseInfo{
		TagName: "v20240104",
		Assets: []releaseAsset{{
			Name:               "geoip.db",
			Digest:             &digest,
			BrowserDownloadURL: "https://example.com/geoip.db",
		}},
	})
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}

	dl := &mockDownloader{
		responses: map[string][]byte{
			ReleaseAPIURL:                  releaseJSON,
			"https://example.com/geoip.db": newContent,
		},
	}
	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		openDB: func(string) (GeoReader, error) {
			return nil, errors.New("candidate reader rejected")
		},
	}

	if err := s.UpdateNow(); err == nil {
		t.Fatal("expected candidate reader failure")
	}
	got, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read existing db: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("existing db was replaced after reader failure: got %q want %q", got, original)
	}
}

func TestUpdateNow_PublishedReaderFailurePreservesOldGeneration(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "geoip.db")
	original := []byte("original-db")
	if err := os.WriteFile(dbPath, original, 0o644); err != nil {
		t.Fatalf("write original db: %v", err)
	}

	newContent := []byte("new-db-content")
	hash := sha256.Sum256(newContent)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	releaseJSON, err := json.Marshal(releaseInfo{
		TagName: "v-published-open-failure",
		Assets: []releaseAsset{{
			Name:               "geoip.db",
			Digest:             &digest,
			BrowserDownloadURL: "https://example.com/geoip.db",
		}},
	})
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}
	dl := &mockDownloader{responses: map[string][]byte{
		ReleaseAPIURL:                  releaseJSON,
		"https://example.com/geoip.db": newContent,
	}}
	oldReader := &mockReader{country: "us"}
	validationReader := &mockReader{country: "jp"}
	openCalls := 0
	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		reader:     oldReader,
		openDB: func(string) (GeoReader, error) {
			openCalls++
			if openCalls == 1 {
				return validationReader, nil
			}
			return nil, errors.New("published reader unavailable")
		},
	}

	err = s.UpdateNow()
	if err == nil {
		t.Fatal("UpdateNow unexpectedly succeeded with a failed published reader open")
	}
	if openCalls != 2 {
		t.Fatalf("OpenDB calls = %d, want validation and published open", openCalls)
	}
	got, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read live database: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("live database changed after failed publish open: got %q want %q", got, original)
	}
	if got := s.Lookup(netip.MustParseAddr("1.2.3.4")); got != "us" {
		t.Fatalf("old reader lookup = %q, want us", got)
	}
	if oldReader.isClosed() {
		t.Fatal("old reader was closed after failed published open")
	}
	if validationReader.isClosed() == false {
		t.Fatal("validation reader leaked after failed publish open")
	}
	tmpFiles, err := filepath.Glob(filepath.Join(dir, "geoip.db.tmp.*"))
	if err != nil {
		t.Fatalf("glob temp files: %v", err)
	}
	if len(tmpFiles) != 0 {
		t.Fatalf("temporary files leaked after failed publish open: %v", tmpFiles)
	}
	for _, pattern := range []string{"geoip.db.rollback.*", "geoip.db.failed.*"} {
		files, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			t.Fatalf("glob %s: %v", pattern, err)
		}
		if len(files) != 0 {
			t.Fatalf("rollback artifacts leaked after failed publish open: %v", files)
		}
	}
}

func TestUpdateNow_CancelAfterPublishedOpenRestoresOldGeneration(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "geoip.db")
	original := []byte("original-db")
	if err := os.WriteFile(dbPath, original, 0644); err != nil {
		t.Fatalf("write original db: %v", err)
	}

	newContent := []byte("new-db-content")
	hash := sha256.Sum256(newContent)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	releaseJSON, err := json.Marshal(releaseInfo{
		TagName: "v-cancel-after-published-open",
		Assets: []releaseAsset{{
			Name:               "geoip.db",
			Digest:             &digest,
			BrowserDownloadURL: "https://example.com/geoip.db",
		}},
	})
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}
	dl := &mockDownloader{responses: map[string][]byte{
		ReleaseAPIURL:                  releaseJSON,
		"https://example.com/geoip.db": newContent,
	}}
	oldReader := &mockReader{country: "us"}
	validationReader := &mockReader{country: "jp"}
	publishedReader := &mockReader{country: "de"}
	var cancel context.CancelFunc
	ctx, cancel := context.WithCancel(context.Background())
	openCalls := 0
	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		reader:     oldReader,
		openDB: func(string) (GeoReader, error) {
			openCalls++
			switch openCalls {
			case 1:
				return validationReader, nil
			case 2:
				cancel()
				return publishedReader, nil
			default:
				return nil, errors.New("unexpected OpenDB call")
			}
		},
	}
	defer cancel()

	err = s.UpdateNowContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateNowContext error = %v, want context.Canceled", err)
	}
	got, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read live database: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("live database changed after canceled published open: got %q want %q", got, original)
	}
	if got := s.Lookup(netip.MustParseAddr("1.2.3.4")); got != "us" {
		t.Fatalf("old reader lookup = %q, want us", got)
	}
	if oldReader.isClosed() {
		t.Fatal("old reader was closed after canceled publish")
	}
	if !validationReader.isClosed() || !publishedReader.isClosed() {
		t.Fatal("staged readers were not both closed after canceled publish")
	}
	for _, pattern := range []string{"geoip.db.tmp.*", "geoip.db.rollback.*", "geoip.db.failed.*"} {
		files, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			t.Fatalf("glob %s: %v", pattern, err)
		}
		if len(files) != 0 {
			t.Fatalf("artifacts leaked after canceled publish: %v", files)
		}
	}
}

func TestUpdateNow_SHA256Mismatch_NoReplace(t *testing.T) {
	dir := t.TempDir()

	// Pre-existing database.
	origContent := []byte("original-db")
	dbPath := filepath.Join(dir, "geoip.db")
	if err := os.WriteFile(dbPath, origContent, 0644); err != nil {
		t.Fatal(err)
	}

	// New download content with wrong hash.
	newContent := []byte("new-db-content")
	badDigest := "sha256:0000000000000000000000000000000000000000000000000000000000000000"

	release := releaseInfo{
		TagName: "v20240102",
		Assets: []releaseAsset{
			{Name: "geoip.db", Digest: &badDigest, BrowserDownloadURL: "https://example.com/geoip.db"},
		},
	}
	releaseJSON, _ := json.Marshal(release)

	dl := &mockDownloader{
		responses: map[string][]byte{
			ReleaseAPIURL:                  releaseJSON,
			"https://example.com/geoip.db": newContent,
		},
	}

	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		openDB: func(path string) (GeoReader, error) {
			t.Fatal("OpenDB should not be called on SHA256 mismatch")
			return nil, nil
		},
	}

	err := s.UpdateNow()
	if err == nil {
		t.Fatal("expected error on SHA256 mismatch")
	}

	// Original file should be untouched.
	data, rErr := os.ReadFile(dbPath)
	if rErr != nil {
		t.Fatalf("read db: %v", rErr)
	}
	if string(data) != string(origContent) {
		t.Fatal("original database was corrupted despite SHA256 mismatch")
	}
}

func TestUpdateNow_NoDownloader(t *testing.T) {
	s := &Service{
		cacheDir:   t.TempDir(),
		dbFilename: "geoip.db",
		// no downloader
	}
	if err := s.UpdateNow(); err == nil {
		t.Fatal("expected error when no downloader configured")
	}
}

// TestUpdateNow_MissingDigest verifies that UpdateNow errors when the
// release asset does not include a digest (mandatory verification).
func TestUpdateNow_MissingDigest(t *testing.T) {
	dir := t.TempDir()

	// Pre-existing database.
	origContent := []byte("original-db")
	dbPath := filepath.Join(dir, "geoip.db")
	if err := os.WriteFile(dbPath, origContent, 0644); err != nil {
		t.Fatal(err)
	}

	newContent := []byte("new-db-content")

	release := releaseInfo{
		TagName: "v20240103",
		Assets: []releaseAsset{
			// Only .db asset, NO digest.
			{Name: "geoip.db", BrowserDownloadURL: "https://example.com/geoip.db"},
		},
	}
	releaseJSON, _ := json.Marshal(release)

	dl := &mockDownloader{
		responses: map[string][]byte{
			ReleaseAPIURL:                  releaseJSON,
			"https://example.com/geoip.db": newContent,
		},
	}

	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		openDB: func(path string) (GeoReader, error) {
			t.Fatal("OpenDB should not be called when digest is missing")
			return nil, nil
		},
	}

	err := s.UpdateNow()
	if err == nil {
		t.Fatal("expected error when digest is missing")
	}

	// Verify error message mentions missing digest.
	if !strings.Contains(err.Error(), "missing valid sha256 digest") {
		t.Fatalf("expected missing digest error, got: %v", err)
	}

	// Original file should be untouched.
	data, rErr := os.ReadFile(dbPath)
	if rErr != nil {
		t.Fatalf("read db: %v", rErr)
	}
	if string(data) != string(origContent) {
		t.Fatal("original database was corrupted despite missing digest")
	}
}

type notifyDownloader struct {
	called chan struct{}
}

func (d *notifyDownloader) Download(_ context.Context, _ string) ([]byte, error) {
	select {
	case d.called <- struct{}{}:
	default:
	}
	return nil, fmt.Errorf("mock download failure")
}

type blockingDownloader struct {
	started chan struct{}
	release chan struct{}
}

func (d *blockingDownloader) Download(_ context.Context, _ string) ([]byte, error) {
	select {
	case d.started <- struct{}{}:
	default:
	}
	<-d.release
	return nil, fmt.Errorf("blocked download failure")
}

func TestGeoIPStart_StatUnexpectedError(t *testing.T) {
	s := NewService(ServiceConfig{
		CacheDir:   t.TempDir(),
		DBFilename: "bad\x00name",
		OpenDB:     NoOpOpen,
	})
	defer s.Stop()

	err := s.Start()
	if err == nil {
		t.Fatal("expected Start to fail on unexpected stat error")
	}
	if !strings.Contains(err.Error(), "stat db") {
		t.Fatalf("expected stat error context, got: %v", err)
	}
}

func TestGeoIPStart_MissingDBTriggersBackgroundUpdate(t *testing.T) {
	dl := &notifyDownloader{called: make(chan struct{}, 1)}
	s := NewService(ServiceConfig{
		CacheDir:   t.TempDir(),
		DBFilename: "geoip.db",
		OpenDB:     NoOpOpen,
		Downloader: dl,
	})
	defer s.Stop()

	if err := s.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	select {
	case <-dl.called:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected background update attempt when db is missing")
	}
}

func TestGeoIPStop_WaitsInFlightUpdateAndClearsReader(t *testing.T) {
	old := &mockReader{country: "us"}
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	downloader := &blockingDownloader{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	s := &Service{
		reader:     old,
		cron:       nil,
		downloader: downloader,
		openDB:     NoOpOpen,
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
	}

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- s.UpdateNow()
	}()

	select {
	case <-downloader.started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("UpdateNow did not start download in time")
	}

	stopDone := make(chan struct{})
	go func() {
		s.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Fatal("Stop returned before in-flight UpdateNow completed")
	case <-time.After(100 * time.Millisecond):
		// expected: Stop is waiting for UpdateNow/updateMu
	}

	close(downloader.release)
	if err := <-updateDone; err == nil {
		t.Fatal("expected UpdateNow to fail from blocked downloader")
	}

	select {
	case <-stopDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop did not return after in-flight UpdateNow finished")
	}

	if got := s.Lookup(netip.MustParseAddr("1.2.3.4")); got != "" {
		t.Fatalf("expected empty lookup after Stop, got %q", got)
	}
	if !old.isClosed() {
		t.Fatal("reader should be closed after Stop")
	}
}

func TestUpdateNow_AfterStopReturnsCanceled(t *testing.T) {
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	downloader := &notifyDownloader{called: make(chan struct{}, 1)}
	s := &Service{
		cacheDir:   t.TempDir(),
		dbFilename: "geoip.db",
		cron:       nil,
		downloader: downloader,
		openDB:     NoOpOpen,
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
	}

	s.Stop()

	err := s.UpdateNow()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	select {
	case <-downloader.called:
		t.Fatal("downloader should not be called after Stop")
	default:
	}
}

func TestParseSHA256Digest(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"},
		{"SHA256:B94D27B9934D3E08A52E52D7DA7DABFAC484EFE37A5380EE9088F7ACE2EFCDE9", "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"},
		{"sha512:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", ""},
		{"sha256:abc", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got := parseSHA256Digest(tt.input)
		if got != tt.want {
			t.Errorf("parseSHA256Digest(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
