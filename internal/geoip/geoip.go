package geoip

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/maxminddb-golang"
	"github.com/robfig/cron/v3"

	"github.com/Resinat/Resin/internal/netutil"
)

// GeoReader abstracts the GeoIP database reader (e.g., maxminddb reader).
// This interface allows different implementations and simplifies testing.
type GeoReader interface {
	Lookup(ip netip.Addr) string
	Close() error
}

// OpenFunc opens a GeoIP database file and returns a GeoReader.
type OpenFunc func(path string) (GeoReader, error)

// noOpReader is a placeholder reader that returns "" for all lookups.
type noOpReader struct{}

func (noOpReader) Lookup(_ netip.Addr) string { return "" }
func (noOpReader) Close() error               { return nil }

// NoOpOpen is a placeholder OpenFunc for tests. Always returns a reader
// that returns empty string.
func NoOpOpen(_ string) (GeoReader, error) { return noOpReader{}, nil }

type mmdbReader struct {
	reader *maxminddb.Reader
}

// maxGeoIPDatabaseBytes bounds the resident memory used by an online reader.
// The downloader enforces the same limit for remote GeoIP payloads; the file
// check remains necessary because the live file may be supplied locally or
// changed between Stat and Read.
const maxGeoIPDatabaseBytes = 16 << 20

type mmdbCountryRecord struct {
	Country struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	RegisteredCountry struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"registered_country"`
}

func (m *mmdbReader) Lookup(ip netip.Addr) string {
	if m == nil || m.reader == nil || !ip.IsValid() {
		return ""
	}
	ip = ip.Unmap()
	var record mmdbCountryRecord
	if err := m.reader.Lookup(net.IP(ip.AsSlice()), &record); err != nil {
		return ""
	}
	if record.Country.ISOCode != "" {
		return strings.ToLower(record.Country.ISOCode)
	}
	if record.RegisteredCountry.ISOCode != "" {
		return strings.ToLower(record.RegisteredCountry.ISOCode)
	}
	return ""
}

func (m *mmdbReader) Close() error {
	if m == nil || m.reader == nil {
		return nil
	}
	return m.reader.Close()
}

// MMDBOpen opens a MaxMind-compatible mmdb database.
func MMDBOpen(path string) (GeoReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() > int64(maxGeoIPDatabaseBytes) {
		_ = file.Close()
		return nil, fmt.Errorf("geoip: database exceeds %d bytes", maxGeoIPDatabaseBytes)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(maxGeoIPDatabaseBytes)+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(data) > maxGeoIPDatabaseBytes {
		return nil, fmt.Errorf("geoip: database exceeds %d bytes", maxGeoIPDatabaseBytes)
	}
	reader, err := maxminddb.FromBytes(data)
	if err != nil {
		return nil, err
	}
	return &mmdbReader{reader: reader}, nil
}

// SingBoxOpen is kept as a compatibility alias; use MMDBOpen for generic mmdb.
func SingBoxOpen(path string) (GeoReader, error) {
	return MMDBOpen(path)
}

// ServiceConfig configures the GeoIP service.
type ServiceConfig struct {
	CacheDir       string             // directory where country.mmdb is stored
	DBFilename     string             // default "country.mmdb"
	UpdateSchedule string             // cron expression, default "0 7 * * *"
	OpenDB         OpenFunc           // function to open the database
	Downloader     netutil.Downloader // shared downloader for fetching releases
}

// ReleaseAPIURL is the GitHub API endpoint for the latest MetaCubeX rules release.
const ReleaseAPIURL = "https://api.github.com/repos/MetaCubeX/meta-rules-dat/releases/latest"

// Service provides GeoIP lookup with hot-reloading via RWMutex.
type Service struct {
	mu     sync.RWMutex
	reader GeoReader // nil until first load

	readerCloseMu      sync.Mutex
	readerClosePending int
	readerCloseDone    chan struct{}

	cacheDir         string
	dbFilename       string
	openDB           OpenFunc
	downloader       netutil.Downloader
	cron             *cron.Cron
	cronEntryID      cron.EntryID
	updateMu         sync.Mutex // serializes UpdateNow calls
	updateStateMu    sync.Mutex
	activeUpdateDone chan struct{}
	startMu          sync.Mutex // serializes Start calls
	lifecycleMu      sync.Mutex
	started          bool
	stopped          bool
	lifeCtx          context.Context
	lifeCancel       context.CancelFunc

	// Package-private seams for deterministic lifecycle tests.
	beforeGeoIPCommitHook       func()
	afterGeoIPStopAdmissionHook func()
}

func (s *Service) isStopped() bool {
	s.lifecycleMu.Lock()
	if s.stopped {
		s.lifecycleMu.Unlock()
		return true
	}
	lifeCtx := s.lifeCtx
	s.lifecycleMu.Unlock()
	if lifeCtx == nil {
		return false
	}
	select {
	case <-lifeCtx.Done():
		return true
	default:
		return false
	}
}

// NewService creates a new GeoIP service.
func NewService(cfg ServiceConfig) *Service {
	if cfg.DBFilename == "" {
		cfg.DBFilename = "country.mmdb"
	}
	if cfg.UpdateSchedule == "" {
		cfg.UpdateSchedule = "0 7 * * *"
	}
	c := cron.New()
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	s := &Service{
		cacheDir:   cfg.CacheDir,
		dbFilename: cfg.DBFilename,
		openDB:     cfg.OpenDB,
		downloader: cfg.Downloader,
		cron:       c,
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
	}

	// Schedule periodic updates.
	entryID, err := c.AddFunc(cfg.UpdateSchedule, func() {
		if err := s.UpdateNow(); err != nil {
			log.Printf("[geoip] scheduled update failed: %v", err)
		}
	})
	if err != nil {
		log.Printf("[geoip] invalid cron expression %q: %v", cfg.UpdateSchedule, err)
	} else {
		s.cronEntryID = entryID
	}

	return s
}

// Start loads the initial database (if present), checks for staleness
// against the cron schedule, and starts the cron scheduler.
func (s *Service) Start() error {
	s.startMu.Lock()
	defer s.startMu.Unlock()

	s.lifecycleMu.Lock()
	if s.stopped {
		s.lifecycleMu.Unlock()
		return context.Canceled
	}
	if s.started {
		s.lifecycleMu.Unlock()
		return nil
	}
	s.lifecycleMu.Unlock()

	// Serialize initial reader loading with Stop. If Stop wins while this
	// initialization is in progress, it waits for the same lifecycle owner as
	// UpdateNow and closes any rejected reader after this method leaves.
	s.updateMu.Lock()
	updateDone := s.beginUpdate()
	defer func() {
		s.endUpdate(updateDone)
		s.updateMu.Unlock()
	}()
	if s.isStopped() {
		return context.Canceled
	}

	dbPath := filepath.Join(s.cacheDir, s.dbFilename)
	info, err := os.Stat(dbPath)
	if err == nil {
		// Load existing database.
		newReader, err := s.openReader(dbPath)
		if err != nil {
			log.Printf("[geoip] failed to load initial db: %v", err)
		} else if !s.installReaderIfRunning(newReader) {
			return context.Canceled
		}

		// Check staleness: if mtime is older than the scheduled interval,
		// trigger an immediate background update.
		if s.isStale(info.ModTime()) {
			log.Println("[geoip] database is stale, triggering background update")
			go func() {
				if err := s.UpdateNow(); err != nil {
					log.Printf("[geoip] startup update failed: %v", err)
				}
			}()
		}
	} else if os.IsNotExist(err) {
		// No local database at all — download immediately in background.
		log.Println("[geoip] no local database found, triggering background download")
		go func() {
			if err := s.UpdateNow(); err != nil {
				log.Printf("[geoip] initial download failed: %v", err)
			}
		}()
	} else {
		return fmt.Errorf("geoip: stat db %s: %w", dbPath, err)
	}

	s.lifecycleMu.Lock()
	if s.stopped {
		s.lifecycleMu.Unlock()
		return context.Canceled
	}
	if s.cron != nil {
		s.cron.Start()
	}
	s.started = true
	s.lifecycleMu.Unlock()
	return nil
}

// isStale returns true if the file's mtime is older than the expected
// cron schedule interval. Uses 2× the gap between two consecutive cron
// firings to tolerate jitter. Falls back to 32 days if the schedule
// cannot be determined.
func (s *Service) isStale(modTime time.Time) bool {
	entry := s.cron.Entry(s.cronEntryID)
	if entry.ID == 0 || entry.Schedule == nil {
		// Cron not configured — fall back to conservative default.
		return time.Since(modTime) > 32*24*time.Hour
	}

	// Compute the gap between two consecutive firings.
	now := time.Now()
	next := entry.Schedule.Next(now)
	nextNext := entry.Schedule.Next(next)
	interval := nextNext.Sub(next)
	if interval <= 0 {
		interval = 32 * 24 * time.Hour
	}

	// Stale if mtime is older than 2× the interval.
	return time.Since(modTime) > 2*interval
}

// Stop stops the cron scheduler and closes the reader.
func (s *Service) Stop() {
	_ = s.StopContext(context.Background())
}

// StopContext stops the cron scheduler and waits for an in-flight update until
// ctx expires. A timed-out update remains canceled and cannot publish a staged
// reader; it finishes its own cleanup after the caller has returned.
func (s *Service) StopContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var stoppedReader GeoReader
	s.lifecycleMu.Lock()
	if !s.stopped {
		s.stopped = true
		if s.lifeCancel != nil {
			s.lifeCancel()
		}
		// Detach under the same lifecycleMu -> mu order used by reader
		// publication. The shutdown admission hook must never observe a
		// reader that Lookup can still reach, while arbitrary Close code runs
		// outside lifecycleMu.
		stoppedReader = s.swapReader(nil)
		if hook := s.afterGeoIPStopAdmissionHook; hook != nil {
			hook()
		}
	}
	c := s.cron
	s.lifecycleMu.Unlock()
	if stoppedReader != nil {
		s.closeReaderAsync(stoppedReader)
	}

	if c != nil {
		// Stop prevents new scheduled jobs. The update owner below tracks the
		// actual UpdateNow call and provides the cancelable wait.
		c.Stop()
	}

	// Detach before waiting for any arbitrary reader Close. A reader is never
	// exposed after shutdown admission, even if its Close implementation blocks.
	s.closeReaderIfStopped()
	for {
		if done := s.activeUpdate(); done != nil {
			if err := waitForDone(ctx, done); err != nil {
				return err
			}
			continue
		}
		if err := s.waitReaderCloses(ctx); err != nil {
			return err
		}
		if s.activeUpdate() == nil {
			return nil
		}
	}
}

func waitForDone(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) activeUpdate() chan struct{} {
	s.updateStateMu.Lock()
	done := s.activeUpdateDone
	s.updateStateMu.Unlock()
	return done
}

func (s *Service) beginUpdate() chan struct{} {
	done := make(chan struct{})
	s.updateStateMu.Lock()
	s.activeUpdateDone = done
	s.updateStateMu.Unlock()
	return done
}

func (s *Service) endUpdate(done chan struct{}) {
	// If shutdown won the lifecycle admission, detach and register the reader
	// close before publishing completion of the update owner. This prevents a
	// waiter from observing activeUpdateDone == nil while the final close is
	// still being admitted.
	s.closeReaderIfStopped()
	s.updateStateMu.Lock()
	if s.activeUpdateDone == done {
		s.activeUpdateDone = nil
	}
	close(done)
	s.updateStateMu.Unlock()
}

func (s *Service) closeReaderIfStopped() {
	s.lifecycleMu.Lock()
	stopped := s.stopped
	s.lifecycleMu.Unlock()
	if !stopped {
		return
	}
	s.mu.Lock()
	r := s.reader
	s.reader = nil
	s.mu.Unlock()
	if r != nil {
		s.closeReaderAsync(r)
	}
}

func (s *Service) beginReaderClose() func() {
	s.readerCloseMu.Lock()
	if s.readerClosePending == 0 {
		s.readerCloseDone = make(chan struct{})
	}
	s.readerClosePending++
	s.readerCloseMu.Unlock()
	return func() {
		s.readerCloseMu.Lock()
		s.readerClosePending--
		if s.readerClosePending == 0 {
			close(s.readerCloseDone)
			s.readerCloseDone = nil
		}
		s.readerCloseMu.Unlock()
	}
}

func (s *Service) closeReader(reader GeoReader) {
	_ = s.closeReaderChecked(reader)
}

func (s *Service) closeReaderChecked(reader GeoReader) error {
	if reader == nil {
		return nil
	}
	finish := s.beginReaderClose()
	defer finish()
	return reader.Close()
}

func (s *Service) closeReaderAsync(reader GeoReader) {
	if reader == nil {
		return
	}
	finish := s.beginReaderClose()
	go func() {
		defer finish()
		_ = reader.Close()
	}()
}

func (s *Service) waitReaderCloses(ctx context.Context) error {
	for {
		s.readerCloseMu.Lock()
		done := s.readerCloseDone
		s.readerCloseMu.Unlock()
		if done == nil {
			return nil
		}
		if err := waitForDone(ctx, done); err != nil {
			return err
		}
	}
}

// Lookup returns the country code for the given IP address.
// Thread-safe: holds RLock for the entire duration of the lookup.
func (s *Service) Lookup(ip netip.Addr) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.reader == nil {
		return ""
	}
	return s.reader.Lookup(ip)
}

// releaseAsset represents a GitHub release asset.
type releaseAsset struct {
	Name               string  `json:"name"`
	Digest             *string `json:"digest"`
	BrowserDownloadURL string  `json:"browser_download_url"`
}

// releaseInfo represents a GitHub release.
type releaseInfo struct {
	TagName string         `json:"tag_name"`
	Assets  []releaseAsset `json:"assets"`
}

// UpdateNow downloads the latest GeoIP database from GitHub, verifies SHA256,
// atomically replaces the local file, and hot-reloads the reader.
// Serialized via updateMu to prevent concurrent temp file races.
func (s *Service) UpdateNow() error {
	return s.UpdateNowContext(context.Background())
}

// UpdateNowContext binds the update to both the caller and service lifetime.
// A request cancellation stops an interactive update, while Stop still
// cancels scheduled/background updates through the service lifetime.
func (s *Service) UpdateNowContext(parent context.Context) error {
	if parent == nil {
		parent = context.Background()
	}
	s.updateMu.Lock()
	updateDone := s.beginUpdate()
	defer func() {
		s.endUpdate(updateDone)
		s.updateMu.Unlock()
	}()
	ctx, releaseContext := s.contextWithLifetime(parent)
	defer releaseContext()

	if s.isStopped() {
		return context.Canceled
	}

	if s.downloader == nil {
		return fmt.Errorf("geoip: no downloader configured")
	}
	if s.openDB == nil {
		return fmt.Errorf("geoip: no OpenDB function configured")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// 1. Fetch latest release metadata.
	releaseBody, err := s.downloader.Download(ctx, ReleaseAPIURL)
	if err != nil {
		return fmt.Errorf("geoip: fetch release info: %w", err)
	}

	var release releaseInfo
	if err := json.Unmarshal(releaseBody, &release); err != nil {
		return fmt.Errorf("geoip: parse release info: %w", err)
	}

	// 2. Find the .db asset URL and its SHA256 digest.
	dbURL, digest := "", ""
	for _, a := range release.Assets {
		if a.Name == s.dbFilename {
			dbURL = a.BrowserDownloadURL
			if a.Digest != nil {
				digest = *a.Digest
			}
		}
	}
	if dbURL == "" {
		return fmt.Errorf("geoip: asset %q not found in release %s", s.dbFilename, release.TagName)
	}
	expectedHash := parseSHA256Digest(digest)
	if expectedHash == "" {
		return fmt.Errorf("geoip: asset %q missing valid sha256 digest in release %s", s.dbFilename, release.TagName)
	}

	// 3. Download .db to unique temp file.
	dbData, err := s.downloader.Download(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("geoip: download db: %w", err)
	}

	tmpFile, err := os.CreateTemp(s.cacheDir, s.dbFilename+".tmp.*")
	if err != nil {
		return fmt.Errorf("geoip: create temp: %w", err)
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.Write(dbData); err != nil {
		_ = tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("geoip: write temp: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("geoip: sync temp: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("geoip: close temp: %w", err)
	}
	// Clean up temp on any error after this point.
	defer func() {
		os.Remove(tmpPath) // no-op if already renamed
	}()

	// 4. Verify SHA256 — mandatory.
	if err := VerifySHA256(tmpPath, expectedHash); err != nil {
		return err
	}

	// Validate the candidate before replacing the live file. If the reader
	// cannot open the downloaded database, the existing file and in-memory
	// reader must remain untouched so a restart does not inherit a broken DB.
	newReader, err := s.openDB(tmpPath)
	if err != nil {
		return fmt.Errorf("geoip: open candidate %s: %w", tmpPath, err)
	}
	if newReader == nil {
		return fmt.Errorf("geoip: open candidate %s: nil reader", tmpPath)
	}
	keepReader := false
	defer func() {
		if !keepReader && newReader != nil {
			s.closeReader(newReader)
		}
	}()
	// The candidate reader only validates the downloaded bytes. On Windows a
	// memory-mapped reader may deny delete/rename while it still owns tmpPath,
	// so it must be closed before the file commit. The reader used for service
	// lookups is opened from the published path below.
	if err := s.closeReaderChecked(newReader); err != nil {
		newReader = nil
		return fmt.Errorf("geoip: close candidate %s: %w", tmpPath, err)
	}
	newReader = nil

	// 5. Atomic rename.
	if err := ctx.Err(); err != nil {
		return err
	}
	if hook := s.beforeGeoIPCommitHook; hook != nil {
		hook()
	}
	dbPath := filepath.Join(s.cacheDir, s.dbFilename)
	rollbackPath, err := stageGeoIPRollbackCopy(dbPath)
	if err != nil {
		return fmt.Errorf("geoip: stage rollback copy: %w", err)
	}
	defer func() {
		if rollbackPath != "" {
			_ = os.Remove(rollbackPath)
		}
	}()

	// Serialize the irreversible file replacement with Stop. If shutdown has
	// already admitted, reject the staged reader without touching the live
	// file. If this lock is acquired first, the replacement linearizes before
	// Stop and Stop will then close the published reader.
	s.lifecycleMu.Lock()
	if s.stopped {
		s.lifecycleMu.Unlock()
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		s.lifecycleMu.Unlock()
		return err
	}
	if err := os.Rename(tmpPath, dbPath); err != nil {
		s.lifecycleMu.Unlock()
		return fmt.Errorf("geoip: atomic replace: %w", err)
	}
	publishedReader, err := s.openReader(dbPath)
	if err != nil {
		rollbackErr := rollbackGeoIPReplacement(dbPath, rollbackPath)
		s.lifecycleMu.Unlock()
		if rollbackErr != nil {
			return fmt.Errorf("geoip: open published %s: %v (rollback failed: %w)", dbPath, err, rollbackErr)
		}
		return fmt.Errorf("geoip: open published %s: %w", dbPath, err)
	}
	newReader = publishedReader
	if err := ctx.Err(); err != nil {
		closeErr := s.closeReaderChecked(newReader)
		newReader = nil
		rollbackErr := rollbackGeoIPReplacement(dbPath, rollbackPath)
		s.lifecycleMu.Unlock()
		if rollbackErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
		}
		if closeErr != nil {
			return fmt.Errorf("%w (close published reader: %v)", err, closeErr)
		}
		return err
	}

	// 6. Hot-reload the validated reader opened from the published path. Keep
	// the open and pointer swap under the same lifecycle lock so shutdown
	// cannot observe a half-published generation. Only the pointer swap belongs
	// lifecycle lock; closing the old reader may run user/driver code and must
	// not block Stop from admitting shutdown.
	oldReader := s.swapReader(newReader)
	keepReader = true
	s.lifecycleMu.Unlock()
	if oldReader != nil {
		s.closeReader(oldReader)
	}
	return nil
}

func (s *Service) contextWithLifetime(parent context.Context) (context.Context, func()) {
	if s.lifeCtx == nil {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancel(parent)
	stopLifetimeCancel := context.AfterFunc(s.lifeCtx, cancel)
	return ctx, func() {
		stopLifetimeCancel()
		cancel()
	}
}

// reloadReader atomically replaces the current reader with a new one.
// Safe: RLock holders finish before old reader is closed.
func (s *Service) reloadReader(path string) error {
	newReader, err := s.openReader(path)
	if err != nil {
		return err
	}
	s.installReader(newReader)
	return nil
}

func (s *Service) openReader(path string) (GeoReader, error) {
	if s.openDB == nil {
		return nil, fmt.Errorf("geoip: no OpenDB function configured")
	}
	newReader, err := s.openDB(path)
	if err != nil {
		if newReader != nil {
			s.closeReader(newReader)
		}
		return nil, fmt.Errorf("geoip: open %s: %w", path, err)
	}
	if newReader == nil {
		return nil, fmt.Errorf("geoip: open %s: nil reader", path)
	}
	return newReader, nil
}

// stageGeoIPRollbackCopy copies the currently published database to a private
// path. It is used only to undo a file replacement if reopening the published
// generation fails; the normal commit never removes the live path first.
func stageGeoIPRollbackCopy(path string) (string, error) {
	source, err := os.Open(path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	backup, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".rollback.*")
	if err != nil {
		_ = source.Close()
		return "", err
	}
	backupPath := backup.Name()
	cleanup := func() {
		_ = backup.Close()
		_ = source.Close()
		_ = os.Remove(backupPath)
	}
	if _, err := io.Copy(backup, source); err != nil {
		cleanup()
		return "", err
	}
	if err := backup.Sync(); err != nil {
		cleanup()
		return "", err
	}
	if err := backup.Close(); err != nil {
		_ = source.Close()
		_ = os.Remove(backupPath)
		return "", err
	}
	if err := source.Close(); err != nil {
		_ = os.Remove(backupPath)
		return "", err
	}
	return backupPath, nil
}

// rollbackGeoIPReplacement restores the old live file after a failed
// post-rename open. The replacement is moved aside first, so the old backup
// is never renamed over a live file that still owns the new generation.
func rollbackGeoIPReplacement(path, backupPath string) error {
	if backupPath == "" {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	failed, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".failed.*")
	if err != nil {
		return err
	}
	failedPath := failed.Name()
	if err := failed.Close(); err != nil {
		_ = os.Remove(failedPath)
		return err
	}
	if err := os.Remove(failedPath); err != nil {
		return err
	}
	if err := os.Rename(path, failedPath); err != nil {
		return err
	}
	if err := os.Rename(backupPath, path); err != nil {
		_ = os.Rename(failedPath, path)
		return err
	}
	return os.Remove(failedPath)
}

func (s *Service) installReader(newReader GeoReader) {
	old := s.swapReader(newReader)
	// Safe to close old: all RLock holders on old have released.
	if old != nil {
		s.closeReader(old)
	}
}

func (s *Service) installReaderIfRunning(newReader GeoReader) bool {
	s.lifecycleMu.Lock()
	if s.stopped {
		s.lifecycleMu.Unlock()
		s.closeReader(newReader)
		return false
	}
	oldReader := s.swapReader(newReader)
	s.lifecycleMu.Unlock()
	if oldReader != nil {
		s.closeReader(oldReader)
	}
	return true
}

func (s *Service) swapReader(newReader GeoReader) GeoReader {
	s.mu.Lock()
	old := s.reader
	s.reader = newReader
	s.mu.Unlock()
	return old
}

// VerifySHA256 checks that the file at path has the expected SHA256 hash.
func VerifySHA256(path, expectedHex string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	got := sha256.Sum256(data)
	gotHex := hex.EncodeToString(got[:])
	if gotHex != expectedHex {
		return fmt.Errorf("geoip: sha256 mismatch: got %s, want %s", gotHex, expectedHex)
	}
	return nil
}

// LastUpdated returns the modification time of the database file.
func (s *Service) LastUpdated() time.Time {
	dbPath := filepath.Join(s.cacheDir, s.dbFilename)
	info, err := os.Stat(dbPath)
	if err != nil {
		return time.Time{}
	}
	return info.ModTime()
}

// NextScheduledUpdate returns the next cron-scheduled update time.
// Returns zero time if cron is not configured.
func (s *Service) NextScheduledUpdate() time.Time {
	if s.cron == nil {
		return time.Time{}
	}
	entry := s.cron.Entry(s.cronEntryID)
	return entry.Next
}

// parseSHA256Digest extracts hex hash from a "sha256:<hash>" formatted digest string.
func parseSHA256Digest(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ToLower(s)
	const prefix = "sha256:"
	if !strings.HasPrefix(s, prefix) {
		return ""
	}
	hash := strings.TrimSpace(strings.TrimPrefix(s, prefix))
	if len(hash) != 64 {
		return ""
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return ""
	}
	return hash
}
