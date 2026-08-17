//go:build windows

package geoip

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

// windowsMappedReader models the sharing contract of the default MMDB reader:
// the source file remains open while the reader is live and does not share
// delete/rename with another handle.
type windowsMappedReader struct {
	handle    syscall.Handle
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func (r *windowsMappedReader) Lookup(netip.Addr) string { return "jp" }

func (r *windowsMappedReader) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = syscall.CloseHandle(r.handle)
		r.closed.Store(true)
	})
	return r.closeErr
}

func TestUpdateNow_WindowsClosesValidationReaderBeforeRename(t *testing.T) {
	dir := t.TempDir()
	livePath := filepath.Join(dir, "geoip.db")
	original := []byte("original-db")
	if err := os.WriteFile(livePath, original, 0o644); err != nil {
		t.Fatalf("write original db: %v", err)
	}

	newContent := []byte("new-db-content")
	hash := sha256.Sum256(newContent)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	releaseJSON, err := json.Marshal(releaseInfo{
		TagName: "v-windows-sharing",
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

	var openCount atomic.Int32
	readers := make([]*windowsMappedReader, 0, 2)
	var readersMu sync.Mutex
	s := &Service{
		cacheDir:   dir,
		dbFilename: "geoip.db",
		downloader: dl,
		openDB: func(path string) (GeoReader, error) {
			handle, err := syscall.CreateFile(
				syscall.StringToUTF16Ptr(path),
				syscall.GENERIC_READ,
				syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE,
				nil,
				syscall.OPEN_EXISTING,
				syscall.FILE_ATTRIBUTE_NORMAL,
				0,
			)
			if err != nil {
				return nil, err
			}
			reader := &windowsMappedReader{handle: handle}
			openCount.Add(1)
			readersMu.Lock()
			readers = append(readers, reader)
			readersMu.Unlock()
			return reader, nil
		},
	}

	if err := s.UpdateNow(); err != nil {
		t.Fatalf("UpdateNow: %v", err)
	}
	defer s.Stop()

	if got, err := os.ReadFile(livePath); err != nil {
		t.Fatalf("read published db: %v", err)
	} else if string(got) != string(newContent) {
		t.Fatalf("published db = %q, want %q", got, newContent)
	}
	if got := s.Lookup(netip.MustParseAddr("1.2.3.4")); got != "jp" {
		t.Fatalf("published reader lookup = %q, want jp", got)
	}
	if got := openCount.Load(); got != 2 {
		t.Fatalf("expected one validation and one post-rename reader, opened %d reader(s)", got)
	}

	readersMu.Lock()
	defer readersMu.Unlock()
	if len(readers) != 2 {
		t.Fatalf("recorded %d readers, want validation and published reader", len(readers))
	}
	if !readers[0].closed.Load() {
		t.Fatal("validation reader remained open across rename")
	}
	if readers[1].closed.Load() {
		t.Fatal("published reader was closed before service shutdown")
	}
}
