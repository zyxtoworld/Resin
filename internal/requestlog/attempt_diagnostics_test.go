package requestlog

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/proxy"
)

func TestRepo_PersistsAttemptDiagnostics(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 2)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	want := []proxy.RequestAttemptDiagnostic{
		{
			Attempt:                 1,
			RouteGeneration:         42,
			PlatformRevisionNs:      123456789,
			NodeHash:                "node-hash",
			EgressIP:                "198.51.100.10",
			Transport:               "*http.Transport",
			RetryBudget:             2,
			RequestTotalTimeoutMs:   350,
			RequestAttemptTimeoutMs: 40,
			MaxAttempts:             2,
			AttemptDeadlineMs:       40,
			StartedMs:               1,
			GotConnMs:               2,
			WroteRequestMs:          3,
			ResponseHeaderMs:        40,
			RoundTripEndMs:          41,
			BodyFinishMs:            42,
			RequestBodyFinishMs:     4,
			ResponseStatus:          504,
			ResponseStarted:         false,
			RequestBodyComplete:     true,
			RequestBodyBytes:        12,
			ResponseBodyBytes:       0,
			ErrorKind:               "response_header_timeout",
			CancelReason:            "attempt_deadline",
			ReleaseReason:           "retry_next",
		},
	}
	wantEntry := proxy.RequestLogEntry{
		ID:                 "attempt-diagnostics",
		StartedAtNs:        time.Now().UnixNano(),
		ProxyType:          proxy.ProxyTypeReverse,
		PlatformID:         "platform-id",
		PlatformName:       "platform-name",
		TargetHost:         "opencode.ai",
		HTTPMethod:         "POST",
		HTTPStatus:         504,
		UpstreamStage:      "reverse_roundtrip",
		UpstreamErrKind:    "timeout",
		AttemptDiagnostics: want,
	}
	if inserted, err := repo.InsertBatch([]proxy.RequestLogEntry{wantEntry}); err != nil || inserted != 1 {
		t.Fatalf("repo.InsertBatch: inserted=%d err=%v", inserted, err)
	}

	list, _, _, err := repo.List(ListFilter{Limit: 10})
	if err != nil {
		t.Fatalf("repo.List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("repo.List len=%d, want 1", len(list))
	}
	if list[0].AttemptDiagnostics != nil {
		t.Fatalf("list loaded detailed attempt diagnostics: %#v", list[0].AttemptDiagnostics)
	}
	if list[0].AttemptCount != 1 || list[0].AttemptFirstMs != 1 || list[0].AttemptLastMs != 42 {
		t.Fatalf("list attempt summary = count=%d first=%d last=%d", list[0].AttemptCount, list[0].AttemptFirstMs, list[0].AttemptLastMs)
	}
	if list[0].AttemptFinalStage != "reverse_roundtrip" || list[0].AttemptFinalKind != "timeout" {
		t.Fatalf("list terminal attempt summary = stage=%q kind=%q", list[0].AttemptFinalStage, list[0].AttemptFinalKind)
	}

	got, err := repo.GetByID("attempt-diagnostics")
	if err != nil {
		t.Fatalf("repo.GetByID: %v", err)
	}
	if got == nil {
		t.Fatal("repo.GetByID returned nil")
	}
	if !reflect.DeepEqual(got.AttemptDiagnostics, want) {
		t.Fatalf("loaded attempt diagnostics = %#v, want %#v", got.AttemptDiagnostics, want)
	}
}

func TestRepo_ListDoesNotDecodeCorruptAttemptDiagnostics(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 2)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	entry := proxy.RequestLogEntry{ID: "corrupt-attempt-diagnostics", StartedAtNs: time.Now().UnixNano(), HTTPStatus: 200}
	if inserted, err := repo.InsertBatch([]proxy.RequestLogEntry{entry}); err != nil || inserted != 1 {
		t.Fatalf("repo.InsertBatch: inserted=%d err=%v", inserted, err)
	}
	if _, err := repo.activeDB.Exec("UPDATE request_logs SET attempt_diagnostics = ? WHERE id = ?", "{broken", entry.ID); err != nil {
		t.Fatalf("corrupt attempt diagnostics: %v", err)
	}
	list, _, _, err := repo.List(ListFilter{Limit: 10})
	if err != nil {
		t.Fatalf("repo.List should ignore detail JSON: %v", err)
	}
	if len(list) != 1 || list[0].AttemptDiagnostics != nil {
		t.Fatalf("list result = %#v", list)
	}
	if _, err := repo.GetByID(entry.ID); err == nil || !strings.Contains(err.Error(), "decode attempt diagnostics") {
		t.Fatalf("repo.GetByID corrupt JSON error = %v", err)
	}
}

func TestRepo_AttemptDiagnosticsAreBoundedAndSanitized(t *testing.T) {
	repo := NewRepo(t.TempDir(), 1<<20, 2)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	diagnostics := make([]proxy.RequestAttemptDiagnostic, proxy.MaxRequestAttemptDiagnostics+5)
	for i := range diagnostics {
		diagnostics[i] = proxy.RequestAttemptDiagnostic{
			Attempt:        i + 1,
			StartedMs:      int64(i + 1),
			RoundTripEndMs: int64(i + 2),
			NodeHash:       strings.Repeat("n", proxy.MaxRequestAttemptDiagnosticStringBytes+20),
			Transport:      "adapter\nwith\rcontrols",
			ErrorKind:      "safe-kind",
		}
	}
	entry := proxy.RequestLogEntry{
		ID:                 "bounded-attempt-diagnostics",
		StartedAtNs:        time.Now().UnixNano(),
		AttemptCount:       len(diagnostics),
		AttemptDiagnostics: diagnostics,
		UpstreamStage:      "reverse_roundtrip",
		UpstreamErrKind:    "timeout",
	}
	if inserted, err := repo.InsertBatch([]proxy.RequestLogEntry{entry}); err != nil || inserted != 1 {
		t.Fatalf("repo.InsertBatch: inserted=%d err=%v", inserted, err)
	}
	got, err := repo.GetByID(entry.ID)
	if err != nil {
		t.Fatalf("repo.GetByID: %v", err)
	}
	if got.AttemptCount != len(diagnostics) || !got.AttemptDiagnosticsTruncated {
		t.Fatalf("bounded summary = count=%d truncated=%v", got.AttemptCount, got.AttemptDiagnosticsTruncated)
	}
	if len(got.AttemptDiagnostics) != proxy.MaxRequestAttemptDiagnostics {
		t.Fatalf("detail diagnostics len=%d, want %d", len(got.AttemptDiagnostics), proxy.MaxRequestAttemptDiagnostics)
	}
	if strings.Contains(got.AttemptDiagnostics[0].Transport, "\n") || len(got.AttemptDiagnostics[0].NodeHash) > proxy.MaxRequestAttemptDiagnosticStringBytes {
		t.Fatalf("diagnostic string was not sanitized/bounded: %#v", got.AttemptDiagnostics[0])
	}
	var encoded string
	if err := repo.activeDB.QueryRow("SELECT attempt_diagnostics FROM request_logs WHERE id = ?", entry.ID).Scan(&encoded); err != nil {
		t.Fatalf("read encoded diagnostics: %v", err)
	}
	if len(encoded) > proxy.MaxRequestAttemptDiagnosticsJSONBytes {
		t.Fatalf("encoded diagnostics len=%d exceeds bound=%d", len(encoded), proxy.MaxRequestAttemptDiagnosticsJSONBytes)
	}
}

func TestAttemptDiagnosticTextRejectsSensitiveMarkers(t *testing.T) {
	for _, value := range []string{
		"Authorization: Bearer secret",
		"cookie=session-secret",
		"https://example.invalid/path?token=secret",
	} {
		got := proxy.SanitizeRequestAttemptDiagnosticText(value)
		if got != "[redacted]" {
			t.Fatalf("sensitive diagnostic text %q normalized to %q", value, got)
		}
	}
}
