package api

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/Resinat/Resin/internal/observability"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/requestlog"
)

func TestRequestLogListKeepsAttemptTimelineDetailOnly(t *testing.T) {
	projector := observability.NewProjector(bytes.Repeat([]byte{1}, 32))
	row := requestlog.LogSummary{
		ID:                "request-log",
		AttemptCount:      2,
		AttemptFirstMs:    3,
		AttemptLastMs:     40,
		AttemptFinalStage: "reverse_roundtrip",
		AttemptFinalKind:  "timeout",
		AttemptDiagnostics: []proxy.RequestAttemptDiagnostic{{
			Attempt:   1,
			ErrorKind: "timeout",
		}},
	}

	listJSON, err := json.Marshal(toLogListItem(projector, row))
	if err != nil {
		t.Fatalf("marshal list item: %v", err)
	}
	if bytes.Contains(listJSON, []byte(`"attempt_diagnostics"`)) {
		t.Fatalf("list item exposed detail timeline: %s", listJSON)
	}
	if !bytes.Contains(listJSON, []byte(`"attempt_count":2`)) {
		t.Fatalf("list item omitted fixed attempt summary: %s", listJSON)
	}

	detailJSON, err := json.Marshal(toLogDetailItem(projector, row))
	if err != nil {
		t.Fatalf("marshal detail item: %v", err)
	}
	if !bytes.Contains(detailJSON, []byte(`"attempt_diagnostics"`)) {
		t.Fatalf("detail item omitted timeline: %s", detailJSON)
	}
}
