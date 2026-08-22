package proxy

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestAttemptDiagnosticElapsedMilestoneNeverUsesZeroForObservedSubMillisecondTime(t *testing.T) {
	d := &attemptDiagnostic{base: time.Now().Add(-500 * time.Microsecond)}
	if got := d.elapsedMs(); got < 1 {
		t.Fatalf("elapsedMs() = %d for an observed milestone, want at least 1", got)
	}
}

func TestAttemptDiagnosticPreservesPartialResponseBytesAndCompleteness(t *testing.T) {
	partial := &attemptDiagnostic{base: time.Now()}
	partial.markBodyFinish(7, false)
	partialValue := partial.snapshot()
	if partialValue.ResponseBodyBytes != 7 {
		t.Fatalf("partial ResponseBodyBytes = %d, want observed 7", partialValue.ResponseBodyBytes)
	}
	partialJSON, err := json.Marshal(partialValue)
	if err != nil {
		t.Fatalf("marshal partial diagnostic: %v", err)
	}
	if !strings.Contains(string(partialJSON), `"response_body_complete":false`) {
		t.Fatalf("partial diagnostic lacks explicit incomplete marker: %s", partialJSON)
	}

	complete := &attemptDiagnostic{base: time.Now()}
	complete.markBodyFinish(9, true)
	completeJSON, err := json.Marshal(complete.snapshot())
	if err != nil {
		t.Fatalf("marshal complete diagnostic: %v", err)
	}
	if !strings.Contains(string(completeJSON), `"response_body_complete":true`) {
		t.Fatalf("complete diagnostic lacks explicit complete marker: %s", completeJSON)
	}
}

func TestAttemptDiagnosticResponseBodyKeepsObservedPartialBytesAndClosesOnce(t *testing.T) {
	inner := &closeCountingResponseBody{reader: strings.NewReader("partial")}
	diagnostic := &attemptDiagnostic{base: time.Now()}
	body := &attemptDiagnosticResponseBody{ReadCloser: inner, diagnostic: diagnostic}
	buf := make([]byte, 3)
	if n, err := body.Read(buf); n != 3 || err != nil {
		t.Fatalf("partial response read = (%d, %v), want (3, nil)", n, err)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("partial response close: %v", err)
	}
	if got := inner.closes.Load(); got != 1 {
		t.Fatalf("underlying response close count = %d, want 1", got)
	}
	value := diagnostic.snapshot()
	if value.ResponseBodyBytes != 3 || value.ResponseBodyComplete {
		t.Fatalf("partial response diagnostic = bytes:%d complete:%v, want bytes:3 complete:false", value.ResponseBodyBytes, value.ResponseBodyComplete)
	}
}
