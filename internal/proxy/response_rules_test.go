package proxy

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"
)

func TestInspectResponseRuleBodyRestoresReadError(t *testing.T) {
	wantErr := errors.New("upstream body reset")
	resp := &http.Response{Body: &errorBody{
		data: []byte(`{"type":"FreeUsageLimitError"}`),
		err:  wantErr,
	}}

	gotPrefix := inspectResponseRuleBody(resp)
	if !bytes.Equal(gotPrefix, respBodyBytes(resp)) {
		t.Fatalf("inspected prefix: got %q", gotPrefix)
	}

	gotBody, err := io.ReadAll(resp.Body)
	if !bytes.Equal(gotBody, gotPrefix) {
		t.Fatalf("restored body: got %q, want %q", gotBody, gotPrefix)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("restored read error: got %v, want %v", err, wantErr)
	}
	_ = resp.Body.Close()
}

func respBodyBytes(resp *http.Response) []byte {
	return []byte(`{"type":"FreeUsageLimitError"}`)
}

type errorBody struct {
	data []byte
	err  error
}

func (b *errorBody) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, b.err
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func (b *errorBody) Close() error { return nil }
