package proxy

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"regexp"
	"testing"

	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
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

func TestApplyResponseRulesDoesNotReadBodyForNonMatchingStatus(t *testing.T) {
	body := &countingResponseBody{Reader: bytes.NewReader([]byte("large enough response"))}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       body,
	}
	route := routing.RouteResult{
		ResponseRules: platform.ResponseRules{{
			StatusCodes:   []int{http.StatusTooManyRequests},
			ResponseRegex: regexp.MustCompile(`quota`),
			Scope:         platform.ResponseRuleScopeNode,
		}},
	}

	applyResponseRules(routing.NewRouter(routing.RouterConfig{}), route, resp)

	if body.readBytes != 0 {
		t.Fatalf("body bytes read for non-matching status: got %d, want 0", body.readBytes)
	}
}

func TestInspectResponseRuleBodyCapsInspectionAndRestoresFullBody(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), responseRuleBodyInspectLimit*2)
	body := &countingResponseBody{Reader: bytes.NewReader(payload)}
	resp := &http.Response{Body: body}

	gotPrefix := inspectResponseRuleBody(resp)
	if len(gotPrefix) != responseRuleBodyInspectLimit {
		t.Fatalf("inspected body length: got %d, want %d", len(gotPrefix), responseRuleBodyInspectLimit)
	}
	if body.readBytes > responseRuleBodyInspectLimit+1 {
		t.Fatalf("body read past inspection bound: got %d bytes, want at most %d", body.readBytes, responseRuleBodyInspectLimit+1)
	}

	gotBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read restored body: %v", err)
	}
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("restored body length: got %d, want %d", len(gotBody), len(payload))
	}
	_ = resp.Body.Close()
}

func TestInspectResponseRuleBodyRestoresLargeBodyTerminalErrorOnce(t *testing.T) {
	sentinelErr := errors.New("large upstream body reset")
	payload := bytes.Repeat([]byte("y"), responseRuleBodyInspectLimit+1)
	body := &terminalOnLastReadBody{payload: payload, err: sentinelErr}
	resp := &http.Response{Body: body}

	gotPrefix := inspectResponseRuleBody(resp)
	if !bytes.Equal(gotPrefix, payload[:responseRuleBodyInspectLimit]) {
		t.Fatalf("inspected prefix length: got %d, want %d", len(gotPrefix), responseRuleBodyInspectLimit)
	}
	if n, err := resp.Body.Read([]byte{}); n != 0 || err != nil {
		t.Fatalf("zero-length read: n=%d err=%v", n, err)
	}

	gotBody, gotErr := readResponseBodyInChunks(resp.Body, 7)
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("restored body length: got %d, want %d", len(gotBody), len(payload))
	}
	if !errors.Is(gotErr, sentinelErr) {
		t.Fatalf("restored terminal error: got %v, want %v", gotErr, sentinelErr)
	}

	buf := make([]byte, 1)
	if n, err := resp.Body.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("terminal error was not consumed exactly once: n=%d err=%v", n, err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close restored body: %v", err)
	}
	if !body.closed {
		t.Fatal("close was not forwarded to original body")
	}
}

func readResponseBodyInChunks(r io.Reader, chunkSize int) ([]byte, error) {
	var got bytes.Buffer
	buf := make([]byte, chunkSize)
	for {
		n, err := r.Read(buf)
		got.Write(buf[:n])
		if err != nil {
			return got.Bytes(), err
		}
	}
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

type countingResponseBody struct {
	io.Reader
	readBytes int
}

func (b *countingResponseBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.readBytes += n
	return n, err
}

func (b *countingResponseBody) Close() error { return nil }

type terminalOnLastReadBody struct {
	payload []byte
	err     error
	offset  int
	closed  bool
}

func (b *terminalOnLastReadBody) Read(p []byte) (int, error) {
	if len(b.payload) == b.offset {
		return 0, io.EOF
	}
	n := copy(p, b.payload[b.offset:])
	b.offset += n
	if b.offset == len(b.payload) {
		return n, b.err
	}
	return n, nil
}

func (b *terminalOnLastReadBody) Close() error {
	b.closed = true
	return nil
}
