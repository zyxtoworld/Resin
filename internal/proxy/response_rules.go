package proxy

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"github.com/Resinat/Resin/internal/routing"
)

const responseRuleBodyInspectLimit = 1 << 20

// applyResponseRules inspects only responses for which the platform has
// configured a body rule, then quarantines the route until the parsed deadline.
// The response body is restored before the caller forwards it to the client.
func applyResponseRules(router *routing.Router, route routing.RouteResult, resp *http.Response) {
	if router == nil || resp == nil || len(route.ResponseRules) == 0 {
		return
	}

	var body []byte
	if route.ResponseRules.NeedsBodyForStatus(resp.StatusCode) {
		body = inspectResponseRuleBody(resp)
	}
	match, ok := route.ResponseRules.Match(resp.StatusCode, body, resp.Header, time.Now())
	if ok {
		router.QuarantineRoute(route, match.Scope, match.Until)
	}
}

func inspectResponseRuleBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil || resp.Body == http.NoBody {
		return nil
	}

	original := resp.Body
	data, readErr := io.ReadAll(io.LimitReader(original, responseRuleBodyInspectLimit+1))
	if len(data) <= responseRuleBodyInspectLimit {
		if readErr == nil {
			_ = original.Close()
			resp.Body = io.NopCloser(bytes.NewReader(data))
		} else {
			resp.Body = &replayReadCloser{
				prefix:   bytes.NewReader(data),
				terminal: readErr,
				closer:   original,
			}
		}
		return data
	}

	prefix := append([]byte(nil), data[:responseRuleBodyInspectLimit]...)
	rest := io.Reader(bytes.NewReader(data[responseRuleBodyInspectLimit:]))
	if readErr == nil {
		rest = io.MultiReader(rest, original)
	}
	resp.Body = &replayReadCloser{
		prefix:   bytes.NewReader(prefix),
		rest:     rest,
		terminal: readErr,
		closer:   original,
	}
	return prefix
}

type replayReadCloser struct {
	prefix   *bytes.Reader
	rest     io.Reader
	terminal error
	closer   io.Closer
}

func (r *replayReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.prefix != nil && r.prefix.Len() > 0 {
		return r.prefix.Read(p)
	}
	if r.rest != nil {
		n, err := r.rest.Read(p)
		if err == io.EOF {
			r.rest = nil
			if r.terminal != nil {
				err = r.terminal
				r.terminal = nil
			}
		}
		return n, err
	}
	if r.terminal != nil {
		err := r.terminal
		r.terminal = nil
		return 0, err
	}
	return 0, io.EOF
}

func (r *replayReadCloser) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}
