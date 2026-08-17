package proxy

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
)

const responseRuleBodyInspectLimit = 1 << 20

// applyResponseRules inspects only responses for which the platform has
// configured a body rule, then quarantines the route until the parsed deadline.
// The response body is restored before the caller forwards it to the client.
// It returns whether a rule matched; callers may use that fact to make a
// bounded, replay-safe retry before writing any response bytes downstream.
func applyResponseRules(router *routing.Router, route routing.RouteResult, resp *http.Response) (platform.ResponseRuleMatch, bool) {
	if router == nil || resp == nil || len(route.ResponseRules) == 0 {
		return platform.ResponseRuleMatch{}, false
	}

	var inspection responseRuleBodyInspection
	if route.ResponseRules.NeedsBodyForStatus(resp.StatusCode) {
		inspection = inspectResponseRuleBodyDetailed(resp)
	}
	match, ok := route.ResponseRules.Match(resp.StatusCode, inspection.prefix, inspection.complete, resp.Header, time.Now())
	if ok && match.Cooldown {
		router.QuarantineRoute(route, match.Scope, match.Until)
	}
	return match, ok
}

func inspectResponseRuleBody(resp *http.Response) []byte {
	return inspectResponseRuleBodyDetailed(resp).prefix
}

type responseRuleBodyInspection struct {
	prefix   []byte
	complete bool
}

func inspectResponseRuleBodyDetailed(resp *http.Response) responseRuleBodyInspection {
	if resp == nil || resp.Body == nil || resp.Body == http.NoBody {
		return responseRuleBodyInspection{complete: true}
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
		return responseRuleBodyInspection{prefix: data, complete: readErr == nil}
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
	return responseRuleBodyInspection{prefix: prefix, complete: false}
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
