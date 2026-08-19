package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
)

// ForwardProxyConfig holds dependencies for the forward proxy.
type ForwardProxyConfig struct {
	ProxyToken        string
	Router            *routing.Router
	Pool              outbound.PoolAccessor
	Health            HealthRecorder
	Events            EventEmitter
	MetricsSink       MetricsEventSink
	OutboundTransport OutboundTransportConfig
	TransportPool     *OutboundTransportPool
	ProxyBypassRules  []string
}

// ForwardProxy implements an HTTP forward proxy with Proxy-Authorization
// authentication, HTTP request forwarding, and CONNECT tunneling.
type ForwardProxy struct {
	token             string
	router            *routing.Router
	pool              outbound.PoolAccessor
	health            HealthRecorder
	events            EventEmitter
	metricsSink       MetricsEventSink
	transportConfig   OutboundTransportConfig
	transportPool     *OutboundTransportPool
	transportPoolOnce sync.Once
	directTransport   atomic.Pointer[http.Transport]
	directOnce        sync.Once
	bypass            *TargetBypassMatcher
}

// NewForwardProxy creates a new forward proxy handler.
func NewForwardProxy(cfg ForwardProxyConfig) *ForwardProxy {
	ev := cfg.Events
	if ev == nil {
		ev = NoOpEventEmitter{}
	}
	transportCfg := normalizeOutboundTransportConfig(cfg.OutboundTransport)
	transportPool := cfg.TransportPool
	if transportPool == nil {
		transportPool = NewOutboundTransportPool(transportCfg)
	}
	return &ForwardProxy{
		token:           cfg.ProxyToken,
		router:          cfg.Router,
		pool:            cfg.Pool,
		health:          cfg.Health,
		events:          ev,
		metricsSink:     cfg.MetricsSink,
		transportConfig: transportCfg,
		transportPool:   transportPool,
		bypass:          NewTargetBypassMatcher(cfg.ProxyBypassRules),
	}
}

func (p *ForwardProxy) outboundHTTPTransport(routed routedOutbound) *http.Transport {
	p.transportPoolOnce.Do(func() {
		if p.transportPool == nil {
			p.transportPool = NewOutboundTransportPool(p.transportConfig)
		}
	})
	return p.transportPool.Get(routed.Route.NodeHash, routed.Entry, routed.Outbound, p.metricsSink)
}

func (p *ForwardProxy) directHTTPTransport() *http.Transport {
	p.directOnce.Do(func() {
		p.directTransport.Store(newDirectHTTPTransport(p.transportConfig, p.metricsSink))
	})
	return p.directTransport.Load()
}

// CloseIdleConnections releases direct/bypass HTTP keep-alive connections.
// Routed transports are owned by OutboundTransportPool and are closed there.
func (p *ForwardProxy) CloseIdleConnections() {
	if p == nil {
		return
	}
	if transport := p.directTransport.Load(); transport != nil {
		transport.CloseIdleConnections()
	}
}

func (p *ForwardProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.handleCONNECT(w, r)
	} else {
		p.handleHTTP(w, r)
	}
}

// authenticate parses Proxy-Authorization and returns (platformName, account, error).
func (p *ForwardProxy) authenticate(r *http.Request) (string, string, *ProxyError) {
	return p.authenticateV1(r)
}

func (p *ForwardProxy) authenticateV1(r *http.Request) (string, string, *ProxyError) {
	auth := r.Header.Get("Proxy-Authorization")
	if p.token == "" {
		credential, ok := parseProxyAuthorizationCredentialV1(auth)
		if !ok {
			if requireProxyAuthInfo(r) {
				return "", "", ErrAuthRequired
			}
			return "", "", nil
		}
		if requireProxyAuthInfo(r) && !hasBasicUserInfo(credential) {
			return "", "", ErrAuthRequired
		}
		platName, account := parseForwardCredentialV1WhenAuthDisabled(credential)
		return platName, account, nil
	}

	credential, ok := parseProxyAuthorizationCredentialV1(auth)
	if !ok {
		return "", "", ErrAuthRequired
	}
	token, platName, account := parseForwardCredentialV1(credential)
	if token != p.token {
		return "", "", ErrAuthFailed
	}
	return platName, account, nil
}

func requireProxyAuthInfo(r *http.Request) bool {
	return r != nil && InboundPolicyFromContext(r.Context()).RequireProxyAuthInfo
}

func hasBasicUserInfo(credential string) bool {
	separator := strings.LastIndexByte(credential, ':')
	return separator > 0
}

// parseProxyAuthorizationCredentialV1 decodes Basic credential for V1
// forward-auth flows.
func parseProxyAuthorizationCredentialV1(auth string) (string, bool) {
	if auth == "" {
		return "", false
	}

	// Expect "<scheme> <base64>"; scheme is case-insensitive per RFC.
	authFields := strings.Fields(auth)
	if len(authFields) != 2 || !strings.EqualFold(authFields[0], "Basic") {
		return "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(authFields[1])
	if err != nil {
		return "", false
	}
	return string(decoded), true
}

// hop-by-hop headers that must not be forwarded to the next hop.
var hopByHopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"TE",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// stripHopByHopHeaders removes hop-by-hop headers from a header map,
// including any headers listed in the Connection header.
func stripHopByHopHeaders(header http.Header) {
	if header == nil {
		return
	}
	// Remove custom headers listed in Connection.
	for _, connHeaders := range header.Values("Connection") {
		for _, h := range strings.Split(connHeaders, ",") {
			if h = strings.TrimSpace(h); h != "" {
				header.Del(h)
			}
		}
	}
	for _, h := range hopByHopHeaders {
		header.Del(h)
	}
}

// copyEndToEndHeaders copies only end-to-end headers from src to dst and
// returns the canonical wire-format header length after filtering.
func copyEndToEndHeaders(dst, src http.Header) int64 {
	if dst == nil || src == nil {
		return 0
	}
	headers := src.Clone()
	stripHopByHopHeaders(headers)
	totalLen := headerWireLen(headers)
	for k, vv := range headers {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
	return totalLen
}

// prepareForwardOutboundRequest clones an inbound forward-proxy request into a
// client request suitable for http.Transport.RoundTrip.
func prepareForwardOutboundRequest(in *http.Request) *http.Request {
	req := in.Clone(in.Context())
	req.RequestURI = ""
	// Do not propagate client-side close semantics to upstream transport reuse.
	req.Close = false
	stripHopByHopHeaders(req.Header)
	return req
}

const responseRuleRetryBodyLimit = 8 << 20

// replayBodyCapture records a request body while the first upstream attempt
// consumes it. A retry is allowed only when the entire body was read without
// an error and stayed within the bounded memory limit.
type replayBodyCapture struct {
	src       io.ReadCloser
	data      bytes.Buffer
	limit     int
	mu        sync.Mutex
	abandoned bool
	overflow  bool
	complete  bool
	failed    bool
}

func newReplayBodyCapture(src io.ReadCloser) *replayBodyCapture {
	return &replayBodyCapture{src: src, limit: responseRuleRetryBodyLimit}
}

func (c *replayBodyCapture) Read(p []byte) (int, error) {
	if c == nil || c.src == nil {
		return 0, io.EOF
	}
	c.mu.Lock()
	if c.abandoned {
		c.mu.Unlock()
		return 0, io.EOF
	}
	c.mu.Unlock()
	n, err := c.src.Read(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abandoned {
		return n, err
	}
	if n > 0 && !c.overflow {
		remaining := c.limit - c.data.Len()
		if n > remaining {
			if remaining > 0 {
				_, _ = c.data.Write(p[:remaining])
			}
			c.overflow = true
		} else {
			_, _ = c.data.Write(p[:n])
		}
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			c.complete = true
		} else {
			c.failed = true
		}
	}
	return n, err
}

func (c *replayBodyCapture) Close() error {
	if c == nil || c.src == nil {
		return nil
	}
	return c.src.Close()
}

func (c *replayBodyCapture) abandon() {
	if c != nil {
		c.mu.Lock()
		c.abandoned = true
		c.mu.Unlock()
	}
}

func (c *replayBodyCapture) Replayable() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abandoned {
		return false
	}
	return c.complete && !c.failed && !c.overflow
}

func (c *replayBodyCapture) Bytes() []byte {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abandoned {
		return nil
	}
	return append([]byte(nil), c.data.Bytes()...)
}

func requestCanBeReplayed(req *http.Request, capture *replayBodyCapture) bool {
	if req == nil || req.Method == http.MethodConnect {
		return false
	}
	if capture == nil {
		return req.Body == nil || req.Body == http.NoBody
	}
	return capture.Replayable()
}

func cloneForwardRequestForRetry(base *http.Request, body []byte) *http.Request {
	retry := base.Clone(base.Context())
	retry.RequestURI = ""
	if len(body) == 0 {
		retry.Body = http.NoBody
		retry.GetBody = func() (io.ReadCloser, error) { return http.NoBody, nil }
		retry.ContentLength = 0
		return retry
	}
	retry.Body = io.NopCloser(bytes.NewReader(body))
	retry.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	retry.ContentLength = int64(len(body))
	return retry
}

func (p *ForwardProxy) handleHTTP(w http.ResponseWriter, r *http.Request) {
	platName, account, authErr := p.authenticate(r)
	if authErr != nil {
		writeProxyError(w, authErr)
		return
	}

	lifecycle := newRequestLifecycle(p.events, r, ProxyTypeForward, false)
	lifecycle.setTarget(r.Host, r.URL.String())
	defer lifecycle.finish()
	lifecycle.setAccount(account)

	baseReq := prepareForwardOutboundRequest(r)
	var bodyCapture *replayBodyCapture
	if baseReq.Body != nil && baseReq.Body != http.NoBody {
		bodyCapture = newReplayBodyCapture(baseReq.Body)
		baseReq.Body = bodyCapture
	}
	upstreamTrace := newUpstreamRequestTrace(lifecycle.markFirstByteReceived)

	var route routing.RouteResult
	var routeEntry *node.NodeEntry
	var hasRoute bool
	var resp *http.Response
	var roundTripErr error
	retryBudget := 1
	attemptedIPs := make(map[string]struct{})
	attemptedEntries := make(map[*node.NodeEntry]struct{})
	var pending *routedOutbound
	finalResponseAccepted := true
	for attempt := 0; attempt < retryBudget; attempt++ {
		upstreamAttemptTrace := upstreamTrace.newAttempt()
		var transport *http.Transport
		var routed routedOutbound
		if p.bypass != nil && p.bypass.ShouldBypass(r.Host) {
			transport = p.directHTTPTransport()
			hasRoute = false
		} else {
			if pending != nil {
				routed = *pending
				pending = nil
			} else {
				var routeErr *ProxyError
				routed, routeErr = resolveRoutedOutbound(p.router, p.pool, platName, account, r.Host)
				if routeErr != nil {
					lifecycle.setProxyError(routeErr)
					lifecycle.setHTTPStatus(routeErr.HTTPCode)
					writeProxyError(w, routeErr)
					return
				}
			}
			if attempt == 0 {
				retryBudget = routed.Route.RetryBudget
				if retryBudget < 1 {
					retryBudget = 1
				}
			}
			if _, duplicateIP := attemptedIPs[routed.Route.EgressIP.String()]; duplicateIP {
				break
			}
			if _, duplicateEntry := attemptedEntries[routed.Entry]; duplicateEntry {
				break
			}
			attemptedIPs[routed.Route.EgressIP.String()] = struct{}{}
			attemptedEntries[routed.Entry] = struct{}{}
			route = routed.Route
			routeEntry = routed.Entry
			hasRoute = true
			lifecycle.setRouteResult(route)
			if p.health != nil {
				recordLatencyAsync(p.health, route.NodeHash, routeEntry, netutil.ExtractDomain(r.Host), nil)
			}
			transport = p.outboundHTTPTransport(routed)
		}

		var outReq *http.Request
		if attempt == 0 {
			outReq = baseReq
		} else {
			outReq = cloneForwardRequestForRetry(baseReq, bodyCapture.Bytes())
		}
		outReq = outReq.WithContext(httptrace.WithClientTrace(outReq.Context(), upstreamAttemptTrace.clientTrace()))
		pendingEgressHeaderBytes := headerWireLen(outReq.Header)

		// Forward the request. A response-rule retry is considered only before
		// any response bytes are written to the downstream client.
		var bodyBytes int64
		var bodyComplete bool
		resp, roundTripErr, bodyBytes, bodyComplete = roundTripWithBodyCompletion(
			r.Context(), transport, outReq,
		)
		if !bodyComplete {
			resp = nil
		}
		if bodyComplete && upstreamAttemptTrace.commitEgress(resp != nil && roundTripErr == nil, roundTripErr) {
			lifecycle.addEgressBytes(pendingEgressHeaderBytes)
			lifecycle.addEgressBytes(bodyBytes)
		}
		if roundTripErr != nil {
			proxyErr := classifyUpstreamError(roundTripErr)
			if proxyErr == nil {
				// context.Canceled — skip health recording, close silently.
				lifecycle.setNetOK(true)
				return
			}
			lifecycle.setProxyError(proxyErr)
			lifecycle.setUpstreamError("forward_roundtrip", roundTripErr)
			lifecycle.setHTTPStatus(proxyErr.HTTPCode)
			if hasRoute {
				recordPassiveResultAsync(p.health, route, routeEntry, false)
			}
			writeProxyError(w, proxyErr)
			return
		}

		var responseMatch platform.ResponseRuleMatch
		matchedRule := false
		if hasRoute {
			responseMatch, matchedRule = applyResponseRules(p.router, route, resp)
		}
		finalResponseAccepted = !matchedRule || responseMatch.Action == platform.ResponseRuleActionPassthrough
		if matchedRule && responseMatch.RetryNext() && requestCanBeReplayed(r, bodyCapture) && attempt+1 < retryBudget {
			if r.Context().Err() != nil {
				_ = resp.Body.Close()
				lifecycle.setNetOK(true)
				return
			}
			exclusions := routing.RouteRetryExclusions{
				Entries:   make([]*node.NodeEntry, 0, len(attemptedEntries)),
				EgressIPs: make([]netip.Addr, 0, len(attemptedIPs)),
			}
			for entry := range attemptedEntries {
				exclusions.Entries = append(exclusions.Entries, entry)
			}
			for rawIP := range attemptedIPs {
				if ip, err := netip.ParseAddr(rawIP); err == nil {
					exclusions.EgressIPs = append(exclusions.EgressIPs, ip)
				}
			}
			nextRoute, nextErr := p.router.RouteRequestNext(route, exclusions)
			if nextErr == nil {
				next, bindErr := bindRoutedOutbound(nextRoute, p.pool)
				if bindErr != nil {
					break
				}
				if _, duplicateIP := attemptedIPs[next.Route.EgressIP.String()]; duplicateIP {
					break
				}
				if _, duplicateEntry := attemptedEntries[next.Entry]; duplicateEntry {
					break
				}
				_ = resp.Body.Close()
				pending = &next
				continue
			}
		}
		break
	}
	if resp == nil {
		return
	}
	defer resp.Body.Close()

	// Response headers have been accepted by the response policy and no
	// downstream bytes have been committed yet. Publish the sticky owner at
	// this boundary so an open SSE/long-lived body does not leave later
	// requests without the successful route.
	if hasRoute && finalResponseAccepted {
		p.router.CommitRouteForAccount(route, account)
	}

	lifecycle.setHTTPStatus(resp.StatusCode)
	lifecycle.setNetOK(true)

	// Copy end-to-end response headers and body.
	lifecycle.addIngressBytes(copyEndToEndHeaders(w.Header(), resp.Header))
	w.WriteHeader(resp.StatusCode)
	copiedBytes, copyErr := io.Copy(w, resp.Body)
	lifecycle.addIngressBytes(copiedBytes)
	if copyErr != nil {
		if shouldRecordForwardCopyFailure(r, copyErr) {
			lifecycle.setProxyError(ErrUpstreamRequestFailed)
			lifecycle.setUpstreamError("forward_upstream_to_client_copy", copyErr)
			lifecycle.setNetOK(false)
			if hasRoute {
				recordPassiveResultAsync(p.health, route, routeEntry, false)
			}
		}
		return
	}

	// Full body transfer succeeded — count as network success even for 5xx HTTP.
	if hasRoute {
		recordPassiveResultAsync(p.health, route, routeEntry, true)
	}
}

func (p *ForwardProxy) handleCONNECT(w http.ResponseWriter, r *http.Request) {
	target := r.Host
	platName, account, authErr := p.authenticate(r)
	if authErr != nil {
		writeProxyError(w, authErr)
		return
	}

	lifecycle := newRequestLifecycle(p.events, r, ProxyTypeForward, true)
	lifecycle.setTarget(target, "")
	defer lifecycle.finish()
	lifecycle.setAccount(account)

	prepare := prepareConnectTunnel(
		r.Context(),
		tunnelDeps{
			router:      p.router,
			pool:        p.pool,
			health:      p.health,
			metricsSink: p.metricsSink,
			bypass:      p.bypass,
		},
		platName,
		account,
		target,
	)
	if prepare.route.PlatformID != "" {
		lifecycle.setRouteResult(prepare.route)
	}
	if prepare.session == nil {
		if prepare.proxyErr != nil {
			lifecycle.setProxyError(prepare.proxyErr)
			if prepare.upstreamStage != "" {
				lifecycle.setUpstreamError(prepare.upstreamStage, prepare.upstreamErr)
			}
			lifecycle.setHTTPStatus(prepare.proxyErr.HTTPCode)
			writeProxyError(w, prepare.proxyErr)
		} else if prepare.canceled {
			lifecycle.setNetOK(true)
		}
		return
	}

	// Hijack the client connection.
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		prepare.session.upstreamConn.Close()
		lifecycle.setProxyError(ErrUpstreamRequestFailed)
		lifecycle.setUpstreamError("connect_hijack", errors.New("response writer does not support hijacking"))
		lifecycle.setHTTPStatus(ErrUpstreamRequestFailed.HTTPCode)
		prepare.session.recordResult(false)
		writeProxyError(w, ErrUpstreamRequestFailed)
		return
	}

	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		prepare.session.upstreamConn.Close()
		lifecycle.setProxyError(ErrUpstreamRequestFailed)
		lifecycle.setUpstreamError("connect_hijack", err)
		prepare.session.recordResult(false)
		return
	}

	// Write the raw CONNECT success line with proper reason phrase.
	if _, err := clientBuf.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		prepare.session.upstreamConn.Close()
		clientConn.Close()
		lifecycle.setProxyError(ErrUpstreamRequestFailed)
		lifecycle.setUpstreamError("connect_client_response_write", err)
		lifecycle.setNetOK(false)
		return
	}
	if err := clientBuf.Flush(); err != nil {
		prepare.session.upstreamConn.Close()
		clientConn.Close()
		lifecycle.setProxyError(ErrUpstreamRequestFailed)
		lifecycle.setUpstreamError("connect_client_response_flush", err)
		lifecycle.setNetOK(false)
		return
	}
	// The CONNECT success response is the acceptance boundary for a tunnel;
	// publish its sticky owner before relay bytes can flow downstream.
	if prepare.route.PlatformID != "" {
		p.router.CommitRouteForAccount(prepare.route, account)
	}
	lifecycle.setHTTPStatus(http.StatusOK)
	relay := pumpPreparedTunnel(clientConn, clientBuf.Reader, prepare.session, tunnelPumpOptions{
		ctx:                         r.Context(),
		requireBidirectionalTraffic: true,
		onFirstIngressByte:          lifecycle.markFirstByteReceived,
	})
	lifecycle.addIngressBytes(relay.ingressBytes)
	lifecycle.addEgressBytes(relay.egressBytes)
	if relay.proxyErr != nil {
		lifecycle.setProxyError(relay.proxyErr)
		lifecycle.setUpstreamError(relay.upstreamStage, relay.upstreamErr)
	}
	lifecycle.setNetOK(relay.netOK)
	if !relay.canceled {
		prepare.session.recordResult(relay.netOK)
	}
}

// shouldRecordForwardCopyFailure decides whether an HTTP response body copy
// error should be treated as an upstream/node failure.
func shouldRecordForwardCopyFailure(r *http.Request, copyErr error) bool {
	if copyErr == nil {
		return false
	}
	// Client-side cancellation while streaming should not penalise node health.
	if r != nil && errors.Is(r.Context().Err(), context.Canceled) {
		return false
	}
	return classifyUpstreamError(copyErr) != nil
}
