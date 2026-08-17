package netutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"time"
)

// maxResourceBodyBytes bounds remote resource responses (subscriptions and
// GeoIP). A remote server is not allowed to turn a timed request into an
// unbounded allocation in the process.
const maxResourceBodyBytes = 16 << 20

func readResourceBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, int64(maxResourceBodyBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxResourceBodyBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxResourceBodyBytes)
	}
	return data, nil
}

func validateResourceContentLength(contentLength int64) error {
	if contentLength > maxResourceBodyBytes {
		return fmt.Errorf("response body exceeds %d bytes", maxResourceBodyBytes)
	}
	return nil
}

// HTTPStatusError indicates the server responded, but with an unexpected
// HTTP status code. This is a non-network failure.
type HTTPStatusError struct {
	StatusCode int
	URL        string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("downloader: unexpected status %d from %s", e.StatusCode, redactURLCredentials(e.URL))
}

type downloadRequestError struct {
	url string
	err error
}

func (e *downloadRequestError) Error() string {
	return fmt.Sprintf("downloader: request %s failed: %v", e.url, e.err)
}

func (e *downloadRequestError) Unwrap() error { return e.err }

func redactURLCredentials(raw string) string {
	u, err := neturl.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "[redacted-url]"
	}
	// Subscription URLs are secrets as a whole: credentials may be in
	// userinfo, query parameters, or path tokens. Keep only the origin for
	// useful status diagnostics and never echo the rest.
	return u.Scheme + "://" + u.Host
}

// unwrapURLRequestError removes net/url's error wrapper because its Error
// method formats the complete request URL. Keep the underlying cause so
// callers can still use errors.Is/As without exposing subscription secrets.
func unwrapURLRequestError(err error) error {
	underlying := err
	for {
		var urlErr *neturl.Error
		if !errors.As(underlying, &urlErr) || urlErr == nil || urlErr.Err == nil {
			return underlying
		}
		underlying = urlErr.Err
	}
}

// NonRetryableError indicates direct request setup failed before any transport
// attempt was made (for example, malformed URL).
type NonRetryableError struct {
	Err error
	url string
}

func (e *NonRetryableError) Error() string {
	if e.url != "" {
		// net/http's parse errors include the complete input URL. Keep the
		// cause available through Unwrap, but never format that raw text.
		return fmt.Sprintf("downloader: invalid request URL %s", e.url)
	}
	return fmt.Sprintf("downloader: %v", e.Err)
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

// Downloader fetches remote resources. Interface allows for proxy-aware
// implementations in later phases.
type Downloader interface {
	Download(ctx context.Context, url string) ([]byte, error)
}

// DirectDownloader downloads via a standard HTTP client (no proxy).
type DirectDownloader struct {
	Client      *http.Client
	TimeoutFn   func() time.Duration
	UserAgentFn func() string
}

// NewDirectDownloader creates a downloader that pulls timeout/user-agent
// from callbacks on each request.
func NewDirectDownloader(timeoutFn func() time.Duration, userAgentFn func() string) *DirectDownloader {
	if timeoutFn == nil {
		panic("netutil: NewDirectDownloader requires non-nil timeoutFn")
	}
	if userAgentFn == nil {
		panic("netutil: NewDirectDownloader requires non-nil userAgentFn")
	}
	return &DirectDownloader{
		Client:      &http.Client{},
		TimeoutFn:   timeoutFn,
		UserAgentFn: userAgentFn,
	}
}

// Download fetches the URL and returns the response body.
func (d *DirectDownloader) Download(ctx context.Context, url string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := d.currentTimeout()
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &NonRetryableError{Err: err, url: redactURLCredentials(url)}
	}
	userAgent := d.currentUserAgent()
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}

	client := d.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		underlying := unwrapURLRequestError(err)
		return nil, &downloadRequestError{
			url: redactURLCredentials(url),
			err: underlying,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPStatusError{StatusCode: resp.StatusCode, URL: redactURLCredentials(url)}
	}

	if err := validateResourceContentLength(resp.ContentLength); err != nil {
		return nil, fmt.Errorf("downloader: %w", err)
	}
	body, err := readResourceBody(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("downloader: %w", err)
	}
	return body, nil
}

func (d *DirectDownloader) currentTimeout() time.Duration {
	return d.TimeoutFn()
}

func (d *DirectDownloader) currentUserAgent() string {
	return d.UserAgentFn()
}
