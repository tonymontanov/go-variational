package variational

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Version is the SDK version reported in the User-Agent header.
const Version = "0.1.0"

// Public endpoints of Variational Omni.
const (
	MainnetBaseURL = "https://omni-client-api.prod.ap-northeast-1.variational.io"
	MainnetWSURL   = "wss://omni-ws-server.prod.ap-northeast-1.variational.io"
	TestnetBaseURL = "https://omni-client-api.testnet.ap-northeast-1.variational.io"
	TestnetWSURL   = "wss://omni-ws-server.testnet.ap-northeast-1.variational.io"

	// DefaultUserAgent identifies the SDK to the exchange.
	DefaultUserAgent = "go-variational/" + Version + " (+https://github.com/tonymontanov/go-variational)"

	// PublicRateLimitRequests and PublicRateLimitWindow are the documented
	// per-IP budget of the public REST API (10 requests per 10 seconds). Pass
	// them to WithRateLimit to enforce the budget client-side.
	PublicRateLimitRequests = 10
	PublicRateLimitWindow   = 10 * time.Second
)

// Option configures a Client.
type Option func(*Client)

// WithBaseURL overrides the REST base URL (no trailing slash).
func WithBaseURL(u string) Option {
	return func(c *Client) { c.baseURL = trimSlash(u) }
}

// WithWSURL overrides the WebSocket base URL (no trailing slash, no path).
func WithWSURL(u string) Option {
	return func(c *Client) { c.wsURL = trimSlash(u) }
}

// WithTestnet points the client at the Omni testnet.
func WithTestnet() Option {
	return func(c *Client) {
		c.baseURL = TestnetBaseURL
		c.wsURL = TestnetWSURL
	}
}

// WithHTTPClient supplies a custom *http.Client (proxy, transport tuning,
// tracing). The client's own Timeout is ignored in favour of WithTimeout.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.httpc = h }
}

// WithTimeout sets the per-request timeout applied to every REST call in
// addition to the caller's context. Zero disables it. Default: 10s.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

// WithUserAgent overrides the User-Agent header used for REST and WebSocket.
func WithUserAgent(ua string) Option {
	return func(c *Client) { c.userAgent = ua }
}

// WithHeader adds a static header to every REST request and WebSocket handshake.
func WithHeader(key, value string) Option {
	return func(c *Client) {
		if c.headers == nil {
			c.headers = http.Header{}
		}
		c.headers.Add(key, value)
	}
}

// WithRateLimit enables a client-side token bucket that allows at most
// requests calls per window (bursting up to requests). requests <= 0 disables
// it. The limiter is off by default; use the PublicRateLimit constants to match
// the exchange budget.
func WithRateLimit(requests int, window time.Duration) Option {
	return func(c *Client) {
		if requests <= 0 || window <= 0 {
			c.limiter = nil
			return
		}
		c.limiter = newRateLimiter(requests, window)
	}
}

// WithRetry sets the REST retry policy. See RetryPolicy.
func WithRetry(p RetryPolicy) Option {
	return func(c *Client) { c.retry = p }
}

// WithDialer supplies a custom WebSocket dialer (proxy, TLS config, buffers).
func WithDialer(d *websocket.Dialer) Option {
	return func(c *Client) { c.dialer = d }
}

// WithReconnect sets the default reconnect policy for streams created from the client.
func WithReconnect(p ReconnectPolicy) Option {
	return func(c *Client) { c.reconnect = p }
}

// WithMaxResponseSize caps the REST response body size. Default: 32 MiB.
func WithMaxResponseSize(n int64) Option {
	return func(c *Client) { c.maxBody = n }
}

// RetryPolicy controls REST retries. Network errors, HTTP 5xx and HTTP 429 are
// retried with exponential backoff and jitter; 429 honours Retry-After when it
// is at most MaxRetryAfter, otherwise the call fails immediately with
// ErrRateLimited so the caller can decide.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts; 1 or less means no retry.
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	// MaxRetryAfter caps the Retry-After the client is willing to sleep for.
	MaxRetryAfter time.Duration
}

// DefaultRetryPolicy is used when WithRetry is not given.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts:   3,
	BaseDelay:     200 * time.Millisecond,
	MaxDelay:      2 * time.Second,
	MaxRetryAfter: 5 * time.Second,
}

func trimSlash(u string) string {
	for len(u) > 0 && u[len(u)-1] == '/' {
		u = u[:len(u)-1]
	}
	return u
}
