package variational

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// REST paths of the public client API.
const (
	pathStats  = "/metadata/stats"
	pathStatus = "/status"
)

// Client is the entry point of the SDK. It is safe for concurrent use. Create
// one with NewClient; the zero value is not usable.
type Client struct {
	baseURL   string
	wsURL     string
	httpc     *http.Client
	timeout   time.Duration
	userAgent string
	headers   http.Header
	limiter   *rateLimiter
	retry     RetryPolicy
	dialer    *websocket.Dialer
	reconnect ReconnectPolicy
	maxBody   int64
}

// NewClient returns a client for Omni mainnet configured by opts.
func NewClient(opts ...Option) *Client {
	c := &Client{
		baseURL:   MainnetBaseURL,
		wsURL:     MainnetWSURL,
		timeout:   10 * time.Second,
		userAgent: DefaultUserAgent,
		retry:     DefaultRetryPolicy,
		reconnect: DefaultReconnectPolicy,
		maxBody:   32 << 20,
	}
	for _, o := range opts {
		o(c)
	}
	if c.httpc == nil {
		c.httpc = &http.Client{Transport: newTransport()}
	}
	if c.dialer == nil {
		c.dialer = &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 10 * time.Second,
			ReadBufferSize:   32 << 10,
			WriteBufferSize:  4 << 10,
		}
	}
	return c
}

func newTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          8,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// BaseURL returns the REST base URL in use.
func (c *Client) BaseURL() string { return c.baseURL }

// WSURL returns the WebSocket base URL in use.
func (c *Client) WSURL() string { return c.wsURL }

// Close releases idle HTTP connections. Streams are closed individually.
func (c *Client) Close() { c.httpc.CloseIdleConnections() }

// GetStats fetches platform-wide and per-listing market statistics
// (GET /metadata/stats). The exchange caches the document for 30-60 seconds and
// the indicative quotes inside it for up to 600 seconds; polling faster than
// that only spends rate-limit budget.
func (c *Client) GetStats(ctx context.Context) (*Stats, error) {
	var s Stats
	if _, err := c.getJSON(ctx, pathStats, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// GetStatsRaw fetches the raw JSON body of GET /metadata/stats for callers that
// prefer their own decoder.
func (c *Client) GetStatsRaw(ctx context.Context) ([]byte, error) {
	r, err := c.do(ctx, pathStats)
	if err != nil {
		return nil, err
	}
	return r.body, nil
}

// GetServerTime fetches the exchange clock (GET /status) and measures the
// round trip so the caller can estimate its clock offset.
func (c *Client) GetServerTime(ctx context.Context) (ServerTime, error) {
	var out statusResponse
	r, err := c.getJSON(ctx, pathStatus, &out)
	if err != nil {
		return ServerTime{}, err
	}
	st := ServerTime{
		Time: time.Unix(0, out.ServerTsNs).UTC(),
		RTT:  r.receivedAt.Sub(r.sentAt),
	}
	st.Offset = st.Time.Sub(r.sentAt.Add(st.RTT / 2))
	return st, nil
}

type response struct {
	status     int
	body       []byte
	sentAt     time.Time
	receivedAt time.Time
}

func (c *Client) getJSON(ctx context.Context, path string, out any) (*response, error) {
	r, err := c.do(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(r.body, out); err != nil {
		return nil, &Error{Kind: ErrKindInvalidResponse, Op: "GET " + path, HTTPStatus: r.status, Err: err}
	}
	return r, nil
}

// do performs a GET with rate limiting and retries and returns the body of a
// 2xx response.
func (c *Client) do(ctx context.Context, path string) (*response, error) {
	op := "GET " + path
	attempts := max(c.retry.MaxAttempts, 1)
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		if c.limiter != nil {
			if err := c.limiter.wait(ctx); err != nil {
				return nil, &Error{Kind: ErrKindNetwork, Op: op, Err: err}
			}
		}
		r, err := c.once(ctx, op, path)
		if err == nil {
			return r, nil
		}
		last = err
		if ctx.Err() != nil {
			return nil, err
		}
		delay, retry := c.retryDelay(err, attempt, attempts)
		if !retry {
			return nil, err
		}
		if !sleepCtx(ctx, delay) {
			return nil, &Error{Kind: ErrKindNetwork, Op: op, Err: ctx.Err()}
		}
	}
	return nil, last
}

func (c *Client) retryDelay(err error, attempt, attempts int) (time.Duration, bool) {
	if attempt >= attempts {
		return 0, false
	}
	var e *Error
	if !errors.As(err, &e) {
		return 0, false
	}
	switch e.Kind {
	case ErrKindRateLimit:
		if e.RetryAfter > c.retry.MaxRetryAfter {
			return 0, false
		}
		if e.RetryAfter > 0 {
			return e.RetryAfter, true
		}
	case ErrKindNetwork, ErrKindExchange:
	default:
		return 0, false
	}
	return backoff(c.retry.BaseDelay, c.retry.MaxDelay, 2, 0.25, attempt), true
}

func (c *Client) once(ctx context.Context, op, path string) (*response, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, &Error{Kind: ErrKindInvalidRequest, Op: op, Err: err}
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	for k, vs := range c.headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	sentAt := time.Now()
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, &Error{Kind: ErrKindNetwork, Op: op, Err: err}
	}
	defer resp.Body.Close()
	body, err := c.readBody(resp)
	receivedAt := time.Now()
	if err != nil {
		return nil, &Error{Kind: ErrKindNetwork, Op: op, HTTPStatus: resp.StatusCode, Err: err}
	}
	if resp.StatusCode/100 != 2 {
		return nil, httpError(op, resp, body)
	}
	return &response{status: resp.StatusCode, body: body, sentAt: sentAt, receivedAt: receivedAt}, nil
}

func (c *Client) readBody(resp *http.Response) ([]byte, error) {
	size := resp.ContentLength
	if size < 0 || size > c.maxBody {
		size = 64 << 10
	}
	buf := bytes.NewBuffer(make([]byte, 0, size+1))
	n, err := buf.ReadFrom(io.LimitReader(resp.Body, c.maxBody+1))
	if err != nil {
		return nil, err
	}
	if n > c.maxBody {
		return nil, errors.New("response body exceeds maximum size")
	}
	return buf.Bytes(), nil
}

// httpError maps a non-2xx response to an *Error.
func httpError(op string, resp *http.Response, body []byte) *Error {
	e := &Error{Op: op, HTTPStatus: resp.StatusCode, Message: bodyExcerpt(body)}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		e.Kind = ErrKindRateLimit
		e.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		e.Kind = ErrKindForbidden
		if isBrowserChallenge(resp, body) {
			e.Message = "browser challenge: endpoint is not available to non-browser clients"
		}
	case resp.StatusCode >= 500:
		e.Kind = ErrKindExchange
	case resp.StatusCode >= 400:
		e.Kind = ErrKindInvalidRequest
	default:
		e.Kind = ErrKindUnknown
	}
	return e
}

func isBrowserChallenge(resp *http.Response, body []byte) bool {
	return strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") &&
		(bytes.Contains(body, []byte("Just a moment")) || bytes.Contains(body, []byte("cf-chl")) ||
			resp.Header.Get("cf-mitigated") != "")
}

func bodyExcerpt(body []byte) string {
	b := bytes.TrimSpace(body)
	if len(b) == 0 {
		return ""
	}
	if bytes.HasPrefix(b, []byte("<")) {
		return "<html response>"
	}
	const max = 200
	if len(b) > max {
		b = b[:max]
	}
	return string(b)
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// backoff returns base*mult^(attempt-1) capped at maxDelay with ±jitter applied.
func backoff(base, maxDelay time.Duration, mult, jitter float64, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	d := float64(base)
	for i := 1; i < attempt && d < float64(maxDelay); i++ {
		d *= mult
	}
	if maxDelay > 0 && d > float64(maxDelay) {
		d = float64(maxDelay)
	}
	if jitter > 0 {
		d *= 1 + jitter*(2*rand.Float64()-1)
	}
	return time.Duration(d)
}

// sleepCtx sleeps for d unless ctx is done first; it reports whether it slept fully.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
