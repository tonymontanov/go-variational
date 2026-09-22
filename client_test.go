package variational

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

var fastRetry = RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond, MaxRetryAfter: 50 * time.Millisecond}

func newTestClient(t *testing.T, h http.HandlerFunc, opts ...Option) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return NewClient(append([]Option{WithBaseURL(srv.URL + "/"), WithRetry(fastRetry)}, opts...)...)
}

func serveFile(t *testing.T, path string) http.HandlerFunc {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
}

func TestGetStats(t *testing.T) {
	var gotUA, gotAccept, gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotUA, gotAccept, gotPath = r.Header.Get("User-Agent"), r.Header.Get("Accept"), r.URL.Path
		serveFile(t, "testdata/stats.json")(w, r)
	}, WithHeader("X-Test", "1"))
	s, err := c.GetStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != pathStats || gotUA != DefaultUserAgent || gotAccept != "application/json" {
		t.Fatalf("request: %q %q %q", gotPath, gotUA, gotAccept)
	}
	if s.NumMarkets != 552 || len(s.Listings) != 12 || s.TotalVolume24h.String() != "3626100496.156893" || s.LossRefund.PoolSize.String() != "0" {
		t.Fatalf("top level: %+v", s)
	}
	btc := s.Listing("BTC")
	if btc == nil || btc.Name != "Bitcoin" || btc.FundingIntervalS != 28800 || btc.IsSwap() {
		t.Fatalf("btc: %+v", btc)
	}
	if btc.Quotes.Size1M == nil || btc.Quotes.Size1K == nil || btc.Quotes.Size100K == nil || !btc.Quotes.Base.Bid.IsSet() {
		t.Fatalf("btc quotes: %+v", btc.Quotes)
	}
	if btc.Quotes.UpdatedAt.IsZero() || btc.Quotes.UpdatedAt.Year() != 2026 {
		t.Fatalf("updated_at: %v", btc.Quotes.UpdatedAt)
	}
	mid, err := btc.Quotes.Base.Mid()
	bid, _ := btc.Quotes.Base.Bid.Float64()
	ask, _ := btc.Quotes.Base.Ask.Float64()
	if err != nil || mid != (bid+ask)/2 || bid >= ask {
		t.Fatalf("mid: %v %v", mid, err)
	}
	if bps, err := btc.Quotes.Base.SpreadBps(); err != nil || bps <= 0 || bps > 100 {
		t.Fatalf("spread bps: %v %v", bps, err)
	}
	googl := s.Listing("GOOGL")
	if googl == nil || googl.Quotes.Size1M != nil {
		t.Fatalf("googl must have no size_1m tier: %+v", googl)
	}
	if sw := s.Listing("US100S"); sw == nil || !sw.IsSwap() || !sw.FundingRate.IsZero() {
		t.Fatalf("swap: %+v", sw)
	}
	if s.Listing("NOPE") != nil {
		t.Fatal("unknown listing")
	}
	idx := s.Index()
	if len(idx) != 12 || idx["ETH"] != &s.Listings[indexOf(s, "ETH")] {
		t.Fatal("index")
	}
	raw, err := c.GetStatsRaw(context.Background())
	if err != nil || len(raw) == 0 || raw[0] != '{' {
		t.Fatalf("raw: %v %d", err, len(raw))
	}
}

func indexOf(s *Stats, ticker string) int {
	for i := range s.Listings {
		if s.Listings[i].Ticker == ticker {
			return i
		}
	}
	return -1
}

func TestGetServerTime(t *testing.T) {
	const skew = 1500 * time.Millisecond
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != pathStatus {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"server_ts_ns":` + strconv.FormatInt(time.Now().Add(skew).UnixNano(), 10) + `}`))
	})
	st, err := c.GetServerTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.RTT <= 0 || st.RTT > time.Second {
		t.Fatalf("rtt = %v", st.RTT)
	}
	if d := st.Offset - skew; d < -200*time.Millisecond || d > 200*time.Millisecond {
		t.Fatalf("offset = %v, want ≈ %v", st.Offset, skew)
	}
	if st.Time.Location() != time.UTC {
		t.Fatal("server time must be UTC")
	}
}

func TestRateLimitedImmediate(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"slow down"}`))
	})
	_, err := c.GetStats(context.Background())
	var e *Error
	if !errors.As(err, &e) || !errors.Is(err, ErrRateLimited) || e.Kind != ErrKindRateLimit || e.RetryAfter != time.Second || e.HTTPStatus != 429 {
		t.Fatalf("err = %#v", err)
	}
	if !e.Temporary() || e.Message != `{"error":"slow down"}` || e.Op != "GET /metadata/stats" {
		t.Fatalf("err = %v", e)
	}
	if calls.Load() != 1 {
		t.Fatalf("Retry-After above MaxRetryAfter must not be retried: %d calls", calls.Load())
	}
}

func TestRateLimitedRetried(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"server_ts_ns":1}`))
	})
	if _, err := c.GetServerTime(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestForbiddenChallenge(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><title>Just a moment...</title></head></html>`))
	})
	_, err := c.GetStats(context.Background())
	var e *Error
	if !errors.As(err, &e) || !errors.Is(err, ErrForbidden) || e.Temporary() {
		t.Fatalf("err = %v", err)
	}
	if e.Message != "browser challenge: endpoint is not available to non-browser clients" {
		t.Fatalf("message = %q", e.Message)
	}
}

func TestServerErrorRetry(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			http.Error(w, "upstream down", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"server_ts_ns":1}`))
	})
	if _, err := c.GetServerTime(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d", calls.Load())
	}

	calls.Store(0)
	c2 := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "upstream down", http.StatusBadGateway)
	})
	_, err := c2.GetServerTime(context.Background())
	if !errors.Is(err, ErrExchange) || calls.Load() != 3 {
		t.Fatalf("err = %v calls = %d", err, calls.Load())
	}
}

func TestInvalidRequestNotRetried(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "nope", http.StatusNotFound)
	})
	_, err := c.GetServerTime(context.Background())
	if !errors.Is(err, ErrInvalidRequest) || calls.Load() != 1 {
		t.Fatalf("err = %v calls = %d", err, calls.Load())
	}
}

func TestInvalidJSON(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"server_ts_ns":"not a number"}`))
	})
	if _, err := c.GetServerTime(context.Background()); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("err = %v", err)
	}
}

func TestContextTimeout(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := c.GetStats(ctx)
	if !errors.Is(err, ErrNetwork) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestClientTimeoutOption(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}, WithTimeout(30*time.Millisecond), WithRetry(RetryPolicy{MaxAttempts: 1}))
	start := time.Now()
	_, err := c.GetStats(context.Background())
	if !errors.Is(err, ErrNetwork) || time.Since(start) > time.Second {
		t.Fatalf("err = %v after %v", err, time.Since(start))
	}
}

func TestMaxResponseSize(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 2048))
	}, WithMaxResponseSize(1024), WithRetry(RetryPolicy{MaxAttempts: 1}))
	if _, err := c.GetStatsRaw(context.Background()); !errors.Is(err, ErrNetwork) {
		t.Fatalf("err = %v", err)
	}
}

func TestRateLimiterPacing(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"server_ts_ns":1}`))
	}, WithRateLimit(2, 200*time.Millisecond))
	start := time.Now()
	for i := 0; i < 3; i++ {
		if _, err := c.GetServerTime(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el < 90*time.Millisecond {
		t.Fatalf("third call should have waited for a token; elapsed %v", el)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.GetServerTime(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait: %v", err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	if d := parseRetryAfter("7"); d != 7*time.Second {
		t.Fatal(d)
	}
	if d := parseRetryAfter(time.Now().Add(3 * time.Second).UTC().Format(http.TimeFormat)); d <= 0 || d > 3*time.Second {
		t.Fatal(d)
	}
	if d := parseRetryAfter("garbage"); d != 0 {
		t.Fatal(d)
	}
}

func TestErrorFormatting(t *testing.T) {
	e := &Error{Kind: ErrKindUnsupportedInstrument, Op: "ws /prices", InstrumentKey: "P-XYZ-USDC-3600", Message: "dropped", Err: errors.New("inner")}
	want := "variational: ws /prices: unsupported_instrument P-XYZ-USDC-3600: dropped: inner"
	if e.Error() != want {
		t.Fatalf("got %q", e.Error())
	}
	if !errors.Is(e, ErrUnsupportedInstrument) || errors.Is(e, ErrNetwork) {
		t.Fatal("Is")
	}
	if errors.Unwrap(e).Error() != "inner" {
		t.Fatal("Unwrap")
	}
	if ErrorKind(200).String() != "kind(200)" || ErrKindRateLimit.String() != "rate_limit" {
		t.Fatal("kind names")
	}
}

func TestBackoff(t *testing.T) {
	for attempt := 1; attempt <= 10; attempt++ {
		d := backoff(100*time.Millisecond, time.Second, 2, 0, attempt)
		want := min(100*time.Millisecond<<(attempt-1), time.Second)
		if d != want {
			t.Fatalf("attempt %d: %v want %v", attempt, d, want)
		}
	}
	for i := 0; i < 100; i++ {
		d := backoff(100*time.Millisecond, time.Second, 2, 0.25, 1)
		if d < 75*time.Millisecond || d > 125*time.Millisecond {
			t.Fatalf("jitter out of range: %v", d)
		}
	}
	if backoff(0, time.Second, 2, 0.5, 3) != 0 {
		t.Fatal("zero base")
	}
}

func TestClientDefaults(t *testing.T) {
	c := NewClient()
	if c.BaseURL() != MainnetBaseURL || c.WSURL() != MainnetWSURL {
		t.Fatal("mainnet defaults")
	}
	c = NewClient(WithTestnet(), WithBaseURL("http://x/"), WithWSURL("ws://y//"))
	if c.BaseURL() != "http://x" || c.WSURL() != "ws://y" {
		t.Fatalf("%q %q", c.BaseURL(), c.WSURL())
	}
	c = NewClient(WithTestnet())
	if c.BaseURL() != TestnetBaseURL || c.WSURL() != TestnetWSURL {
		t.Fatal("testnet")
	}
	c.Close()
}
