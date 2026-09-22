package variational

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var fastReconnect = ReconnectPolicy{InitialDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond, Multiplier: 2, Jitter: 0}

// wireInstrument mirrors the subscription payload for the fake server.
type wireInstrument struct {
	Underlying       string           `json:"underlying"`
	InstrumentType   string           `json:"instrument_type"`
	SettlementAsset  string           `json:"settlement_asset"`
	Kind             string           `json:"kind"`
	FundingIntervalS int              `json:"funding_interval_s"`
	Dex              *DexTokenDetails `json:"dex_token_details"`
}

func (w wireInstrument) key() string {
	return Instrument{Underlying: w.Underlying, Type: InstrumentType(w.InstrumentType), SettlementAsset: w.SettlementAsset,
		Kind: AssetClass(w.Kind), FundingIntervalS: w.FundingIntervalS, DexToken: w.Dex}.Key()
}

// fakeWS emulates the /prices and /market_status endpoints.
type fakeWS struct {
	t   *testing.T
	srv *httptest.Server

	mu            sync.Mutex
	unsupported   map[string]bool // keys rejected per instrument
	rejectRequest map[string]bool // keys that make the whole request fail
	subscribes    [][]string
	conns         int
	dropAfter     int // close the TCP connection after this many price frames (0 = never)
	noHeartbeat   bool
	tick          time.Duration
	statusUpdate  string // extra market status frame sent after the snapshot
	statusDrop    time.Duration
}

func newFakeWS(t *testing.T) *fakeWS {
	f := &fakeWS{t: t, unsupported: map[string]bool{}, rejectRequest: map[string]bool{}, tick: 5 * time.Millisecond}
	mux := http.NewServeMux()
	mux.HandleFunc("/prices", f.prices)
	mux.HandleFunc("/market_status", f.marketStatus)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeWS) url(path string) string { return "ws" + strings.TrimPrefix(f.srv.URL, "http") + path }

func (f *fakeWS) subscribeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subscribes)
}

func (f *fakeWS) connCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conns
}

func priceFrame(key string, n int) string {
	return fmt.Sprintf(`{"channel":"instrument_price:%s","pricing":{"price":"%d.5","native_price":"0.9995","delta":"1","gamma":"0","theta":"0","vega":"0","rho":"0","iv":"0","underlying_price":"%d","interest_rate":"0.0000528800000000000019269482","timestamp":"2026-09-22T13:57:33.229028Z"}}`, key, 86000+n, 86000+n)
}

func (f *fakeWS) prices(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{}
	c, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	f.mu.Lock()
	f.conns++
	drop, noHB, tick := f.dropAfter, f.noHeartbeat, f.tick
	f.mu.Unlock()

	var wmu sync.Mutex
	send := func(s string) error {
		wmu.Lock()
		defer wmu.Unlock()
		return c.WriteMessage(websocket.TextMessage, []byte(s))
	}
	var smu sync.Mutex
	subs := map[string]bool{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				Action      string           `json:"action"`
				Instruments []wireInstrument `json:"instruments"`
			}
			if err := json.Unmarshal(msg, &req); err != nil || (req.Action != "subscribe" && req.Action != "unsubscribe") {
				_ = send(msgUnexpectedFormat)
				c.Close()
				return
			}
			keys := make([]string, 0, len(req.Instruments))
			for _, wi := range req.Instruments {
				keys = append(keys, wi.key())
			}
			f.mu.Lock()
			if req.Action == "subscribe" {
				f.subscribes = append(f.subscribes, keys)
			}
			for _, k := range keys {
				if f.rejectRequest[k] {
					f.mu.Unlock()
					_ = send(msgInvalidInstrument)
					c.Close()
					return
				}
			}
			f.mu.Unlock()
			smu.Lock()
			for _, k := range keys {
				if req.Action == "unsubscribe" {
					delete(subs, k)
					continue
				}
				f.mu.Lock()
				bad := f.unsupported[k]
				f.mu.Unlock()
				if bad {
					_ = send(msgUnsupportedInstrument + k)
					continue
				}
				subs[k] = true
			}
			empty := len(subs) == 0
			smu.Unlock()
			if empty {
				_ = send(msgNoSubscriptions)
				c.Close()
				return
			}
		}
	}()

	hb := time.NewTicker(20 * time.Millisecond)
	defer hb.Stop()
	pt := time.NewTicker(tick)
	defer pt.Stop()
	n := 0
	for {
		select {
		case <-done:
			return
		case <-hb.C:
			if !noHB {
				if err := send(frameHeartbeat); err != nil {
					return
				}
			}
		case <-pt.C:
			smu.Lock()
			keys := make([]string, 0, len(subs))
			for k := range subs {
				keys = append(keys, k)
			}
			smu.Unlock()
			sort.Strings(keys)
			for _, k := range keys {
				n++
				if err := send(priceFrame(k, n)); err != nil {
					return
				}
				if drop > 0 && n >= drop {
					c.NetConn().Close() // abrupt TCP drop, no close frame
					return
				}
			}
		}
	}
}

func (f *fakeWS) marketStatus(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{}
	c, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	f.mu.Lock()
	f.conns++
	update, drop := f.statusUpdate, f.statusDrop
	f.mu.Unlock()
	go func() {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()
	if err := c.WriteMessage(websocket.TextMessage, []byte(marketSnapshot)); err != nil {
		return
	}
	if update != "" {
		time.Sleep(10 * time.Millisecond)
		if err := c.WriteMessage(websocket.TextMessage, []byte(update)); err != nil {
			return
		}
	}
	if drop > 0 {
		time.Sleep(drop)
		c.NetConn().Close()
		return
	}
	time.Sleep(5 * time.Second)
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

type collector struct {
	mu      sync.Mutex
	updates map[string][]PriceUpdate
	errs    []error
	states  []StreamState
}

func newCollector() *collector { return &collector{updates: map[string][]PriceUpdate{}} }

func (c *collector) onUpdate(u *PriceUpdate) {
	c.mu.Lock()
	c.updates[u.Key] = append(c.updates[u.Key], *u) // copy by value
	c.mu.Unlock()
}

func (c *collector) onErr(err error) {
	c.mu.Lock()
	c.errs = append(c.errs, err)
	c.mu.Unlock()
}

func (c *collector) onState(s StreamState) {
	c.mu.Lock()
	c.states = append(c.states, s)
	c.mu.Unlock()
}

func (c *collector) count(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.updates[key])
}

func (c *collector) errors() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

func (c *collector) lastState() StreamState {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.states) == 0 {
		return -1
	}
	return c.states[len(c.states)-1]
}

func newTestPriceStream(f *fakeWS, col *collector, opts ...StreamOption) *PriceStream {
	base := []StreamOption{WithStreamURL(f.url("/prices")), WithStreamReconnect(fastReconnect), WithStreamStateHandler(col.onState)}
	return NewPriceStream(col.onUpdate, col.onErr, append(base, opts...)...)
}

func runStream(t *testing.T, run func(context.Context) error) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- run(ctx) }()
	return cancel, errc
}

func TestPriceStreamBasic(t *testing.T) {
	f := newFakeWS(t)
	col := newCollector()
	s := newTestPriceStream(f, col)
	btc, swap := Perpetual("BTC"), Swap("US100S", AssetClassIndex)
	if err := s.Subscribe(btc, swap, btc); err != nil {
		t.Fatal(err)
	}
	if len(s.Subscriptions()) != 2 || !s.IsSubscribed(btc) {
		t.Fatal("subscriptions")
	}
	cancel, errc := runStream(t, s.Run)
	waitFor(t, 2*time.Second, func() bool { return col.count(btc.Key()) >= 3 && col.count(swap.Key()) >= 3 }, "updates")

	col.mu.Lock()
	u := col.updates[btc.Key()][0]
	col.mu.Unlock()
	if u.Instrument != btc || u.Key != btc.Key() || !u.Price.IsSet() || u.ReceivedAt.IsZero() || u.Timestamp.IsZero() {
		t.Fatalf("update: %+v", u)
	}
	if st := s.Stats(); st.Messages < 6 || st.Connects != 1 || st.Reconnects != 0 || st.ConnectedAt.IsZero() || st.LastMessageAt.IsZero() {
		t.Fatalf("stats: %+v", st)
	}
	if s.State() != StreamConnected {
		t.Fatalf("state = %v", s.State())
	}
	if f.subscribeCount() != 1 {
		t.Fatalf("subscribe requests = %d", f.subscribeCount())
	}
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	if s.State() != StreamStopped || col.lastState() != StreamStopped {
		t.Fatalf("final state %v / %v", s.State(), col.lastState())
	}
	if errs := col.errors(); len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
}

func TestPriceStreamSubscribeWhileRunning(t *testing.T) {
	f := newFakeWS(t)
	col := newCollector()
	s := newTestPriceStream(f, col)
	cancel, errc := runStream(t, s.Run)
	defer cancel()
	waitFor(t, time.Second, func() bool { return col.lastState() == StreamIdle }, "idle without subscriptions")
	if f.connCount() != 0 {
		t.Fatal("must not connect without subscriptions")
	}
	btc := Perpetual("BTC")
	if err := s.Subscribe(btc); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return col.count(btc.Key()) >= 2 }, "updates after late subscribe")
	eth := Perpetual("ETH")
	if err := s.Subscribe(eth); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return col.count(eth.Key()) >= 2 }, "updates for second subscribe on live connection")
	if f.subscribeCount() != 2 || f.connCount() != 1 {
		t.Fatalf("subscribes=%d conns=%d", f.subscribeCount(), f.connCount())
	}
	s.Close()
	if err := <-errc; !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("Run returned %v", err)
	}
	s.Close() // idempotent
	if err := s.Run(context.Background()); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("Run after Close returned %v", err)
	}
}

func TestPriceStreamUnsupportedInstrument(t *testing.T) {
	f := newFakeWS(t)
	xyz := Perpetual("XYZ")
	f.unsupported[xyz.Key()] = true
	col := newCollector()
	s := newTestPriceStream(f, col)
	btc := Perpetual("BTC")
	if err := s.Subscribe(btc, xyz); err != nil {
		t.Fatal(err)
	}
	cancel, _ := runStream(t, s.Run)
	defer cancel()
	waitFor(t, 2*time.Second, func() bool { return len(col.errors()) >= 1 && col.count(btc.Key()) >= 1 }, "unsupported error and BTC updates")
	err := col.errors()[0]
	var e *Error
	if !errors.As(err, &e) || !errors.Is(err, ErrUnsupportedInstrument) || e.InstrumentKey != xyz.Key() {
		t.Fatalf("err = %v", err)
	}
	if s.IsSubscribed(xyz) || !s.IsSubscribed(btc) {
		t.Fatal("rejected instrument must be dropped from the registry")
	}
	if st := s.Stats(); st.Errors != 1 {
		t.Fatalf("stats errors = %d", st.Errors)
	}
}

func TestPriceStreamRequestRejected(t *testing.T) {
	f := newFakeWS(t)
	bad := Perpetual("BAD")
	f.rejectRequest[bad.Key()] = true
	col := newCollector()
	s := newTestPriceStream(f, col)
	if err := s.Subscribe(Perpetual("BTC"), bad); err != nil {
		t.Fatal(err)
	}
	cancel, _ := runStream(t, s.Run)
	defer cancel()
	waitFor(t, 2*time.Second, func() bool { return len(col.errors()) >= 1 }, "request rejection")
	err := col.errors()[0]
	if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), bad.Key()) || !strings.Contains(err.Error(), "P-BTC-USDC-3600") {
		t.Fatalf("err = %v", err)
	}
	waitFor(t, time.Second, func() bool { return s.State() == StreamIdle }, "idle after the whole batch was dropped")
	if len(s.Subscriptions()) != 0 {
		t.Fatal("batch must be dropped")
	}
}

func TestPriceStreamValidation(t *testing.T) {
	s := NewPriceStream(nil, nil)
	err := s.Subscribe(Perpetual("BTC"), Instrument{Underlying: "X", Type: InstrumentSwap})
	if !errors.Is(err, ErrInvalidRequest) || !errors.Is(err, errInstrumentKind) {
		t.Fatalf("err = %v", err)
	}
	if len(s.Subscriptions()) != 0 {
		t.Fatal("nothing must be registered on validation failure")
	}
	if err := s.Unsubscribe(Perpetual("BTC")); err != nil {
		t.Fatal(err)
	}
}

func TestPriceStreamReconnect(t *testing.T) {
	f := newFakeWS(t)
	f.dropAfter = 3
	col := newCollector()
	s := newTestPriceStream(f, col)
	btc := Perpetual("BTC")
	if err := s.Subscribe(btc); err != nil {
		t.Fatal(err)
	}
	cancel, _ := runStream(t, s.Run)
	defer cancel()
	waitFor(t, 3*time.Second, func() bool { return f.connCount() >= 3 && col.count(btc.Key()) >= 7 }, "reconnects with resubscribe")
	if f.subscribeCount() < 3 {
		t.Fatalf("subscribe requests = %d", f.subscribeCount())
	}
	if st := s.Stats(); st.Reconnects < 2 || st.Connects < 3 {
		t.Fatalf("stats: %+v", st)
	}
	errs := col.errors()
	if len(errs) == 0 || !errors.Is(errs[0], ErrNetwork) {
		t.Fatalf("drop must be reported as a network error: %v", errs)
	}
	col.mu.Lock()
	seen := map[StreamState]bool{}
	for _, st := range col.states {
		seen[st] = true
	}
	col.mu.Unlock()
	for _, st := range []StreamState{StreamConnecting, StreamConnected, StreamDisconnected} {
		if !seen[st] {
			t.Fatalf("state %v never observed: %v", st, col.states)
		}
	}
}

func TestPriceStreamReadTimeout(t *testing.T) {
	f := newFakeWS(t)
	f.noHeartbeat = true
	f.tick = time.Hour
	col := newCollector()
	s := newTestPriceStream(f, col, WithStreamReadTimeout(40*time.Millisecond))
	if err := s.Subscribe(Perpetual("BTC")); err != nil {
		t.Fatal(err)
	}
	cancel, _ := runStream(t, s.Run)
	defer cancel()
	waitFor(t, 3*time.Second, func() bool { return f.connCount() >= 2 }, "reconnect after silence")
	errs := col.errors()
	if len(errs) == 0 || !strings.Contains(errs[0].Error(), "read timeout") {
		t.Fatalf("errs = %v", errs)
	}
}

func TestPriceStreamIdleAfterUnsubscribe(t *testing.T) {
	f := newFakeWS(t)
	col := newCollector()
	s := newTestPriceStream(f, col)
	btc := Perpetual("BTC")
	if err := s.Subscribe(btc); err != nil {
		t.Fatal(err)
	}
	cancel, _ := runStream(t, s.Run)
	defer cancel()
	waitFor(t, 2*time.Second, func() bool { return col.count(btc.Key()) >= 2 }, "updates")
	if err := s.Unsubscribe(btc); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return s.State() == StreamIdle }, "idle after last unsubscribe")
	if errs := col.errors(); len(errs) != 0 {
		t.Fatalf("server close on empty subscriptions must not be an error: %v", errs)
	}
	before := col.count(btc.Key())
	if err := s.Subscribe(btc); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return col.count(btc.Key()) > before+1 }, "updates after resubscribe")
	if f.connCount() != 2 {
		t.Fatalf("conns = %d", f.connCount())
	}
}

func TestPriceStreamRunTwice(t *testing.T) {
	f := newFakeWS(t)
	col := newCollector()
	s := newTestPriceStream(f, col)
	if s.State() != StreamStopped {
		t.Fatalf("initial state %v", s.State())
	}
	cancel, errc := runStream(t, s.Run)
	waitFor(t, time.Second, func() bool { return col.lastState() == StreamIdle }, "idle")
	if err := s.Run(context.Background()); !errors.Is(err, errStreamRunning) {
		t.Fatalf("second Run = %v", err)
	}
	cancel()
	<-errc
}

func TestPriceStreamHandlerNoAlloc(t *testing.T) {
	s := NewPriceStream(func(u *PriceUpdate) { sinkF += u.MarkPrice() }, nil)
	if err := s.Subscribe(Perpetual("BTC")); err != nil {
		t.Fatal(err)
	}
	frame := []byte(frameBTC)
	allocs := testing.AllocsPerRun(2000, func() {
		if err := s.onMessage(frame); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs per message = %v, want 0", allocs)
	}
	// Heartbeats are free as well.
	hb := []byte(frameHeartbeat)
	if allocs := testing.AllocsPerRun(1000, func() { _ = s.onMessage(hb) }); allocs != 0 {
		t.Fatalf("heartbeat allocs = %v", allocs)
	}
}

func BenchmarkPriceStreamOnMessage(b *testing.B) {
	s := NewPriceStream(func(u *PriceUpdate) { sinkF += u.MarkPrice() }, nil)
	_ = s.Subscribe(Perpetual("BTC"))
	frame := []byte(frameBTC)
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		_ = s.onMessage(frame)
	}
}

func TestMarketStatusStreamLive(t *testing.T) {
	f := newFakeWS(t)
	f.statusUpdate = `["GOOGL","open"]`
	f.statusDrop = 60 * time.Millisecond
	var mu sync.Mutex
	var events []MarketStatusEvent
	s := NewMarketStatusStream(func(ev *MarketStatusEvent) {
		mu.Lock()
		cp := *ev
		cp.Updates = append([]MarketStatusUpdate(nil), ev.Updates...)
		events = append(events, cp)
		mu.Unlock()
	}, nil, WithStreamURL(f.url("/market_status")), WithStreamReconnect(fastReconnect), WithStreamPingInterval(10*time.Millisecond))
	cancel, errc := runStream(t, s.Run)
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(events) }
	waitFor(t, 3*time.Second, func() bool { return count() >= 3 && f.connCount() >= 2 }, "snapshot, update, snapshot after reconnect")
	mu.Lock()
	ev0, ev1, ev2 := events[0], events[1], events[2]
	mu.Unlock()
	if !ev0.Snapshot || len(ev0.Updates) != 5 || ev1.Snapshot || len(ev1.Updates) != 1 || ev1.Updates[0].Status != MarketStatusOpen || !ev2.Snapshot {
		t.Fatalf("events: %+v %+v %+v", ev0, ev1, ev2)
	}
	if st, ok := s.Status("XAU"); !ok || st != MarketStatusHaltedTrading {
		t.Fatalf("XAU = %q", st)
	}
	if st := s.Stats(); st.Reconnects < 1 {
		t.Fatalf("stats: %+v", st)
	}
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestStreamNoGoroutineLeak(t *testing.T) {
	f := newFakeWS(t)
	f.dropAfter = 2
	runtime.GC()
	base := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		col := newCollector()
		s := newTestPriceStream(f, col)
		_ = s.Subscribe(Perpetual("BTC"), Perpetual("ETH"))
		cancel, errc := runStream(t, s.Run)
		waitFor(t, 2*time.Second, func() bool { return col.count("P-BTC-USDC-3600") >= 2 }, "updates")
		if i%2 == 0 {
			cancel()
		} else {
			s.Close()
		}
		<-errc
		cancel()
	}
	// Server-side handler goroutines of dropped connections wind down asynchronously.
	waitFor(t, 3*time.Second, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= base+2
	}, fmt.Sprintf("goroutines to settle (base %d, now %d)", base, runtime.NumGoroutine()))
}

func TestStreamDialErrors(t *testing.T) {
	// 429 on the handshake is classified as a rate limit and backs off.
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "Just a moment...", http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	col := newCollector()
	s := NewPriceStream(col.onUpdate, col.onErr, WithStreamURL("ws"+strings.TrimPrefix(srv.URL, "http")+"/prices"), WithStreamReconnect(fastReconnect))
	_ = s.Subscribe(Perpetual("BTC"))
	cancel, errc := runStream(t, s.Run)
	waitFor(t, 2*time.Second, func() bool { return len(col.errors()) >= 1 }, "dial error")
	time.Sleep(100 * time.Millisecond)
	if hits.Load() != 1 {
		t.Fatalf("rate-limited handshake must back off at least %v; got %d attempts", rateLimitedDialDelay, hits.Load())
	}
	var e *Error
	if err := col.errors()[0]; !errors.As(err, &e) || e.Kind != ErrKindRateLimit || e.RetryAfter != time.Second || e.HTTPStatus != 429 {
		t.Fatalf("err = %v", err)
	}
	cancel()
	<-errc
}

func TestClientStreamFactories(t *testing.T) {
	c := NewClient(WithTestnet(), WithUserAgent("custom/1"), WithHeader("X-Api", "v"), WithReconnect(fastReconnect))
	ps := c.NewPriceStream(nil, nil)
	if ps.core.cfg.url != TestnetWSURL+"/prices" || ps.core.cfg.header.Get("User-Agent") != "custom/1" || ps.core.cfg.header.Get("X-Api") != "v" {
		t.Fatalf("price stream cfg: %+v", ps.core.cfg)
	}
	if ps.core.cfg.reconnect != fastReconnect || ps.core.cfg.dialer != c.dialer {
		t.Fatal("client settings must propagate")
	}
	ms := c.NewMarketStatusStream(nil, nil, WithStreamURL("ws://override/x"))
	if ms.core.cfg.url != "ws://override/x" {
		t.Fatal("stream options must override client settings")
	}
	if ps.State() != StreamStopped || ps.State().String() != "stopped" || StreamIdle.String() != "idle" || StreamState(99).String() != "unknown" {
		t.Fatal("state strings")
	}
}
