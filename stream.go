package variational

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// StreamState is the connection state of a stream.
type StreamState int32

const (
	// StreamStopped means Run is not active: it was never called or has returned.
	StreamStopped StreamState = iota
	// StreamIdle means Run is active but there is nothing to do (no
	// subscriptions), so no connection is held.
	StreamIdle
	// StreamConnecting means a dial is in progress.
	StreamConnecting
	// StreamConnected means the connection is established and messages flow.
	StreamConnected
	// StreamDisconnected means the connection dropped and a reconnect is pending.
	StreamDisconnected
)

func (s StreamState) String() string {
	switch s {
	case StreamStopped:
		return "stopped"
	case StreamIdle:
		return "idle"
	case StreamConnecting:
		return "connecting"
	case StreamConnected:
		return "connected"
	case StreamDisconnected:
		return "disconnected"
	}
	return "unknown"
}

// ReconnectPolicy controls the exponential backoff between reconnect attempts.
type ReconnectPolicy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	// Multiplier grows the delay after each consecutive failure (default 2).
	Multiplier float64
	// Jitter is the relative random spread applied to every delay, 0..1 (default 0.25).
	Jitter float64
}

// DefaultReconnectPolicy is used when none is given.
var DefaultReconnectPolicy = ReconnectPolicy{
	InitialDelay: 250 * time.Millisecond,
	MaxDelay:     30 * time.Second,
	Multiplier:   2,
	Jitter:       0.25,
}

func (p ReconnectPolicy) delay(attempt int) time.Duration {
	mult := p.Multiplier
	if mult < 1 {
		mult = 2
	}
	return backoff(p.InitialDelay, p.MaxDelay, mult, p.Jitter, attempt)
}

// StreamStats are cumulative counters of a stream, safe to read concurrently.
type StreamStats struct {
	// Connects counts successful WebSocket handshakes; Reconnects counts those
	// after the first.
	Connects   uint64
	Reconnects uint64
	// Messages and Bytes count received data frames.
	Messages uint64
	Bytes    uint64
	// Errors counts errors delivered to the error handler.
	Errors uint64
	// LastMessageAt is the local time of the last received frame (zero if none).
	LastMessageAt time.Time
	// ConnectedAt is the local time of the current connection (zero if not connected).
	ConnectedAt time.Time
}

// StreamOption configures a stream.
type StreamOption func(*streamConfig)

type streamConfig struct {
	url          string
	dialer       *websocket.Dialer
	header       http.Header
	reconnect    ReconnectPolicy
	readTimeout  time.Duration
	pingInterval time.Duration
	writeTimeout time.Duration
	onState      func(StreamState)
	bufSize      int
}

// WithStreamURL overrides the full WebSocket URL of the stream (scheme, host and path).
func WithStreamURL(u string) StreamOption {
	return func(c *streamConfig) { c.url = u }
}

// WithStreamDialer supplies a custom dialer.
func WithStreamDialer(d *websocket.Dialer) StreamOption {
	return func(c *streamConfig) { c.dialer = d }
}

// WithStreamHeader adds a handshake header.
func WithStreamHeader(key, value string) StreamOption {
	return func(c *streamConfig) {
		if c.header == nil {
			c.header = http.Header{}
		}
		c.header.Add(key, value)
	}
}

// WithStreamReconnect sets the reconnect backoff policy.
func WithStreamReconnect(p ReconnectPolicy) StreamOption {
	return func(c *streamConfig) { c.reconnect = p }
}

// WithStreamReadTimeout sets the maximum silence (no frames, no pongs) before
// the connection is considered dead and re-established. Zero disables it.
func WithStreamReadTimeout(d time.Duration) StreamOption {
	return func(c *streamConfig) { c.readTimeout = d }
}

// WithStreamPingInterval sets how often a WebSocket ping is sent. Zero disables pings.
func WithStreamPingInterval(d time.Duration) StreamOption {
	return func(c *streamConfig) { c.pingInterval = d }
}

// WithStreamStateHandler registers a callback invoked on every state change.
// It runs on the stream goroutine and must not block.
func WithStreamStateHandler(fn func(StreamState)) StreamOption {
	return func(c *streamConfig) { c.onState = fn }
}

var errStreamRunning = errors.New("variational: stream is already running")

// rateLimitedDialDelay is the minimum pause after a rate-limited handshake.
const rateLimitedDialDelay = 10 * time.Second

// streamCore implements connection management shared by all streams:
// dial, read loop with a reusable buffer, liveness (read deadline + ping),
// reconnect with backoff, and orderly shutdown.
type streamCore struct {
	cfg  streamConfig
	name string // "ws /prices"

	errFn     func(error)
	onOpen    func() error       // send subscriptions after connect
	onMessage func([]byte) error // decode one data frame; errors are reported, not fatal
	wantConn  func() bool        // whether a connection is currently desired

	mu   sync.Mutex // guards conn
	conn *websocket.Conn
	wmu  sync.Mutex // serialises writes

	state     atomic.Int32
	running   atomic.Bool
	ready     atomic.Bool // connection open and initial subscriptions sent
	closed    chan struct{}
	closeOnce sync.Once
	wake      chan struct{}

	buf []byte

	nConnects, nReconnects, nMessages, nBytes, nErrors atomic.Uint64
	lastMsgNs, connectedNs                             atomic.Int64
}

func newStreamCore(name, defaultURL string, readTimeout, pingInterval time.Duration, opts []StreamOption) *streamCore {
	cfg := streamConfig{
		url:          defaultURL,
		reconnect:    DefaultReconnectPolicy,
		readTimeout:  readTimeout,
		pingInterval: pingInterval,
		writeTimeout: 5 * time.Second,
		bufSize:      16 << 10,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.dialer == nil {
		cfg.dialer = &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 10 * time.Second,
			ReadBufferSize:   32 << 10,
			WriteBufferSize:  4 << 10,
		}
	}
	if cfg.header == nil {
		cfg.header = http.Header{}
	}
	if cfg.header.Get("User-Agent") == "" {
		cfg.header.Set("User-Agent", DefaultUserAgent)
	}
	return &streamCore{
		cfg:    cfg,
		name:   name,
		closed: make(chan struct{}),
		wake:   make(chan struct{}, 1),
		buf:    make([]byte, 0, cfg.bufSize),
	}
}

// clientStreamOptions derives stream options from a Client's configuration.
func (c *Client) clientStreamOptions(path string) []StreamOption {
	opts := []StreamOption{
		WithStreamURL(c.wsURL + path),
		WithStreamDialer(c.dialer),
		WithStreamReconnect(c.reconnect),
		WithStreamHeader("User-Agent", c.userAgent),
	}
	for k, vs := range c.headers {
		for _, v := range vs {
			opts = append(opts, WithStreamHeader(k, v))
		}
	}
	return opts
}

func (s *streamCore) getState() StreamState { return StreamState(s.state.Load()) }

func (s *streamCore) setState(st StreamState) {
	if StreamState(s.state.Swap(int32(st))) == st {
		return
	}
	if s.cfg.onState != nil {
		s.cfg.onState(st)
	}
}

func (s *streamCore) stats() StreamStats {
	st := StreamStats{
		Connects:   s.nConnects.Load(),
		Reconnects: s.nReconnects.Load(),
		Messages:   s.nMessages.Load(),
		Bytes:      s.nBytes.Load(),
		Errors:     s.nErrors.Load(),
	}
	if ns := s.lastMsgNs.Load(); ns != 0 {
		st.LastMessageAt = time.Unix(0, ns)
	}
	if ns := s.connectedNs.Load(); ns != 0 {
		st.ConnectedAt = time.Unix(0, ns)
	}
	return st
}

func (s *streamCore) reportErr(err error) {
	s.nErrors.Add(1)
	if s.errFn != nil {
		s.errFn(err)
	}
}

func (s *streamCore) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// close stops Run and drops the connection. Idempotent.
func (s *streamCore) close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.mu.Lock()
		conn := s.conn
		s.mu.Unlock()
		if conn != nil {
			conn.Close()
		}
	})
}

// notify wakes the run loop (e.g. after the first subscription).
func (s *streamCore) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *streamCore) setConn(c *websocket.Conn) {
	s.mu.Lock()
	s.conn = c
	if c == nil {
		s.ready.Store(false)
	}
	s.mu.Unlock()
}

// markReady declares the connection usable for ad-hoc sends. Streams call it
// from onOpen while holding their subscription lock, so a subscription added
// after the initial batch was snapshotted is sent by the caller itself.
func (s *streamCore) markReady() { s.ready.Store(true) }

// connected reports whether ad-hoc sends should be issued now; otherwise the
// next onOpen will carry the state.
func (s *streamCore) connected() bool { return s.ready.Load() }

// send writes a text frame on the current connection.
func (s *streamCore) send(payload []byte) error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return &Error{Kind: ErrKindNetwork, Op: s.name, Message: "not connected"}
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.writeTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return &Error{Kind: ErrKindNetwork, Op: s.name, Err: err}
	}
	return nil
}

// run drives the connection until ctx is done or close is called. It returns
// ctx.Err() or ErrStreamClosed respectively.
func (s *streamCore) run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return errStreamRunning
	}
	defer s.running.Store(false)
	defer s.setState(StreamStopped)

	attempt := 0
	for {
		if err := s.done(ctx); err != nil {
			return err
		}
		if !s.wantConn() {
			s.setState(StreamIdle)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.closed:
				return ErrStreamClosed
			case <-s.wake:
			}
			continue
		}

		s.setState(StreamConnecting)
		conn, err := s.dial(ctx)
		if err != nil {
			if e := s.done(ctx); e != nil {
				return e
			}
			s.reportErr(err)
			attempt++
			delay := s.cfg.reconnect.delay(attempt)
			// The WebSocket host rate-limits handshakes (HTTP 429 with a
			// challenge page); back off harder so the ban does not escalate.
			var e *Error
			if errors.As(err, &e) && e.Kind == ErrKindRateLimit {
				delay = max(delay, e.RetryAfter, rateLimitedDialDelay)
			}
			if !s.sleep(ctx, delay) {
				return s.done(ctx)
			}
			continue
		}

		if s.nConnects.Add(1) > 1 {
			s.nReconnects.Add(1)
		}
		start := time.Now()
		s.connectedNs.Store(start.UnixNano())
		s.setConn(conn)
		s.setState(StreamConnected)

		err = s.serve(ctx, conn)

		s.setConn(nil)
		s.connectedNs.Store(0)
		conn.Close()
		s.setState(StreamDisconnected)

		if e := s.done(ctx); e != nil {
			return e
		}
		want := s.wantConn()
		if err != nil && (want || !isCloseError(err)) {
			s.reportErr(err)
		}
		if !want {
			attempt = 0
			continue
		}
		if time.Since(start) > s.cfg.reconnect.MaxDelay {
			attempt = 0 // a stable session resets the backoff
		}
		attempt++
		if !s.sleep(ctx, s.cfg.reconnect.delay(attempt)) {
			return s.done(ctx)
		}
	}
}

func (s *streamCore) done(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return ErrStreamClosed
	}
	return nil
}

func (s *streamCore) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return s.done(ctx) == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-s.closed:
		return false
	}
}

func (s *streamCore) dial(ctx context.Context) (*websocket.Conn, error) {
	conn, resp, err := s.cfg.dialer.DialContext(ctx, s.cfg.url, s.cfg.header)
	if err != nil {
		e := &Error{Kind: ErrKindNetwork, Op: s.name, Err: err}
		if resp != nil {
			e.HTTPStatus = resp.StatusCode
			switch {
			case resp.StatusCode == http.StatusTooManyRequests:
				e.Kind = ErrKindRateLimit
				e.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
			case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized:
				e.Kind = ErrKindForbidden
			case resp.StatusCode >= 500:
				e.Kind = ErrKindExchange
			}
		}
		return nil, e
	}
	return conn, nil
}

func (s *streamCore) extendDeadline(conn *websocket.Conn) error {
	if s.cfg.readTimeout <= 0 {
		return nil
	}
	return conn.SetReadDeadline(time.Now().Add(s.cfg.readTimeout))
}

// serve runs the read loop on conn until it fails or the stream stops.
func (s *streamCore) serve(ctx context.Context, conn *websocket.Conn) error {
	done := make(chan struct{})
	defer close(done)

	// The closer goroutine unblocks the reader on shutdown.
	go func() {
		select {
		case <-ctx.Done():
		case <-s.closed:
		case <-done:
			return
		}
		s.wmu.Lock()
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		s.wmu.Unlock()
		conn.Close()
	}()

	if s.cfg.pingInterval > 0 {
		go s.pingLoop(conn, done)
	}
	conn.SetPongHandler(func(string) error { return s.extendDeadline(conn) })
	_ = conn.SetReadDeadline(time.Time{})

	if err := s.onOpen(); err != nil {
		return err
	}

	for {
		if err := s.extendDeadline(conn); err != nil {
			return &Error{Kind: ErrKindNetwork, Op: s.name, Err: err}
		}
		mt, r, err := conn.NextReader()
		if err != nil {
			return s.readError(ctx, err)
		}
		s.buf, err = readInto(r, s.buf[:0])
		if err != nil {
			return s.readError(ctx, err)
		}
		if mt != websocket.TextMessage && mt != websocket.BinaryMessage {
			continue
		}
		s.nMessages.Add(1)
		s.nBytes.Add(uint64(len(s.buf)))
		s.lastMsgNs.Store(time.Now().UnixNano())
		if err := s.onMessage(s.buf); err != nil {
			s.reportErr(err)
		}
	}
}

func (s *streamCore) readError(ctx context.Context, err error) error {
	if s.done(ctx) != nil {
		return nil
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return &Error{Kind: ErrKindNetwork, Op: s.name, Message: "read timeout: no data within " + s.cfg.readTimeout.String(), Err: err}
	}
	return &Error{Kind: ErrKindNetwork, Op: s.name, Err: err}
}

func isCloseError(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		err = e.Err
	}
	var ce *websocket.CloseError
	return errors.As(err, &ce) || errors.Is(err, io.EOF)
}

func (s *streamCore) pingLoop(conn *websocket.Conn, done <-chan struct{}) {
	t := time.NewTicker(s.cfg.pingInterval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			s.wmu.Lock()
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.cfg.writeTimeout))
			s.wmu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// trimLeadingWS drops leading JSON whitespace without allocating.
func trimLeadingWS(b []byte) []byte {
	i := 0
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return b[i:]
}

// readInto reads r to completion into buf, growing it when needed, and returns
// the filled slice. Steady state performs no allocation.
func readInto(r io.Reader, buf []byte) ([]byte, error) {
	for {
		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return buf, err
		}
	}
}
