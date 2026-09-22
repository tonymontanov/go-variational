package variational

import (
	"context"
	"strings"
	"sync"
	"time"
)

// MarketStatus is the trading status of a market as reported by /market_status.
type MarketStatus string

const (
	MarketStatusOpen          MarketStatus = "open"
	MarketStatusClosed        MarketStatus = "closed"
	MarketStatusHaltedTrading MarketStatus = "halted_trading"
)

// MarketStatusUpdate is one symbol/status pair.
type MarketStatusUpdate struct {
	// Symbol is the listing ticker (matches Listing.Ticker).
	Symbol string
	Status MarketStatus
}

// MarketStatusEvent is one frame of the /market_status stream: the full
// snapshot sent on every (re)connect, or an incremental update.
//
// Handlers receive a pointer to a buffer reused for the next frame; copy the
// Updates slice to retain it.
type MarketStatusEvent struct {
	// Snapshot is true for the first frame after a connection is established,
	// which lists every market.
	Snapshot bool
	Updates  []MarketStatusUpdate
	// ReceivedAt is the local time the frame was read from the socket.
	ReceivedAt time.Time
}

// MarketStatusHandler receives market status events on the stream goroutine.
type MarketStatusHandler func(ev *MarketStatusEvent)

type marketEntry struct {
	symbol string
	status MarketStatus
}

// MarketStatusStream follows open/closed/halted transitions of all markets
// (WebSocket /market_status). Besides delivering events it keeps the latest
// status of every market for lookup via Status and Snapshot.
type MarketStatusStream struct {
	core    *streamCore
	handler MarketStatusHandler

	mu    sync.RWMutex
	state map[string]marketEntry

	first bool // next frame is a snapshot; touched only by the stream goroutine
	ev    MarketStatusEvent
}

// NewMarketStatusStream returns a stream for Omni mainnet. See
// Client.NewMarketStatusStream to inherit a client's settings.
func NewMarketStatusStream(handler MarketStatusHandler, errHandler ErrHandler, opts ...StreamOption) *MarketStatusStream {
	s := &MarketStatusStream{handler: handler, state: map[string]marketEntry{}}
	// The endpoint sends no heartbeats; liveness relies on ping/pong.
	s.core = newStreamCore("ws /market_status", MainnetWSURL+"/market_status", 45*time.Second, 15*time.Second, opts)
	s.core.errFn = errHandler
	s.core.onOpen = s.onOpen
	s.core.onMessage = s.onMessage
	s.core.wantConn = func() bool { return true }
	return s
}

// NewMarketStatusStream returns a market status stream bound to the client's endpoint.
func (c *Client) NewMarketStatusStream(handler MarketStatusHandler, errHandler ErrHandler, opts ...StreamOption) *MarketStatusStream {
	return NewMarketStatusStream(handler, errHandler, append(c.clientStreamOptions("/market_status"), opts...)...)
}

// Run connects and serves the stream until ctx is done (returns ctx.Err()) or
// Close is called (returns ErrStreamClosed). Reconnects are automatic.
func (s *MarketStatusStream) Run(ctx context.Context) error { return s.core.run(ctx) }

// Close stops the stream. Idempotent.
func (s *MarketStatusStream) Close() { s.core.close() }

// State returns the current connection state.
func (s *MarketStatusStream) State() StreamState { return s.core.getState() }

// Stats returns cumulative counters.
func (s *MarketStatusStream) Stats() StreamStats { return s.core.stats() }

// Status returns the last known status of a market.
func (s *MarketStatusStream) Status(symbol string) (MarketStatus, bool) {
	s.mu.RLock()
	e, ok := s.state[symbol]
	s.mu.RUnlock()
	return e.status, ok
}

// Snapshot returns a copy of the last known status of every market.
func (s *MarketStatusStream) Snapshot() map[string]MarketStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]MarketStatus, len(s.state))
	for k, e := range s.state {
		out[k] = e.status
	}
	return out
}

func (s *MarketStatusStream) onOpen() error {
	s.first = true
	s.core.markReady()
	return nil
}

func (s *MarketStatusStream) onMessage(data []byte) error {
	data = trimLeadingWS(data)
	if len(data) == 0 {
		return nil
	}
	if data[0] != '[' {
		if data[0] == '{' {
			return nil
		}
		return &Error{Kind: ErrKindExchange, Op: s.core.name, Message: strings.TrimSpace(string(data))}
	}
	s.ev.Updates = s.ev.Updates[:0]
	s.ev.Snapshot = s.first
	s.ev.ReceivedAt = time.Now()
	if err := s.parse(data); err != nil {
		return &Error{Kind: ErrKindInvalidResponse, Op: s.core.name, Message: bodyExcerpt(data), Err: err}
	}
	s.first = false
	s.apply()
	if s.handler != nil {
		s.handler(&s.ev)
	}
	return nil
}

// parse decodes either a single ["SYM","status"] pair or an array of pairs into
// s.ev.Updates, interning symbols already known to the state map.
func (s *MarketStatusStream) parse(data []byte) error {
	sc := scanner{b: data}
	if !sc.expect('[') {
		return errMalformed
	}
	sc.skipWS()
	s.mu.RLock()
	defer s.mu.RUnlock()
	switch sc.peek() {
	case '"':
		return s.readPair(&sc)
	case ']':
		return nil
	case '[':
		for {
			if !sc.expect('[') {
				return errMalformed
			}
			if err := s.readPair(&sc); err != nil {
				return err
			}
			sc.skipWS()
			switch sc.peek() {
			case ',':
				sc.i++
				sc.skipWS()
			case ']':
				return nil
			default:
				return errMalformed
			}
		}
	}
	return errMalformed
}

// readPair reads `"SYM","status"]` (the opening bracket is already consumed).
func (s *MarketStatusStream) readPair(sc *scanner) error {
	sym, ok := sc.readString()
	if !ok || !sc.expect(',') {
		return errMalformed
	}
	st, ok := sc.readString()
	if !ok || !sc.expect(']') {
		return errMalformed
	}
	var u MarketStatusUpdate
	if e, ok := s.state[string(sym)]; ok {
		u.Symbol = e.symbol
	} else {
		u.Symbol = string(sym)
	}
	u.Status = internStatus(st)
	s.ev.Updates = append(s.ev.Updates, u)
	return nil
}

func internStatus(b []byte) MarketStatus {
	switch string(b) {
	case string(MarketStatusOpen):
		return MarketStatusOpen
	case string(MarketStatusClosed):
		return MarketStatusClosed
	case string(MarketStatusHaltedTrading):
		return MarketStatusHaltedTrading
	}
	return MarketStatus(string(b))
}

func (s *MarketStatusStream) apply() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ev.Snapshot {
		s.state = make(map[string]marketEntry, len(s.ev.Updates))
	}
	for i := range s.ev.Updates {
		u := &s.ev.Updates[i]
		s.state[u.Symbol] = marketEntry{symbol: u.Symbol, status: u.Status}
	}
}
