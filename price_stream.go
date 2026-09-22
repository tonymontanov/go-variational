package variational

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PriceHandler receives price updates. It runs on the stream goroutine: keep it
// fast and never retain u (copy it by value instead).
type PriceHandler func(u *PriceUpdate)

// ErrHandler receives asynchronous stream errors (*Error). Transient network
// errors are followed by an automatic reconnect; the handler is informational.
type ErrHandler func(err error)

type priceSub struct {
	key  string
	inst Instrument
}

// Server-side text messages of the /prices endpoint.
const (
	msgUnsupportedInstrument = "unsupported instrument: "
	msgInvalidInstrument     = "invalid instrument in request"
	msgUnexpectedFormat      = "unexpected request format"
	msgNoSubscriptions       = "no subscriptions open, closing connection"
)

// PriceStream subscribes to the real-time instrument price feed (WebSocket
// /prices). Create it with NewPriceStream or Client.NewPriceStream, add
// instruments with Subscribe (before or after Run) and drive it with Run.
//
// The stream holds a connection only while at least one instrument is
// subscribed: the exchange closes idle connections, so an empty stream sits in
// StreamIdle until the first Subscribe. Subscriptions survive reconnects.
type PriceStream struct {
	core    *streamCore
	handler PriceHandler

	mu        sync.Mutex // serialises subscription changes
	subs      atomic.Pointer[map[string]*priceSub]
	lastBatch []string // keys of the last subscribe request, dropped on request-level rejection

	upd     PriceUpdate
	scratch []byte
}

// NewPriceStream returns a stream for Omni mainnet. See Client.NewPriceStream to
// inherit a client's endpoint, dialer and reconnect settings.
func NewPriceStream(handler PriceHandler, errHandler ErrHandler, opts ...StreamOption) *PriceStream {
	s := &PriceStream{handler: handler}
	s.core = newStreamCore("ws /prices", MainnetWSURL+"/prices", 15*time.Second, 0, opts)
	s.core.errFn = errHandler
	s.core.onOpen = s.onOpen
	s.core.onMessage = s.onMessage
	s.core.wantConn = s.wantConn
	empty := map[string]*priceSub{}
	s.subs.Store(&empty)
	return s
}

// NewPriceStream returns a price stream bound to the client's WebSocket endpoint.
func (c *Client) NewPriceStream(handler PriceHandler, errHandler ErrHandler, opts ...StreamOption) *PriceStream {
	return NewPriceStream(handler, errHandler, append(c.clientStreamOptions("/prices"), opts...)...)
}

// Run connects and serves the stream until ctx is done (returns ctx.Err()) or
// Close is called (returns ErrStreamClosed). Reconnects are automatic. Run may
// be called once at a time.
func (s *PriceStream) Run(ctx context.Context) error { return s.core.run(ctx) }

// Close stops the stream; Run returns ErrStreamClosed. Idempotent.
func (s *PriceStream) Close() { s.core.close() }

// State returns the current connection state.
func (s *PriceStream) State() StreamState { return s.core.getState() }

// Stats returns cumulative counters.
func (s *PriceStream) Stats() StreamStats { return s.core.stats() }

// Subscriptions returns the currently registered instruments.
func (s *PriceStream) Subscriptions() []Instrument {
	subs := *s.subs.Load()
	out := make([]Instrument, 0, len(subs))
	for _, sub := range subs {
		out = append(out, sub.inst)
	}
	return out
}

// IsSubscribed reports whether the instrument is registered.
func (s *PriceStream) IsSubscribed(inst Instrument) bool {
	_, ok := (*s.subs.Load())[inst.Key()]
	return ok
}

func (s *PriceStream) wantConn() bool { return len(*s.subs.Load()) > 0 }

// Subscribe registers instruments and, when connected, sends the subscription
// immediately. All instruments are validated first; on a validation error
// nothing is registered. Already subscribed instruments are ignored.
func (s *PriceStream) Subscribe(instruments ...Instrument) error {
	for i := range instruments {
		if err := instruments[i].Validate(); err != nil {
			return &Error{Kind: ErrKindInvalidRequest, Op: s.core.name, InstrumentKey: instruments[i].Key(), Err: err}
		}
	}
	s.mu.Lock()
	cur := *s.subs.Load()
	next := make(map[string]*priceSub, len(cur)+len(instruments))
	for k, v := range cur {
		next[k] = v
	}
	added := make([]*priceSub, 0, len(instruments))
	for i := range instruments {
		key := instruments[i].Key()
		if _, ok := next[key]; ok {
			continue
		}
		sub := &priceSub{key: key, inst: instruments[i]}
		next[key] = sub
		added = append(added, sub)
	}
	if len(added) == 0 {
		s.mu.Unlock()
		return nil
	}
	s.subs.Store(&next)
	connected := s.core.connected()
	var payload []byte
	if connected {
		payload = buildPriceRequest(nil, "subscribe", added)
		s.lastBatch = subKeys(added)
	}
	s.mu.Unlock()
	if !connected {
		s.core.notify()
		return nil
	}
	return s.core.send(payload)
}

// Unsubscribe removes instruments. Unsubscribing the last instrument makes the
// exchange close the connection; the stream then idles until the next Subscribe.
func (s *PriceStream) Unsubscribe(instruments ...Instrument) error {
	s.mu.Lock()
	cur := *s.subs.Load()
	next := make(map[string]*priceSub, len(cur))
	for k, v := range cur {
		next[k] = v
	}
	removed := make([]*priceSub, 0, len(instruments))
	for i := range instruments {
		key := instruments[i].Key()
		if sub, ok := next[key]; ok {
			delete(next, key)
			removed = append(removed, sub)
		}
	}
	if len(removed) == 0 {
		s.mu.Unlock()
		return nil
	}
	s.subs.Store(&next)
	connected := s.core.connected()
	var payload []byte
	if connected {
		payload = buildPriceRequest(nil, "unsubscribe", removed)
	}
	s.mu.Unlock()
	if !connected {
		return nil
	}
	return s.core.send(payload)
}

// remove drops a single key (server-side rejection).
func (s *PriceStream) remove(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := *s.subs.Load()
	if _, ok := cur[key]; !ok {
		return false
	}
	next := make(map[string]*priceSub, len(cur))
	for k, v := range cur {
		if k != key {
			next[k] = v
		}
	}
	s.subs.Store(&next)
	return true
}

// dropLastBatch removes the instruments of the last subscribe request after
// the exchange rejected the request as a whole. It returns the dropped keys.
func (s *PriceStream) dropLastBatch() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := s.lastBatch
	s.lastBatch = nil
	if len(keys) == 0 {
		return nil
	}
	cur := *s.subs.Load()
	next := make(map[string]*priceSub, len(cur))
	for k, v := range cur {
		next[k] = v
	}
	for _, k := range keys {
		delete(next, k)
	}
	s.subs.Store(&next)
	return keys
}

func (s *PriceStream) onOpen() error {
	s.mu.Lock()
	subs := *s.subs.Load()
	all := make([]*priceSub, 0, len(subs))
	for _, sub := range subs {
		all = append(all, sub)
	}
	s.scratch = buildPriceRequest(s.scratch[:0], "subscribe", all)
	s.lastBatch = subKeys(all)
	payload := s.scratch
	// From here on Subscribe/Unsubscribe send their own deltas; everything
	// registered so far is in this batch.
	s.core.markReady()
	s.mu.Unlock()
	if len(all) == 0 {
		return nil
	}
	return s.core.send(payload)
}

func (s *PriceStream) onMessage(data []byte) error {
	data = trimLeadingWS(data)
	if len(data) == 0 {
		return nil
	}
	if data[0] == '{' {
		if bytes.HasPrefix(data, priceMarker) {
			s.upd = PriceUpdate{}
			key, err := parsePriceMessage(data, &s.upd)
			if err != nil {
				return &Error{Kind: ErrKindInvalidResponse, Op: s.core.name, Message: bodyExcerpt(data), Err: err}
			}
			s.upd.ReceivedAt = time.Now()
			if sub := (*s.subs.Load())[string(key)]; sub != nil {
				s.upd.Key = sub.key
				s.upd.Instrument = sub.inst
			} else {
				s.upd.Key = string(key)
				s.upd.Instrument, _ = ParseInstrumentKey(s.upd.Key)
			}
			if s.handler != nil {
				s.handler(&s.upd)
			}
			return nil
		}
		// Heartbeats (every 5s) and any unknown JSON are ignored; the read
		// deadline already covers liveness.
		return nil
	}
	return s.onText(data)
}

// onText handles the plain-text diagnostics the exchange sends before closing
// or after a partial rejection.
func (s *PriceStream) onText(data []byte) error {
	text := strings.TrimSpace(string(data))
	switch {
	case strings.HasPrefix(text, msgUnsupportedInstrument):
		key := strings.TrimSpace(text[len(msgUnsupportedInstrument):])
		s.remove(key)
		return &Error{Kind: ErrKindUnsupportedInstrument, Op: s.core.name, InstrumentKey: key,
			Message: "exchange does not list this instrument; subscription dropped"}
	case text == msgInvalidInstrument || text == msgUnexpectedFormat:
		keys := s.dropLastBatch()
		return &Error{Kind: ErrKindInvalidRequest, Op: s.core.name,
			Message: text + "; dropped subscriptions: " + strings.Join(keys, ", ")}
	case text == msgNoSubscriptions:
		return nil
	}
	return &Error{Kind: ErrKindExchange, Op: s.core.name, Message: text}
}

func buildPriceRequest(dst []byte, action string, subs []*priceSub) []byte {
	dst = append(dst, `{"action":"`...)
	dst = append(dst, action...)
	dst = append(dst, `","instruments":[`...)
	for i, sub := range subs {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = sub.inst.AppendJSON(dst)
	}
	return append(dst, "]}"...)
}

func subKeys(subs []*priceSub) []string {
	keys := make([]string, len(subs))
	for i, sub := range subs {
		keys[i] = sub.key
	}
	return keys
}
