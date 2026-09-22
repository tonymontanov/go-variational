// Package variational is a read-only, low-latency Go SDK for the public
// (unauthenticated) API of the Variational Omni exchange.
//
// The SDK covers two transports:
//
//   - REST: platform and per-listing market statistics (GET /metadata/stats)
//     and the exchange clock (GET /status), served by the public client API host.
//   - WebSocket: the real-time instrument price stream (/prices) and the
//     market status stream (/market_status), served by the public WS host.
//
// Nothing in this package places orders or requires credentials. The API surface
// follows the style of adshao/go-binance: a Client with typed request methods and
// Watch-style streams driven by handler callbacks and cancelled via context.
//
// # Quick start
//
//	c := variational.NewClient()
//	stats, err := c.GetStats(ctx)
//	// ...
//	stream := c.NewPriceStream(func(u *variational.PriceUpdate) {
//	    fmt.Println(u.Key, u.Price.String(), u.UnderlyingPrice.String())
//	}, func(err error) { log.Println(err) })
//	_ = stream.Subscribe(variational.Perpetual("BTC"), variational.Swap("US100S", variational.AssetClassIndex))
//	go stream.Run(ctx)
//
// # Performance notes
//
// The WebSocket hot path (frame read, JSON parse, handler dispatch) performs no
// heap allocations per message: frames are read into a reusable buffer and
// decoded by a hand-written scanner into a reusable PriceUpdate whose numeric
// fields are fixed-capacity Decimal values. Handlers must therefore not retain
// the *PriceUpdate they receive; copy the struct by value if it has to outlive
// the callback (a value copy is a full deep copy).
//
// # Coverage and limits
//
// Only the endpoints that Variational exposes to non-browser clients are covered.
// The Omni web application uses additional routes under /api/* (candles, funding
// history, supported assets, quotes) that sit behind a browser challenge and are
// not part of the public API; they are intentionally not wrapped here.
package variational
