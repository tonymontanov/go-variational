# go-variational

Read-only, low-latency Go SDK for the **public API of [Variational Omni](https://omni.variational.io)**.

The package wraps everything the exchange exposes to unauthenticated, non-browser
clients: the REST statistics and clock endpoints, the real-time instrument price
stream and the market status stream. It contains no trading or account code and
needs no credentials. The API style follows
[adshao/go-binance](https://github.com/adshao/go-binance): a `Client` with typed
request methods, and handler-driven streams cancelled through `context`.

| Transport | Endpoint | SDK |
|-----------|----------|-----|
| REST | `GET /metadata/stats` — platform metrics and per-listing stats (mark price, volume, OI, funding, indicative quotes) | `Client.GetStats`, `Client.GetStatsRaw` |
| REST | `GET /status` — exchange clock in nanoseconds | `Client.GetServerTime` (with RTT and clock-offset estimate) |
| WebSocket | `/prices` — mark/index price ticks per instrument (~1/s) | `PriceStream` |
| WebSocket | `/market_status` — open/closed/halted per market, snapshot + updates | `MarketStatusStream` |

Mainnet and testnet hosts are built in (`WithTestnet`). The single third-party
dependency is `github.com/gorilla/websocket`.

> **Status.** The REST endpoints are documented by Variational
> ([docs](https://docs.variational.io/technical-documentation/api)). The
> WebSocket protocol is not publicly documented; it was derived from the Omni
> web application and verified against the live service on 2026-09-22. See
> [docs/OMNI-PUBLIC-API.md](docs/OMNI-PUBLIC-API.md) for the protocol notes and
> the list of routes that are deliberately *not* wrapped.

## Install

```sh
go get github.com/tonymontanov/go-variational
```

Requires Go 1.25+.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	variational "github.com/tonymontanov/go-variational"
)

func main() {
	ctx := context.Background()
	c := variational.NewClient(
		// Enforce the documented public budget (10 requests / 10 s per IP) client-side.
		variational.WithRateLimit(variational.PublicRateLimitRequests, variational.PublicRateLimitWindow),
	)

	// REST: platform and listing statistics.
	stats, err := c.GetStats(ctx)
	if err != nil {
		log.Fatal(err)
	}
	btc := stats.Listing("BTC")
	fmt.Println(stats.NumMarkets, btc.MarkPrice.String(), btc.FundingRate.String(), btc.Quotes.Base.Bid.String())

	// REST: exchange clock and local clock offset.
	st, _ := c.GetServerTime(ctx)
	fmt.Println(st.Time, st.RTT, st.Offset)

	// WebSocket: real-time prices.
	stream := c.NewPriceStream(
		func(u *variational.PriceUpdate) { // runs on the stream goroutine; do not retain u
			fmt.Println(u.Key, u.Price.String(), u.UnderlyingPrice.String(), u.Timestamp)
		},
		func(err error) { log.Println("stream:", err) },
	)
	_ = stream.Subscribe(
		variational.Perpetual("BTC"),                                 // crypto perpetual
		variational.RWAPerpetual("GOOGL", variational.AssetClassEquity), // stock perpetual
		variational.Swap("US100S", variational.AssetClassIndex),      // TradFi swap
	)
	runCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	log.Println(stream.Run(runCtx)) // blocks; reconnects automatically
}
```

Runnable programs live in [examples/](examples/): `stats`, `prices`, `market_status`.

## Instruments

The price stream identifies markets by an *instrument* object. Build it with the
constructors below; each encoding was verified against the live endpoint.

| Market | Constructor | Wire `instrument_type` | Channel key |
|--------|-------------|------------------------|-------------|
| Crypto perpetual (BTC, ETH, SOL, …) | `Perpetual("BTC")` | `perpetual_future`, `funding_interval_s: 3600` | `P-BTC-USDC-3600` |
| DEX-token perpetual | `DexPerpetual("PEPE", "ethereum", "0x…")` | `perpetual_future` + `dex_token_details` | `P-ethereum_0x…-PEPE-USDC-3600` |
| Stock / pre-IPO perpetual (GOOGL, AAPL, ANTHROPIC) | `RWAPerpetual("GOOGL", AssetClassEquity)` | `perpetual_rwa_future`, `kind: equity` | `P-RWA/EQY-GOOGL-USDC` |
| ETF perpetual (EWJ, QQQ, US500) | `RWAPerpetual("EWJ", AssetClassETF)` | `perpetual_rwa_future`, `kind: etf` | `P-RWA/ETF-EWJ-USDC` |
| Commodity perpetual (XAU, XAG) | `RWAPerpetual("XAU", AssetClassCommodity)` | `perpetual_rwa_future`, `kind: commodity` | `P-RWA/CMD-XAU-USDC` |
| Index swap (US100S, US500S) | `Swap("US100S", AssetClassIndex)` | `swap`, `kind: index` | `C-IDX-US100S-USDC-0` |
| Commodity swap (XAUS, XAGS, USOILP, UKOILP) | `Swap("XAGS", AssetClassCommodity)` | `swap`, `kind: commodity` | `C-CMD-XAGS-USDC-0` |
| Perpetual CFD (type exists in the app, no listing verified) | `CFD("XAU", AssetClassCommodity, 14400)` | `perpetual_cfd` | `C-CMD-XAU-USDC-14400` |

Notes:

* The `3600` in a crypto perpetual key is a protocol constant, not the funding
  interval of the listing (`Listing.FundingIntervalS` is 4h or 8h for most
  markets). Sending any other value makes the exchange reject the whole request.
* Only `equity`, `index`, `commodity` and `etf` are accepted as `kind`;
  `Instrument.Validate` enforces this before anything is sent.
* `ParseInstrumentKey` converts a key (or full channel name) back into an
  `Instrument`, e.g. for keys reported in `Error.InstrumentKey`.
* `GET /metadata/stats` does not carry the instrument type. Swaps are recognisable
  by `Listing.IsSwap()` (funding interval 0 and a "Swap on …" name); whether a
  ticker is a crypto or a TradFi perpetual is knowledge of the listing universe.

## Streams

Both streams share one connection manager:

* **Lifecycle.** `Run(ctx)` blocks and returns `ctx.Err()` when the context ends
  or `ErrStreamClosed` after `Close()`. `Subscribe` may be called before or
  after `Run`, from any goroutine; subscriptions are re-sent after every
  reconnect. A `PriceStream` with no subscriptions holds no connection (state
  `StreamIdle`) because the exchange closes idle sockets.
* **Reconnect.** Exponential backoff with jitter (`ReconnectPolicy`, default
  250 ms → 30 s). A handshake rejected with HTTP 429 (the WebSocket host throttles
  frequent connects) waits at least 10 s. A session that stayed up longer than
  `MaxDelay` resets the backoff.
* **Liveness.** `/prices` sends a heartbeat every 5 s; the stream reconnects
  after 15 s of silence (`WithStreamReadTimeout`). `/market_status` sends nothing
  between changes, so the stream pings every 15 s and reconnects when pongs stop.
* **Handlers** run on the stream goroutine. The `*PriceUpdate` and
  `*MarketStatusEvent` arguments are buffers reused for the next frame: copy the
  struct (or the `Updates` slice) to keep it. Errors go to the error handler and
  never stop the stream, except that the stream drops subscriptions the exchange
  rejected (`ErrUnsupportedInstrument`, or `ErrInvalidRequest` for a whole
  batch) so it does not loop on them.
* **Observability.** `State()`, `Stats()` (frames, bytes, reconnects, errors,
  timestamps) and `WithStreamStateHandler`.

## Errors

Every failure is a `*variational.Error` with a `Kind`:

| Kind | Meaning | `Temporary()` |
|------|---------|---------------|
| `ErrKindNetwork` | dial/timeout/reset/EOF, read timeout | yes |
| `ErrKindRateLimit` | HTTP 429 (`RetryAfter` set when the header is present) | yes |
| `ErrKindForbidden` | HTTP 401/403, incl. the browser challenge on web-app routes | no |
| `ErrKindInvalidRequest` | other 4xx, or a WebSocket request the exchange refused to parse | no |
| `ErrKindUnsupportedInstrument` | the exchange does not list the instrument (`InstrumentKey` set) | no |
| `ErrKindExchange` | HTTP 5xx or a server-side error text | yes |
| `ErrKindInvalidResponse` | undecodable payload | no |
| `ErrKindClosed` | `Run` ended after `Close()` | no |

Sentinels (`ErrRateLimited`, `ErrForbidden`, `ErrUnsupportedInstrument`, …) work
with `errors.Is`; the wrapped cause (for example `context.DeadlineExceeded`) is
reachable through `errors.Is`/`errors.As` as well.

REST calls retry network errors, 5xx and 429 with backoff (`RetryPolicy`, default
3 attempts). A 429 whose `Retry-After` exceeds `MaxRetryAfter` is returned
immediately so the caller can decide.

## Performance

The WebSocket hot path is allocation-free in steady state:

* frames are read into a reusable buffer (`NextReader` + `readInto`);
* a hand-written scanner decodes the fixed-shape price message straight into a
  reusable `PriceUpdate`;
* numbers are kept verbatim in `Decimal`, a 48-byte inline value that converts
  to `float64` without touching the heap and preserves the exchange's full
  precision (`Rat()` for exact arithmetic);
* the channel key is resolved through a copy-on-write map keyed by the
  subscription's interned key, so the handler receives a pre-built
  `Instrument` and a stable `Key` string;
* market status symbols are interned after the first snapshot.

Measured with `go test -bench . -benchmem` on an Apple M4 Pro (Go 1.25):

| Benchmark | ns/op | allocs/op |
|-----------|------:|----------:|
| `ParsePriceMessage` (288-byte frame → `PriceUpdate`) | 225 | 0 |
| `PriceStreamOnMessage` (parse + key lookup + handler dispatch) | 270 | 0 |
| `DecimalFloat64` | 15 | 0 |

That is roughly three orders of magnitude under the 100 µs per-message budget
typical for an HFT desk; the remaining cost per tick is the WebSocket frame
read in gorilla (one small allocation per frame for its reader object).

Tests assert zero allocations for the parser, the stream message path, the
market-status update path and key/JSON encoding (`testing.AllocsPerRun`).

## Configuration

| Option | Default |
|--------|---------|
| `WithBaseURL`, `WithWSURL`, `WithTestnet` | mainnet hosts |
| `WithHTTPClient`, `WithTimeout` | tuned `http.Transport` (HTTP/2, keep-alive), 10 s per request |
| `WithUserAgent`, `WithHeader` | `go-variational/<version>` |
| `WithRateLimit(n, window)` | off; use `PublicRateLimitRequests`/`PublicRateLimitWindow` |
| `WithRetry(RetryPolicy)` | 3 attempts, 200 ms → 2 s, honour `Retry-After` ≤ 5 s |
| `WithDialer`, `WithReconnect` | gorilla dialer with 10 s handshake; `DefaultReconnectPolicy` |
| `WithMaxResponseSize` | 32 MiB |
| Stream options: `WithStreamURL`, `WithStreamReconnect`, `WithStreamReadTimeout`, `WithStreamPingInterval`, `WithStreamHeader`, `WithStreamDialer`, `WithStreamStateHandler` | see above |

## Testing

```sh
go test -race ./...                       # unit tests with an in-process fake exchange
go test -bench . -benchmem                # benchmarks
go test -tags integration -run Live -v .  # live smoke tests against mainnet (one WS connection each)
```

## Not covered

* Routes of the Omni web application under `/api/*` (candles, funding history,
  supported assets, quotes, leaderboard, …). They are served only to browsers
  behind a challenge page; the SDK does not attempt to work around that.
* Real-time bid/ask. The public surface only exposes indicative quotes inside
  `GET /metadata/stats`, cached by the exchange for up to 600 s
  (`Quotes.UpdatedAt`). The price stream carries mark and index prices.
* The trading API, which Variational has not released.
* DEX-token perpetual encoding follows the web app but no such listing was
  available to verify against.

## Кратко по-русски

Библиотека покрывает весь публичный (неавторизованный) API Variational Omni,
доступный вне браузера: REST `GET /metadata/stats` и `GET /status`, WebSocket
`/prices` (mark/index-цены по инструментам, ~1 обновление/с) и `/market_status`
(статусы рынков). Торговых методов нет. Горячий путь WS не аллоцирует: кадр
читается в переиспользуемый буфер, разбирается ручным сканером в
переиспользуемый `PriceUpdate`, числа хранятся как `Decimal` (inline, без heap,
точность биржи сохранена). Реконнект с экспоненциальным бэкоффом и джиттером,
автоматическая переподписка, контроль живости по heartbeat/ping. Ошибки — единый
тип `*Error` с `Kind` и sentinel-значениями для `errors.Is`. Детали протокола —
в [docs/OMNI-PUBLIC-API.md](docs/OMNI-PUBLIC-API.md).

## License

Apache-2.0. This is an independent project, not affiliated with Variational.
