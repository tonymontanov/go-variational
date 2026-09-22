# Variational Omni — public API notes

Reference for the endpoints wrapped by this SDK. The REST part is documented by
Variational; the WebSocket part was reconstructed from the Omni web application
(SvelteKit bundle, `omni-v3.10.4`) and verified against the live service on
2026-09-22. Everything here is unauthenticated and read-only.

## Hosts

| | Mainnet | Testnet |
|--|---------|---------|
| REST | `https://omni-client-api.prod.ap-northeast-1.variational.io` | `https://omni-client-api.testnet.ap-northeast-1.variational.io` |
| WebSocket | `wss://omni-ws-server.prod.ap-northeast-1.variational.io` | `wss://omni-ws-server.testnet.ap-northeast-1.variational.io` |

Both sit behind Cloudflare. No `Origin` header, cookie or token is required.

## REST

### `GET /metadata/stats`

Documented at <https://docs.variational.io/technical-documentation/api>.

* Rate limit: 10 requests / 10 s per IP, 1000 / min globally. No rate-limit
  headers are returned; a 429 may carry `Retry-After`.
* Caching: `cache-control: public, s-maxage=60, max-age=30`; `last-modified`
  is set. Quotes inside the document may be up to 600 s old (`quotes.updated_at`).
* Body: ~280 KB JSON, ~550 listings. All numbers are strings.
* Query parameters are ignored.

```json
{
  "total_volume_24h": "3626100496.156893",
  "cumulative_volume": "348402580227.375250",
  "tvl": "229362569.698083101125",
  "open_interest": "1816223014.2037356077068758884",
  "num_markets": 552,
  "loss_refund": {"pool_size": "0", "refunded_24h": "0"},
  "listings": [
    {
      "ticker": "BTC",
      "name": "Bitcoin",
      "mark_price": "85734.2744737329",
      "volume_24h": "1581665161.507424",
      "open_interest": {"long_open_interest": "23658826.20", "short_open_interest": "10883942.41"},
      "funding_rate": "0.075426",
      "funding_interval_s": 28800,
      "base_spread_bps": "1.0143632523440326224474320000",
      "quotes": {
        "updated_at": "2026-09-22T13:31:39.081291976Z",
        "base":      {"bid": "85802.92", "ask": "85857.71"},
        "size_1k":   {"bid": "…", "ask": "…"},
        "size_100k": {"bid": "…", "ask": "…"},
        "size_1m":   {"bid": "…", "ask": "…"}
      }
    }
  ]
}
```

Observed: `funding_interval_s` ∈ {0 (swaps), 3600, 14400, 28800}; `size_1m` is
present for 16 of 552 listings; `funding_rate` is a per-interval fraction,
positive = longs pay shorts.

### `GET /status`

Undocumented but public. Returns `{"server_ts_ns": 1790084725944213889}`. Used by
the SDK for clock-offset estimation.

## WebSocket

All frames are text. The server closes the socket with code 1005 (no status)
after protocol errors and when a `/prices` connection has no subscriptions.
Ping frames are answered with pongs on every endpoint.

### `/prices`

Client → server:

```json
{"action": "subscribe",   "instruments": [ <instrument>, … ]}
{"action": "unsubscribe", "instruments": [ <instrument>, … ]}
```

`<instrument>` (serde-strict; unknown `kind` or a perpetual with a non-3600
interval fails the whole request):

```json
{"underlying":"BTC","instrument_type":"perpetual_future","settlement_asset":"USDC","funding_interval_s":3600}
{"underlying":"BTC","instrument_type":"perpetual_future","settlement_asset":"USDC","funding_interval_s":3600,
 "dex_token_details":{"network":"…","underlying_address":"0x…"}}
{"underlying":"GOOGL","instrument_type":"perpetual_rwa_future","settlement_asset":"USDC","kind":"equity"}
{"underlying":"US100S","instrument_type":"swap","settlement_asset":"USDC","kind":"index","funding_interval_s":0}
{"underlying":"XAU","instrument_type":"perpetual_cfd","settlement_asset":"USDC","kind":"commodity","funding_interval_s":14400}
```

`kind` accepted values: `equity`, `index`, `commodity`, `etf`. Rejected:
`currency`, `fx`, `forex` (whole request fails with `unexpected request format`).
`funding_interval_s` may be omitted for swaps (defaults to 0).

Server → client:

```json
{"timestamp":"2026-09-22T13:51:56.087708510Z","type":"heartbeat"}          // every 5 s, also before any subscription
{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{
  "price":"86162.06","native_price":"0.9995","delta":"1","gamma":"0","theta":"0","vega":"0","rho":"0","iv":"0",
  "underlying_price":"86201.63","interest_rate":"0.0000528800000000000019269482","timestamp":"2026-09-22T13:57:33.229028Z"}}
```

* `price` is the mark price, `underlying_price` the index price;
  `native_price ≈ price / underlying_price` (exactly 1 for swaps).
* Roughly one frame per second per instrument; frames for different instruments
  arrive back to back within the same second.
* No acknowledgement for subscribe/unsubscribe.

Plain-text diagnostics:

| Text | Meaning | Connection |
|------|---------|------------|
| `unsupported instrument: <key>` | one instrument of a request is not listed; the rest are subscribed | stays open unless nothing is subscribed |
| `invalid instrument in request` | an instrument failed semantic validation (e.g. perpetual with interval ≠ 3600) | closed |
| `unexpected request format` | JSON did not match the schema (unknown enum value, wrong shape) | closed |
| `no subscriptions open, closing connection` | last subscription removed or rejected | closed |

### Instrument keys

```
perpetual_future      P-<underlying>-<settlement>-<funding_interval_s>          P-BTC-USDC-3600
  with dex token      P-<network>_<address>-<underlying>-<settlement>-<fi>      P-ethereum_0x…-PEPE-USDC-3600
perpetual_rwa_future  P-RWA/<CODE>-<underlying>-<settlement>                    P-RWA/EQY-GOOGL-USDC
swap / perpetual_cfd  C-<CODE>-<underlying>-<settlement>-<funding_interval_s>   C-IDX-US100S-USDC-0
CODE: equity=EQY, index=IDX, commodity=CMD, etf=ETF (currency=FX exists in the app but is not accepted)
```

### `/market_status`

No client messages. On connect the server sends the full snapshot, then
incremental updates. Both shapes must be handled:

```json
[["0G","open"],["1000000MOG","open"],…]   // snapshot: all markets (552 pairs, ~8 KB)
["GOOGL","closed"]                          // single update
[["GOOGL","closed"],["AAPL","closed"]]      // batched update
```

Statuses seen in the app enum: `open`, `closed`, `halted_trading`. The endpoint
sends no heartbeats; use ping/pong for liveness.

### Handshake throttling

The WebSocket host rate-limits handshakes per IP: after roughly 25 connects in a
few minutes it answers `429 Too Many Requests` with a challenge HTML page. The
SDK classifies that as `ErrKindRateLimit` and waits at least 10 s before the
next attempt. Keep one long-lived connection per stream.

## Not wrapped: Omni web-app routes

The application also calls JSON routes on `https://omni.variational.io/api/*`
(and the same paths on the client-API host). They are protected by a Cloudflare
managed challenge and answer non-browser clients with `403` + "Just a moment…".
For reference, the public-looking ones are:

```
/api/metadata/config            /api/metadata/supported_assets   /api/metadata/tiers
/api/metadata/v2/open_interest  /api/metadata/v2/risk_limits     /api/markets/v2/sparklines
/api/candles?period=<Minutes|FiveMinutes|FifteenMinutes|ThirtyMinutes|Hours|FourHours|Days>&start=<RFC3339>&end=<RFC3339>&cex_asset=<TICKER>
/api/funding/v2                 /api/funding/swap                /api/historical_trades
/api/quotes/simple              /api/quotes/indicative           /api/leaderboard/v2
/api/status  /api/version  /api/ping  /api/ff  /api/banner
```

The authenticated WebSocket endpoints `/events` and `/portfolio` on the WS host
require a session and are out of scope for a read-only public SDK.
