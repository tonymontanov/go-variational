package variational

import (
	"errors"
	"math"
	"time"
)

// Stats is the response of GET /metadata/stats: platform-wide metrics plus one
// Listing per market. Prices and volumes are denominated in USDC.
type Stats struct {
	// TotalVolume24h is the 24-hour trading volume.
	TotalVolume24h Decimal `json:"total_volume_24h"`
	// CumulativeVolume is the lifetime trading volume.
	CumulativeVolume Decimal `json:"cumulative_volume"`
	// TVL is the value locked in settlement pools and the OLP vault.
	TVL Decimal `json:"tvl"`
	// OpenInterest is the total open interest across all markets.
	OpenInterest Decimal `json:"open_interest"`
	// NumMarkets is the number of listed markets.
	NumMarkets int `json:"num_markets"`
	// LossRefund describes the loss refund pool.
	LossRefund LossRefund `json:"loss_refund"`
	// Listings holds per-market statistics.
	Listings []Listing `json:"listings"`
}

// LossRefund is the loss refund pool section of Stats.
type LossRefund struct {
	PoolSize    Decimal `json:"pool_size"`
	Refunded24h Decimal `json:"refunded_24h"`
}

// Listing is a single market's statistics.
type Listing struct {
	// Ticker is the market symbol, e.g. "BTC", "GOOGL", "US100S".
	Ticker string `json:"ticker"`
	// Name is the human readable description.
	Name string `json:"name"`
	// MarkPrice is the current mark price.
	MarkPrice Decimal `json:"mark_price"`
	// Volume24h is the 24-hour traded volume.
	Volume24h Decimal `json:"volume_24h"`
	// OpenInterest splits open interest by side.
	OpenInterest ListingOpenInterest `json:"open_interest"`
	// FundingRate is the current funding rate as a decimal fraction per interval
	// (multiply by 100 for a percentage). Positive means longs pay shorts.
	FundingRate Decimal `json:"funding_rate"`
	// FundingIntervalS is the funding interval in seconds. Swaps report 0: they
	// accrue financing once per day at the market close instead.
	FundingIntervalS int `json:"funding_interval_s"`
	// BaseSpreadBps is the base bid/ask spread in basis points.
	BaseSpreadBps Decimal `json:"base_spread_bps"`
	// Quotes are indicative bid/ask levels at several notional sizes.
	Quotes Quotes `json:"quotes"`
}

// IsSwap reports whether the listing is a swap (daily financing, no funding interval).
func (l *Listing) IsSwap() bool { return l.FundingIntervalS == 0 }

// ListingOpenInterest is open interest by side, in base units.
type ListingOpenInterest struct {
	Long  Decimal `json:"long_open_interest"`
	Short Decimal `json:"short_open_interest"`
}

// Quotes are the indicative quotes of a listing. The exchange may cache them for
// up to 600 seconds; UpdatedAt tells when they were last refreshed. Size tiers
// that the market does not quote are nil (many listings have no Size1M tier).
type Quotes struct {
	UpdatedAt time.Time `json:"updated_at"`
	// Base is the quote for a minimal size.
	Base Quote `json:"base"`
	// Size1K, Size100K and Size1M are quotes for 1k, 100k and 1M USDC notional.
	Size1K   *Quote `json:"size_1k"`
	Size100K *Quote `json:"size_100k"`
	Size1M   *Quote `json:"size_1m"`
}

// Quote is a bid/ask pair.
type Quote struct {
	Bid Decimal `json:"bid"`
	Ask Decimal `json:"ask"`
}

var errQuoteIncomplete = errors.New("variational: quote has no bid or ask")

// Mid returns (bid+ask)/2 as float64.
func (q *Quote) Mid() (float64, error) {
	b, a, err := q.floats()
	if err != nil {
		return 0, err
	}
	return (b + a) / 2, nil
}

// Spread returns ask-bid as float64.
func (q *Quote) Spread() (float64, error) {
	b, a, err := q.floats()
	if err != nil {
		return 0, err
	}
	return a - b, nil
}

// SpreadBps returns the spread relative to the mid price in basis points.
func (q *Quote) SpreadBps() (float64, error) {
	b, a, err := q.floats()
	if err != nil {
		return 0, err
	}
	mid := (b + a) / 2
	if mid == 0 {
		return math.NaN(), nil
	}
	return (a - b) / mid * 1e4, nil
}

func (q *Quote) floats() (bid, ask float64, err error) {
	if !q.Bid.IsSet() || !q.Ask.IsSet() {
		return 0, 0, errQuoteIncomplete
	}
	if bid, err = q.Bid.Float64(); err != nil {
		return 0, 0, err
	}
	if ask, err = q.Ask.Float64(); err != nil {
		return 0, 0, err
	}
	return bid, ask, nil
}

// Listing returns the listing with the given ticker, or nil. It is a linear
// scan; build an index with Index for repeated lookups.
func (s *Stats) Listing(ticker string) *Listing {
	for i := range s.Listings {
		if s.Listings[i].Ticker == ticker {
			return &s.Listings[i]
		}
	}
	return nil
}

// Index returns a ticker -> listing map pointing into s.Listings.
func (s *Stats) Index() map[string]*Listing {
	m := make(map[string]*Listing, len(s.Listings))
	for i := range s.Listings {
		m[s.Listings[i].Ticker] = &s.Listings[i]
	}
	return m
}

// ServerTime is the response of GET /status together with a local round-trip
// measurement, which lets callers estimate their clock offset to the exchange.
type ServerTime struct {
	// Time is the exchange clock at the moment it produced the response.
	Time time.Time
	// RTT is the local round-trip time of the request.
	RTT time.Duration
	// Offset estimates exchange clock minus local clock (assuming symmetric
	// latency): local + Offset ≈ exchange.
	Offset time.Duration
}

type statusResponse struct {
	ServerTsNs int64 `json:"server_ts_ns"`
}
