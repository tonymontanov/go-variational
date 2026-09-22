//go:build integration

package variational

import (
	"context"
	"testing"
	"time"
)

// Live tests against the public mainnet endpoints. Run with:
//
//	go test -tags integration -run Live -v ./...
//
// They open one WebSocket connection each; the WS host rate-limits handshakes,
// so do not loop them.

func TestLiveStats(t *testing.T) {
	c := NewClient(WithRateLimit(PublicRateLimitRequests, PublicRateLimitWindow))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := c.GetStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.NumMarkets < 100 || len(s.Listings) < 100 {
		t.Fatalf("suspicious stats: %d markets, %d listings", s.NumMarkets, len(s.Listings))
	}
	btc := s.Listing("BTC")
	if btc == nil || btc.MarkPrice.MustFloat64() <= 0 || !btc.Quotes.Base.Bid.IsSet() {
		t.Fatalf("btc: %+v", btc)
	}
	t.Logf("markets=%d volume24h=%s BTC mark=%s funding=%s/%ds", s.NumMarkets, s.TotalVolume24h.String(), btc.MarkPrice.String(), btc.FundingRate.String(), btc.FundingIntervalS)

	st, err := c.GetServerTime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Time.Year() < 2026 || st.RTT <= 0 {
		t.Fatalf("server time: %+v", st)
	}
	t.Logf("server time %v rtt=%v offset=%v", st.Time, st.RTT, st.Offset)
}

func TestLivePrices(t *testing.T) {
	c := NewClient()
	got := make(chan PriceUpdate, 64)
	errs := make(chan error, 16)
	s := c.NewPriceStream(func(u *PriceUpdate) {
		select {
		case got <- *u:
		default:
		}
	}, func(err error) { errs <- err })
	instruments := []Instrument{Perpetual("BTC"), RWAPerpetual("GOOGL", AssetClassEquity), Swap("US100S", AssetClassIndex)}
	if err := s.Subscribe(instruments...); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go s.Run(ctx)

	seen := map[string]int{}
	deadline := time.After(12 * time.Second)
	for len(seen) < len(instruments) {
		select {
		case u := <-got:
			seen[u.Key]++
			if seen[u.Key] == 1 {
				t.Logf("%s mark=%s index=%s ts=%v", u.Key, u.Price.String(), u.UnderlyingPrice.String(), u.Timestamp)
			}
		case err := <-errs:
			t.Fatalf("stream error: %v", err)
		case <-deadline:
			t.Fatalf("timeout; seen %v", seen)
		}
	}
	if st := s.Stats(); st.Messages == 0 || st.Connects != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestLiveMarketStatus(t *testing.T) {
	c := NewClient()
	got := make(chan MarketStatusEvent, 4)
	s := c.NewMarketStatusStream(func(ev *MarketStatusEvent) {
		cp := *ev
		cp.Updates = append([]MarketStatusUpdate(nil), ev.Updates...)
		select {
		case got <- cp:
		default:
		}
	}, func(err error) { t.Errorf("stream error: %v", err) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go s.Run(ctx)
	select {
	case ev := <-got:
		if !ev.Snapshot || len(ev.Updates) < 100 {
			t.Fatalf("snapshot: %+v", ev)
		}
		t.Logf("snapshot with %d markets; BTC=%v", len(ev.Updates), ev.Updates[0])
	case <-time.After(8 * time.Second):
		t.Fatal("no snapshot")
	}
	if _, ok := s.Status("BTC"); !ok {
		t.Fatal("BTC status missing")
	}
}
