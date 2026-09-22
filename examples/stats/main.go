// Command stats prints platform statistics and the most active listings.
//
//	go run ./examples/stats [-testnet] [-top 15]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	variational "github.com/tonymontanov/go-variational"
)

func main() {
	testnet := flag.Bool("testnet", false, "use the Omni testnet")
	top := flag.Int("top", 15, "number of listings to print, sorted by 24h volume")
	flag.Parse()

	opts := []variational.Option{
		variational.WithRateLimit(variational.PublicRateLimitRequests, variational.PublicRateLimitWindow),
	}
	if *testnet {
		opts = append(opts, variational.WithTestnet())
	}
	c := variational.NewClient(opts...)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	st, err := c.GetServerTime(ctx)
	if err != nil {
		log.Fatalf("server time: %v", err)
	}
	fmt.Printf("exchange time %s  rtt=%s  local clock offset=%s\n\n", st.Time.Format(time.RFC3339Nano), st.RTT, st.Offset)

	stats, err := c.GetStats(ctx)
	if err != nil {
		log.Fatalf("stats: %v", err)
	}
	fmt.Printf("markets=%d  volume24h=%s  cumulative=%s  tvl=%s  open interest=%s  loss refund pool=%s\n\n",
		stats.NumMarkets, stats.TotalVolume24h.String(), stats.CumulativeVolume.String(), stats.TVL.String(),
		stats.OpenInterest.String(), stats.LossRefund.PoolSize.String())

	listings := stats.Listings
	sort.Slice(listings, func(i, j int) bool {
		return listings[i].Volume24h.MustFloat64() > listings[j].Volume24h.MustFloat64()
	})
	if *top < len(listings) {
		listings = listings[:*top]
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TICKER\tNAME\tMARK\tVOL 24H\tFUNDING\tINTERVAL\tOI LONG\tOI SHORT\tBID\tASK\tSPREAD BPS\tQUOTE AGE")
	for i := range listings {
		l := &listings[i]
		interval := "daily"
		if !l.IsSwap() {
			interval = (time.Duration(l.FundingIntervalS) * time.Second).String()
		}
		bps, _ := l.Quotes.Base.SpreadBps()
		fmt.Fprintf(w, "%s\t%s\t%s\t%.0f\t%s\t%s\t%.0f\t%.0f\t%s\t%s\t%.2f\t%s\n",
			l.Ticker, l.Name, l.MarkPrice.String(), l.Volume24h.MustFloat64(), l.FundingRate.String(), interval,
			l.OpenInterest.Long.MustFloat64(), l.OpenInterest.Short.MustFloat64(),
			l.Quotes.Base.Bid.String(), l.Quotes.Base.Ask.String(), bps, time.Since(l.Quotes.UpdatedAt).Truncate(time.Second))
	}
	w.Flush()
}
