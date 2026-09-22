// Command prices follows the real-time price stream for a set of instruments.
//
//	go run ./examples/prices -perp BTC,ETH -rwa GOOGL:equity,XAU:commodity -swap US100S:index
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	variational "github.com/tonymontanov/go-variational"
)

func main() {
	perps := flag.String("perp", "BTC,ETH", "comma separated crypto perpetual tickers")
	rwa := flag.String("rwa", "GOOGL:equity", "comma separated TradFi perpetuals as TICKER:class (equity|etf|commodity)")
	swaps := flag.String("swap", "US100S:index", "comma separated swaps as TICKER:class (index|commodity)")
	testnet := flag.Bool("testnet", false, "use the Omni testnet")
	flag.Parse()

	var instruments []variational.Instrument
	for _, t := range split(*perps) {
		instruments = append(instruments, variational.Perpetual(t))
	}
	for _, t := range split(*rwa) {
		ticker, class, _ := strings.Cut(t, ":")
		instruments = append(instruments, variational.RWAPerpetual(ticker, variational.AssetClass(class)))
	}
	for _, t := range split(*swaps) {
		ticker, class, _ := strings.Cut(t, ":")
		instruments = append(instruments, variational.Swap(ticker, variational.AssetClass(class)))
	}
	if len(instruments) == 0 {
		log.Fatal("no instruments")
	}

	var opts []variational.Option
	if *testnet {
		opts = append(opts, variational.WithTestnet())
	}
	c := variational.NewClient(opts...)

	// The handler runs on the stream goroutine; keep it short. Here we only
	// format and print, so no copy of the update is needed.
	onUpdate := func(u *variational.PriceUpdate) {
		age := u.ReceivedAt.Sub(u.Timestamp) // includes local clock offset
		fmt.Printf("%-28s mark=%-14s index=%-14s basis=%+.4f  exchange ts=%s  age=%s\n",
			u.Key, u.Price.String(), u.UnderlyingPrice.String(), u.Basis(),
			u.Timestamp.Format("15:04:05.000000"), age.Truncate(time.Millisecond))
	}
	onErr := func(err error) { log.Printf("stream: %v", err) }
	onState := func(st variational.StreamState) { log.Printf("state: %s", st) }

	stream := c.NewPriceStream(onUpdate, onErr, variational.WithStreamStateHandler(onState))
	if err := stream.Subscribe(instruments...); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s := stream.Stats()
				log.Printf("stats: messages=%d bytes=%d reconnects=%d errors=%d", s.Messages, s.Bytes, s.Reconnects, s.Errors)
			}
		}
	}()
	if err := stream.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
