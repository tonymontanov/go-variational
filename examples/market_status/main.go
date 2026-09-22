// Command market_status follows open/closed/halted transitions of all markets.
//
//	go run ./examples/market_status
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	variational "github.com/tonymontanov/go-variational"
)

func main() {
	testnet := flag.Bool("testnet", false, "use the Omni testnet")
	flag.Parse()

	var opts []variational.Option
	if *testnet {
		opts = append(opts, variational.WithTestnet())
	}
	c := variational.NewClient(opts...)

	stream := c.NewMarketStatusStream(func(ev *variational.MarketStatusEvent) {
		if ev.Snapshot {
			counts := map[variational.MarketStatus]int{}
			for _, u := range ev.Updates {
				counts[u.Status]++
			}
			fmt.Printf("snapshot: %d markets %v\n", len(ev.Updates), counts)
			return
		}
		for _, u := range ev.Updates {
			fmt.Printf("%s -> %s\n", u.Symbol, u.Status)
		}
	}, func(err error) { log.Printf("stream: %v", err) })

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := stream.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
