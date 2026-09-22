package variational

import (
	"testing"
)

const marketSnapshot = `[["0G","open"],["1000000MOG","open"],["BTC","open"],["GOOGL","closed"],["XAU","halted_trading"]]`

func TestMarketStatusParse(t *testing.T) {
	var events []MarketStatusEvent
	s := NewMarketStatusStream(func(ev *MarketStatusEvent) {
		cp := *ev
		cp.Updates = append([]MarketStatusUpdate(nil), ev.Updates...)
		events = append(events, cp)
	}, nil)

	if err := s.onOpen(); err != nil {
		t.Fatal(err)
	}
	if err := s.onMessage([]byte(marketSnapshot)); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !events[0].Snapshot || len(events[0].Updates) != 5 || events[0].ReceivedAt.IsZero() {
		t.Fatalf("snapshot event: %+v", events)
	}
	if st, ok := s.Status("GOOGL"); !ok || st != MarketStatusClosed {
		t.Fatalf("GOOGL = %q %v", st, ok)
	}
	if st, ok := s.Status("XAU"); !ok || st != MarketStatusHaltedTrading {
		t.Fatalf("XAU = %q %v", st, ok)
	}
	if _, ok := s.Status("NOPE"); ok {
		t.Fatal("unknown symbol")
	}

	// Single pair update.
	if err := s.onMessage([]byte(`["GOOGL","open"]`)); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Snapshot || len(events[1].Updates) != 1 || events[1].Updates[0] != (MarketStatusUpdate{"GOOGL", MarketStatusOpen}) {
		t.Fatalf("update event: %+v", events[1])
	}
	if st, _ := s.Status("GOOGL"); st != MarketStatusOpen {
		t.Fatal("state not applied")
	}
	// Nested update array with an unknown status and a new symbol.
	if err := s.onMessage([]byte(` [ ["NEW","opening"] , ["BTC","closed"] ] `)); err != nil {
		t.Fatal(err)
	}
	if st, ok := s.Status("NEW"); !ok || st != "opening" {
		t.Fatalf("NEW = %q", st)
	}
	snap := s.Snapshot()
	if len(snap) != 6 || snap["BTC"] != MarketStatusClosed {
		t.Fatalf("snapshot map: %v", snap)
	}
	// Empty array and JSON objects are ignored; text is an exchange error.
	if err := s.onMessage([]byte(`[]`)); err != nil {
		t.Fatal(err)
	}
	if err := s.onMessage([]byte(`{"type":"heartbeat"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.onMessage([]byte(`something went wrong`)); err == nil {
		t.Fatal("text must be reported")
	}
	for _, bad := range []string{`[`, `[["A"]]`, `[["A","open"`, `["A"]`, `[[1,2]]`, `[["A","open"],x]`} {
		if err := s.onMessage([]byte(bad)); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}

	// After a reconnect the first frame is a snapshot again and replaces the state.
	_ = s.onOpen()
	if err := s.onMessage([]byte(`[["BTC","open"]]`)); err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if !last.Snapshot || len(s.Snapshot()) != 1 {
		t.Fatalf("reconnect snapshot: %+v %v", last, s.Snapshot())
	}
}

func TestMarketStatusUpdateNoAlloc(t *testing.T) {
	s := NewMarketStatusStream(nil, nil)
	_ = s.onOpen()
	if err := s.onMessage([]byte(marketSnapshot)); err != nil {
		t.Fatal(err)
	}
	frame := []byte(`["GOOGL","open"]`)
	allocs := testing.AllocsPerRun(1000, func() {
		if err := s.onMessage(frame); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %v, want 0 (symbols and statuses are interned)", allocs)
	}
}
