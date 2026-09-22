package variational

import (
	"errors"
	"testing"
	"time"
)

// Frames captured from the live /prices endpoint.
const (
	frameBTC       = `{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"price":"86162.06","native_price":"0.9995","delta":"1","gamma":"0","theta":"0","vega":"0","rho":"0","iv":"0","underlying_price":"86201.63","interest_rate":"0.0000528800000000000019269482","timestamp":"2026-09-22T13:57:33.229028Z"}}`
	frameSwap      = `{"channel":"instrument_price:C-IDX-US100S-USDC-0","pricing":{"price":"30629.35","native_price":"1","delta":"1","gamma":"0","theta":"0","vega":"0","rho":"0","iv":"0","underlying_price":"30629.35","interest_rate":"0","timestamp":"2026-09-22T13:57:58.533087Z"}}`
	frameRWA       = `{"channel":"instrument_price:P-RWA/EQY-GOOGL-USDC","pricing":{"price":"360.557","native_price":"1.00036","delta":"1","gamma":"0","theta":"0","vega":"0","rho":"0","iv":"0","underlying_price":"360.427","interest_rate":"0","timestamp":"2026-09-22T14:08:47.386121Z"}}`
	frameHeartbeat = `{"timestamp":"2026-09-22T13:51:56.087708510Z","type":"heartbeat"}`
)

func TestParsePriceMessage(t *testing.T) {
	var u PriceUpdate
	key, err := parsePriceMessage([]byte(frameBTC), &u)
	if err != nil {
		t.Fatal(err)
	}
	if string(key) != "P-BTC-USDC-3600" {
		t.Fatalf("key = %q", key)
	}
	want := map[string]string{
		"price": "86162.06", "native": "0.9995", "underlying": "86201.63",
		"rate": "0.0000528800000000000019269482", "delta": "1", "gamma": "0", "theta": "0", "vega": "0", "rho": "0", "iv": "0",
	}
	got := map[string]string{
		"price": u.Price.String(), "native": u.NativePrice.String(), "underlying": u.UnderlyingPrice.String(),
		"rate": u.InterestRate.String(), "delta": u.Delta.String(), "gamma": u.Gamma.String(), "theta": u.Theta.String(),
		"vega": u.Vega.String(), "rho": u.Rho.String(), "iv": u.IV.String(),
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s = %q, want %q", k, got[k], w)
		}
	}
	if !u.Timestamp.Equal(time.Date(2026, 9, 22, 13, 57, 33, 229028000, time.UTC)) {
		t.Fatalf("timestamp = %v", u.Timestamp)
	}
	if u.MarkPrice() != 86162.06 || u.IndexPrice() != 86201.63 || u.Basis() >= 0 {
		t.Fatalf("helpers: %v %v %v", u.MarkPrice(), u.IndexPrice(), u.Basis())
	}

	key, err = parsePriceMessage([]byte(frameRWA), &u)
	if err != nil || string(key) != "P-RWA/EQY-GOOGL-USDC" || u.Price.String() != "360.557" {
		t.Fatalf("rwa: %v %q %s", err, key, u.Price.String())
	}
}

func TestParsePriceMessageTolerant(t *testing.T) {
	// Unknown fields, nested values, whitespace, unquoted numbers and nulls
	// must be handled; field order must not matter.
	frame := ` { "extra" : {"a":[1,2,{"b":"}"}],"c":"x\"y"} , "pricing" : { "timestamp": null, "vega": 0.5, "price" : 100 , "new_field":[true,null], "underlying_price":"99" }, "channel":"instrument_price:P-ETH-USDC-3600" , "n": 1 } `
	var u PriceUpdate
	key, err := parsePriceMessage([]byte(frame), &u)
	if err != nil {
		t.Fatal(err)
	}
	if string(key) != "P-ETH-USDC-3600" || u.Price.String() != "100" || u.Vega.String() != "0.5" || u.UnderlyingPrice.String() != "99" {
		t.Fatalf("parsed %q %+v", key, u)
	}
	if !u.Timestamp.IsZero() || u.Delta.IsSet() {
		t.Fatal("null/absent fields must stay unset")
	}
}

func TestParsePriceMessageMalformed(t *testing.T) {
	bad := []string{
		``, `{}`, `[]`, `{"channel":"instrument_price:P-BTC-USDC-3600"}`,
		`{"pricing":{"price":"1"}}`,
		`{"channel":"other:P-BTC-USDC-3600","pricing":{"price":"1"}}`,
		`{"channel":"instrument_price:","pricing":{"price":"1"}}`,
		`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"price":"abc"}}`,
		`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"price":"1"`,
		`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":["price"]}`,
		`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"price":"1"}}}`,
		`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"price":"1"}} {"more":1}`,
		`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"price":"1"} extra}`,
		`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"timestamp":"nope"}}`,
		"{\"channel\":\"instrument_price:P-BTC\\u002dUSDC-3600\",\"pricing\":{\"price\":\"1\"}}", // escaped channel
		`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"price":"` + string(make([]byte, DecimalCap+1)) + `"}}`,
	}
	for _, f := range bad {
		var u PriceUpdate
		if _, err := parsePriceMessage([]byte(f), &u); err == nil {
			t.Errorf("expected error for %q", f)
		}
	}
	// Minimal valid frame: channel plus a (possibly empty) pricing object.
	var u PriceUpdate
	if key, err := parsePriceMessage([]byte(`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{}} `), &u); err != nil || string(key) != "P-BTC-USDC-3600" {
		t.Fatalf("minimal frame: %v %q", err, key)
	}
	_, err := parsePriceMessage([]byte(`{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"price":"abc"}}`), &u)
	if !errors.Is(err, ErrDecimalSyntax) {
		t.Fatalf("want ErrDecimalSyntax, got %v", err)
	}
}

func TestParsePriceMessageNoAlloc(t *testing.T) {
	frames := [][]byte{[]byte(frameBTC), []byte(frameSwap), []byte(frameRWA)}
	var u PriceUpdate
	allocs := testing.AllocsPerRun(2000, func() {
		for _, f := range frames {
			u = PriceUpdate{}
			if _, err := parsePriceMessage(f, &u); err != nil {
				t.Fatal(err)
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs per run = %v, want 0", allocs)
	}
}

func BenchmarkParsePriceMessage(b *testing.B) {
	frame := []byte(frameBTC)
	var u PriceUpdate
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		u = PriceUpdate{}
		if _, err := parsePriceMessage(frame, &u); err != nil {
			b.Fatal(err)
		}
	}
	sinkF += u.MarkPrice()
}

func TestParseRFC3339UTC(t *testing.T) {
	for _, s := range []string{
		"2026-09-22T13:57:33.229028Z", "2026-09-22T13:51:56.087708510Z", "2026-01-01T00:00:00Z",
		"2026-09-22T13:57:33.1Z", "1999-12-31T23:59:59.999999999Z",
	} {
		want, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := parseRFC3339UTC([]byte(s))
		if !ok || !got.Equal(want) {
			t.Errorf("%s: got %v ok=%v want %v", s, got, ok, want)
		}
	}
	for _, s := range []string{"", "2026-09-22T13:57:33", "2026-09-22T13:57:33+02:00", "2026-09-22 13:57:33Z", "2026-09-22T13:57:33.Z", "2026-09-22T13:57:33.1", "20x6-09-22T13:57:33Z"} {
		if _, ok := parseRFC3339UTC([]byte(s)); ok {
			t.Errorf("%q: expected !ok", s)
		}
	}
	// Non-UTC offsets fall back to time.Parse.
	got, err := parseTimestamp([]byte("2026-09-22T15:57:33.5+02:00"))
	if err != nil || !got.Equal(time.Date(2026, 9, 22, 13, 57, 33, 500000000, time.UTC)) {
		t.Fatalf("fallback: %v %v", got, err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, ok := parseRFC3339UTC([]byte("2026-09-22T13:57:33.229028Z")); !ok {
			t.Fatal("parse")
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %v", allocs)
	}
}

func TestScannerSkipValue(t *testing.T) {
	src := `{"a":[1,{"b":"}\"]"}],"c":"x","d":null,"e":-1.5e3,"f":true}`
	sc := scanner{b: []byte(src)}
	if !sc.expect('{') {
		t.Fatal("expect {")
	}
	n := 0
	for {
		k, ok := sc.readString()
		if !ok || !sc.expect(':') {
			t.Fatalf("key %d", n)
		}
		if !sc.skipValue() {
			t.Fatalf("skipValue after %q", k)
		}
		n++
		sc.skipWS()
		if sc.peek() == ',' {
			sc.i++
			continue
		}
		if !sc.expect('}') {
			t.Fatal("expect }")
		}
		break
	}
	if n != 5 || sc.i != len(src) {
		t.Fatalf("n=%d i=%d", n, sc.i)
	}
	if v, isNull, ok := (&scanner{b: []byte("null,")}).readScalar(); !ok || !isNull || string(v) != "null" {
		t.Fatal("readScalar null")
	}
	if _, ok := (&scanner{b: []byte(`"esc\"aped"`)}).readString(); ok {
		t.Fatal("escaped strings must be rejected by readString")
	}
}
