package variational

import (
	"bytes"
	"time"
)

const priceChannelPrefix = "instrument_price:"

var (
	priceMarker     = []byte(`{"channel":"instrument_price:`)
	heartbeatMarker = []byte(`"type":"heartbeat"`)
)

// PriceUpdate is one tick of the /prices stream for a single instrument. The
// exchange publishes roughly one update per second per instrument.
//
// Handlers receive a pointer to a buffer that the stream reuses for the next
// message; copy the struct by value to retain it.
type PriceUpdate struct {
	// Key is the instrument key of the channel, e.g. "P-BTC-USDC-3600".
	Key string
	// Instrument is the subscribed instrument that produced the update.
	Instrument Instrument
	// Price is the mark price in the settlement asset.
	Price Decimal
	// NativePrice is the price expressed relative to the underlying index
	// (mark/index); it is 1 for swaps, which track their index exactly.
	NativePrice Decimal
	// UnderlyingPrice is the index price of the underlying asset.
	UnderlyingPrice Decimal
	// InterestRate is the interest component of the funding rate (per interval).
	InterestRate Decimal
	// Greeks as reported by the pricing engine (delta is 1 for linear contracts).
	Delta, Gamma, Theta, Vega, Rho, IV Decimal
	// Timestamp is the exchange-side pricing time.
	Timestamp time.Time
	// ReceivedAt is the local time the frame was read from the socket.
	ReceivedAt time.Time
}

// MarkPrice is a convenience returning Price as float64 (0 when unset or invalid).
func (u *PriceUpdate) MarkPrice() float64 {
	f, _ := u.Price.Float64()
	return f
}

// IndexPrice is a convenience returning UnderlyingPrice as float64 (0 when unset or invalid).
func (u *PriceUpdate) IndexPrice() float64 {
	f, _ := u.UnderlyingPrice.Float64()
	return f
}

// Basis returns mark minus index as float64.
func (u *PriceUpdate) Basis() float64 { return u.MarkPrice() - u.IndexPrice() }

// parsePriceMessage decodes a `{"channel":"instrument_price:KEY","pricing":{...}}`
// frame into u and returns KEY as a slice aliasing data. It performs no heap
// allocation. Fields absent from the message are left unset.
func parsePriceMessage(data []byte, u *PriceUpdate) ([]byte, error) {
	sc := scanner{b: data}
	if !sc.expect('{') {
		return nil, errMalformed
	}
	var key []byte
	gotPricing := false
	sc.skipWS()
	if sc.peek() == '}' {
		return nil, errMalformed
	}
	for {
		k, ok := sc.readString()
		if !ok || !sc.expect(':') {
			return nil, errMalformed
		}
		switch string(k) {
		case "channel":
			v, ok := sc.readString()
			if !ok || !bytes.HasPrefix(v, []byte(priceChannelPrefix)) {
				return nil, errMalformed
			}
			key = v[len(priceChannelPrefix):]
		case "pricing":
			if err := parsePricing(&sc, u); err != nil {
				return nil, err
			}
			gotPricing = true
		default:
			if !sc.skipValue() {
				return nil, errMalformed
			}
		}
		sc.skipWS()
		switch sc.peek() {
		case ',':
			sc.i++
			sc.skipWS()
		case '}':
			sc.i++
			sc.skipWS()
			if len(key) == 0 || !gotPricing || sc.i != len(sc.b) {
				return nil, errMalformed
			}
			return key, nil
		default:
			return nil, errMalformed
		}
	}
}

func parsePricing(sc *scanner, u *PriceUpdate) error {
	if !sc.expect('{') {
		return errMalformed
	}
	sc.skipWS()
	if sc.peek() == '}' {
		sc.i++
		return nil
	}
	for {
		k, ok := sc.readString()
		if !ok || !sc.expect(':') {
			return errMalformed
		}
		var dst *Decimal
		switch string(k) {
		case "price":
			dst = &u.Price
		case "native_price":
			dst = &u.NativePrice
		case "underlying_price":
			dst = &u.UnderlyingPrice
		case "interest_rate":
			dst = &u.InterestRate
		case "delta":
			dst = &u.Delta
		case "gamma":
			dst = &u.Gamma
		case "theta":
			dst = &u.Theta
		case "vega":
			dst = &u.Vega
		case "rho":
			dst = &u.Rho
		case "iv":
			dst = &u.IV
		case "timestamp":
			v, isNull, ok := sc.readScalar()
			if !ok {
				return errMalformed
			}
			if !isNull {
				t, err := parseTimestamp(v)
				if err != nil {
					return err
				}
				u.Timestamp = t
			}
		default:
			if !sc.skipValue() {
				return errMalformed
			}
		}
		if dst != nil {
			v, isNull, ok := sc.readScalar()
			if !ok {
				return errMalformed
			}
			if !isNull {
				if err := dst.SetBytes(v); err != nil {
					return err
				}
			}
		}
		sc.skipWS()
		switch sc.peek() {
		case ',':
			sc.i++
			sc.skipWS()
		case '}':
			sc.i++
			return nil
		default:
			return errMalformed
		}
	}
}
