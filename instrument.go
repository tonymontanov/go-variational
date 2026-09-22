package variational

import (
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"
)

// InstrumentType identifies the contract family of an Omni market.
type InstrumentType string

const (
	// InstrumentPerpetualFuture is a crypto perpetual future (cross-margined, 24/7).
	InstrumentPerpetualFuture InstrumentType = "perpetual_future"
	// InstrumentSwap is a TradFi total-return swap (daily financing, 23/5 hours).
	InstrumentSwap InstrumentType = "swap"
	// InstrumentPerpetualCFD is a perpetual contract for difference on a TradFi underlying.
	InstrumentPerpetualCFD InstrumentType = "perpetual_cfd"
	// InstrumentPerpetualRWAFuture is a perpetual on a real-world asset.
	InstrumentPerpetualRWAFuture InstrumentType = "perpetual_rwa_future"
)

// AssetClass qualifies TradFi instruments (swaps, CFDs, RWA perpetuals).
type AssetClass string

// Asset classes accepted by the exchange. Stocks and pre-IPO markets are
// AssetClassEquity; ETFs are AssetClassETF; metals and energy are
// AssetClassCommodity; index swaps (US100S, US500S) are AssetClassIndex.
const (
	AssetClassEquity    AssetClass = "equity"
	AssetClassIndex     AssetClass = "index"
	AssetClassCommodity AssetClass = "commodity"
	AssetClassETF       AssetClass = "etf"
)

// Code returns the short code used inside instrument keys (EQY, IDX, CMD, ETF).
func (a AssetClass) Code() string {
	switch a {
	case AssetClassEquity:
		return "EQY"
	case AssetClassIndex:
		return "IDX"
	case AssetClassCommodity:
		return "CMD"
	case AssetClassETF:
		return "ETF"
	case "currency":
		return "FX"
	}
	return strings.ToUpper(string(a))
}

func (a AssetClass) valid() bool {
	switch a {
	case AssetClassEquity, AssetClassIndex, AssetClassCommodity, AssetClassETF:
		return true
	}
	return false
}

func assetClassFromCode(code string) (AssetClass, bool) {
	switch code {
	case "EQY":
		return AssetClassEquity, true
	case "IDX":
		return AssetClassIndex, true
	case "CMD":
		return AssetClassCommodity, true
	case "ETF":
		return AssetClassETF, true
	case "FX":
		return "currency", true
	}
	return "", false
}

const (
	// SettlementUSDC is the only settlement asset on Omni.
	SettlementUSDC = "USDC"
	// PerpetualFundingIntervalS is the funding_interval_s the protocol uses inside
	// the identifier of every crypto perpetual future, regardless of the interval
	// at which funding is actually paid for that listing (see Listing.FundingIntervalS).
	PerpetualFundingIntervalS = 3600
)

// DexTokenDetails identifies a DEX-listed token underlying by chain and contract.
type DexTokenDetails struct {
	Network           string `json:"network"`
	UnderlyingAddress string `json:"underlying_address"`
}

// Instrument is the wire representation of a market used when subscribing to the
// price stream. Build it with the constructors (Perpetual, Swap, CFD,
// RWAPerpetual, DexPerpetual) or parse it from a channel key with
// ParseInstrumentKey.
type Instrument struct {
	// Underlying is the listing ticker, e.g. "BTC", "US100S", "GOOGL".
	Underlying string
	Type       InstrumentType
	// SettlementAsset defaults to USDC when empty.
	SettlementAsset string
	// FundingIntervalS is part of the instrument identity for perpetual futures
	// (always PerpetualFundingIntervalS), swaps (0) and CFDs (the listing's interval).
	// It is not used for RWA perpetuals.
	FundingIntervalS int
	// Kind is the asset class of swaps, CFDs and RWA perpetuals.
	Kind AssetClass
	// DexToken is set for DEX-listed crypto perpetual futures only.
	DexToken *DexTokenDetails
}

// Perpetual returns the crypto perpetual future for underlying (e.g. "BTC").
func Perpetual(underlying string) Instrument {
	return Instrument{
		Underlying:       underlying,
		Type:             InstrumentPerpetualFuture,
		SettlementAsset:  SettlementUSDC,
		FundingIntervalS: PerpetualFundingIntervalS,
	}
}

// DexPerpetual returns the perpetual future for a DEX-listed token identified by
// chain network and contract address.
func DexPerpetual(underlying, network, address string) Instrument {
	i := Perpetual(underlying)
	i.DexToken = &DexTokenDetails{Network: network, UnderlyingAddress: address}
	return i
}

// Swap returns the TradFi swap for underlying (e.g. "US100S", AssetClassIndex).
func Swap(underlying string, class AssetClass) Instrument {
	return Instrument{
		Underlying:      underlying,
		Type:            InstrumentSwap,
		SettlementAsset: SettlementUSDC,
		Kind:            class,
	}
}

// CFD returns the perpetual CFD for underlying with the listing's funding interval.
func CFD(underlying string, class AssetClass, fundingIntervalS int) Instrument {
	return Instrument{
		Underlying:       underlying,
		Type:             InstrumentPerpetualCFD,
		SettlementAsset:  SettlementUSDC,
		Kind:             class,
		FundingIntervalS: fundingIntervalS,
	}
}

// RWAPerpetual returns the real-world-asset perpetual for underlying.
func RWAPerpetual(underlying string, class AssetClass) Instrument {
	return Instrument{
		Underlying:      underlying,
		Type:            InstrumentPerpetualRWAFuture,
		SettlementAsset: SettlementUSDC,
		Kind:            class,
	}
}

var (
	errInstrumentUnderlying = errors.New("variational: instrument underlying is empty or contains '-', quotes or backslashes")
	errInstrumentType       = errors.New("variational: unknown instrument type")
	errInstrumentKind       = errors.New("variational: instrument kind must be one of equity, index, commodity, etf")
	errInstrumentInterval   = errors.New("variational: perpetual futures must use FundingIntervalS == PerpetualFundingIntervalS (3600)")
	errInstrumentDexToken   = errors.New("variational: dex token details require network and underlying address")
	errInstrumentKey        = errors.New("variational: malformed instrument key")
)

func (i *Instrument) settlement() string {
	if i.SettlementAsset == "" {
		return SettlementUSDC
	}
	return i.SettlementAsset
}

// Validate checks that the instrument can be encoded for the exchange. The
// exchange rejects the whole subscription request (and closes the connection)
// when any instrument in it is malformed, so streams validate before sending.
func (i Instrument) Validate() error {
	if i.Underlying == "" || strings.ContainsAny(i.Underlying, "-\"\\") || !utf8.ValidString(i.Underlying) {
		return errInstrumentUnderlying
	}
	switch i.Type {
	case InstrumentPerpetualFuture:
		if i.FundingIntervalS != PerpetualFundingIntervalS {
			return errInstrumentInterval
		}
		if i.DexToken != nil && (i.DexToken.Network == "" || i.DexToken.UnderlyingAddress == "" ||
			strings.ContainsAny(i.DexToken.Network+i.DexToken.UnderlyingAddress, "-_\"\\")) {
			return errInstrumentDexToken
		}
	case InstrumentSwap, InstrumentPerpetualCFD, InstrumentPerpetualRWAFuture:
		if !i.Kind.valid() {
			return errInstrumentKind
		}
	default:
		return errInstrumentType
	}
	return nil
}

// Key returns the canonical channel key of the instrument as used by the
// exchange, e.g. "P-BTC-USDC-3600", "C-IDX-US100S-USDC-0", "P-RWA/EQY-GOOGL-USDC".
func (i Instrument) Key() string {
	return string(i.AppendKey(make([]byte, 0, 32)))
}

// AppendKey appends Key to dst.
func (i *Instrument) AppendKey(dst []byte) []byte {
	switch i.Type {
	case InstrumentSwap, InstrumentPerpetualCFD:
		dst = append(dst, "C-"...)
		dst = append(dst, i.Kind.Code()...)
		dst = append(dst, '-')
		dst = append(dst, i.Underlying...)
		dst = append(dst, '-')
		dst = append(dst, i.settlement()...)
		dst = append(dst, '-')
		dst = strconv.AppendInt(dst, int64(i.FundingIntervalS), 10)
	case InstrumentPerpetualRWAFuture:
		dst = append(dst, "P-RWA/"...)
		dst = append(dst, i.Kind.Code()...)
		dst = append(dst, '-')
		dst = append(dst, i.Underlying...)
		dst = append(dst, '-')
		dst = append(dst, i.settlement()...)
	default: // perpetual future
		dst = append(dst, "P-"...)
		if i.DexToken != nil {
			dst = append(dst, i.DexToken.Network...)
			dst = append(dst, '_')
			dst = append(dst, i.DexToken.UnderlyingAddress...)
			dst = append(dst, '-')
		}
		dst = append(dst, i.Underlying...)
		dst = append(dst, '-')
		dst = append(dst, i.settlement()...)
		dst = append(dst, '-')
		dst = strconv.AppendInt(dst, int64(i.FundingIntervalS), 10)
	}
	return dst
}

// ParseInstrumentKey is the inverse of Key. It also accepts the full channel
// name ("instrument_price:P-BTC-USDC-3600").
func ParseInstrumentKey(key string) (Instrument, error) {
	key = strings.TrimPrefix(key, priceChannelPrefix)
	parts := strings.Split(key, "-")
	var i Instrument
	switch {
	case len(parts) == 5 && parts[0] == "C":
		class, ok := assetClassFromCode(parts[1])
		fi, err := strconv.Atoi(parts[4])
		if !ok || err != nil || parts[2] == "" {
			return i, errInstrumentKey
		}
		i = Instrument{Underlying: parts[2], SettlementAsset: parts[3], Kind: class, FundingIntervalS: fi}
		if fi == 0 {
			i.Type = InstrumentSwap
		} else {
			i.Type = InstrumentPerpetualCFD
		}
	case len(parts) == 4 && parts[0] == "P" && strings.HasPrefix(parts[1], "RWA/"):
		class, ok := assetClassFromCode(strings.TrimPrefix(parts[1], "RWA/"))
		if !ok || parts[2] == "" {
			return i, errInstrumentKey
		}
		i = Instrument{Underlying: parts[2], Type: InstrumentPerpetualRWAFuture, SettlementAsset: parts[3], Kind: class}
	case len(parts) == 4 && parts[0] == "P":
		fi, err := strconv.Atoi(parts[3])
		if err != nil || parts[1] == "" {
			return i, errInstrumentKey
		}
		i = Instrument{Underlying: parts[1], Type: InstrumentPerpetualFuture, SettlementAsset: parts[2], FundingIntervalS: fi}
	case len(parts) == 5 && parts[0] == "P":
		fi, err := strconv.Atoi(parts[4])
		net, addr, ok := strings.Cut(parts[1], "_")
		if err != nil || !ok || parts[2] == "" {
			return i, errInstrumentKey
		}
		i = Instrument{Underlying: parts[2], Type: InstrumentPerpetualFuture, SettlementAsset: parts[3], FundingIntervalS: fi,
			DexToken: &DexTokenDetails{Network: net, UnderlyingAddress: addr}}
	default:
		return i, errInstrumentKey
	}
	if i.SettlementAsset == "" {
		return i, errInstrumentKey
	}
	return i, nil
}

// MarshalJSON encodes the instrument exactly as the exchange expects it in
// subscription requests.
func (i Instrument) MarshalJSON() ([]byte, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}
	return i.AppendJSON(make([]byte, 0, 128)), nil
}

// AppendJSON appends the JSON encoding of the instrument to dst. The caller is
// responsible for calling Validate first.
func (i *Instrument) AppendJSON(dst []byte) []byte {
	dst = append(dst, `{"underlying":`...)
	dst = appendJSONString(dst, i.Underlying)
	dst = append(dst, `,"instrument_type":`...)
	dst = appendJSONString(dst, string(i.Type))
	dst = append(dst, `,"settlement_asset":`...)
	dst = appendJSONString(dst, i.settlement())
	switch i.Type {
	case InstrumentSwap, InstrumentPerpetualCFD:
		dst = append(dst, `,"kind":`...)
		dst = appendJSONString(dst, string(i.Kind))
		dst = append(dst, `,"funding_interval_s":`...)
		dst = strconv.AppendInt(dst, int64(i.FundingIntervalS), 10)
	case InstrumentPerpetualRWAFuture:
		dst = append(dst, `,"kind":`...)
		dst = appendJSONString(dst, string(i.Kind))
	default:
		dst = append(dst, `,"funding_interval_s":`...)
		dst = strconv.AppendInt(dst, int64(i.FundingIntervalS), 10)
		if i.DexToken != nil {
			dst = append(dst, `,"dex_token_details":{"network":`...)
			dst = appendJSONString(dst, i.DexToken.Network)
			dst = append(dst, `,"underlying_address":`...)
			dst = appendJSONString(dst, i.DexToken.UnderlyingAddress)
			dst = append(dst, '}')
		}
	}
	return append(dst, '}')
}

// String returns the instrument key.
func (i Instrument) String() string { return i.Key() }

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string literal with the minimal escaping
// required by RFC 8259.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for j := 0; j < len(s); j++ {
		c := s[j]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		dst = append(dst, s[start:j]...)
		switch c {
		case '"', '\\':
			dst = append(dst, '\\', c)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
		}
		start = j + 1
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
