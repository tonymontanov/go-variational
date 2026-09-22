package variational

import (
	"encoding/json"
	"errors"
	"testing"
)

// Golden encodings verified live against the /prices endpoint.
var instrumentGolden = []struct {
	name string
	inst Instrument
	key  string
	json string
}{
	{
		name: "crypto perpetual",
		inst: Perpetual("BTC"),
		key:  "P-BTC-USDC-3600",
		json: `{"underlying":"BTC","instrument_type":"perpetual_future","settlement_asset":"USDC","funding_interval_s":3600}`,
	},
	{
		name: "index swap",
		inst: Swap("US100S", AssetClassIndex),
		key:  "C-IDX-US100S-USDC-0",
		json: `{"underlying":"US100S","instrument_type":"swap","settlement_asset":"USDC","kind":"index","funding_interval_s":0}`,
	},
	{
		name: "commodity swap",
		inst: Swap("XAGS", AssetClassCommodity),
		key:  "C-CMD-XAGS-USDC-0",
		json: `{"underlying":"XAGS","instrument_type":"swap","settlement_asset":"USDC","kind":"commodity","funding_interval_s":0}`,
	},
	{
		name: "equity rwa perpetual",
		inst: RWAPerpetual("GOOGL", AssetClassEquity),
		key:  "P-RWA/EQY-GOOGL-USDC",
		json: `{"underlying":"GOOGL","instrument_type":"perpetual_rwa_future","settlement_asset":"USDC","kind":"equity"}`,
	},
	{
		name: "etf rwa perpetual",
		inst: RWAPerpetual("EWJ", AssetClassETF),
		key:  "P-RWA/ETF-EWJ-USDC",
		json: `{"underlying":"EWJ","instrument_type":"perpetual_rwa_future","settlement_asset":"USDC","kind":"etf"}`,
	},
	{
		name: "commodity rwa perpetual",
		inst: RWAPerpetual("XAU", AssetClassCommodity),
		key:  "P-RWA/CMD-XAU-USDC",
		json: `{"underlying":"XAU","instrument_type":"perpetual_rwa_future","settlement_asset":"USDC","kind":"commodity"}`,
	},
	{
		name: "cfd",
		inst: CFD("XAU", AssetClassCommodity, 14400),
		key:  "C-CMD-XAU-USDC-14400",
		json: `{"underlying":"XAU","instrument_type":"perpetual_cfd","settlement_asset":"USDC","kind":"commodity","funding_interval_s":14400}`,
	},
	{
		name: "dex perpetual",
		inst: DexPerpetual("PEPE", "ethereum", "0x6982508145454ce325ddbe47a25d4ec3d2311933"),
		key:  "P-ethereum_0x6982508145454ce325ddbe47a25d4ec3d2311933-PEPE-USDC-3600",
		json: `{"underlying":"PEPE","instrument_type":"perpetual_future","settlement_asset":"USDC","funding_interval_s":3600,"dex_token_details":{"network":"ethereum","underlying_address":"0x6982508145454ce325ddbe47a25d4ec3d2311933"}}`,
	},
}

func TestInstrumentGolden(t *testing.T) {
	for _, g := range instrumentGolden {
		t.Run(g.name, func(t *testing.T) {
			if err := g.inst.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if got := g.inst.Key(); got != g.key {
				t.Fatalf("Key = %q, want %q", got, g.key)
			}
			if got := g.inst.String(); got != g.key {
				t.Fatalf("String = %q", got)
			}
			b, err := json.Marshal(g.inst)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != g.json {
				t.Fatalf("json = %s\nwant   %s", b, g.json)
			}
			if got := string(g.inst.AppendJSON(nil)); got != g.json {
				t.Fatalf("AppendJSON = %s", got)
			}
			back, err := ParseInstrumentKey(g.key)
			if err != nil {
				t.Fatalf("ParseInstrumentKey: %v", err)
			}
			if back.Key() != g.key {
				t.Fatalf("round trip key = %q", back.Key())
			}
			if back.Type != g.inst.Type || back.Underlying != g.inst.Underlying || back.Kind != g.inst.Kind ||
				back.FundingIntervalS != g.inst.FundingIntervalS || back.SettlementAsset != g.inst.SettlementAsset {
				t.Fatalf("round trip = %+v, want %+v", back, g.inst)
			}
			if (back.DexToken == nil) != (g.inst.DexToken == nil) || (back.DexToken != nil && *back.DexToken != *g.inst.DexToken) {
				t.Fatalf("dex token round trip = %+v", back.DexToken)
			}
			withPrefix, err := ParseInstrumentKey(priceChannelPrefix + g.key)
			if err != nil || withPrefix.Key() != g.key {
				t.Fatalf("channel prefix parse: %v %q", err, withPrefix.Key())
			}
		})
	}
}

func TestParseInstrumentKeyInvalid(t *testing.T) {
	for _, k := range []string{"", "BTC", "P-BTC-USDC", "P-BTC-USDC-x", "C-ZZZ-US100S-USDC-0", "C-IDX-US100S-USDC", "P-RWA/XXX-GOOGL-USDC", "X-BTC-USDC-3600", "P--USDC-3600", "P-net-BTC-USDC-3600"} {
		if _, err := ParseInstrumentKey(k); err == nil {
			t.Fatalf("%q: expected error", k)
		}
	}
}

func TestInstrumentValidate(t *testing.T) {
	bad := Perpetual("BTC")
	bad.FundingIntervalS = 28800
	cases := map[string]struct {
		inst Instrument
		err  error
	}{
		"empty":            {Instrument{}, errInstrumentUnderlying},
		"dash":             {Perpetual("A-B"), errInstrumentUnderlying},
		"quote":            {Perpetual(`A"B`), errInstrumentUnderlying},
		"interval":         {bad, errInstrumentInterval},
		"swap kind":        {Instrument{Underlying: "US100S", Type: InstrumentSwap}, errInstrumentKind},
		"currency kind":    {Swap("EURUSDS", "currency"), errInstrumentKind},
		"rwa kind":         {Instrument{Underlying: "GOOGL", Type: InstrumentPerpetualRWAFuture}, errInstrumentKind},
		"type":             {Instrument{Underlying: "BTC", Type: "dated_future"}, errInstrumentType},
		"dex missing addr": {DexPerpetual("PEPE", "ethereum", ""), errInstrumentDexToken},
	}
	for name, c := range cases {
		if err := c.inst.Validate(); !errors.Is(err, c.err) {
			t.Errorf("%s: got %v, want %v", name, err, c.err)
		}
	}
	if _, err := json.Marshal(Instrument{}); err == nil {
		t.Fatal("MarshalJSON must validate")
	}
	// Settlement asset defaults to USDC.
	i := Instrument{Underlying: "BTC", Type: InstrumentPerpetualFuture, FundingIntervalS: PerpetualFundingIntervalS}
	if i.Key() != "P-BTC-USDC-3600" {
		t.Fatalf("default settlement: %q", i.Key())
	}
}

func TestAssetClassCodes(t *testing.T) {
	for class, code := range map[AssetClass]string{AssetClassEquity: "EQY", AssetClassIndex: "IDX", AssetClassCommodity: "CMD", AssetClassETF: "ETF", "currency": "FX", "other": "OTHER"} {
		if class.Code() != code {
			t.Errorf("%s.Code() = %s", class, class.Code())
		}
	}
}

func TestAppendJSONString(t *testing.T) {
	for in, want := range map[string]string{
		"BTC":              `"BTC"`,
		`a"b`:              `"a\"b"`,
		`a\b`:              `"a\\b"`,
		"a\nb":             `"a\nb"`,
		"a\x01b":           `"a\u0001b"`,
		"ünïcode":          `"ünïcode"`,
		"":                 `""`,
		"tab\there\r\n":    `"tab\there\r\n"`,
		"end with quote\"": `"end with quote\""`,
	} {
		if got := string(appendJSONString(nil, in)); got != want {
			t.Errorf("%q: got %s want %s", in, got, want)
		}
		var back string
		if err := json.Unmarshal(appendJSONString(nil, in), &back); err != nil || back != in {
			t.Errorf("%q: not valid JSON round trip: %v %q", in, err, back)
		}
	}
}

func TestInstrumentKeyNoAlloc(t *testing.T) {
	inst := Swap("US100S", AssetClassIndex)
	buf := make([]byte, 0, 64)
	allocs := testing.AllocsPerRun(1000, func() {
		buf = inst.AppendKey(buf[:0])
		buf = inst.AppendJSON(buf[:0])
	})
	if allocs != 0 {
		t.Fatalf("allocs = %v", allocs)
	}
}
