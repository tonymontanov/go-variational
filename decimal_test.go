package variational

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestDecimalParseValid(t *testing.T) {
	for _, s := range []string{"0", "86162.06", "-1.5", "+2", "0.0000528800000000000019269482", "1e5", "1.5E-3", ".5", "5.", "1816223014.2037356077068758884"} {
		d, err := ParseDecimal(s)
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		if d.String() != s || d.Len() != len(s) || !d.IsSet() {
			t.Fatalf("%q: round trip mismatch: %q", s, d.String())
		}
	}
}

func TestDecimalParseInvalid(t *testing.T) {
	for _, s := range []string{"", "-", ".", "1.2.3", "abc", "1e", "1e+", "--1", " 1", "1 ", "0x10", "NaN", "\"1\""} {
		if _, err := ParseDecimal(s); !errors.Is(err, ErrDecimalSyntax) {
			t.Fatalf("%q: want ErrDecimalSyntax, got %v", s, err)
		}
	}
	if _, err := ParseDecimal(strings.Repeat("9", DecimalCap+1)); !errors.Is(err, ErrDecimalTooLong) {
		t.Fatalf("want ErrDecimalTooLong, got %v", err)
	}
	if _, err := ParseDecimal(strings.Repeat("9", DecimalCap)); err != nil {
		t.Fatalf("exact capacity should parse: %v", err)
	}
}

func TestDecimalConversions(t *testing.T) {
	d := MustDecimal("86162.06")
	f, err := d.Float64()
	if err != nil || f != 86162.06 {
		t.Fatalf("Float64 = %v, %v", f, err)
	}
	if d.MustFloat64() != 86162.06 {
		t.Fatal("MustFloat64")
	}
	r, err := d.Rat()
	if err != nil || r.FloatString(2) != "86162.06" {
		t.Fatalf("Rat = %v, %v", r, err)
	}
	var unset Decimal
	if _, err := unset.Float64(); !errors.Is(err, ErrDecimalUnset) {
		t.Fatalf("unset Float64: %v", err)
	}
	if unset.IsSet() || unset.IsZero() || unset.Sign() != 0 {
		t.Fatal("unset predicates")
	}
	for s, zero := range map[string]bool{"0": true, "0.000": true, "-0": true, "0e5": true, "0.1": false, "10": false, "-3": false} {
		d := MustDecimal(s)
		if d.IsZero() != zero {
			t.Fatalf("%q IsZero = %v", s, d.IsZero())
		}
	}
	for s, sign := range map[string]int{"0": 0, "-0.0": 0, "-3": -1, "+3": 1, "0.1": 1} {
		d := MustDecimal(s)
		if d.Sign() != sign {
			t.Fatalf("%q Sign = %d", s, d.Sign())
		}
	}
	a, b := MustDecimal("1"), MustDecimal("1")
	if !a.Equal(b) || a != b {
		t.Fatal("equal decimals must compare equal with ==")
	}
	if a.Equal(MustDecimal("1.0")) {
		t.Fatal("Equal is textual")
	}
	buf := a.AppendTo([]byte("x="))
	if string(buf) != "x=1" {
		t.Fatalf("AppendTo = %q", buf)
	}
}

func TestDecimalJSON(t *testing.T) {
	type payload struct {
		S Decimal `json:"s"`
		N Decimal `json:"n"`
		Z Decimal `json:"z"`
		M Decimal `json:"m,omitempty"`
	}
	var p payload
	if err := json.Unmarshal([]byte(`{"s":"12.5","n":-7e2,"z":null}`), &p); err != nil {
		t.Fatal(err)
	}
	if p.S.String() != "12.5" || p.N.String() != "-7e2" || p.Z.IsSet() || p.M.IsSet() {
		t.Fatalf("decoded %+v", p)
	}
	out, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"s":"12.5","n":"-7e2","z":null,"m":null}` {
		t.Fatalf("encoded %s", out)
	}
	if err := json.Unmarshal([]byte(`{"s":"abc"}`), &p); !errors.Is(err, ErrDecimalSyntax) {
		t.Fatalf("want syntax error, got %v", err)
	}
	var d Decimal
	if err := d.UnmarshalText([]byte("3.25")); err != nil || d.String() != "3.25" {
		t.Fatal("UnmarshalText")
	}
	txt, _ := d.MarshalText()
	if string(txt) != "3.25" {
		t.Fatal("MarshalText")
	}
}

var sinkF float64

func TestDecimalNoAlloc(t *testing.T) {
	src := []byte("86162.06")
	allocs := testing.AllocsPerRun(1000, func() {
		var d Decimal
		if err := d.SetBytes(src); err != nil {
			t.Fatal(err)
		}
		sinkF += d.MustFloat64()
		sinkF += float64(d.Sign())
	})
	if allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
}

func BenchmarkDecimalFloat64(b *testing.B) {
	d := MustDecimal("86162.06")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sinkF += d.MustFloat64()
	}
}
