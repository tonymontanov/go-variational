package variational

import (
	"errors"
	"math/big"
	"strconv"
	"unsafe"
)

// DecimalCap is the maximum number of characters a Decimal can hold. The
// exchange emits fixed-point strings of at most ~31 characters; the extra
// headroom keeps the type future proof without making it large.
const DecimalCap = 47

// Decimal is an exchange numeric value kept verbatim as its decimal string
// representation, stored inline in a fixed-capacity byte array. It preserves the
// exact precision sent by the exchange (all Variational numbers are JSON strings)
// and never allocates: it can be copied by value, compared with ==, and converted
// to float64 on the hot path without touching the heap.
//
// The zero value is "unset" (IsSet reports false); it marshals to JSON null.
type Decimal struct {
	n uint8
	b [DecimalCap]byte
}

var (
	// ErrDecimalTooLong is returned when a numeric literal exceeds DecimalCap.
	ErrDecimalTooLong = errors.New("variational: decimal literal exceeds capacity")
	// ErrDecimalSyntax is returned for a literal that is not a decimal number.
	ErrDecimalSyntax = errors.New("variational: invalid decimal literal")
	// ErrDecimalUnset is returned when converting an unset Decimal.
	ErrDecimalUnset = errors.New("variational: decimal is unset")
)

// ParseDecimal validates s and returns it as a Decimal.
func ParseDecimal(s string) (Decimal, error) {
	var d Decimal
	err := d.SetBytes(unsafe.Slice(unsafe.StringData(s), len(s)))
	return d, err
}

// MustDecimal is like ParseDecimal but panics on error. Intended for constants
// and tests.
func MustDecimal(s string) Decimal {
	d, err := ParseDecimal(s)
	if err != nil {
		panic(err)
	}
	return d
}

// Set validates s and stores it.
func (d *Decimal) Set(s string) error {
	return d.SetBytes(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// SetBytes validates b and stores a copy of it.
func (d *Decimal) SetBytes(b []byte) error {
	if len(b) > DecimalCap {
		return ErrDecimalTooLong
	}
	if !validDecimal(b) {
		return ErrDecimalSyntax
	}
	d.n = uint8(len(b))
	copy(d.b[:], b)
	clear(d.b[len(b):])
	return nil
}

// Reset clears d to the unset state.
func (d *Decimal) Reset() {
	d.n = 0
	clear(d.b[:])
}

// validDecimal accepts -?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)? with at least one digit.
func validDecimal(b []byte) bool {
	i := 0
	if i < len(b) && (b[i] == '-' || b[i] == '+') {
		i++
	}
	digits := 0
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		i++
		digits++
	}
	if i < len(b) && b[i] == '.' {
		i++
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '-' || b[i] == '+') {
			i++
		}
		exp := 0
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
			exp++
		}
		if exp == 0 {
			return false
		}
	}
	return i == len(b)
}

// IsSet reports whether d holds a value.
func (d *Decimal) IsSet() bool { return d.n > 0 }

// Len returns the number of characters in the literal.
func (d *Decimal) Len() int { return int(d.n) }

// String returns the literal exactly as sent by the exchange. It allocates.
func (d Decimal) String() string { return string(d.b[:d.n]) }

// AppendTo appends the literal to dst and returns the extended slice.
func (d *Decimal) AppendTo(dst []byte) []byte { return append(dst, d.b[:d.n]...) }

// view returns a string aliasing the internal storage. The result must not
// outlive d and must never be retained.
func (d *Decimal) view() string {
	if d.n == 0 {
		return ""
	}
	return unsafe.String(&d.b[0], int(d.n))
}

// Float64 converts the literal to float64 without allocating.
func (d *Decimal) Float64() (float64, error) {
	if d.n == 0 {
		return 0, ErrDecimalUnset
	}
	return strconv.ParseFloat(d.view(), 64)
}

// MustFloat64 is like Float64 but panics on error. Values produced by the
// exchange always validate, so this is safe on the hot path.
func (d *Decimal) MustFloat64() float64 {
	f, err := d.Float64()
	if err != nil {
		panic(err)
	}
	return f
}

// Rat converts the literal to an exact big.Rat.
func (d *Decimal) Rat() (*big.Rat, error) {
	if d.n == 0 {
		return nil, ErrDecimalUnset
	}
	r, ok := new(big.Rat).SetString(d.String())
	if !ok {
		return nil, ErrDecimalSyntax
	}
	return r, nil
}

// IsZero reports whether the literal is numerically zero ("0", "0.000", "-0").
// An unset Decimal is not zero.
func (d *Decimal) IsZero() bool {
	if d.n == 0 {
		return false
	}
	for _, c := range d.b[:d.n] {
		switch c {
		case '0', '.', '-', '+':
		case 'e', 'E':
			return true // mantissa was all zeros
		default:
			return false
		}
	}
	return true
}

// Sign returns -1, 0 or +1 according to the sign of the literal.
func (d *Decimal) Sign() int {
	if d.n == 0 || d.IsZero() {
		return 0
	}
	if d.b[0] == '-' {
		return -1
	}
	return 1
}

// Equal reports whether both literals are byte-for-byte identical. Note that
// "1.0" and "1" are not Equal; compare via Float64 or Rat for numeric equality.
func (d Decimal) Equal(o Decimal) bool { return d == o }

// MarshalJSON encodes the literal as a JSON string, or null when unset.
func (d Decimal) MarshalJSON() ([]byte, error) {
	if d.n == 0 {
		return []byte("null"), nil
	}
	out := make([]byte, 0, int(d.n)+2)
	out = append(out, '"')
	out = append(out, d.b[:d.n]...)
	out = append(out, '"')
	return out, nil
}

// UnmarshalJSON accepts a JSON string ("123.4"), a bare number (123.4) or null.
func (d *Decimal) UnmarshalJSON(b []byte) error {
	if len(b) == 4 && string(b) == "null" {
		d.Reset()
		return nil
	}
	if len(b) >= 2 && b[0] == '"' && b[len(b)-1] == '"' {
		b = b[1 : len(b)-1]
	}
	return d.SetBytes(b)
}

// MarshalText implements encoding.TextMarshaler.
func (d Decimal) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Decimal) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		d.Reset()
		return nil
	}
	return d.SetBytes(b)
}
