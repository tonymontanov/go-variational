package variational

import (
	"errors"
	"time"
)

// A tiny allocation-free JSON scanner sufficient for the fixed-shape messages
// of the WebSocket API. It deliberately does not unescape strings: the exchange
// never emits escape sequences in the fields the SDK reads, and a backslash in
// such a field is treated as a decoding error.

var errMalformed = errors.New("malformed message")

type scanner struct {
	b []byte
	i int
}

func (s *scanner) skipWS() {
	for s.i < len(s.b) {
		switch s.b[s.i] {
		case ' ', '\t', '\n', '\r':
			s.i++
		default:
			return
		}
	}
}

func (s *scanner) peek() byte {
	if s.i < len(s.b) {
		return s.b[s.i]
	}
	return 0
}

// expect consumes c (after optional whitespace) and reports success.
func (s *scanner) expect(c byte) bool {
	s.skipWS()
	if s.i < len(s.b) && s.b[s.i] == c {
		s.i++
		return true
	}
	return false
}

// readString consumes a JSON string and returns its raw content. Escaped
// strings are rejected (ok=false).
func (s *scanner) readString() ([]byte, bool) {
	s.skipWS()
	if s.i >= len(s.b) || s.b[s.i] != '"' {
		return nil, false
	}
	start := s.i + 1
	for j := start; j < len(s.b); j++ {
		switch s.b[j] {
		case '"':
			s.i = j + 1
			return s.b[start:j], true
		case '\\':
			return nil, false
		}
	}
	return nil, false
}

// readScalar consumes a string (returning its content), number or literal
// (returning its raw text). isNull reports a JSON null.
func (s *scanner) readScalar() (v []byte, isNull bool, ok bool) {
	s.skipWS()
	if s.i >= len(s.b) {
		return nil, false, false
	}
	if s.b[s.i] == '"' {
		v, ok = s.readString()
		return v, false, ok
	}
	start := s.i
	for s.i < len(s.b) {
		switch s.b[s.i] {
		case ',', '}', ']', ' ', '\t', '\n', '\r':
			v = s.b[start:s.i]
			return v, len(v) == 4 && string(v) == "null", len(v) > 0
		}
		s.i++
	}
	v = s.b[start:]
	return v, len(v) == 4 && string(v) == "null", len(v) > 0
}

// skipValue consumes any JSON value.
func (s *scanner) skipValue() bool {
	s.skipWS()
	if s.i >= len(s.b) {
		return false
	}
	switch s.b[s.i] {
	case '"':
		s.i++
		for s.i < len(s.b) {
			switch s.b[s.i] {
			case '\\':
				s.i += 2
				continue
			case '"':
				s.i++
				return true
			}
			s.i++
		}
		return false
	case '{', '[':
		depth := 0
		inStr := false
		for s.i < len(s.b) {
			c := s.b[s.i]
			s.i++
			if inStr {
				switch c {
				case '\\':
					s.i++
				case '"':
					inStr = false
				}
				continue
			}
			switch c {
			case '"':
				inStr = true
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return true
				}
			}
		}
		return false
	default:
		_, _, ok := s.readScalar()
		return ok
	}
}

// parseRFC3339UTC parses "YYYY-MM-DDTHH:MM:SS[.fraction]Z" without allocating.
// Other RFC 3339 forms return ok=false so callers can fall back to time.Parse.
func parseRFC3339UTC(b []byte) (time.Time, bool) {
	if len(b) < 20 || b[4] != '-' || b[7] != '-' || b[10] != 'T' || b[13] != ':' || b[16] != ':' {
		return time.Time{}, false
	}
	y, ok1 := atoiN(b[0:4])
	mo, ok2 := atoiN(b[5:7])
	d, ok3 := atoiN(b[8:10])
	h, ok4 := atoiN(b[11:13])
	mi, ok5 := atoiN(b[14:16])
	sec, ok6 := atoiN(b[17:19])
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6) {
		return time.Time{}, false
	}
	i := 19
	nsec := 0
	if i < len(b) && b[i] == '.' {
		i++
		digits := 0
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			if digits < 9 {
				nsec = nsec*10 + int(b[i]-'0')
				digits++
			}
			i++
		}
		if digits == 0 {
			return time.Time{}, false
		}
		for ; digits < 9; digits++ {
			nsec *= 10
		}
	}
	if i != len(b)-1 || b[i] != 'Z' {
		return time.Time{}, false
	}
	return time.Date(y, time.Month(mo), d, h, mi, sec, nsec, time.UTC), true
}

func atoiN(b []byte) (int, bool) {
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// parseTimestamp parses an exchange timestamp, using the fast path for UTC
// values and time.Parse otherwise.
func parseTimestamp(b []byte) (time.Time, error) {
	if t, ok := parseRFC3339UTC(b); ok {
		return t, nil
	}
	return time.Parse(time.RFC3339Nano, string(b))
}
