package variational

import (
	"fmt"
	"strconv"
	"time"
)

// ErrorKind classifies an *Error so callers can decide on retry policy.
type ErrorKind uint8

const (
	// ErrKindUnknown is the default classification.
	ErrKindUnknown ErrorKind = iota
	// ErrKindNetwork covers dial failures, timeouts, resets and unexpected EOF.
	ErrKindNetwork
	// ErrKindRateLimit is an HTTP 429 (see Error.RetryAfter).
	ErrKindRateLimit
	// ErrKindForbidden is an HTTP 401/403, including the browser challenge that
	// guards the Omni web-app routes.
	ErrKindForbidden
	// ErrKindInvalidRequest is a client-side problem: HTTP 4xx other than the
	// above, or a WebSocket request the server refused to parse.
	ErrKindInvalidRequest
	// ErrKindUnsupportedInstrument is returned by the price stream when the
	// exchange rejects a subscription for an instrument it does not list.
	ErrKindUnsupportedInstrument
	// ErrKindExchange is a server-side failure (HTTP 5xx or a server error text).
	ErrKindExchange
	// ErrKindInvalidResponse is a payload the SDK could not decode.
	ErrKindInvalidResponse
	// ErrKindClosed is returned by Run when a stream was closed by the caller.
	ErrKindClosed
)

var errorKindNames = [...]string{
	ErrKindUnknown:               "unknown",
	ErrKindNetwork:               "network",
	ErrKindRateLimit:             "rate_limit",
	ErrKindForbidden:             "forbidden",
	ErrKindInvalidRequest:        "invalid_request",
	ErrKindUnsupportedInstrument: "unsupported_instrument",
	ErrKindExchange:              "exchange",
	ErrKindInvalidResponse:       "invalid_response",
	ErrKindClosed:                "closed",
}

func (k ErrorKind) String() string {
	if int(k) < len(errorKindNames) {
		return errorKindNames[k]
	}
	return "kind(" + strconv.Itoa(int(k)) + ")"
}

// Error is the single error type produced by the SDK. Use errors.As to inspect
// it, or errors.Is against the sentinel values below to test the Kind only.
type Error struct {
	Kind ErrorKind
	// Op names the operation, e.g. "GET /metadata/stats" or "ws /prices".
	Op string
	// HTTPStatus is set for REST errors.
	HTTPStatus int
	// Message is the exchange-provided text (or a body excerpt) when available.
	Message string
	// RetryAfter is the server-suggested wait for rate-limit errors, if any.
	RetryAfter time.Duration
	// InstrumentKey is set for unsupported-instrument errors (e.g. "P-XYZ-USDC-3600").
	InstrumentKey string
	// Err is the wrapped underlying error, if any.
	Err error
}

// Sentinel errors for use with errors.Is. They match any *Error of the same Kind.
var (
	ErrNetwork               error = &Error{Kind: ErrKindNetwork}
	ErrRateLimited           error = &Error{Kind: ErrKindRateLimit}
	ErrForbidden             error = &Error{Kind: ErrKindForbidden}
	ErrInvalidRequest        error = &Error{Kind: ErrKindInvalidRequest}
	ErrUnsupportedInstrument error = &Error{Kind: ErrKindUnsupportedInstrument}
	ErrExchange              error = &Error{Kind: ErrKindExchange}
	ErrInvalidResponse       error = &Error{Kind: ErrKindInvalidResponse}
	ErrStreamClosed          error = &Error{Kind: ErrKindClosed, Message: "stream closed"}
)

func (e *Error) Error() string {
	s := "variational: "
	if e.Op != "" {
		s += e.Op + ": "
	}
	s += e.Kind.String()
	if e.HTTPStatus != 0 {
		s += " (HTTP " + strconv.Itoa(e.HTTPStatus) + ")"
	}
	if e.InstrumentKey != "" {
		s += " " + e.InstrumentKey
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

// Unwrap returns the wrapped error.
func (e *Error) Unwrap() error { return e.Err }

// Is reports whether target is an *Error of the same Kind, which makes the
// sentinel values above usable with errors.Is.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Kind == e.Kind
}

// Temporary reports whether retrying the same operation later may succeed.
func (e *Error) Temporary() bool {
	switch e.Kind {
	case ErrKindNetwork, ErrKindRateLimit, ErrKindExchange:
		return true
	}
	return false
}

func newError(kind ErrorKind, op string, err error) *Error {
	return &Error{Kind: kind, Op: op, Err: err}
}

func errorf(kind ErrorKind, op, format string, args ...any) *Error {
	return &Error{Kind: kind, Op: op, Message: fmt.Sprintf(format, args...)}
}
