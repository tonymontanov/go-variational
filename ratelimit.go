package variational

import (
	"context"
	"sync"
	"time"
)

// rateLimiter is a minimal token bucket with reservations: every waiter takes a
// distinct slot so concurrent callers are spread out instead of stampeding.
type rateLimiter struct {
	mu     sync.Mutex
	tokens float64
	burst  float64
	rate   float64 // tokens per nanosecond
	last   time.Time
}

func newRateLimiter(requests int, window time.Duration) *rateLimiter {
	return &rateLimiter{
		tokens: float64(requests),
		burst:  float64(requests),
		rate:   float64(requests) / float64(window),
		last:   time.Now(),
	}
}

// wait blocks until a token is available or ctx is done.
func (r *rateLimiter) wait(ctx context.Context) error {
	r.mu.Lock()
	now := time.Now()
	r.tokens += float64(now.Sub(r.last)) * r.rate
	r.last = now
	if r.tokens > r.burst {
		r.tokens = r.burst
	}
	r.tokens--
	var delay time.Duration
	if r.tokens < 0 {
		delay = time.Duration(-r.tokens / r.rate)
	}
	r.mu.Unlock()
	if delay <= 0 {
		return nil
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		// Give the reservation back so the slot is not lost.
		r.mu.Lock()
		r.tokens++
		r.mu.Unlock()
		return ctx.Err()
	}
}
