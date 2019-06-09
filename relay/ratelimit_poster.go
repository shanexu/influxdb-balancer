package relay

import (
	"context"
	"golang.org/x/time/rate"
	"strings"
	"sync/atomic"
	"unsafe"
)

type rateLimitPoster struct {
	poster   Poster
	_limiter *unsafe.Pointer
}

func NewRateLimitPoster(_limiter *unsafe.Pointer, poster Poster) *rateLimitPoster {
	return &rateLimitPoster{
		_limiter: _limiter,
		poster:   poster,
	}
}

func (rlp *rateLimitPoster) Post(data []byte, query string, auth string) (*ResponseData, error) {
	limiter := (*rate.Limiter)(atomic.LoadPointer(rlp._limiter))
	n := strings.Count(string(data), "\n")
	if n == 0 {
		n = 1
	}
	ctx := context.Background()
	if limiter.Burst() > 0 {
		for n > limiter.Burst() {
			limiter.WaitN(ctx, limiter.Burst())
			n = n - limiter.Burst()
		}
		if n > 0 {
			limiter.WaitN(ctx, n)
		}
	}
	limiter.WaitN(ctx, n)
	return rlp.poster.Post(data, query, auth)
}
