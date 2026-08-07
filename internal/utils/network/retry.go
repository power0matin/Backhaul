package network

import (
	"context"
	"math/rand"
	"time"
)

const maxRetryBackoff = 5 * time.Second

func retryBackoff(attempt int) time.Duration {
	backoff := time.Second
	for i := 0; i < attempt && backoff < maxRetryBackoff; i++ {
		backoff *= 2
		if backoff > maxRetryBackoff {
			backoff = maxRetryBackoff
		}
	}

	// +/- 20% jitter prevents a pool of failed dials from reconnecting in a
	// synchronized burst after an outage.
	span := backoff / 5
	if span <= 0 {
		return backoff
	}
	return backoff - span + time.Duration(rand.Int63n(int64(2*span)+1))
}

func waitRetry(ctx context.Context, attempt int) bool {
	timer := time.NewTimer(retryBackoff(attempt))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
