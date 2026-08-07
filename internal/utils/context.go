package utils

import (
	"context"
	"time"
)

// WaitForDelay waits without making shutdown wait for an outstanding sleep.
// It returns false when ctx is canceled before the delay elapses.
func WaitForDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
