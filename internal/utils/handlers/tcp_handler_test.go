package handlers

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestTCPConnectionHandlerCancelsIdleConnections(t *testing.T) {
	leftTunnel, rightTunnel := net.Pipe()
	leftLocal, rightLocal := net.Pipe()
	t.Cleanup(func() {
		_ = leftTunnel.Close()
		_ = rightTunnel.Close()
		_ = leftLocal.Close()
		_ = rightLocal.Close()
	})

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		TCPConnectionHandler(ctx, false, leftTunnel, leftLocal, logger, nil, 0, false)
		close(done)
	}()

	// Give both copy directions time to block in Read, then cancel the owner.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		// Unblock the baseline implementation before failing so the test itself
		// does not leave goroutines behind.
		_ = rightTunnel.Close()
		_ = rightLocal.Close()
		<-done
		t.Fatal("idle proxy did not terminate promptly after context cancellation")
	}
}
