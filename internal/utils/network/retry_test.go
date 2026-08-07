package network

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestTcpDialerCancellationInterruptsBackoff(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := TcpDialer(ctx, addr, "", 100*time.Millisecond, time.Second, true, 10, 0, 0, 0)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("TcpDialer unexpectedly succeeded")
		}
		if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
			t.Fatalf("canceled dialer returned after %s", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("canceled dialer remained blocked in retry backoff")
	}
}
