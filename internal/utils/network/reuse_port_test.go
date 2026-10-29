//go:build linux

package network

import (
	"testing"
	"time"
)

func TestListenWithBuffersRejectsSecondListenerOnSameAddress(t *testing.T) {
	first, err := ListenWithBuffers("tcp", "127.0.0.1:0", 0, 0, 0, time.Second, true)
	if err != nil {
		t.Fatalf("open initial listener: %v", err)
	}
	defer first.Close()

	second, err := ListenWithBuffers("tcp", first.Addr().String(), 0, 0, 0, time.Second, true)
	if err == nil {
		_ = second.Close()
		t.Fatal("second listener unexpectedly shared the same address; independent Backhaul instances must not split traffic")
	}
}
