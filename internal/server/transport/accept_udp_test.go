package transport

import (
	"context"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

func TestAcceptUDPNewFlowMemoryIsBounded(t *testing.T) {
	probe, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.LocalAddr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	s := NewTCPServer(ctx, &TcpConfig{ChannelSize: 1, Heartbeat: time.Second}, logger)

	done := make(chan struct{})
	go func() {
		s.udpListener(addr, "127.0.0.1:9")
		close(done)
	}()

	// Let the listener bind before taking the baseline heap sample.
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	const flows = 32
	for i := 0; i < flows; i++ {
		conn, err := net.Dial("udp", addr)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte{byte(i)}); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		_ = conn.Close()
	}

	time.Sleep(150 * time.Millisecond)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	const maxRetained = 32 << 20
	if after.HeapAlloc > before.HeapAlloc+maxRetained {
		t.Fatalf("%d short-lived UDP flows retained %d bytes of heap; limit is %d", flows, after.HeapAlloc-before.HeapAlloc, maxRetained)
	}
	var retained uint64
	if after.HeapAlloc > before.HeapAlloc {
		retained = after.HeapAlloc - before.HeapAlloc
	}
	t.Logf("%d short-lived UDP flows retained %d bytes after GC", flows, retained)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("UDP listener did not stop after cancellation")
	}
}

func TestAcceptUDPCongestedFlowRemovedWhenHandlerEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	usage := web.NewDataStore("127.0.0.1", 0, ctx, "", false, "Connected", "", "", logger)

	tunnel, peer := net.Pipe()
	clientAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 41000}
	flow := &LocalAcceptUDPConn{
		payload:    make(chan []byte, 1),
		clientAddr: clientAddr,
	}
	flow.isCongested.Store(true)
	flows := map[string]*LocalAcceptUDPConn{clientAddr.String(): flow}
	var mu sync.Mutex
	budget := newPacketBudget(8)

	done := make(chan struct{})
	go func() {
		UDPConnectionHandler(ctx, flow, tunnel, logger, usage, 0, false, 100, &flows, &mu, budget)
		close(done)
	}()

	_ = peer.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("congested UDP handler did not exit")
	}

	mu.Lock()
	_, exists := flows[clientAddr.String()]
	mu.Unlock()
	if exists {
		t.Fatal("completed congested UDP flow remained in active flow map")
	}
}
