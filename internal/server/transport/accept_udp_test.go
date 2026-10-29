package transport

import (
	"context"
	"encoding/binary"
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


func TestPacketAgeMillisHandlesTimestampWrap(t *testing.T) {
	tests := []struct {
		name         string
		nowMillis    int64
		packetMillis int64
		wantAge      int64
	}{
		{name: "same window", nowMillis: 200_000, packetMillis: 199_000, wantAge: 1_000},
		{name: "crosses ten minute wrap", nowMillis: 600_002, packetMillis: 599_999, wantAge: 3},
		{name: "invalid timestamp", nowMillis: 42, packetMillis: 600_000, wantAge: congestionTimestampWindowMillis},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := packetAgeMillis(tt.nowMillis, tt.packetMillis); got != tt.wantAge {
				t.Fatalf("packetAgeMillis(%d, %d) = %d, want %d", tt.nowMillis, tt.packetMillis, got, tt.wantAge)
			}
		})
	}
}

func TestTCPToUDPForwardsZeroLengthDatagram(t *testing.T) {
	receiver, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := receiver.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	sender, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	left, right := net.Pipe()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	flow := &LocalAcceptUDPConn{
		listener:   sender,
		clientAddr: receiver.LocalAddr().(*net.UDPAddr),
	}
	done := make(chan struct{})
	go func() {
		tcpToUDP(left, flow, logger, nil, 0, false, 100)
		close(done)
	}()

	frame := make([]byte, 6) // 4-byte timestamp plus a 2-byte zero packet length
	binary.BigEndian.PutUint32(frame[:4], uint32(time.Now().UnixMilli()%(10*60*1000)))
	binary.BigEndian.PutUint16(frame[4:], 0)
	if _, err := right.Write(frame); err != nil {
		t.Fatalf("write zero-length UDP frame: %v", err)
	}

	buf := make([]byte, 1)
	n, _, err := receiver.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read forwarded empty datagram: %v", err)
	}
	if n != 0 {
		t.Fatalf("forwarded datagram length = %d, want 0", n)
	}

	_ = right.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("TCP-to-UDP handler did not stop after stream close")
	}
}


func TestTCPToUDPForwardsLargeDatagramWithoutTruncation(t *testing.T) {
	receiver, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := receiver.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}

	sender, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	left, right := net.Pipe()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	flow := &LocalAcceptUDPConn{
		listener:   sender,
		clientAddr: receiver.LocalAddr().(*net.UDPAddr),
	}
	done := make(chan struct{})
	go func() {
		tcpToUDP(left, flow, logger, nil, 0, false, 100)
		close(done)
	}()

	payload := make([]byte, 20*1024+17)
	for i := range payload {
		payload[i] = byte((i*31 + 7) % 251)
	}
	frame := make([]byte, 6+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(time.Now().UnixMilli()%(10*60*1000)))
	binary.BigEndian.PutUint16(frame[4:6], uint16(len(payload)))
	copy(frame[6:], payload)
	go func() { _, _ = right.Write(frame) }()

	got := make([]byte, len(payload)+1)
	n, _, err := receiver.ReadFromUDP(got)
	if err != nil {
		t.Fatalf("read large forwarded datagram: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("forwarded datagram length = %d, want %d", n, len(payload))
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("payload differs at byte %d", i)
		}
	}
	_ = right.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("TCP-to-UDP handler did not stop after stream close")
	}
}

func TestCongestedAcceptUDPFlowRetainsFlowSlot(t *testing.T) {
	flow := &LocalAcceptUDPConn{}
	if isCongestedAcceptUDPFlow(flow) {
		t.Fatal("new flow was incorrectly treated as congested")
	}
	flow.isCongested.Store(true)
	if !isCongestedAcceptUDPFlow(flow) {
		t.Fatal("congested flow was not identified for drop-without-replacement")
	}
	if got, want := acceptUDPFlowInactivityTimeout, 30*time.Second; got != want {
		t.Fatalf("flow inactivity timeout = %s, want %s", got, want)
	}
}

