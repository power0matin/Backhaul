package transport

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestUDPToTCPForwardsLargeDatagramWithoutTruncation(t *testing.T) {
	service, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	udp, err := net.DialUDP("udp", nil, service.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()

	left, right := net.Pipe()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	done := make(chan struct{})
	go func() {
		udpToTCP(left, udp, logger, nil, 0, false)
		close(done)
	}()

	payload := make([]byte, 20*1024+17)
	for i := range payload {
		payload[i] = byte((i*17 + 11) % 253)
	}
	if _, err := service.WriteToUDP(payload, udp.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatalf("send large UDP datagram: %v", err)
	}
	if err := right.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 6)
	if _, err := io.ReadFull(right, header); err != nil {
		t.Fatalf("read UDP-over-TCP frame header: %v", err)
	}
	packetSize := int(binary.BigEndian.Uint16(header[4:6]))
	if packetSize != len(payload) {
		t.Fatalf("framed UDP payload size = %d, want %d", packetSize, len(payload))
	}
	got := make([]byte, packetSize)
	if _, err := io.ReadFull(right, got); err != nil {
		t.Fatalf("read framed UDP payload: %v", err)
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("payload differs at byte %d", i)
		}
	}
	_ = right.Close()
	_ = udp.Close() // Unblock the receiver's next UDP read after verifying the frame.
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("UDP-to-TCP handler did not stop after stream close")
	}
}
