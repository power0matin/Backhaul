package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/sirupsen/logrus"
)

func TestNativeUDPServerReportsConnectedAfterControlHandshake(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	server := NewUDPServer(ctx, &UdpConfig{
		BindAddr:    addr,
		Token:       "udp-status-token",
		ChannelSize: 4,
		Heartbeat:   time.Second,
	}, logger)
	go server.Start()

	var conn net.Conn
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial UDP control listener: %v", err)
	}
	defer conn.Close()

	if err := utils.SendBinaryTransportString(conn, "udp-status-token", utils.SG_Chan); err != nil {
		t.Fatal(err)
	}
	message, signal, err := utils.ReceiveBinaryTransportString(conn)
	if err != nil {
		t.Fatal(err)
	}
	if signal != utils.SG_Chan || message != "udp-status-token" {
		t.Fatalf("unexpected handshake response: signal=%d message=%q", signal, message)
	}

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := server.usageMonitor.TunnelStatus(); got == "Connected (UDP)" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("native UDP status = %q, want Connected (UDP)", server.usageMonitor.TunnelStatus())
}

func TestNativeUDPControlHandlerCancellationInterruptsPeerIO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	server := NewUDPServer(ctx, &UdpConfig{Heartbeat: time.Hour}, logger)
	control, peer := net.Pipe()
	defer peer.Close()
	server.controlChannel = control

	done := make(chan struct{})
	go func() {
		server.channelHandler()
		close(done)
	}()

	// channelHandler sends an RTT probe before entering its select loop.
	var probe [1]byte
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(peer, probe[:]); err != nil {
		t.Fatalf("read initial RTT probe: %v", err)
	}

	// Do not read again. Without tying control.Close to ctx cancellation, the
	// best-effort shutdown signal can block forever against this peer.
	cancel()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("control handler remained blocked after context cancellation")
	}
}
