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

func TestTCPControlHandshakeDoesNotSerializeStalledClients(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	s := NewTCPServer(ctx, &TcpConfig{
		Token: "strong-test-token",
		ChannelSize: 64,
		UDPQueueSize: 8,
		UDPQueueLimit: 64,
		UDPMaxFlows: 8,
	}, logger)

	done := make(chan struct{})
	go func() {
		s.channelHandshake()
		close(done)
	}()
	t.Cleanup(func() {
		if control := s.getControlChannel(); control != nil {
			_ = control.Close()
		}
	})

	const stalled = 20
	var clients []net.Conn
	for i := 0; i < stalled; i++ {
		serverEnd, clientEnd := net.Pipe()
		clients = append(clients, clientEnd)
		s.handshakeChannel <- serverEnd
	}
	t.Cleanup(func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
	})

	validServerEnd, validClientEnd := net.Pipe()
	s.handshakeChannel <- validServerEnd
	ready := make(chan error, 1)
	go func() {
		if err := utils.SendBinaryTransportString(validClientEnd, s.config.Token, utils.SG_Chan); err != nil {
			ready <- err
			return
		}
		_, signal, err := utils.ReceiveBinaryTransportString(validClientEnd)
		if err == nil && signal != utils.SG_Chan {
			err = io.ErrUnexpectedEOF
		}
		ready <- err
	}()

	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("valid control client did not complete authentication promptly: %v", err)
		}
	case <-time.After(350 * time.Millisecond):
		t.Fatal("valid control client was delayed by stalled unauthenticated handshakes")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("control handshake loop did not return after authentication succeeded")
	}
	_ = validClientEnd.Close()
}
