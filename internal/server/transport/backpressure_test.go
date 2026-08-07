package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestTcpMuxLocalListenerAbsorbsShortQueueBurst(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	s := &TcpMuxTransport{
		config: &TcpMuxConfig{
			KeepAlive: 5 * time.Second,
			MuxCon:    1,
		},
		ctx:            ctx,
		logger:         logger,
		localChannel:   make(chan LocalTCPConn, 1),
		reqNewConnChan: make(chan struct{}, 1),
	}

	// Saturate the userspace queue briefly. The listener should apply bounded
	// backpressure instead of discarding a connection that can be admitted a
	// few milliseconds later.
	s.localChannel <- LocalTCPConn{}
	go s.acceptLocalConn(listener, "127.0.0.1:9")

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	time.Sleep(75 * time.Millisecond)
	<-s.localChannel

	select {
	case accepted := <-s.localChannel:
		if accepted.conn == nil {
			t.Fatal("listener queued a nil connection")
		}
		_ = accepted.conn.Close()
	case <-time.After(500 * time.Millisecond):
		t.Fatal("connection was discarded during a short-lived full-queue burst")
	}
}
