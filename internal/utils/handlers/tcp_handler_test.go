package handlers

import (
	"context"
	"fmt"
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

func makeTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan *net.TCPConn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptTCP()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case server := <-accepted:
		t.Cleanup(func() {
			_ = client.Close()
			_ = server.Close()
		})
		return client, server
	case err := <-acceptErr:
		_ = client.Close()
		t.Fatalf("accept TCP pair: %v", err)
	case <-time.After(time.Second):
		_ = client.Close()
		t.Fatal("timed out creating TCP pair")
	}
	return nil, nil
}

func TestTCPConnectionHandlerPreservesTCPHalfClose(t *testing.T) {
	client, frontend := makeTCPPair(t)
	backendSide, backend := makeTCPPair(t)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handlerDone := make(chan struct{})
	go func() {
		TCPConnectionHandler(ctx, false, frontend, backendSide, logger, nil, 0, false)
		close(handlerDone)
	}()

	backendResult := make(chan error, 1)
	go func() {
		request, err := io.ReadAll(backend)
		if err != nil {
			backendResult <- err
			return
		}
		if string(request) != "request-before-eof" {
			backendResult <- fmt.Errorf("request = %q", request)
			return
		}
		if _, err := backend.Write([]byte("response-after-eof")); err != nil {
			backendResult <- err
			return
		}
		if err := backend.CloseWrite(); err != nil {
			backendResult <- err
			return
		}
		backendResult <- nil
	}()

	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.Write([]byte("request-before-eof")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatalf("half-close client write side: %v", err)
	}
	response, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("read response after request half-close: %v", err)
	}
	if string(response) != "response-after-eof" {
		t.Fatalf("response = %q, want %q", response, "response-after-eof")
	}
	select {
	case err := <-backendResult:
		if err != nil {
			t.Fatalf("backend exchange: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("backend did not finish request/response exchange")
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("proxy handler did not close after both directions reached EOF")
	}
}

