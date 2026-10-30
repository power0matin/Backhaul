package utils

import (
	"net"
	"testing"
	"time"
)

func TestDataChannelAuthenticationRoundTrip(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	serverResult := make(chan error, 1)
	go func() {
		legacy, err := AuthenticateDataChannelServer(serverConn, "correct-horse-battery-staple", false)
		if legacy && err == nil {
			serverResult <- err
			return
		}
		serverResult <- err
	}()

	if err := AuthenticateDataChannelClient(clientConn, "correct-horse-battery-staple"); err != nil {
		t.Fatalf("client authentication failed: %v", err)
	}
	select {
	case err := <-serverResult:
		if err != nil {
			t.Fatalf("server rejected authenticated data channel: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server authentication did not complete")
	}
}

func TestDataChannelAuthenticationRejectsWrongToken(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	serverResult := make(chan error, 1)
	go func() {
		_, err := AuthenticateDataChannelServer(serverConn, "server-secret", false)
		serverResult <- err
	}()

	if err := AuthenticateDataChannelClient(clientConn, "different-secret"); err != nil {
		t.Fatalf("client should complete the challenge exchange before server rejects its proof: %v", err)
	}
	select {
	case err := <-serverResult:
		if err == nil {
			t.Fatal("server accepted a proof generated from the wrong token")
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish rejecting the invalid proof")
	}
}

func TestDataChannelAuthenticationRejectsUnauthenticatedSameIPPeer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	serverConn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer serverConn.Close()

	clientIP := clientConn.LocalAddr().(*net.TCPAddr).IP
	serverObservedIP := serverConn.RemoteAddr().(*net.TCPAddr).IP
	if !clientIP.Equal(serverObservedIP) {
		t.Fatalf("test precondition failed: client IP %s != server-observed IP %s", clientIP, serverObservedIP)
	}

	legacy, err := AuthenticateDataChannelServer(serverConn, "shared-secret", false)
	if err == nil {
		t.Fatal("server accepted a same-IP connection without a token proof")
	}
	if legacy {
		t.Fatal("strict mode unexpectedly classified the unauthenticated peer as legacy")
	}
}

func TestDataChannelLegacyModeRequiresExplicitOptIn(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	legacy, err := AuthenticateDataChannelServer(serverConn, "shared-secret", true)
	if err != nil {
		t.Fatalf("explicit legacy mode rejected a silent old client: %v", err)
	}
	if !legacy {
		t.Fatal("silent old client was not classified as legacy")
	}
}
