package network

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/musix/backhaul/config"
)

func TestWebSocketDialerHonorsConfiguredHandshakeTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	started := time.Now()
	_, err = WebSocketDialer(context.Background(), listener.Addr().String(), "", "/channel", 100*time.Millisecond, time.Second, true, "token", config.WS, false, 1, 0, 0)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("WebSocket dial unexpectedly succeeded against a peer that never answered the HTTP upgrade")
	}
	if elapsed > time.Second {
		t.Fatalf("configured 100ms handshake timeout returned after %s", elapsed)
	}
	select {
	case conn := <-accepted:
		_ = conn.Close()
	default:
	}
}

func TestWebSocketDialerTLSVerificationIsConfigurable(t *testing.T) {
	upgraded := make(chan *websocket.Conn, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			upgraded <- conn
		}
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()

	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	client, err := WebSocketDialer(context.Background(), parsed.Host, "", "/channel", time.Second, time.Second, true, "token", config.WSS, false, 1, 0, 0)
	if err != nil {
		t.Fatalf("backward-compatible TLS mode rejected self-signed test certificate: %v", err)
	}
	serverConn := <-upgraded
	_ = client.Close()
	_ = serverConn.Close()

	if _, err := WebSocketDialer(context.Background(), parsed.Host, "", "/channel", time.Second, time.Second, true, "token", config.WSS, true, 1, 0, 0); err == nil {
		t.Fatal("tls_verify=true accepted an untrusted self-signed certificate")
	}
}

func TestWebSocketDialerSubprotocolNegotiationIsBackwardCompatible(t *testing.T) {
	const capability = "backhaul.test-capability.v1"

	for _, tc := range []struct {
		name            string
		clientProtocols []string
		serverProtocols []string
		wantProtocol    string
	}{
		{
			name:            "new client remains compatible with legacy server",
			clientProtocols: []string{capability},
			wantProtocol:    "",
		},
		{
			name:            "legacy client remains compatible with new server",
			serverProtocols: []string{capability},
			wantProtocol:    "",
		},
		{
			name:            "new peers negotiate capability",
			clientProtocols: []string{capability},
			serverProtocols: []string{capability},
			wantProtocol:    capability,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upgraded := make(chan *websocket.Conn, 1)
			upgrader := websocket.Upgrader{Subprotocols: tc.serverProtocols}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err == nil {
					upgraded <- conn
				}
			}))
			defer server.Close()

			parsed, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			client, err := WebSocketDialer(context.Background(), parsed.Host, "", "/channel", time.Second, time.Second, true, "token", config.WS, false, 1, 0, 0, tc.clientProtocols...)
			if err != nil {
				t.Fatalf("dial with offered subprotocol: %v", err)
			}
			serverConn := <-upgraded
			defer client.Close()
			defer serverConn.Close()

			if got := client.Subprotocol(); got != tc.wantProtocol {
				t.Fatalf("negotiated subprotocol = %q, want %q", got, tc.wantProtocol)
			}
		})
	}
}
