package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/musix/backhaul/cmd"
	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/client"
)

func TestInvalidHotReloadKeepsCurrentGeneration(t *testing.T) {
	target, targetAddr := startTestEcho(t)
	defer target.Close()
	tunnelAddr := testFreeTCPAddr(t)
	forwardAddr := testFreeTCPAddr(t)
	configPath := filepath.Join(t.TempDir(), "backhaul.toml")
	valid := fmt.Sprintf(`[server]
bind_addr = %q
transport = "tcp"
token = "reload-test-token"
channel_size = 128
heartbeat = 1
skip_optz = true
ports = [%q]
`, tunnelAddr, forwardAddr+"="+targetAddr)
	if err := os.WriteFile(configPath, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := cmd.LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, cancelServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- supervise(serverCtx, configPath, cfg) }()

	clientCtx, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	clientCfg := config.ClientConfig{
		RemoteAddr:     tunnelAddr,
		Transport:      config.TCP,
		Token:          "reload-test-token",
		ConnectionPool: 4,
		MaxPoolSize:    16,
		RetryInterval:  1,
		Keepalive:      5,
		LogLevel:       "error",
		DialTimeout:    1,
	}
	cl := client.NewClient(&clientCfg, clientCtx)
	go cl.Start()

	if err := testEchoRoundTrip(forwardAddr, 8*time.Second); err != nil {
		cancelServer()
		t.Fatal(err)
	}

	invalid := `[server]
bind_addr = "127.0.0.1:12345"
transport = "invalid"
`
	if err := os.WriteFile(configPath, []byte(invalid), 0o600); err != nil {
		cancelServer()
		t.Fatal(err)
	}
	time.Sleep(reloadPollInterval + 500*time.Millisecond)

	select {
	case err := <-serverDone:
		cancelServer()
		t.Fatalf("supervisor stopped on invalid replacement config: %v", err)
	default:
	}
	if err := testEchoRoundTrip(forwardAddr, 2*time.Second); err != nil {
		cancelServer()
		t.Fatalf("existing tunnel stopped after invalid hot reload: %v", err)
	}

	// A subsequent valid change must still reload normally and release the old
	// forwarding listener before binding the replacement generation.
	replacementAddr := testFreeTCPAddr(t)
	replacement := fmt.Sprintf(`[server]
bind_addr = %q
transport = "tcp"
token = "reload-test-token"
channel_size = 128
heartbeat = 1
skip_optz = true
ports = [%q]
`, tunnelAddr, replacementAddr+"="+targetAddr)
	if err := os.WriteFile(configPath, []byte(replacement), 0o600); err != nil {
		cancelServer()
		t.Fatal(err)
	}
	time.Sleep(reloadPollInterval + 500*time.Millisecond)
	if err := testEchoRoundTrip(replacementAddr, 8*time.Second); err != nil {
		cancelServer()
		t.Fatalf("valid replacement generation did not recover: %v", err)
	}

	cancelServer()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("supervisor shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not stop")
	}
}

func startTestEcho(t *testing.T) (net.Listener, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener, listener.Addr().String()
}

func testFreeTCPAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

func testEchoRoundTrip(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		payload := []byte("reload-round-trip")
		response := make([]byte, len(payload))
		_, writeErr := conn.Write(payload)
		_, readErr := io.ReadFull(conn, response)
		_ = conn.Close()
		if writeErr == nil && readErr == nil && bytes.Equal(payload, response) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("echo through %s did not succeed within %s", addr, timeout)
}
