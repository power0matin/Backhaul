package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/client"
	"github.com/musix/backhaul/internal/server"
)

const integrationToken = "backhaul-integration-token"

type streamFixture struct {
	addr   string
	cancel context.CancelFunc
}

func TestStreamTransportsBidirectional(t *testing.T) {
	transports := []config.TransportType{
		config.TCP,
		config.TCPMUX,
		config.WS,
		config.WSS,
		config.WSMUX,
		config.WSSMUX,
	}

	for _, transport := range transports {
		transport := transport
		t.Run(string(transport), func(t *testing.T) {
			fixture := startStreamFixture(t, transport)
			t.Cleanup(fixture.cancel)

			conn := dialReady(t, fixture.addr, 8*time.Second)
			defer conn.Close()

			payload := bytes.Repeat([]byte("backhaul-e2e-"), 64*1024)
			if err := conn.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write(payload); err != nil {
				t.Fatalf("write: %v", err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, got); err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
			}
		})
	}
}

func TestNativeUDPBidirectional(t *testing.T) {
	target, targetAddr := startUDPEcho(t)
	defer target.Close()

	tunnelAddr := freeTCPAddr(t)
	forwardAddr := freeUDPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvCfg := baseServerConfig(config.UDP, tunnelAddr, forwardAddr+"="+targetAddr)
	clCfg := baseClientConfig(config.UDP, tunnelAddr)
	startPair(ctx, &srvCfg, &clCfg)

	conn := dialUDPReady(t, forwardAddr, 8*time.Second)
	defer conn.Close()
	payload := []byte("native-udp-round-trip")
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Fatalf("UDP payload mismatch: got %q want %q", buf[:n], payload)
	}
}

func TestAcceptUDPOverTCPBidirectional(t *testing.T) {
	target, targetAddr := startUDPEcho(t)
	defer target.Close()

	tunnelAddr := freeTCPAddr(t)
	forwardAddr := freeUDPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvCfg := baseServerConfig(config.TCP, tunnelAddr, forwardAddr+"="+targetAddr)
	srvCfg.AcceptUDP = true
	clCfg := baseClientConfig(config.TCP, tunnelAddr)
	startPair(ctx, &srvCfg, &clCfg)

	conn := dialUDPReady(t, forwardAddr, 8*time.Second)
	defer conn.Close()
	payload := []byte("udp-over-tcp-round-trip")
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Fatalf("UDP-over-TCP payload mismatch: got %q want %q", buf[:n], payload)
	}
}

func TestNativeUDPRecoversAfterServerRestart(t *testing.T) {
	target, targetAddr := startUDPEcho(t)
	defer target.Close()

	tunnelAddr := freeTCPAddr(t)
	forwardAddr := freeUDPAddr(t)
	srvCfg := baseServerConfig(config.UDP, tunnelAddr, forwardAddr+"="+targetAddr)
	clCfg := baseClientConfig(config.UDP, tunnelAddr)

	clientCtx, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	serverCtx, cancelServer := context.WithCancel(context.Background())
	srv := server.NewServer(&srvCfg, serverCtx)
	cl := client.NewClient(&clCfg, clientCtx)
	go srv.Start()
	time.Sleep(20 * time.Millisecond)
	go cl.Start()

	ready := dialUDPReady(t, forwardAddr, 8*time.Second)
	_ = ready.Close()
	cancelServer()
	time.Sleep(150 * time.Millisecond)

	restartStarted := time.Now()
	serverCtx2, cancelServer2 := context.WithCancel(context.Background())
	defer cancelServer2()
	srv2 := server.NewServer(&srvCfg, serverCtx2)
	go srv2.Start()
	recovered := dialUDPReady(t, forwardAddr, 8*time.Second)
	_ = recovered.Close()
	t.Logf("native UDP recovered %.3fs after the replacement server started", time.Since(restartStarted).Seconds())
}

func TestStreamTransportsRecoverAfterServerRestart(t *testing.T) {
	for _, mode := range []config.TransportType{config.TCP, config.WSMUX} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			target, targetAddr := startTCPEcho(t)
			defer target.Close()

			tunnelAddr := freeTCPAddr(t)
			forwardAddr := freeTCPAddr(t)
			srvCfg := baseServerConfig(mode, tunnelAddr, forwardAddr+"="+targetAddr)
			clCfg := baseClientConfig(mode, tunnelAddr)

			clientCtx, cancelClient := context.WithCancel(context.Background())
			defer cancelClient()
			serverCtx, cancelServer := context.WithCancel(context.Background())
			srv := server.NewServer(&srvCfg, serverCtx)
			cl := client.NewClient(&clCfg, clientCtx)
			go srv.Start()
			time.Sleep(20 * time.Millisecond)
			go cl.Start()

			ready := dialReady(t, forwardAddr, 8*time.Second)
			_ = ready.Close()

			cancelServer()
			waitTCPClosed(t, forwardAddr, 2*time.Second)

			restartStarted := time.Now()
			serverCtx2, cancelServer2 := context.WithCancel(context.Background())
			defer cancelServer2()
			srv2 := server.NewServer(&srvCfg, serverCtx2)
			go srv2.Start()
			recovered := dialReady(t, forwardAddr, 8*time.Second)
			_ = recovered.Close()
			t.Logf("%s recovered %.3fs after the replacement server started", mode, time.Since(restartStarted).Seconds())
		})
	}
}

func TestWSMUXRecoversAfterClientRestart(t *testing.T) {
	target, targetAddr := startTCPEcho(t)
	defer target.Close()

	tunnelAddr := freeTCPAddr(t)
	forwardAddr := freeTCPAddr(t)
	srvCfg := baseServerConfig(config.WSMUX, tunnelAddr, forwardAddr+"="+targetAddr)
	clCfg := baseClientConfig(config.WSMUX, tunnelAddr)

	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	srv := server.NewServer(&srvCfg, serverCtx)
	go srv.Start()

	clientCtx, cancelClient := context.WithCancel(context.Background())
	cl := client.NewClient(&clCfg, clientCtx)
	go cl.Start()
	ready := dialReady(t, forwardAddr, 8*time.Second)
	_ = ready.Close()

	cancelClient()
	restartStarted := time.Now()
	clientCtx2, cancelClient2 := context.WithCancel(context.Background())
	defer cancelClient2()
	cl2 := client.NewClient(&clCfg, clientCtx2)
	go cl2.Start()

	recovered := dialReady(t, forwardAddr, 8*time.Second)
	_ = recovered.Close()
	t.Logf("WSMUX recovered %.3fs after the replacement client started", time.Since(restartStarted).Seconds())
}

func BenchmarkEndToEnd(b *testing.B) {
	transports := []config.TransportType{config.TCP, config.TCPMUX, config.WS, config.WSMUX}
	for _, transport := range transports {
		transport := transport
		b.Run(string(transport), func(b *testing.B) {
			fixture := startStreamFixture(b, transport)
			defer fixture.cancel()

			conn := dialReady(b, fixture.addr, 8*time.Second)
			defer conn.Close()
			payload := bytes.Repeat([]byte{0x5a}, 256*1024)
			response := make([]byte, len(payload))
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := conn.Write(payload); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(conn, response); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func startStreamFixture(tb testing.TB, transport config.TransportType) streamFixture {
	tb.Helper()
	target, targetAddr := startTCPEcho(tb)
	tunnelAddr := freeTCPAddr(tb)
	forwardAddr := freeTCPAddr(tb)
	ctx, cancel := context.WithCancel(context.Background())

	srvCfg := baseServerConfig(transport, tunnelAddr, forwardAddr+"="+targetAddr)
	clCfg := baseClientConfig(transport, tunnelAddr)
	if transport == config.WSS || transport == config.WSSMUX {
		cert, key := makeTLSCertificate(tb)
		srvCfg.TLSCertFile = cert
		srvCfg.TLSKeyFile = key
	}

	startPair(ctx, &srvCfg, &clCfg)
	return streamFixture{
		addr: forwardAddr,
		cancel: func() {
			cancel()
			_ = target.Close()
		},
	}
}

func startPair(ctx context.Context, srvCfg *config.ServerConfig, clCfg *config.ClientConfig) {
	srv := server.NewServer(srvCfg, ctx)
	cl := client.NewClient(clCfg, ctx)
	go srv.Start()
	time.Sleep(20 * time.Millisecond)
	go cl.Start()
}

func baseServerConfig(transport config.TransportType, tunnelAddr, mapping string) config.ServerConfig {
	return config.ServerConfig{
		BindAddr:         tunnelAddr,
		Transport:        transport,
		Token:            integrationToken,
		Nodelay:          true,
		Keepalive:        5,
		ChannelSize:      128,
		LogLevel:         "error",
		Ports:            []string{mapping},
		MuxVersion:       1,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4 * 1024 * 1024,
		MaxStreamBuffer:  64 * 1024,
		Heartbeat:        1,
		MuxCon:           8,
	}
}

func baseClientConfig(transport config.TransportType, tunnelAddr string) config.ClientConfig {
	return config.ClientConfig{
		RemoteAddr:       tunnelAddr,
		Transport:        transport,
		Token:            integrationToken,
		ConnectionPool:   4,
		MaxPoolSize:      16,
		RetryInterval:    1,
		Nodelay:          true,
		Keepalive:        5,
		LogLevel:         "error",
		MuxVersion:       1,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4 * 1024 * 1024,
		MaxStreamBuffer:  64 * 1024,
		DialTimeout:      1,
	}
}

func startTCPEcho(tb testing.TB) (net.Listener, string) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return ln, ln.Addr().String()
}

func startUDPEcho(tb testing.TB) (*net.UDPConn, string) {
	tb.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		tb.Fatal(err)
	}
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteToUDP(buf[:n], addr)
		}
	}()
	return conn, conn.LocalAddr().String()
}

func dialReady(tb testing.TB, addr string, timeout time.Duration) net.Conn {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			if err := conn.SetDeadline(time.Now().Add(time.Second)); err == nil {
				probe := []byte("ready")
				got := make([]byte, len(probe))
				if _, err = conn.Write(probe); err == nil {
					_, err = io.ReadFull(conn, got)
				}
				if err == nil && bytes.Equal(got, probe) {
					_ = conn.SetDeadline(time.Time{})
					return conn
				}
			}
			_ = conn.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("tunnel at %s did not become ready within %s", addr, timeout)
	return nil
}

func dialUDPReady(tb testing.TB, addr string, timeout time.Duration) net.Conn {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("udp", addr)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(400 * time.Millisecond))
		probe := []byte("ready")
		buf := make([]byte, len(probe))
		_, writeErr := conn.Write(probe)
		_, readErr := io.ReadFull(conn, buf)
		if writeErr == nil && readErr == nil && bytes.Equal(buf, probe) {
			_ = conn.SetDeadline(time.Time{})
			return conn
		}
		_ = conn.Close()
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("UDP tunnel at %s did not become ready within %s", addr, timeout)
	return nil
}

func waitTCPClosed(tb testing.TB, addr string, timeout time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		time.Sleep(25 * time.Millisecond)
	}
	tb.Fatalf("listener at %s did not close within %s", addr, timeout)
}

func freeTCPAddr(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		tb.Fatal(err)
	}
	return addr
}

func freeUDPAddr(tb testing.TB) string {
	tb.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		tb.Fatal(err)
	}
	addr := conn.LocalAddr().String()
	if err := conn.Close(); err != nil {
		tb.Fatal(err)
	}
	return addr
}

func makeTLSCertificate(tb testing.TB) (string, string) {
	tb.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		tb.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		tb.Fatal(err)
	}
	dir := tb.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		tb.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		tb.Fatal(err)
	}
	return certPath, keyPath
}
