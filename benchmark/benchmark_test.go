package benchmark_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/client"
	"github.com/musix/backhaul/internal/server"
)

const benchmarkToken = "backhaul-benchmark-token"

func BenchmarkStreamThroughput(b *testing.B) {
	for _, transport := range []config.TransportType{config.TCP, config.TCPMUX, config.WS, config.WSMUX} {
		transport := transport
		b.Run(string(transport), func(b *testing.B) {
			fixture := newStreamFixture(b, transport)
			defer fixture.close()
			conn := waitTCP(b, fixture.forwardAddr)
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

func BenchmarkConnectionSetup(b *testing.B) {
	for _, transport := range []config.TransportType{config.TCP, config.TCPMUX, config.WS, config.WSMUX} {
		transport := transport
		b.Run(string(transport), func(b *testing.B) {
			fixture := newStreamFixture(b, transport)
			defer fixture.close()
			probe := waitTCP(b, fixture.forwardAddr)
			_ = probe.Close()
			payload := []byte("setup")
			response := make([]byte, len(payload))

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn, err := net.DialTimeout("tcp", fixture.forwardAddr, 2*time.Second)
				if err != nil {
					b.Fatal(err)
				}
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				if _, err := conn.Write(payload); err != nil {
					_ = conn.Close()
					b.Fatal(err)
				}
				if _, err := io.ReadFull(conn, response); err != nil {
					_ = conn.Close()
					b.Fatal(err)
				}
				_ = conn.Close()
			}
		})
	}
}

func BenchmarkUDPThroughput(b *testing.B) {
	tests := []struct {
		name      string
		transport config.TransportType
		acceptUDP bool
	}{
		{name: "native", transport: config.UDP},
		{name: "accept_udp", transport: config.TCP, acceptUDP: true},
	}
	for _, tt := range tests {
		tt := tt
		b.Run(tt.name, func(b *testing.B) {
			fixture := newUDPFixture(b, tt.transport, tt.acceptUDP)
			defer fixture.close()
			conn := waitUDP(b, fixture.forwardAddr)
			defer conn.Close()
			payload := bytes.Repeat([]byte{0x7c}, 1200)
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

type fixture struct {
	forwardAddr string
	cancel      context.CancelFunc
	closeTarget func()
}

func (f fixture) close() {
	f.cancel()
	f.closeTarget()
}

func newStreamFixture(b *testing.B, transport config.TransportType) fixture {
	b.Helper()
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		for {
			conn, err := target.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	tunnelAddr := freeTCP(b)
	forwardAddr := freeTCP(b)
	for samePort(tunnelAddr, forwardAddr) {
		forwardAddr = freeTCP(b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srvCfg := benchmarkServerConfig(transport, tunnelAddr, forwardAddr+"="+target.Addr().String())
	clCfg := benchmarkClientConfig(transport, tunnelAddr)
	start(ctx, &srvCfg, &clCfg)
	return fixture{forwardAddr: forwardAddr, cancel: cancel, closeTarget: func() { _ = target.Close() }}
}

func newUDPFixture(b *testing.B, transport config.TransportType, acceptUDP bool) fixture {
	b.Helper()
	target, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, addr, err := target.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = target.WriteToUDP(buf[:n], addr)
		}
	}()

	tunnelAddr := freeTCP(b)
	forwardAddr := freeUDP(b)
	for samePort(tunnelAddr, forwardAddr) {
		forwardAddr = freeUDP(b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srvCfg := benchmarkServerConfig(transport, tunnelAddr, forwardAddr+"="+target.LocalAddr().String())
	srvCfg.AcceptUDP = acceptUDP
	clCfg := benchmarkClientConfig(transport, tunnelAddr)
	start(ctx, &srvCfg, &clCfg)
	return fixture{forwardAddr: forwardAddr, cancel: cancel, closeTarget: func() { _ = target.Close() }}
}

func benchmarkServerConfig(transport config.TransportType, tunnelAddr, mapping string) config.ServerConfig {
	return config.ServerConfig{
		BindAddr:         tunnelAddr,
		Transport:        transport,
		Token:            benchmarkToken,
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

func benchmarkClientConfig(transport config.TransportType, tunnelAddr string) config.ClientConfig {
	return config.ClientConfig{
		RemoteAddr:       tunnelAddr,
		Transport:        transport,
		Token:            benchmarkToken,
		ConnectionPool:   4,
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

func start(ctx context.Context, srvCfg *config.ServerConfig, clCfg *config.ClientConfig) {
	srv := server.NewServer(srvCfg, ctx)
	cl := client.NewClient(clCfg, ctx)
	go srv.Start()
	time.Sleep(20 * time.Millisecond)
	go cl.Start()
}

func waitTCP(b *testing.B, addr string) net.Conn {
	b.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			payload := []byte("ready")
			response := make([]byte, len(payload))
			_, writeErr := conn.Write(payload)
			_, readErr := io.ReadFull(conn, response)
			if writeErr == nil && readErr == nil && bytes.Equal(payload, response) {
				_ = conn.SetDeadline(time.Time{})
				return conn
			}
			_ = conn.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	b.Fatalf("tunnel %s did not become ready", addr)
	return nil
}

func waitUDP(b *testing.B, addr string) net.Conn {
	b.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("udp", addr)
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(400 * time.Millisecond))
			payload := []byte("ready")
			response := make([]byte, len(payload))
			_, writeErr := conn.Write(payload)
			_, readErr := io.ReadFull(conn, response)
			if writeErr == nil && readErr == nil && bytes.Equal(payload, response) {
				_ = conn.SetDeadline(time.Time{})
				return conn
			}
			_ = conn.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	b.Fatalf("UDP tunnel %s did not become ready", addr)
	return nil
}

func freeTCP(b *testing.B) string {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

func freeUDP(b *testing.B) string {
	b.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		b.Fatal(err)
	}
	addr := conn.LocalAddr().String()
	_ = conn.Close()
	return addr
}

func samePort(a, b string) bool {
	_, aPort, aErr := net.SplitHostPort(a)
	_, bPort, bErr := net.SplitHostPort(b)
	return aErr == nil && bErr == nil && aPort == bPort
}
