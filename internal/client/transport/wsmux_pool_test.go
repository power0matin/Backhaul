package transport

import (
	"context"
	"io"
	"net"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/server"
	"github.com/sirupsen/logrus"
)

func TestWSMUXBurstSessionsRetireAfterIdle(t *testing.T) {
	target, targetAddr := startWSMUXPoolEcho(t)
	defer target.Close()

	tunnelAddr := freeWSMUXPoolAddr(t)
	forwardAddr := freeWSMUXPoolAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverConfig := config.ServerConfig{
		BindAddr:         tunnelAddr,
		Transport:        config.WSMUX,
		Token:            "wsmux-pool-lifecycle-test",
		Nodelay:          true,
		Keepalive:        5,
		ChannelSize:      64,
		LogLevel:         "panic",
		Ports:            []string{forwardAddr + "=" + targetAddr},
		MuxVersion:       1,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4 * 1024 * 1024,
		MaxStreamBuffer:  64 * 1024,
		Heartbeat:        1,
		MuxCon:           1,
	}
	serverInstance := server.NewServer(&serverConfig, ctx)
	go serverInstance.Start()

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	client := NewWSMuxClient(ctx, &WsMuxConfig{
		RemoteAddr:       tunnelAddr,
		Token:            serverConfig.Token,
		Nodelay:          true,
		KeepAlive:        5 * time.Second,
		RetryInterval:    50 * time.Millisecond,
		DialTimeOut:      500 * time.Millisecond,
		MuxVersion:       1,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4 * 1024 * 1024,
		MaxStreamBuffer:  64 * 1024,
		ConnPoolSize:     1,
		MaxPoolSize:      4,
		Mode:             config.WSMUX,
	}, logger)
	client.Start()

	if !waitWSMUXPool(3*time.Second, func() bool {
		return atomic.LoadInt32(&client.poolConnections) == int32(client.config.ConnPoolSize)
	}) {
		t.Fatalf("initial WSMUX pool did not become ready (pool=%d)", atomic.LoadInt32(&client.poolConnections))
	}
	time.Sleep(100 * time.Millisecond)
	baseFDs := openWSMUXPoolFDs()
	baseGoroutines := runtime.NumGoroutine()

	const burstConnections = 6
	held := make([]net.Conn, 0, burstConnections)
	for i := 0; i < burstConnections; i++ {
		conn := dialWSMUXPoolEcho(t, forwardAddr, 5*time.Second)
		held = append(held, conn)
	}

	if !waitWSMUXPool(5*time.Second, func() bool {
		return atomic.LoadInt32(&client.poolConnections) >= burstConnections
	}) {
		t.Fatalf("WSMUX burst did not grow the session population (pool=%d)", atomic.LoadInt32(&client.poolConnections))
	}
	peak := atomic.LoadInt32(&client.poolConnections)
	peakFDs := openWSMUXPoolFDs()
	peakGoroutines := runtime.NumGoroutine()

	// Leave one established stream active while the idle sessions are retired.
	// The retirement path must never sacrifice healthy user traffic merely to
	// hit the pool target.
	survivor := held[0]
	for _, conn := range held[1:] {
		_ = conn.Close()
	}

	// The adaptive controller evaluates a ten-second load window. Once the
	// burst is gone, excess idle sessions must be retired instead of preserving
	// the historical peak for the lifetime of the process.
	if !waitWSMUXPool(15*time.Second, func() bool {
		return atomic.LoadInt32(&client.poolConnections) <= int32(client.config.ConnPoolSize)
	}) {
		t.Fatalf("idle WSMUX sessions did not retire to the configured base pool (peak=%d final=%d base=%d)", peak, atomic.LoadInt32(&client.poolConnections), client.config.ConnPoolSize)
	}
	assertWSMUXPoolEcho(t, survivor, []byte("survived-retirement"))
	_ = survivor.Close()
	if !waitWSMUXPool(2*time.Second, func() bool {
		fds := openWSMUXPoolFDs()
		fdsStable := baseFDs < 0 || fds < 0 || fds <= baseFDs+2
		return fdsStable && runtime.NumGoroutine() <= baseGoroutines+8
	}) {
		t.Fatalf("WSMUX resources did not settle after retirement: fds=%d (base=%d) goroutines=%d (base=%d)", openWSMUXPoolFDs(), baseFDs, runtime.NumGoroutine(), baseGoroutines)
	}
	t.Logf("WSMUX burst-to-idle: sessions=%d->%d (base=%d), fds=%d->%d (base=%d), goroutines=%d->%d (base=%d)", peak, atomic.LoadInt32(&client.poolConnections), client.config.ConnPoolSize, peakFDs, openWSMUXPoolFDs(), baseFDs, peakGoroutines, runtime.NumGoroutine(), baseGoroutines)
}

func startWSMUXPoolEcho(t *testing.T) (net.Listener, string) {
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
				buf := make([]byte, 4*1024)
				for {
					n, err := conn.Read(buf)
					if n > 0 {
						for written := 0; written < n; {
							m, writeErr := conn.Write(buf[written:n])
							if writeErr != nil {
								return
							}
							written += m
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return listener, listener.Addr().String()
}

func freeWSMUXPoolAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func dialWSMUXPoolEcho(t *testing.T, addr string, timeout time.Duration) net.Conn {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		probe := []byte("pool-ready")
		response := make([]byte, len(probe))
		if _, err = conn.Write(probe); err == nil {
			_, err = io.ReadFull(conn, response)
		}
		if err == nil && string(response) == string(probe) {
			_ = conn.SetDeadline(time.Time{})
			return conn
		}
		_ = conn.Close()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for WSMUX echo on %s", addr)
	return nil
}

func assertWSMUXPoolEcho(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("active stream write after pool retirement: %v", err)
	}
	response := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatalf("active stream read after pool retirement: %v", err)
	}
	if string(response) != string(payload) {
		t.Fatalf("active stream payload changed after pool retirement: got %q want %q", response, payload)
	}
	_ = conn.SetDeadline(time.Time{})
}

func waitWSMUXPool(timeout time.Duration, ready func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ready()
}

func openWSMUXPoolFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}
