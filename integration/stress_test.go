package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/client"
	"github.com/musix/backhaul/internal/server"
)

func TestStreamTransportConcurrentChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped with -short")
	}

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
			defer fixture.cancel()
			ready := dialReady(t, fixture.addr, 8*time.Second)
			_ = ready.Close()

			payload := bytes.Repeat([]byte("churn"), 8*1024)
			for round := 0; round < 3; round++ {
				if err := concurrentRoundTrips(fixture.addr, payload, 16); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
			}
		})
	}
}

func TestWSMUXLifecycleResourcesReturnToStableRange(t *testing.T) {
	if testing.Short() {
		t.Skip("lifecycle stress test skipped with -short")
	}

	target, targetAddr := startTCPEcho(t)
	tunnelAddr := freeTCPAddr(t)
	forwardAddr := freeTCPAddr(t)
	runtime.GC()
	beforeGoroutines := runtime.NumGoroutine()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	ctx, cancel := context.WithCancel(context.Background())
	srvCfg := baseServerConfig(config.WSMUX, tunnelAddr, forwardAddr+"="+targetAddr)
	clCfg := baseClientConfig(config.WSMUX, tunnelAddr)
	startPair(ctx, &srvCfg, &clCfg)
	ready := dialReady(t, forwardAddr, 8*time.Second)
	_ = ready.Close()

	payload := bytes.Repeat([]byte{0xa5}, 32*1024)
	for round := 0; round < 12; round++ {
		if err := concurrentRoundTrips(forwardAddr, payload, 24); err != nil {
			cancel()
			_ = target.Close()
			t.Fatalf("churn round %d: %v", round, err)
		}
	}

	cancel()
	waitTCPClosed(t, forwardAddr, 3*time.Second)
	_ = target.Close()

	deadline := time.Now().Add(4 * time.Second)
	var after runtime.MemStats
	for {
		runtime.GC()
		runtime.ReadMemStats(&after)
		if runtime.NumGoroutine() <= beforeGoroutines+10 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	finalGoroutines := runtime.NumGoroutine()
	var retained uint64
	if after.HeapAlloc > before.HeapAlloc {
		retained = after.HeapAlloc - before.HeapAlloc
	}
	t.Logf("WSMUX lifecycle: goroutines before=%d after=%d, live heap delta after GC=%d bytes", beforeGoroutines, finalGoroutines, retained)
	if finalGoroutines > beforeGoroutines+10 {
		t.Fatalf("goroutines did not return to stable range: before=%d after=%d", beforeGoroutines, finalGoroutines)
	}
	if retained > 12<<20 {
		t.Fatalf("live heap did not return to stable range: retained=%d bytes", retained)
	}
}

func TestAcceptUDPLifecycleResourcesReturnToStableRange(t *testing.T) {
	if testing.Short() {
		t.Skip("lifecycle stress test skipped with -short")
	}

	target, targetAddr := startUDPEcho(t)
	tunnelAddr := freeTCPAddr(t)
	forwardAddr := freeUDPAddr(t)
	runtime.GC()
	beforeGoroutines := runtime.NumGoroutine()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	ctx, cancel := context.WithCancel(context.Background())
	srvCfg := baseServerConfig(config.TCP, tunnelAddr, forwardAddr+"="+targetAddr)
	srvCfg.AcceptUDP = true
	srvCfg.UDPQueueSize = 64
	srvCfg.UDPQueueLimit = 4096
	srvCfg.UDPMaxFlows = 2048
	clCfg := baseClientConfig(config.TCP, tunnelAddr)
	startPair(ctx, &srvCfg, &clCfg)
	ready := dialUDPReady(t, forwardAddr, 8*time.Second)
	_ = ready.Close()

	for i := 0; i < 64; i++ {
		conn, err := net.Dial("udp", forwardAddr)
		if err != nil {
			cancel()
			_ = target.Close()
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		payload := []byte{byte(i), 0x42, 0x99}
		if _, err := conn.Write(payload); err != nil {
			_ = conn.Close()
			cancel()
			_ = target.Close()
			t.Fatal(err)
		}
		response := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, response); err != nil {
			_ = conn.Close()
			cancel()
			_ = target.Close()
			t.Fatal(err)
		}
		_ = conn.Close()
	}

	cancel()
	_ = target.Close()
	deadline := time.Now().Add(4 * time.Second)
	var after runtime.MemStats
	for {
		runtime.GC()
		runtime.ReadMemStats(&after)
		if runtime.NumGoroutine() <= beforeGoroutines+10 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	finalGoroutines := runtime.NumGoroutine()
	var retained uint64
	if after.HeapAlloc > before.HeapAlloc {
		retained = after.HeapAlloc - before.HeapAlloc
	}
	t.Logf("accept_udp lifecycle: goroutines before=%d after=%d, live heap delta after GC=%d bytes", beforeGoroutines, finalGoroutines, retained)
	if finalGoroutines > beforeGoroutines+10 {
		t.Fatalf("accept_udp goroutines did not return to stable range: before=%d after=%d", beforeGoroutines, finalGoroutines)
	}
	if retained > 12<<20 {
		t.Fatalf("accept_udp live heap did not return to stable range: retained=%d bytes", retained)
	}
}

func TestWSMUXRecoversAfterInitialConnectionRefusal(t *testing.T) {
	target, targetAddr := startTCPEcho(t)
	defer target.Close()

	tunnelAddr := freeTCPAddr(t)
	forwardAddr := freeTCPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clCfg := baseClientConfig(config.WSMUX, tunnelAddr)
	cl := client.NewClient(&clCfg, ctx)
	go cl.Start()

	// Allow at least one refused dial before the server appears.
	time.Sleep(400 * time.Millisecond)
	recoveryStarted := time.Now()
	srvCfg := baseServerConfig(config.WSMUX, tunnelAddr, forwardAddr+"="+targetAddr)
	srv := server.NewServer(&srvCfg, ctx)
	go srv.Start()

	conn := dialReady(t, forwardAddr, 8*time.Second)
	_ = conn.Close()
	t.Logf("WSMUX recovered from initial connection refusal in %.3fs", time.Since(recoveryStarted).Seconds())
}

// TestWSMUXSoak is opt-in so mandatory CI stays deterministic and fast. Run
// with BACKHAUL_SOAK=1; BACKHAUL_SOAK_DURATION optionally overrides 30s.
func TestWSMUXSoak(t *testing.T) {
	if os.Getenv("BACKHAUL_SOAK") == "" {
		t.Skip("set BACKHAUL_SOAK=1 to run the extended soak")
	}
	duration := 30 * time.Second
	if configured := os.Getenv("BACKHAUL_SOAK_DURATION"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed < 5*time.Second {
			t.Fatalf("invalid BACKHAUL_SOAK_DURATION %q (minimum 5s)", configured)
		}
		duration = parsed
	}

	fixture := startStreamFixture(t, config.WSMUX)
	defer fixture.cancel()
	ready := dialReady(t, fixture.addr, 8*time.Second)
	_ = ready.Close()

	payload := bytes.Repeat([]byte{0x6d}, 16*1024)
	deadline := time.Now().Add(duration)
	var successes atomic.Uint64
	var failures atomic.Uint64
	var minHeap uint64 = ^uint64(0)
	var maxHeap uint64
	maxGoroutines := 0
	nextSample := time.Now()

	for time.Now().Before(deadline) {
		if err := concurrentRoundTrips(fixture.addr, payload, 16); err != nil {
			failures.Add(1)
		} else {
			successes.Add(16)
		}
		if time.Now().Before(nextSample) {
			continue
		}
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if mem.HeapAlloc < minHeap {
			minHeap = mem.HeapAlloc
		}
		if mem.HeapAlloc > maxHeap {
			maxHeap = mem.HeapAlloc
		}
		goroutines := runtime.NumGoroutine()
		if goroutines > maxGoroutines {
			maxGoroutines = goroutines
		}
		t.Logf("soak sample: successes=%d failures=%d heap=%d goroutines=%d rss=%d", successes.Load(), failures.Load(), mem.HeapAlloc, goroutines, processRSS())
		nextSample = time.Now().Add(2 * time.Second)
	}

	if failures.Load() != 0 {
		t.Fatalf("soak observed %d failed transfer batches", failures.Load())
	}
	if maxHeap > minHeap+16<<20 {
		t.Fatalf("live heap range grew beyond stability allowance: min=%d max=%d", minHeap, maxHeap)
	}
	t.Logf("soak summary: duration=%s successful_connections=%d heap_range=%d..%d max_goroutines=%d rss=%d", duration, successes.Load(), minHeap, maxHeap, maxGoroutines, processRSS())
}

func concurrentRoundTrips(addr string, payload []byte, concurrency int) error {
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				errs <- err
				return
			}
			if _, err := conn.Write(payload); err != nil {
				errs <- err
				return
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, got); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, payload) {
				errs <- fmt.Errorf("payload mismatch")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func processRSS() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
