package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	"github.com/sirupsen/logrus"
)

type fakeProcessMetrics struct {
	cpuPercent float64
	rssBytes   uint64
	threads    int32
	fds        int32
	fdsErr     error
}

func (f fakeProcessMetrics) Percent(time.Duration) (float64, error) {
	return f.cpuPercent, nil
}

func (f fakeProcessMetrics) MemoryInfo() (*process.MemoryInfoStat, error) {
	return &process.MemoryInfoStat{RSS: f.rssBytes}, nil
}

func (f fakeProcessMetrics) NumThreads() (int32, error) {
	return f.threads, nil
}

func (f fakeProcessMetrics) NumFDs() (int32, error) {
	return f.fds, f.fdsErr
}

func TestMonitorBasicAuthentication(t *testing.T) {
	usage := NewDataStore("127.0.0.1", 0, context.Background(), t.TempDir()+"/usage.json", false, "Connected", "operator", "secret", logrus.New())
	protected := usage.authenticated(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name       string
		username   string
		password   string
		wantStatus int
	}{
		{name: "missing", wantStatus: http.StatusUnauthorized},
		{name: "wrong", username: "operator", password: "bad", wantStatus: http.StatusUnauthorized},
		{name: "valid", username: "operator", password: "secret", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "http://monitor/stats", nil)
			if tt.username != "" || tt.password != "" {
				req.SetBasicAuth(tt.username, tt.password)
			}
			recorder := httptest.NewRecorder()
			protected.ServeHTTP(recorder, req)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
		})
	}
}

func TestTunnelStatusConcurrentAccess(t *testing.T) {
	usage := NewDataStore("127.0.0.1", 0, context.Background(), t.TempDir()+"/usage.json", false, "Disconnected", "", "", logrus.New())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				usage.SetTunnelStatus("Connected")
				_, _ = usage.tunnelStatus.Load().(string)
			}
		}()
	}
	wg.Wait()
}

func TestRuntimeMetrics(t *testing.T) {
	usage := NewDataStore("127.0.0.1", 0, context.Background(), t.TempDir()+"/usage.json", false, "Connected", "", "", logrus.New())
	usage.ConnectionOpened()
	usage.ConnectionOpened()
	usage.ConnectionClosed()
	usage.SetPoolConnections(7)
	usage.RecordReconnect()
	firstTotal, firstLog := usage.RecordRejected()
	secondTotal, secondLog := usage.RecordRejected()
	if got := usage.runtime.activeConnections.Load(); got != 1 {
		t.Fatalf("active connections = %d, want 1", got)
	}
	if got := usage.runtime.poolConnections.Load(); got != 7 {
		t.Fatalf("pool connections = %d, want 7", got)
	}
	if got := usage.runtime.reconnects.Load(); got != 1 {
		t.Fatalf("reconnects = %d, want 1", got)
	}
	if firstTotal != 1 || secondTotal != 2 || !firstLog || secondLog {
		t.Fatalf("rejection counter/rate limit = (%d,%t), (%d,%t)", firstTotal, firstLog, secondTotal, secondLog)
	}

	next := NewDataStore("127.0.0.1", 0, context.Background(), t.TempDir()+"/next.json", false, "Disconnected", "", "", logrus.New())
	next.InheritRuntimeMetrics(usage)
	if next.runtime != usage.runtime {
		t.Fatal("automatic reconnect did not inherit runtime counters")
	}
}

func TestProcessStatsReportCurrentProcess(t *testing.T) {
	usage := NewDataStore("127.0.0.1", 0, context.Background(), t.TempDir()+"/usage.json", false, "Connected", "", "", logrus.New())
	usage.selfProcess = fakeProcessMetrics{
		cpuPercent: 3.25,
		rssBytes:   64 << 20,
		threads:    10,
		fds:        530,
	}
	stats := usage.getProcessStats()

	if stats.cpuPercent == nil || *stats.cpuPercent != 3.25 {
		t.Fatalf("process CPU = %v, want 3.25", stats.cpuPercent)
	}
	if stats.rssBytes == nil || *stats.rssBytes != 64<<20 {
		t.Fatalf("process RSS = %v, want %d", stats.rssBytes, uint64(64<<20))
	}
	if stats.threads == nil || *stats.threads != 10 {
		t.Fatalf("process threads = %v, want 10", stats.threads)
	}
	if stats.fds == nil || *stats.fds != 530 {
		t.Fatalf("process FDs = %v, want 530", stats.fds)
	}
}

func TestProcessStatsIsolateUnsupportedMetric(t *testing.T) {
	usage := NewDataStore("127.0.0.1", 0, context.Background(), t.TempDir()+"/usage.json", false, "Connected", "", "", logrus.New())
	usage.selfProcess = fakeProcessMetrics{
		cpuPercent: 1.5,
		rssBytes:   32 << 20,
		threads:    4,
		fdsErr:     errors.New("not implemented"),
	}

	stats := usage.getProcessStats()
	if stats.cpuPercent == nil || stats.rssBytes == nil || stats.threads == nil {
		t.Fatal("one unsupported process metric suppressed supported metrics")
	}
	if stats.fds != nil {
		t.Fatalf("unsupported FD metric = %d, want unavailable", *stats.fds)
	}
}

func TestSystemStatsJSONCompatibility(t *testing.T) {
	encoded, err := json.Marshal(SystemStats{})
	if err != nil {
		t.Fatalf("marshal SystemStats: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("unmarshal SystemStats: %v", err)
	}

	wantFields := []string{
		"tunnelStatus", "cpuUsage", "ramUsage", "diskUsage", "swapUsage",
		"networkTraffic", "uploadSpeed", "downloadSpeed", "backhaulTraffic",
		"sniffer", "allConnections", "uptime", "goroutines", "heapAlloc",
		"activeConnections", "poolConnections", "reconnects", "rejected",
		"processCpuPercent", "processRssBytes", "processThreads", "processFds",
		"heapAllocBytes",
	}
	for _, field := range wantFields {
		if _, ok := fields[field]; !ok {
			t.Errorf("SystemStats JSON missing %q", field)
		}
	}
}

func TestMonitorLabelsHostAndProcessMetrics(t *testing.T) {
	page, err := indexHTML.ReadFile("index.html")
	if err != nil {
		t.Fatalf("read embedded monitor page: %v", err)
	}
	html := string(page)
	for _, want := range []string{
		"Backhaul Process", "Process CPU", "Process RSS", "Open FDs",
		"Active Connections", "Host CPU Usage", "Host RAM Usage",
		"Host Network", "stats.processCpuPercent", "stats.processRssBytes",
		"stats.sniffer === 'Running'", "Sniffer disabled",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("monitor page missing %q", want)
		}
	}
}
