package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
)

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
