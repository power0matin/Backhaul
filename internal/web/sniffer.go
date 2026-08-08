package web

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	stdnet "net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	gnet "github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"

	"github.com/sirupsen/logrus"
)

type Usage struct {
	dataStore    sync.Map
	listenAddr   string
	shutdownCtx  context.Context
	server       *http.Server
	logger       *logrus.Logger
	sniffer      bool
	snifferLog   string
	webUsername  string
	webPassword  string
	mu           sync.Mutex
	saveMu       sync.Mutex
	totalTraffic atomic.Uint64
	tunnelStatus atomic.Value
	runtime      *RuntimeMetrics
	statsMu      sync.Mutex
	cachedStats  *SystemStats
	cachedAt     time.Time
	previousNet  *gnet.IOCountersStat
	previousAt   time.Time
	selfProcess  processMetricsSource
}

type RuntimeMetrics struct {
	startedAt         time.Time
	activeConnections atomic.Int64
	poolConnections   atomic.Int64
	reconnects        atomic.Uint64
	rejected          atomic.Uint64
	lastRejectedLog   atomic.Int64
}

type PortUsage struct {
	Port  int
	Usage uint64
}

type SystemStats struct {
	// CPUUsage, RAMUsage, DiskUsage, SwapUsage, NetworkTraffic, speeds, and
	// AllConnections are legacy v0.8.0 fields and intentionally remain
	// host-wide for API compatibility. Process-specific metrics are below.
	TunnelStatus      string   `json:"tunnelStatus"`
	CPUUsage          string   `json:"cpuUsage"`
	RAMUsage          string   `json:"ramUsage"`
	DiskUsage         string   `json:"diskUsage"`
	SwapUsage         string   `json:"swapUsage"`
	NetworkTraffic    string   `json:"networkTraffic"`
	UploadSpeed       string   `json:"uploadSpeed"`
	DownloadSpeed     string   `json:"downloadSpeed"`
	BackhaulTraffic   string   `json:"backhaulTraffic"`
	Sniffer           string   `json:"sniffer"`
	AllConnections    string   `json:"allConnections"`
	Uptime            string   `json:"uptime"`
	Goroutines        int      `json:"goroutines"`
	HeapAlloc         string   `json:"heapAlloc"`
	ActiveConnections int64    `json:"activeConnections"`
	PoolConnections   int64    `json:"poolConnections"`
	Reconnects        uint64   `json:"reconnects"`
	Rejected          uint64   `json:"rejected"`
	ProcessCPUPercent *float64 `json:"processCpuPercent"`
	ProcessRSSBytes   *uint64  `json:"processRssBytes"`
	ProcessThreads    *int32   `json:"processThreads"`
	ProcessFDs        *int32   `json:"processFds"`
	HeapAllocBytes    uint64   `json:"heapAllocBytes"`
}

func NewDataStore(bindAddr string, webPort int, shutdownCtx context.Context, snifferLog string, sniffer bool, tunnelStatus, webUsername, webPassword string, logger *logrus.Logger) *Usage {
	selfProcess, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		logger.Warnf("process metrics unavailable: %v", err)
	} else {
		// Prime the non-blocking CPU sampler so the first /stats request reports
		// an interval instead of the sampler's initialization value.
		_, _ = selfProcess.Percent(0)
	}
	u := &Usage{
		listenAddr:  stdnet.JoinHostPort(bindAddr, strconv.Itoa(webPort)),
		shutdownCtx: shutdownCtx,
		logger:      logger,
		sniffer:     sniffer,
		snifferLog:  snifferLog,
		webUsername: webUsername,
		webPassword: webPassword,
		mu:          sync.Mutex{},
		runtime:     &RuntimeMetrics{startedAt: time.Now()},
		selfProcess: selfProcess,
	}
	u.tunnelStatus.Store(tunnelStatus)
	return u
}

// SetTunnelStatus publishes transport lifecycle state without racing the web
// monitor. atomic.Value is used because status reads are frequent and tiny.
func (m *Usage) SetTunnelStatus(status string) {
	m.tunnelStatus.Store(status)
}

func (m *Usage) TunnelStatus() string {
	status, _ := m.tunnelStatus.Load().(string)
	return status
}

// InheritRuntimeMetrics keeps counters and uptime continuous across an
// automatic transport reconnect while the previous monitor shuts down.
func (m *Usage) InheritRuntimeMetrics(previous *Usage) {
	if previous != nil && previous.runtime != nil {
		m.runtime = previous.runtime
	}
}

func (m *Usage) ConnectionOpened() {
	m.runtime.activeConnections.Add(1)
}

func (m *Usage) ConnectionClosed() {
	m.runtime.activeConnections.Add(-1)
}

func (m *Usage) SetPoolConnections(count int64) {
	m.runtime.poolConnections.Store(count)
}

func (m *Usage) RecordReconnect() {
	m.runtime.reconnects.Add(1)
}

// RecordRejected returns a cumulative count and whether the caller should log
// this event. Overload warnings are limited to one every five seconds.
func (m *Usage) RecordRejected() (uint64, bool) {
	total := m.runtime.rejected.Add(1)
	now := time.Now().UnixNano()
	for {
		previous := m.runtime.lastRejectedLog.Load()
		if previous != 0 && now-previous < int64(5*time.Second) {
			return total, false
		}
		if m.runtime.lastRejectedLog.CompareAndSwap(previous, now) {
			return total, true
		}
	}
}

func (m *Usage) Monitor() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", m.handleIndex) // handle index
	mux.HandleFunc("/stats", m.statsHandler)
	if m.sniffer {
		mux.HandleFunc("/data", m.handleData) // New route for JSON data
	}
	m.server = &http.Server{
		Addr:              m.listenAddr,
		Handler:           m.authenticated(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-m.shutdownCtx.Done()
		if m.sniffer {
			m.saveUsageData()
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Attempt to gracefully shut down the server
		if err := m.server.Shutdown(shutdownCtx); err != nil {
			m.logger.Errorf("sniffer server shutdown error: %v", err)
		}
	}()

	// start save data
	if m.sniffer {
		m.loadPersistedTraffic()
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					m.saveUsageData()
				case <-m.shutdownCtx.Done():
					return
				}
			}
		}()
	}
	// Start the server
	m.logger.Info("sniffer service listening on port: ", m.listenAddr)
	if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		m.logger.Errorf("sniffer server error: %v", err)
	}
}

func (m *Usage) authenticated(next http.Handler) http.Handler {
	if m.webUsername == "" && m.webPassword == "" {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		userHash := sha256.Sum256([]byte(username))
		expectedUserHash := sha256.Sum256([]byte(m.webUsername))
		passwordHash := sha256.Sum256([]byte(password))
		expectedPasswordHash := sha256.Sum256([]byte(m.webPassword))
		userOK := subtle.ConstantTimeCompare(userHash[:], expectedUserHash[:]) == 1
		passOK := subtle.ConstantTimeCompare(passwordHash[:], expectedPasswordHash[:]) == 1
		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="Backhaul monitor", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

//go:embed index.html
var indexHTML embed.FS

func (m *Usage) handleIndex(w http.ResponseWriter, r *http.Request) {
	usageData := m.getUsageFromFile()
	readableData := m.usageDataWithReadableUsage(usageData)

	tmpl, err := template.ParseFS(indexHTML, "index.html")
	if err != nil {
		m.logger.Errorf("error parsing template: %v", err)
		return
	}

	err = tmpl.Execute(w, readableData)
	if err != nil {
		m.logger.Errorf("error executing template: %v", err)
	}
}

func (m *Usage) handleData(w http.ResponseWriter, r *http.Request) {
	usageData := m.getUsageFromFile()
	readableData := m.usageDataWithReadableUsage(usageData)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(readableData); err != nil {
		m.logger.Errorf("error encoding JSON response: %v", err)
	}
}

func (m *Usage) statsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := m.getSystemStats()
	if err != nil {
		m.logger.Error("Error fetching system stats:", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		m.logger.Error("Error encoding JSON:", err)
	}
}

func (m *Usage) AddOrUpdatePort(port int, usage uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Retrieve current usage data for the port
	value, ok := m.dataStore.Load(port)
	if ok {
		// Port exists, update usage
		portUsage := value.(PortUsage)
		portUsage.Usage += usage
		m.dataStore.Store(port, portUsage)
	} else {
		// Port does not exist, create new entry
		m.dataStore.Store(port, PortUsage{Port: port, Usage: usage})
	}
	m.totalTraffic.Add(usage)
}

func (m *Usage) saveUsageData() {
	m.saveMu.Lock()
	defer m.saveMu.Unlock()

	// Step 1: Load existing usage data from the JSON file
	var existingUsageData []PortUsage
	file, err := os.Open(m.snifferLog)
	if err == nil {
		// If the file exists, decode the JSON data into existingUsageData
		defer file.Close()
		err = json.NewDecoder(file).Decode(&existingUsageData)
		if err != nil {
			m.logger.Errorf("error decoding JSON data: %v", err)
			return
		}
	} else if !os.IsNotExist(err) {
		// Log any error except file not existing
		m.logger.Errorf("error opening JSON file: %v", err)
		return
	}

	// Step 2: Get current usage data from sync.Map
	currentUsageData := m.collectUsageDataFromSyncMap()

	// Step 3: Merge the existing and current usage data into a map to avoid duplicates
	usageMap := make(map[int]PortUsage)

	// Add existing usage data to the map
	for _, usage := range existingUsageData {
		usageMap[usage.Port] = usage
	}

	// Append or update current usage data in the map
	for _, usage := range currentUsageData {
		if existing, exists := usageMap[usage.Port]; exists {
			// Update existing port usage
			existing.Usage += usage.Usage
			usageMap[usage.Port] = existing
		} else {
			// Add new port usage
			usageMap[usage.Port] = usage
		}
	}

	// Step 4: Convert the map back to a slice
	var mergedUsageData []PortUsage
	for _, usage := range usageMap {
		mergedUsageData = append(mergedUsageData, usage)
	}

	// Step 5: Convert merged data to JSON
	data, err := json.MarshalIndent(mergedUsageData, "", "  ")
	if err != nil {
		m.logger.Errorf("error marshalling usage data: %v", err)
		return
	}

	// Step 6: Write JSON data to file
	err = os.WriteFile(m.snifferLog, data, 0600)
	if err != nil {
		m.restoreUsageData(currentUsageData)
		m.logger.Errorf("error writing usage data to file: %v", err)
	}
}

func (m *Usage) restoreUsageData(usageData []PortUsage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, usage := range usageData {
		value, ok := m.dataStore.Load(usage.Port)
		if ok {
			current := value.(PortUsage)
			usage.Usage += current.Usage
		}
		m.dataStore.Store(usage.Port, usage)
	}
}

func (m *Usage) loadPersistedTraffic() {
	m.saveMu.Lock()
	defer m.saveMu.Unlock()

	file, err := os.Open(m.snifferLog)
	if err != nil {
		return
	}
	defer file.Close()

	var persisted []PortUsage
	if err := json.NewDecoder(file).Decode(&persisted); err != nil {
		return
	}
	for _, usage := range persisted {
		m.totalTraffic.Add(usage.Usage)
	}
}

func (m *Usage) getUsageFromFile() []PortUsage {
	m.saveMu.Lock()
	defer m.saveMu.Unlock()

	// Check if the file exists
	if _, err := os.Stat(m.snifferLog); os.IsNotExist(err) {
		// If the file does not exist, create it and write "null"
		file, err := os.OpenFile(m.snifferLog, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			m.logger.Errorf("error creating file: %v", err)
			return nil
		}

		defer file.Close()
		// Use an empty array so a newly-created log has the same JSON shape as a
		// populated usage log.
		if _, err := file.Write([]byte("[]")); err != nil {
			m.logger.Errorf("error writing 'null' to the file: %v", err)
			return nil
		}

		return nil
	}

	var usageData []PortUsage

	// Open the JSON file
	file, err := os.Open(m.snifferLog)
	if err != nil {
		m.logger.Errorf("error opening JSON file: %v", err)
		return nil
	}
	defer file.Close()

	// Decode the JSON file into the usageData slice
	err = json.NewDecoder(file).Decode(&usageData)
	if err != nil {
		m.logger.Errorf("error decoding JSON data: %v", err)
		return nil
	}

	// Sort usageData by Port in ascending order
	sort.Slice(usageData, func(i, j int) bool {
		return usageData[i].Port < usageData[j].Port
	})

	return usageData
}

// converts the byte usage to a human-readable format
func (m *Usage) usageDataWithReadableUsage(usageData []PortUsage) []struct {
	Port          int
	ReadableUsage string
} {
	var result []struct {
		Port          int
		ReadableUsage string
	}

	for _, portUsage := range usageData {
		result = append(result, struct {
			Port          int
			ReadableUsage string
		}{
			Port:          portUsage.Port,
			ReadableUsage: m.convertBytesToReadable(portUsage.Usage),
		})
	}

	return result
}

// collectUsageDataFromSyncMap gathers data from sync.Map
func (m *Usage) collectUsageDataFromSyncMap() []PortUsage {
	m.mu.Lock()
	defer m.mu.Unlock()

	var usageData []PortUsage
	m.dataStore.Range(func(key, value interface{}) bool {
		if portUsage, ok := value.(PortUsage); ok {
			usageData = append(usageData, portUsage)
			m.dataStore.Delete(key)
		}
		return true
	})
	return usageData
}

// ConvertBytesToReadable converts bytes into a human-readable format (KB, MB, GB)
func (m *Usage) convertBytesToReadable(bytes uint64) string {
	const (
		KB = 1 << (10 * 1) // 1024 bytes
		MB = 1 << (10 * 2) // 1024 KB
		GB = 1 << (10 * 3) // 1024 MB
		TB = 1 << (10 * 4) // 1024 TB
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes) // Bytes
	}
}

func (m *Usage) getSystemStats() (*SystemStats, error) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()

	now := time.Now()
	if m.cachedStats != nil && now.Sub(m.cachedAt) < time.Second {
		copy := *m.cachedStats
		return &copy, nil
	}

	currentNet, err := m.getNetworkStats()
	if err != nil {
		return nil, err
	}

	// Get CPU usage
	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}

	// Get RAM usage
	memStats, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	// Get Disk usage
	diskStats, err := disk.Usage("/")
	if err != nil {
		return nil, err
	}

	// Get Swap usage
	swapStats, err := mem.SwapMemory()
	if err != nil {
		return nil, err
	}

	// Get all active network connections (TCP, UDP, etc.)
	connections, err := gnet.Connections("all")
	if err != nil {
		return nil, err
	}

	var uploadSpeed, downloadSpeed float64
	if m.previousNet != nil {
		elapsed := now.Sub(m.previousAt).Seconds()
		if elapsed > 0 {
			uploadSpeed = float64(currentNet.BytesSent-m.previousNet.BytesSent) / elapsed
			downloadSpeed = float64(currentNet.BytesRecv-m.previousNet.BytesRecv) / elapsed
		}
	}
	netCopy := *currentNet
	m.previousNet = &netCopy
	m.previousAt = now

	var runtimeStats runtime.MemStats
	runtime.ReadMemStats(&runtimeStats)
	processStats := m.getProcessStats()
	status, _ := m.tunnelStatus.Load().(string)
	cpuUsage := float64(0)
	if len(cpuPercent) > 0 {
		cpuUsage = cpuPercent[0]
	}

	stats := &SystemStats{
		TunnelStatus:      status,
		CPUUsage:          m.formatFloat(cpuUsage),
		RAMUsage:          m.convertBytesToReadable(memStats.Used),
		DiskUsage:         m.convertBytesToReadable(diskStats.Used),
		SwapUsage:         m.convertBytesToReadable(swapStats.Used),
		NetworkTraffic:    m.convertBytesToReadable(currentNet.BytesSent + currentNet.BytesRecv),
		DownloadSpeed:     m.formatSpeed(downloadSpeed),
		UploadSpeed:       m.formatSpeed(uploadSpeed),
		BackhaulTraffic:   m.convertBytesToReadable(m.totalTraffic.Load()),
		Sniffer:           map[bool]string{true: "Running", false: "Not running"}[m.sniffer],
		AllConnections:    fmt.Sprintf("%d", len(connections)),
		Uptime:            time.Since(m.runtime.startedAt).Round(time.Second).String(),
		Goroutines:        runtime.NumGoroutine(),
		HeapAlloc:         m.convertBytesToReadable(runtimeStats.HeapAlloc),
		ActiveConnections: m.runtime.activeConnections.Load(),
		PoolConnections:   m.runtime.poolConnections.Load(),
		Reconnects:        m.runtime.reconnects.Load(),
		Rejected:          m.runtime.rejected.Load(),
		ProcessCPUPercent: processStats.cpuPercent,
		ProcessRSSBytes:   processStats.rssBytes,
		ProcessThreads:    processStats.threads,
		ProcessFDs:        processStats.fds,
		HeapAllocBytes:    runtimeStats.HeapAlloc,
	}

	m.cachedStats = stats
	m.cachedAt = now
	copy := *stats
	return &copy, nil
}

type processStats struct {
	cpuPercent *float64
	rssBytes   *uint64
	threads    *int32
	fds        *int32
}

type processMetricsSource interface {
	Percent(time.Duration) (float64, error)
	MemoryInfo() (*process.MemoryInfoStat, error)
	NumThreads() (int32, error)
	NumFDs() (int32, error)
}

// getProcessStats returns metrics for Backhaul itself. Each metric is
// collected independently so an unsupported platform-specific metric (for
// example file-descriptor counts on Darwin) does not make /stats unavailable.
// Percent(0) is non-blocking and measures CPU since the previous collection.
func (m *Usage) getProcessStats() processStats {
	var stats processStats
	if m.selfProcess == nil {
		return stats
	}

	if cpuPercent, err := m.selfProcess.Percent(0); err == nil {
		stats.cpuPercent = &cpuPercent
	}
	if memoryInfo, err := m.selfProcess.MemoryInfo(); err == nil {
		rssBytes := memoryInfo.RSS
		stats.rssBytes = &rssBytes
	}
	if threads, err := m.selfProcess.NumThreads(); err == nil {
		stats.threads = &threads
	}
	if fds, err := m.selfProcess.NumFDs(); err == nil {
		stats.fds = &fds
	}

	return stats
}

func (m *Usage) formatSpeed(bytesPerSec float64) string {
	if bytesPerSec >= 1e9 {
		return fmt.Sprintf("%.2f GB/s", bytesPerSec/1e9)
	} else if bytesPerSec >= 1e6 {
		return fmt.Sprintf("%.2f MB/s", bytesPerSec/1e6)
	} else if bytesPerSec >= 1e3 {
		return fmt.Sprintf("%.2f KB/s", bytesPerSec/1e3)
	}
	return fmt.Sprintf("%.2f B/s", bytesPerSec)
}

func (m *Usage) formatFloat(value float64) string {
	return fmt.Sprintf("%.2f%%", value)
}

func (m *Usage) getNetworkStats() (*gnet.IOCountersStat, error) {
	ioCounters, err := gnet.IOCounters(false)
	if err != nil {
		return nil, err
	}
	if len(ioCounters) == 0 {
		return nil, fmt.Errorf("no network IO counters found")
	}
	return &ioCounters[0], nil
}
