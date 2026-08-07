package cmd

import (
	"github.com/musix/backhaul/config"

	"github.com/sirupsen/logrus"
)

const ( // Default values
	defaultToken          = "musix"
	defaultChannelSize    = 2048
	defaultRetryInterval  = 3 // only for client
	defaultConnectionPool = 8
	defaultLogLevel       = "info"
	defaultMuxSession     = 1
	defaultKeepAlive      = 75
	deafultHeartbeat      = 40 // 40 seconds
	defaultDialTimeout    = 10 // 10 seconds
	// related to smux
	defaultMuxVersion       = 1
	defaultMaxFrameSize     = 32768   // 32KB
	defaultMaxReceiveBuffer = 4194304 // 4MB
	defaultMaxStreamBuffer  = 65536   // 64KB
	defaultSnifferLog       = "backhaul.json"
	defaultMuxCon           = 8
	defaultUDPQueueSize     = 64
	defaultUDPQueueLimit    = 4096
	defaultUDPMaxFlows      = 2048
	defaultWebBindAddr      = "127.0.0.1"
)

func applyDefaults(cfg *config.Config) {
	// Token
	if cfg.Server.Token == "" {
		cfg.Server.Token = defaultToken
	}
	if cfg.Client.Token == "" {
		cfg.Client.Token = defaultToken
	}

	// Nodelay default is false if not valid value found

	// Channel size
	if cfg.Server.ChannelSize <= 0 {
		cfg.Server.ChannelSize = defaultChannelSize
	}

	// Loglevel
	if _, err := logrus.ParseLevel(cfg.Client.LogLevel); err != nil {
		cfg.Client.LogLevel = defaultLogLevel
	}

	if _, err := logrus.ParseLevel(cfg.Server.LogLevel); err != nil {
		cfg.Server.LogLevel = defaultLogLevel
	}

	// Retry interval
	if cfg.Client.RetryInterval <= 0 {
		cfg.Client.RetryInterval = defaultRetryInterval
	}

	// Connection pool
	if cfg.Client.ConnectionPool <= 0 {
		cfg.Client.ConnectionPool = defaultConnectionPool
	}
	if cfg.Client.MaxPoolSize <= 0 {
		if cfg.Client.ConnectionPool > maxConnectionPool {
			// Validation below will reject the base size; avoid overflowing while
			// deriving its default ceiling.
			cfg.Client.MaxPoolSize = cfg.Client.ConnectionPool
		} else {
			cfg.Client.MaxPoolSize = cfg.Client.ConnectionPool * 4
			if minimum := cfg.Client.ConnectionPool + 16; cfg.Client.MaxPoolSize < minimum {
				cfg.Client.MaxPoolSize = minimum
			}
		}
	}

	// Mux Session
	if cfg.Server.MuxSession <= 0 {
		cfg.Server.MuxSession = defaultMuxSession
	}
	if cfg.Client.MuxSession <= 0 {
		cfg.Client.MuxSession = defaultMuxSession
	}

	// PPROF default is false if not valid value found

	// keep alive
	if cfg.Server.Keepalive <= 0 {
		cfg.Server.Keepalive = defaultKeepAlive
	}
	if cfg.Client.Keepalive <= 0 {
		cfg.Client.Keepalive = defaultKeepAlive
	}

	// Mux version
	if cfg.Server.MuxVersion <= 0 || cfg.Server.MuxVersion > 2 {
		cfg.Server.MuxVersion = defaultMuxVersion
	}
	if cfg.Client.MuxVersion <= 0 || cfg.Client.MuxVersion > 2 {
		cfg.Client.MuxVersion = defaultMuxVersion
	}
	// MaxFrameSize
	if cfg.Server.MaxFrameSize <= 0 {
		cfg.Server.MaxFrameSize = defaultMaxFrameSize
	}
	if cfg.Client.MaxFrameSize <= 0 {
		cfg.Client.MaxFrameSize = defaultMaxFrameSize
	}
	// MaxReceiveBuffer
	if cfg.Server.MaxReceiveBuffer <= 0 {
		cfg.Server.MaxReceiveBuffer = defaultMaxReceiveBuffer
	}
	if cfg.Client.MaxReceiveBuffer <= 0 {
		cfg.Client.MaxReceiveBuffer = defaultMaxReceiveBuffer
	}
	// MaxStreamBuffer
	if cfg.Server.MaxStreamBuffer <= 0 {
		cfg.Server.MaxStreamBuffer = defaultMaxStreamBuffer
	}
	if cfg.Client.MaxStreamBuffer <= 0 {
		cfg.Client.MaxStreamBuffer = defaultMaxStreamBuffer
	}
	// WebPort returns 0 if not exists
	// The v0.7.2 monitor listened on every interface. New configurations default
	// to loopback so enabling the monitor does not accidentally expose host
	// metrics to the public Internet. Set web_bind_addr explicitly for remote
	// access.
	if cfg.Server.WebBindAddr == "" {
		cfg.Server.WebBindAddr = defaultWebBindAddr
	}
	if cfg.Client.WebBindAddr == "" {
		cfg.Client.WebBindAddr = defaultWebBindAddr
	}

	// SnifferLog
	if cfg.Server.SnifferLog == "" {
		cfg.Server.SnifferLog = defaultSnifferLog
	}
	if cfg.Client.SnifferLog == "" {
		cfg.Client.SnifferLog = defaultSnifferLog
	}
	// Heartbeat
	if cfg.Server.Heartbeat < 1 { // Minimum accepted interval is 1 second
		cfg.Server.Heartbeat = deafultHeartbeat
	}

	// Timeout
	if cfg.Client.DialTimeout < 1 { // Minimum accepted value is 1 second
		cfg.Client.DialTimeout = defaultDialTimeout
	}

	// Mux concurrancy
	if cfg.Server.MuxCon < 1 {
		cfg.Server.MuxCon = defaultMuxCon
	}

	// UDP queues are bounded both per flow and globally. v0.7.2 allocated a
	// 100,000-entry channel for every source address, which made memory usage
	// proportional to untrusted flow cardinality even when the queues were idle.
	if cfg.Server.UDPQueueSize <= 0 {
		cfg.Server.UDPQueueSize = defaultUDPQueueSize
	}
	if cfg.Server.UDPQueueLimit <= 0 {
		cfg.Server.UDPQueueLimit = defaultUDPQueueLimit
	}
	if cfg.Server.UDPMaxFlows <= 0 {
		cfg.Server.UDPMaxFlows = defaultUDPMaxFlows
	}
}
