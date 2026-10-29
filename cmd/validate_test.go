package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/musix/backhaul/config"
)

func validServerConfig() *config.Config {
	cfg := &config.Config{Server: config.ServerConfig{
		BindAddr:  "127.0.0.1:3080",
		Transport: config.TCP,
		Token:     "test-token",
	}}
	applyDefaults(cfg)
	return cfg
}

func TestValidateConfig(t *testing.T) {
	t.Run("valid server", func(t *testing.T) {
		if err := ValidateConfig(validServerConfig()); err != nil {
			t.Fatalf("ValidateConfig: %v", err)
		}
	})

	t.Run("both roles", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Client.RemoteAddr = "127.0.0.1:3080"
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("configuration with both server and client roles was accepted")
		}
	})

	t.Run("oversized token", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.Token = strings.Repeat("x", 65536)
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("oversized framed token was accepted")
		}
	})

	t.Run("udp queue memory bound", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.UDPQueueSize = 4096
		cfg.Server.UDPMaxFlows = 2048
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("configuration capable of excessive per-flow channel allocation was accepted")
		}
	})

	t.Run("udp global packet budget bound", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.UDPQueueLimit = maxUDPQueueLimit + 1
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("configuration with an excessive global UDP packet budget was accepted")
		}
	})

	t.Run("monitor credentials must be paired", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.WebPort = 2060
		cfg.Server.WebUsername = "operator"
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("monitor with username but no password was accepted")
		}
	})

	t.Run("wss requires certificate", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.Transport = config.WSSMUX
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("WSSMUX without TLS certificate/key was accepted")
		}
	})


	t.Run("missing authentication token", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.Token = ""
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("configuration without a shared authentication token was accepted")
		}
	})

	t.Run("built-in authentication token is rejected", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.Token = defaultToken
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("configuration using the built-in token was accepted")
		}
	})

	t.Run("non-loopback monitor requires authentication", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.WebPort = 2060
		cfg.Server.WebBindAddr = "0.0.0.0"
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("public monitor without credentials was accepted")
		}
		cfg.Server.WebUsername = "operator"
		cfg.Server.WebPassword = "strong-test-password"
		if err := ValidateConfig(cfg); err != nil {
			t.Fatalf("public monitor with paired credentials rejected: %v", err)
		}
	})

	t.Run("invalid web bind hostname is rejected", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.WebPort = 2060
		cfg.Server.WebBindAddr = "bad host"
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("web bind address containing whitespace was accepted")
		}
	})

	t.Run("mux receive buffer upper bound", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.Transport = config.TCPMUX
		cfg.Server.MaxReceiveBuffer = maxMuxBufferSize + 1
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("oversized mux receive buffer was accepted")
		}
	})

	t.Run("bracketed ipv6", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Server.BindAddr = "[::1]:3080"
		if err := ValidateConfig(cfg); err != nil {
			t.Fatalf("valid IPv6 endpoint rejected: %v", err)
		}
	})
}

func TestSecurityAndPoolDefaults(t *testing.T) {
	cfg := validServerConfig()
	if cfg.Server.WebBindAddr != "127.0.0.1" {
		t.Fatalf("server web bind default = %q, want loopback", cfg.Server.WebBindAddr)
	}
	if cfg.Client.WebBindAddr != "127.0.0.1" {
		t.Fatalf("client web bind default = %q, want loopback", cfg.Client.WebBindAddr)
	}
	if cfg.Client.MaxPoolSize < cfg.Client.ConnectionPool || cfg.Client.MaxPoolSize <= 0 {
		t.Fatalf("invalid adaptive pool default: base=%d max=%d", cfg.Client.ConnectionPool, cfg.Client.MaxPoolSize)
	}
}

func TestValidateServerPortConflicts(t *testing.T) {
	tests := []struct {
		name    string
		bind    string
		webPort int
		webHost string
		ports   []string
		wantErr bool
	}{
		{name: "duplicate wildcard ports", bind: "0.0.0.0:3080", ports: []string{"443", "443=8443"}, wantErr: true},
		{name: "overlapping ranges", bind: "0.0.0.0:3080", ports: []string{"443-500", "500-600=700"}, wantErr: true},
		{name: "mapping collides with control listener", bind: "0.0.0.0:3080", ports: []string{"3080"}, wantErr: true},
		{name: "mapping collides with monitor", bind: "0.0.0.0:3080", webPort: 443, webHost: "127.0.0.1", ports: []string{"443"}, wantErr: true},
		{name: "monitor collides with wildcard control bind", bind: "0.0.0.0:2060", webPort: 2060, webHost: "127.0.0.1", wantErr: true},
		{name: "distinct explicit IPs may share a port", bind: "127.0.0.1:3080", ports: []string{"127.0.0.2:443=8443", "127.0.0.3:443=9443"}},
		{name: "non-overlapping port ranges", bind: "0.0.0.0:3080", ports: []string{"443-450=8443", "500-510=9443"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.ServerConfig{BindAddr: tt.bind, Ports: tt.ports, WebPort: tt.webPort, WebBindAddr: tt.webHost}
			err := validateServerPortConflicts(cfg)
			if tt.wantErr && err == nil {
				t.Fatal("expected local listener collision to be rejected")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("non-conflicting listeners rejected: %v", err)
			}
		})
	}
}

func TestValidateServerTransportOptions(t *testing.T) {
	tests := []struct {
		name          string
		transport     config.TransportType
		acceptUDP     bool
		proxyProtocol bool
		wantErr       bool
	}{
		{name: "tcp supports both options", transport: config.TCP, acceptUDP: true, proxyProtocol: true},
		{name: "tcpmux supports proxy protocol", transport: config.TCPMUX, proxyProtocol: true},
		{name: "wsmux supports proxy protocol", transport: config.WSMUX, proxyProtocol: true},
		{name: "wssmux supports proxy protocol", transport: config.WSSMUX, proxyProtocol: true},
		{name: "udp rejects accept udp", transport: config.UDP, acceptUDP: true, wantErr: true},
		{name: "tcpmux rejects accept udp", transport: config.TCPMUX, acceptUDP: true, wantErr: true},
		{name: "ws rejects proxy protocol", transport: config.WS, proxyProtocol: true, wantErr: true},
		{name: "wss rejects proxy protocol", transport: config.WSS, proxyProtocol: true, wantErr: true},
		{name: "udp rejects proxy protocol", transport: config.UDP, proxyProtocol: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServerTransportOptions(tt.transport, tt.acceptUDP, tt.proxyProtocol)
			if tt.wantErr && err == nil {
				t.Fatal("expected unsupported option combination to fail")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("supported option combination rejected: %v", err)
			}
		})
	}
}

func TestValidateEndpointHostnameAndWhitespace(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		value   string
		wantErr bool
	}{
		{name: "valid DNS endpoint", field: "client.remote_addr", value: "server-node.example.com:443"},
		{name: "valid wildcard bind", field: "server.bind_addr", value: ":3080"},
		{name: "empty remote host", field: "client.remote_addr", value: ":443", wantErr: true},
		{name: "invalid hostname whitespace", field: "client.remote_addr", value: "bad host:443", wantErr: true},
		{name: "surrounding whitespace", field: "client.remote_addr", value: " server.example.com:443 ", wantErr: true},
		{name: "empty hostname label", field: "client.remote_addr", value: "bad..example:443", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEndpoint(tt.field, tt.value)
			if tt.wantErr && err == nil {
				t.Fatal("invalid endpoint was accepted")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("valid endpoint rejected: %v", err)
			}
		})
	}
}

func TestValidateLocalMappingHyphenatedHostname(t *testing.T) {
	if err := validateLocalMappingAddress("my-local-host.example.com:443"); err != nil {
		t.Fatalf("hyphenated local hostname was interpreted as a port range: %v", err)
	}
}

func TestValidateDurationBounds(t *testing.T) {
	tests := []struct {
		name   string
		server bool
		field  string
		set    func(*config.Config, int)
	}{
		{name: "server keepalive upper bound", server: true, field: "server.keepalive_period", set: func(c *config.Config, v int) { c.Server.Keepalive = v }},
		{name: "server heartbeat upper bound", server: true, field: "server.heartbeat", set: func(c *config.Config, v int) { c.Server.Heartbeat = v }},
		{name: "client keepalive upper bound", field: "client.keepalive_period", set: func(c *config.Config, v int) { c.Client.Keepalive = v }},
		{name: "client retry upper bound", field: "client.retry_interval", set: func(c *config.Config, v int) { c.Client.RetryInterval = v }},
		{name: "client dial timeout upper bound", field: "client.dial_timeout", set: func(c *config.Config, v int) { c.Client.DialTimeout = v }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg *config.Config
			if tt.server {
				cfg = validServerConfig()
			} else {
				cfg = &config.Config{Client: config.ClientConfig{
					RemoteAddr: "server.example.com:3080",
					Transport:  config.TCP,
					Token:      "test-token",
				}}
				applyDefaults(cfg)
			}
			tt.set(cfg, maxIntervalSeconds+1)
			if err := ValidateConfig(cfg); err == nil || !strings.Contains(err.Error(), tt.field) {
				t.Fatalf("expected upper-bound error for %s, got %v", tt.field, err)
			}
		})
	}
}

func TestValidatePortMapping(t *testing.T) {
	valid := []string{
		"443-600",
		"443",
		"4000=5000",
		"127.0.0.2:443=5201",
		"443=1.1.1.1:5201",
		"127.0.0.2:443=1.1.1.1:5201",
		"[::1]:443=[2001:db8::2]:53",
	}
	for _, mapping := range valid {
		if err := validatePortMapping(mapping); err != nil {
			t.Errorf("valid mapping %q rejected: %v", mapping, err)
		}
	}

	invalid := []string{"", "0", "65536", "600-443", "443=", "a=b=c", "443-600:5201"}
	for _, mapping := range invalid {
		if err := validatePortMapping(mapping); err == nil {
			t.Errorf("invalid mapping %q accepted", mapping)
		}
	}
}


func TestLoadConfigSecurityDefaultsAndUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	secureClient := `[client]
remote_addr = "server.example.com:443"
transport = "wss"
token = "a-long-random-test-token"
`
	if err := os.WriteFile(path, []byte(secureClient), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig with omitted tls_verify: %v", err)
	}
	if !cfg.Client.TLSVerify {
		t.Fatal("omitted client.tls_verify did not default to true")
	}
	if !cfg.Client.SkipOptz {
		t.Fatal("omitted client.skip_optz must disable host-wide TCP tuning by default")
	}

	explicitTuningOptIn := secureClient + "skip_optz = false\n"
	if err := os.WriteFile(path, []byte(explicitTuningOptIn), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig with explicit skip_optz=false: %v", err)
	}
	if cfg.Client.SkipOptz {
		t.Fatal("explicit client.skip_optz=false was not preserved")
	}

	explicitOptOut := secureClient + "tls_verify = false\n"
	if err := os.WriteFile(path, []byte(explicitOptOut), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig with explicit tls_verify=false: %v", err)
	}
	if cfg.Client.TLSVerify {
		t.Fatal("explicit client.tls_verify=false was not preserved")
	}

	unknownKey := secureClient + "tls_verfy = false\n"
	if err := os.WriteFile(path, []byte(unknownKey), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "unknown configuration key") {
		t.Fatalf("unknown/misspelled key was not rejected, error = %v", err)
	}
}

func TestLoadConfigDefaultsSnifferLogBesideConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "client.toml")
	configText := `[client]
remote_addr = "server.example.com:443"
transport = "tcp"
token = "a-long-random-test-token"
`
	if err := os.WriteFile(path, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	want := filepath.Join(dir, defaultSnifferLog)
	if cfg.Client.SnifferLog != want {
		t.Fatalf("default sniffer_log = %q, want %q", cfg.Client.SnifferLog, want)
	}
}

func TestValidateServerTLSFilesPreflight(t *testing.T) {
	cfg := &config.ServerConfig{
		Transport:  config.WSS,
		TLSCertFile: filepath.Join(t.TempDir(), "missing-cert.pem"),
		TLSKeyFile:  filepath.Join(t.TempDir(), "missing-key.pem"),
	}
	if err := validateServerTLSFiles(cfg); err == nil {
		t.Fatal("missing WSS certificate/key passed preflight")
	}
	cfg.Transport = config.TCP
	if err := validateServerTLSFiles(cfg); err != nil {
		t.Fatalf("non-TLS transport unexpectedly required certificate files: %v", err)
	}
}
