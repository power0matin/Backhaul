package cmd

import (
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
