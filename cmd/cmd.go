package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/client"

	"github.com/musix/backhaul/internal/server"
	"github.com/musix/backhaul/internal/utils"

	"github.com/BurntSushi/toml"
)

var (
	logger = utils.NewLogger("info")
)

func Run(configPath string, ctx context.Context) error {
	// Load and parse the configuration file
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	return RunConfig(cfg, ctx)
}

// RunConfig runs one validated configuration generation until ctx is canceled.
// Callers can load and validate a replacement before stopping the current
// generation, which makes hot reload fail-safe on malformed configuration.
func RunConfig(cfg *config.Config, ctx context.Context) error {
	if err := ValidateConfig(cfg); err != nil {
		return err
	}

	if cfg.Server.BindAddr != "" {
		if err := validateServerTLSFiles(&cfg.Server); err != nil {
			return err
		}
		warnSecurityDefaults("server", cfg.Server.Token, cfg.Server.WebPort, cfg.Server.WebBindAddr, cfg.Server.WebUsername)
		// Apply temporary TCP optimizations at startup
		if !cfg.Server.SkipOptz {
			ApplyTCPTuning()
		}

		srv := server.NewServer(&cfg.Server, ctx)
		srv.Start()
		logger.Println("shutting down server...")
		return nil
	}

	if cfg.Client.RemoteAddr != "" {
		warnSecurityDefaults("client", cfg.Client.Token, cfg.Client.WebPort, cfg.Client.WebBindAddr, cfg.Client.WebUsername)
		// Apply temporary TCP optimizations at startup
		if !cfg.Client.SkipOptz {
			ApplyTCPTuning()
		}

		clnt := client.NewClient(&cfg.Client, ctx)
		clnt.Start()
		logger.Println("shutting down client...")
		return nil
	}

	return fmt.Errorf("neither server nor client configuration is properly set")
}

func validateServerTLSFiles(cfg *config.ServerConfig) error {
	if cfg.Transport != config.WSS && cfg.Transport != config.WSSMUX {
		return nil
	}
	if _, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
		return fmt.Errorf("server TLS certificate/key could not be loaded or do not match: %w", err)
	}
	return nil
}

func warnSecurityDefaults(role, token string, webPort int, webBindAddr, webUsername string) {
	if token == defaultToken {
		logger.Warnf("%s is using the default authentication token; configure an explicit token", role)
	}
	if webPort <= 0 || webBindAddr == "127.0.0.1" || webBindAddr == "::1" || webBindAddr == "localhost" {
		return
	}
	if webUsername == "" {
		logger.Warnf("%s web monitor is bound to %s without authentication", role, webBindAddr)
	} else {
		logger.Warnf("%s web monitor uses HTTP Basic authentication without transport encryption; use a TLS reverse proxy or SSH tunnel on untrusted networks", role)
	}
}

// LoadConfig loads, defaults, and validates a TOML configuration file.
func LoadConfig(configPath string) (*config.Config, error) {
	var cfg config.Config
	metadata, err := toml.DecodeFile(configPath, &cfg)
	if err != nil {
		return &cfg, err
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return &cfg, fmt.Errorf("unknown configuration key(s): %v; check spelling and Backhaul version compatibility", undecoded)
	}
	// Preserve an explicit false, but make omitted WSS verification secure by default.
	if !metadata.IsDefined("client", "tls_verify") {
		cfg.Client.TLSVerify = true
	}
	// ApplyTCPTuning mutates host-wide sysctls; require an explicit opt-in.
	if !metadata.IsDefined("server", "skip_optz") {
		cfg.Server.SkipOptz = true
	}
	if !metadata.IsDefined("client", "skip_optz") {
		cfg.Client.SkipOptz = true
	}
	applyDefaults(&cfg)
	if err := ValidateConfig(&cfg); err != nil {
		return &cfg, err
	}

	if info, err := os.Stat(configPath); err == nil && info.Mode().Perm()&0o077 != 0 {
		logger.Warnf("configuration file %s is readable by group or others; restrict permissions because it contains authentication secrets", configPath)
	}
	return &cfg, nil
}
