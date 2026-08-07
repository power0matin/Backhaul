package cmd

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/musix/backhaul/config"
)

const (
	maxChannelSize     = 1_000_000
	maxUDPQueueSize    = 4096
	maxUDPQueueLimit   = 16_384
	maxUDPFlows        = 65_536
	maxUDPQueueSlots   = 1_000_000
	maxConnectionPool  = 100_000
	maxMuxConcurrency  = 4096
	maxAuthTokenLength = 65_535
)

// ValidateConfig rejects values that would otherwise fail late in goroutines,
// overflow the wire framing, or permit accidental unbounded/very large startup
// allocations. Defaults should be applied before calling it.
func ValidateConfig(cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("configuration is nil")
	}

	hasServer := strings.TrimSpace(cfg.Server.BindAddr) != ""
	hasClient := strings.TrimSpace(cfg.Client.RemoteAddr) != ""
	if hasServer == hasClient {
		return fmt.Errorf("configure exactly one of server.bind_addr or client.remote_addr")
	}

	if hasServer {
		if err := validateTransport("server.transport", cfg.Server.Transport); err != nil {
			return err
		}
		if err := validateEndpoint("server.bind_addr", cfg.Server.BindAddr); err != nil {
			return err
		}
		if len(cfg.Server.Token) > maxAuthTokenLength {
			return fmt.Errorf("server.token is %d bytes; maximum is %d for protocol framing", len(cfg.Server.Token), maxAuthTokenLength)
		}
		if cfg.Server.ChannelSize < 1 || cfg.Server.ChannelSize > maxChannelSize {
			return fmt.Errorf("server.channel_size must be between 1 and %d", maxChannelSize)
		}
		if cfg.Server.MuxCon < 1 || cfg.Server.MuxCon > maxMuxConcurrency {
			return fmt.Errorf("server.mux_con must be between 1 and %d", maxMuxConcurrency)
		}
		if err := validateMux("server", cfg.Server.Transport, cfg.Server.MuxVersion, cfg.Server.MaxFrameSize, cfg.Server.MaxReceiveBuffer, cfg.Server.MaxStreamBuffer); err != nil {
			return err
		}
		if err := validateWeb("server", cfg.Server.WebPort, cfg.Server.WebBindAddr, cfg.Server.WebUsername, cfg.Server.WebPassword); err != nil {
			return err
		}
		if cfg.Server.Transport == config.WSS || cfg.Server.Transport == config.WSSMUX {
			if strings.TrimSpace(cfg.Server.TLSCertFile) == "" || strings.TrimSpace(cfg.Server.TLSKeyFile) == "" {
				return fmt.Errorf("server.tls_cert and server.tls_key are required for %s", cfg.Server.Transport)
			}
		}
		if cfg.Server.UDPQueueSize < 1 || cfg.Server.UDPQueueSize > maxUDPQueueSize {
			return fmt.Errorf("server.udp_queue_size must be between 1 and %d", maxUDPQueueSize)
		}
		if cfg.Server.UDPQueueLimit < 1 || cfg.Server.UDPQueueLimit > maxUDPQueueLimit {
			return fmt.Errorf("server.udp_queue_limit must be between 1 and %d", maxUDPQueueLimit)
		}
		if cfg.Server.UDPMaxFlows < 1 || cfg.Server.UDPMaxFlows > maxUDPFlows {
			return fmt.Errorf("server.udp_max_flows must be between 1 and %d", maxUDPFlows)
		}
		if cfg.Server.UDPQueueSize*cfg.Server.UDPMaxFlows > maxUDPQueueSlots {
			return fmt.Errorf("server.udp_queue_size * server.udp_max_flows must not exceed %d", maxUDPQueueSlots)
		}
		for i, mapping := range cfg.Server.Ports {
			if err := validatePortMapping(mapping); err != nil {
				return fmt.Errorf("server.ports[%d] %q: %w", i, mapping, err)
			}
		}
		return nil
	}

	if err := validateTransport("client.transport", cfg.Client.Transport); err != nil {
		return err
	}
	if err := validateEndpoint("client.remote_addr", cfg.Client.RemoteAddr); err != nil {
		return err
	}
	if len(cfg.Client.Token) > maxAuthTokenLength {
		return fmt.Errorf("client.token is %d bytes; maximum is %d for protocol framing", len(cfg.Client.Token), maxAuthTokenLength)
	}
	if cfg.Client.ConnectionPool < 1 || cfg.Client.ConnectionPool > maxConnectionPool {
		return fmt.Errorf("client.connection_pool must be between 1 and %d", maxConnectionPool)
	}
	if cfg.Client.MaxPoolSize < cfg.Client.ConnectionPool || cfg.Client.MaxPoolSize > maxConnectionPool {
		return fmt.Errorf("client.max_pool_size must be between client.connection_pool (%d) and %d", cfg.Client.ConnectionPool, maxConnectionPool)
	}
	if err := validateMux("client", cfg.Client.Transport, cfg.Client.MuxVersion, cfg.Client.MaxFrameSize, cfg.Client.MaxReceiveBuffer, cfg.Client.MaxStreamBuffer); err != nil {
		return err
	}
	if err := validateWeb("client", cfg.Client.WebPort, cfg.Client.WebBindAddr, cfg.Client.WebUsername, cfg.Client.WebPassword); err != nil {
		return err
	}
	return nil
}

func validateTransport(field string, transport config.TransportType) error {
	switch transport {
	case config.TCP, config.TCPMUX, config.UDP, config.WS, config.WSS, config.WSMUX, config.WSSMUX:
		return nil
	default:
		return fmt.Errorf("%s must be one of tcp, tcpmux, udp, ws, wss, wsmux, or wssmux; got %q", field, transport)
	}
}

func validateEndpoint(field, value string) error {
	_, portText, err := net.SplitHostPort(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%s must be host:port (IPv6 must be bracketed): %w", field, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("%s port must be between 1 and 65535", field)
	}
	return nil
}

func validateWeb(prefix string, port int, bindAddr, username, password string) error {
	if port < 0 || port > 65535 {
		return fmt.Errorf("%s.web_port must be 0 (disabled) or between 1 and 65535", prefix)
	}
	if (username == "") != (password == "") {
		return fmt.Errorf("%s.web_username and %s.web_password must either both be set or both be empty", prefix, prefix)
	}
	if port > 0 && strings.TrimSpace(bindAddr) == "" {
		return fmt.Errorf("%s.web_bind_addr must not be empty when web_port is enabled", prefix)
	}
	if strings.Contains(bindAddr, "[") || strings.Contains(bindAddr, "]") {
		return fmt.Errorf("%s.web_bind_addr must be a host or IP without brackets or a port", prefix)
	}
	if net.ParseIP(bindAddr) == nil && strings.Contains(bindAddr, ":") {
		return fmt.Errorf("%s.web_bind_addr must not include a port", prefix)
	}
	return nil
}

func validatePortMapping(mapping string) error {
	mapping = strings.TrimSpace(mapping)
	if mapping == "" {
		return fmt.Errorf("mapping must not be empty")
	}
	if strings.Count(mapping, "=") > 1 {
		return fmt.Errorf("expected local[=remote] with at most one '='")
	}

	parts := strings.SplitN(mapping, "=", 2)
	local := strings.TrimSpace(parts[0])
	if len(parts) == 1 {
		if err := validatePortOrRange(local); err != nil {
			return fmt.Errorf("local side must be a port or port range: %w", err)
		}
		return nil
	}

	if err := validateLocalMappingAddress(local); err != nil {
		return fmt.Errorf("invalid local side: %w", err)
	}
	remote := strings.TrimSpace(parts[1])
	if remote == "" {
		return fmt.Errorf("remote side must not be empty")
	}
	if !strings.Contains(remote, ":") {
		if _, err := parsePort(remote); err != nil {
			return fmt.Errorf("invalid remote port: %w", err)
		}
		return nil
	}
	return validateEndpoint("remote target", remote)
}

func validateLocalMappingAddress(value string) error {
	if strings.Contains(value, "-") {
		return validatePortOrRange(value)
	}
	if _, err := strconv.Atoi(value); err == nil {
		_, err = parsePort(value)
		return err
	}
	return validateEndpoint("local bind", value)
}

func validatePortOrRange(value string) error {
	if !strings.Contains(value, "-") {
		_, err := parsePort(value)
		return err
	}
	parts := strings.Split(value, "-")
	if len(parts) != 2 {
		return fmt.Errorf("invalid range %q", value)
	}
	start, err := parsePort(strings.TrimSpace(parts[0]))
	if err != nil {
		return err
	}
	end, err := parsePort(strings.TrimSpace(parts[1]))
	if err != nil {
		return err
	}
	if end < start {
		return fmt.Errorf("range end %d is below start %d", end, start)
	}
	return nil
}

func parsePort(value string) (int, error) {
	port, err := strconv.Atoi(value)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("port %q must be between 1 and 65535", value)
	}
	return port, nil
}

func validateMux(prefix string, transport config.TransportType, version, frameSize, receiveBuffer, streamBuffer int) error {
	if transport != config.TCPMUX && transport != config.WSMUX && transport != config.WSSMUX {
		return nil
	}
	if version != 1 && version != 2 {
		return fmt.Errorf("%s.mux_version must be 1 or 2", prefix)
	}
	if frameSize < 1 || frameSize > 65535 {
		return fmt.Errorf("%s.mux_framesize must be between 1 and 65535", prefix)
	}
	if receiveBuffer < 1 {
		return fmt.Errorf("%s.mux_recievebuffer must be positive", prefix)
	}
	if streamBuffer < 1 || streamBuffer > receiveBuffer {
		return fmt.Errorf("%s.mux_streambuffer must be positive and no larger than mux_recievebuffer", prefix)
	}
	return nil
}
