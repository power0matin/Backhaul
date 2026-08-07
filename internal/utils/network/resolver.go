package network

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func ResolveRemoteAddr(remoteAddr string) (int, string, error) {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if remoteAddr == "" {
		return 0, "", fmt.Errorf("empty remote address")
	}

	// A port-only mapping remains wire-compatible with v0.7.2 and resolves to
	// loopback on the client side.
	if !strings.Contains(remoteAddr, ":") {
		port, err := strconv.Atoi(remoteAddr)
		if err != nil {
			return 0, "", fmt.Errorf("invalid port %q: %w", remoteAddr, err)
		}
		if port < 1 || port > 65535 {
			return 0, "", fmt.Errorf("port %d out of range (1-65535)", port)
		}
		return port, fmt.Sprintf("127.0.0.1:%d", port), nil
	}

	// net.SplitHostPort correctly handles hostnames, IPv4, and bracketed IPv6.
	// It also rejects ambiguous unbracketed IPv6 instead of silently parsing the
	// wrong component as a port.
	_, portString, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return 0, "", fmt.Errorf("invalid remote address %q: %w", remoteAddr, err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		return 0, "", fmt.Errorf("invalid port %q: %w", portString, err)
	}
	if port < 1 || port > 65535 {
		return 0, "", fmt.Errorf("port %d out of range (1-65535)", port)
	}

	return port, remoteAddr, nil
}
