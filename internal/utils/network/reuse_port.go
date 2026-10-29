package network

import (
	"fmt"
	"syscall"
)

// SocketControl configures only SO_REUSEADDR. SO_REUSEPORT is intentionally
// disabled: without an explicit configuration option it lets independent
// Backhaul processes bind the same listener and splits new connections between
// separate control/session state.
func SocketControl(network, address string, s syscall.RawConn) error {
	var controlErr error
	err := s.Control(func(fd uintptr) {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
			controlErr = fmt.Errorf("failed to set SO_REUSEADDR: %v", err)
		}
	})
	if err != nil {
		return err
	}
	return controlErr
}
