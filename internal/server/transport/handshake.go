package transport

import (
	"net"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/sirupsen/logrus"
)

const (
	maxConcurrentControlHandshakes = 32
	controlHandshakeTimeout = 500 * time.Millisecond
)

func readControlHandshake(conn net.Conn, token string, logger *logrus.Logger) bool {
	if err := conn.SetReadDeadline(time.Now().Add(controlHandshakeTimeout)); err != nil {
		logger.Debugf("failed to set control handshake deadline: %v", err)
		return false
	}
	msg, signal, err := utils.ReceiveBinaryTransportString(conn)
	if err != nil {
		logger.Debugf("rejected incomplete control handshake: %v", err)
		return false
	}
	if signal != utils.SG_Chan {
		logger.Debug("rejected connection with an unexpected control handshake signal")
		return false
	}
	if !utils.SecureTokenEqual(msg, token) {
		logger.Warn("rejected control connection with an invalid token")
		return false
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		logger.Debugf("failed to clear control handshake deadline: %v", err)
		return false
	}
	return true
}

func writeControlHandshakeResponse(conn net.Conn, token string, logger *logrus.Logger) bool {
	if err := utils.SendBinaryTransportString(conn, token, utils.SG_Chan); err != nil {
		logger.Debugf("failed to send control handshake response: %v", err)
		return false
	}
	return true
}
