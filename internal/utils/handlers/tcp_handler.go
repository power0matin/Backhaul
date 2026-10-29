package handlers

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

func TCPConnectionHandler(ctx context.Context, proxyProtocol bool, from net.Conn, to net.Conn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool) {
	if usage != nil {
		usage.ConnectionOpened()
		defer usage.ConnectionClosed()
	}
	// Write Proxy Protocol V2 Header
	if proxyProtocol {
		err := WriteProxyProtocol(from, to)
		if err != nil {
			logger.Error(err)
			from.Close()
			to.Close()
			return
		}
	}

	// Closing both endpoints from the cancellation callback interrupts a blocking
	// Read without requiring an extra goroutine for the second copy direction.
	// Either copy direction ending also closes the pair so its peer cannot remain
	// blocked indefinitely on a half-dead tunnel.
	closeBoth := func() {
		_ = from.Close()
		_ = to.Close()
	}
	stopCancel := context.AfterFunc(ctx, closeBoth)
	defer stopCancel()

	done := make(chan struct{}, 1)
	go func() {
		if transferData(from, to, logger, usage, remotePort, sniffer) {
			halfCloseWrite(to, logger)
		} else {
			closeBoth()
		}
		done <- struct{}{}
	}()
	if transferData(to, from, logger, usage, remotePort, sniffer) {
		halfCloseWrite(from, logger)
	} else {
		closeBoth()
	}
	<-done
	closeBoth()
}

// halfCloseWrite propagates EOF without dropping the reverse direction when the
// connection type supports TCP half-close. Multiplexed streams at the pinned
// smux version do not expose CloseWrite; those are closed fully as a fallback.
func halfCloseWrite(conn net.Conn, logger *logrus.Logger) {
	if closeWriter, ok := conn.(interface{ CloseWrite() error }); ok {
		if err := closeWriter.CloseWrite(); err != nil {
			logger.Trace("unable to half-close write side: ", err)
			_ = conn.Close()
		}
		return
	}
	_ = conn.Close()
}

// Using direct Read and Write for transferring data. The return value is true
// only for a clean EOF; other read/write failures must close both endpoints.
func transferData(from net.Conn, to net.Conn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool) bool {
	buf := make([]byte, 16*1024) // 16K
	for {
		r, err := from.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				logger.Trace("reader stream reached EOF")
				return true
			}
			if errors.Is(err, net.ErrClosed) {
				logger.Trace("reader stream closed")
			} else {
				logger.Trace("unable to read from the connection: ", err)
			}
			return false
		}

		totalWritten := 0
		for totalWritten < r {
			w, err := to.Write(buf[totalWritten:r])
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					logger.Trace("writer stream closed")
				} else {
					logger.Trace("unable to write to the connection: ", err)
				}
				return false
			}
			if w == 0 {
				logger.Trace("writer returned zero bytes without an error")
				return false
			}
			totalWritten += w
		}

		logger.Tracef("read data: %d bytes, written data: %d bytes", r, totalWritten)
		if sniffer && usage != nil {
			usage.AddOrUpdatePort(remotePort, uint64(totalWritten))
		}
	}
}
