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
		transferData(from, to, logger, usage, remotePort, sniffer)
		closeBoth()
		done <- struct{}{}
	}()
	transferData(to, from, logger, usage, remotePort, sniffer)
	closeBoth()
	<-done
}

// Using direct Read and Write for transferring data
func transferData(from net.Conn, to net.Conn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool) {
	buf := make([]byte, 16*1024) // 16K
	for {
		// Read data from the source connection
		r, err := from.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				logger.Trace("reader stream closed or EOF received")
			} else {
				logger.Trace("unable to read from the connection: ", err)
			}
			return
		}

		totalWritten := 0
		for totalWritten < r {
			// Write data to the destination connection
			w, err := to.Write(buf[totalWritten:r])
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					logger.Trace("writer stream closed or EOF received")
				} else {
					logger.Trace("unable to write to the connection: ", err)
				}
				return

			}
			totalWritten += w
		}

		logger.Tracef("read data: %d bytes, written data: %d bytes", r, totalWritten)
		if sniffer {
			usage.AddOrUpdatePort(remotePort, uint64(totalWritten))
		}
	}

}
