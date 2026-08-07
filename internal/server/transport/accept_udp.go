package transport

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

const BufferSize = 16 * 1024

func (s *TcpTransport) udpListener(localAddr string, remoteAddr string) {
	localUDPAddr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		s.logger.Errorf("failed to resolve local UDP forwarding address: %v", err)
		return
	}

	listener, err := net.ListenUDP("udp", localUDPAddr)
	if err != nil {
		s.logger.Errorf("failed to listen on local UDP forwarding port: %v", err)
		return
	}

	defer listener.Close()

	s.logger.Infof("UDP listener started successfully, listening on address: %s", listener.LocalAddr().String())

	// Track active connections
	activeConnections := map[string]*LocalAcceptUDPConn{}

	// Buffer for UDP reads
	buf := make([]byte, BufferSize-2) // 2 bytes reserved for header

	// make a new channel for recieve udp packets
	udpChan := make(chan *LocalAcceptUDPConn, s.config.ChannelSize)

	//mutex
	mu := &sync.Mutex{}

	// handle channel
	go s.handleUDPLoop(udpChan, &activeConnections, mu)

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				n, addr, err := listener.ReadFromUDP(buf)
				if err != nil {
					if s.ctx.Err() != nil {
						return
					}
					s.logger.Errorf("failed to read from UDP listener: %v", err)
					continue
				}

				// Create a unique identifier for the connection based on IP and port
				key := addr.String()

				mu.Lock()
				// Check if the connection is already active
				if existingConn, exists := activeConnections[key]; exists {
					if existingConn.isCongested.Load() {
						s.logger.Debugf("connection with timestamp %d congested. Removing %s from active connections due to network congestion", existingConn.timeCreated, addr.String())
						// For congested connections, closing the payload channel immediately can cause abrupt TCP disconnection,
						// potentially leading to data loss. Instead, allow the connection to keep transferring data for 30 more
						// seconds (or until the payload channel becomes idle). The timer will close the TCP connection once it
						// times out. Further testing is needed to confirm this strategy's effect on overall performance and congestion handling.

						delete(activeConnections, key)
					} else {
						// If it exists, send the payload to the existing connection's payload channel
						if s.udpBudget.enqueueFramedUDP(existingConn.payload, buf[:n]) {
							s.logger.Tracef("buffered %d bytes for existing connection %s", n, addr.String())
						} else {
							recordRejected(s.usageMonitor, s.logger, "UDP per-flow queue full; dropping packet")
						}
						mu.Unlock()
						continue
					}
				}

				if len(activeConnections) >= udpMaxFlows(s.config.UDPMaxFlows) {
					mu.Unlock()
					recordRejected(s.usageMonitor, s.logger, "maximum UDP flow limit reached; dropping new flow")
					continue
				}
				mu.Unlock()

				payloadChan := make(chan []byte, udpQueueSize(s.config.UDPQueueSize))
				if !s.udpBudget.enqueueFramedUDP(payloadChan, buf[:n]) {
					recordRejected(s.usageMonitor, s.logger, "global UDP queue budget exhausted; dropping new flow")
					continue
				}

				// build the UDP packet
				newUDPConn := LocalAcceptUDPConn{
					timeCreated: time.Now().UnixMilli(),
					payload:     payloadChan,
					remoteAddr:  remoteAddr,
					listener:    listener,
					clientAddr:  addr,
				}

				mu.Lock()
				// store the connection info
				activeConnections[key] = &newUDPConn
				mu.Unlock()

				select {
				case udpChan <- &newUDPConn:
					s.logger.Debugf("accepted UDP connection from %s", addr.String())

					select {
					case s.reqNewConnChan <- struct{}{}: // Successfully requested a new tcp connection
					default: // The channel is full, do nothing
						recordRejected(s.usageMonitor, s.logger, "new tunnel request queue full; dropping request")
					}

				default:
					recordRejected(s.usageMonitor, s.logger, "UDP flow queue full; dropping new flow")
					mu.Lock()
					if current, ok := activeConnections[key]; ok && current == &newUDPConn {
						delete(activeConnections, key)
						s.udpBudget.closeQueue(newUDPConn.payload)
					}
					mu.Unlock()
				}
			}
		}
	}()

	<-s.ctx.Done()
}

func (s *TcpTransport) handleUDPLoop(udpChan chan *LocalAcceptUDPConn, activeConnections *map[string]*LocalAcceptUDPConn, mu *sync.Mutex) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case localConn := <-udpChan:
			wait := 3*time.Second - time.Duration(time.Now().UnixMilli()-localConn.timeCreated)*time.Millisecond
			if wait <= 0 {
				s.removeAcceptUDPFlow(localConn, activeConnections, mu)
				continue
			}
			timer := time.NewTimer(wait)
		loop:
			for {
				select {
				case <-s.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
					s.logger.Debugf("timed out queued UDP flow from %s", localConn.clientAddr.String())
					s.removeAcceptUDPFlow(localConn, activeConnections, mu)
					break loop

				case tunnelConn := <-s.tunnelChannel:
					timer.Stop()
					// Send the target addr over the connection
					if err := utils.SendBinaryTransportString(tunnelConn, localConn.remoteAddr, utils.SG_UDP); err != nil {
						s.logger.Errorf("%v", err)
						tunnelConn.Close()
						continue loop
					}

					// Handle data exchange between connections
					go UDPConnectionHandler(s.ctx, localConn, tunnelConn, s.logger, s.usageMonitor, localConn.listener.LocalAddr().(*net.UDPAddr).Port, s.config.Sniffer, s.rtt.Load(), activeConnections, mu, s.udpBudget)

					s.logger.Debugf("initiate new handler for connection %s with timestamp %d", localConn.clientAddr.String(), localConn.timeCreated)
					break loop
				}
			}
		}
	}
}

func (s *TcpTransport) removeAcceptUDPFlow(udp *LocalAcceptUDPConn, activeConnections *map[string]*LocalAcceptUDPConn, mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
	key := udp.clientAddr.String()
	if current, ok := (*activeConnections)[key]; ok && current == udp {
		delete(*activeConnections, key)
		s.udpBudget.closeQueue(udp.payload)
	}
}

func UDPConnectionHandler(ctx context.Context, udp *LocalAcceptUDPConn, tcp net.Conn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool, rtt int64, activeConnections *map[string]*LocalAcceptUDPConn, mu *sync.Mutex, budget *packetBudget) {
	usage.ConnectionOpened()
	defer usage.ConnectionClosed()
	done := make(chan struct{})
	flowCtx, cancelFlow := context.WithCancel(ctx)
	defer cancelFlow()
	stopClose := context.AfterFunc(ctx, func() { _ = tcp.Close() })
	defer stopClose()

	if rtt == 0 {
		// RTT of 0 indicates that either the backhaul is running in a local environment
		// (with negligible latency), or RTT measurement failed.
		// Set a default RTT of 100ms to ensure proper functioning of TCP congestion control.
		rtt = 100
	}

	go func() {
		udpToTCP(flowCtx, tcp, udp, logger, usage, remotePort, sniffer, budget)
		tcp.Close()
		done <- struct{}{}
	}()

	tcpToUDP(tcp, udp, logger, usage, remotePort, sniffer, rtt)
	cancelFlow()
	tcp.Close()

	<-done

	mu.Lock()
	budget.closeQueue(udp.payload)
	key := udp.clientAddr.String()
	// A congested flow can be replaced by the listener before this handler
	// finishes. Remove our entry when it is still current, but never delete a
	// newer replacement for the same source address.
	if current, ok := (*activeConnections)[key]; ok && current == udp {
		delete(*activeConnections, key)
	}
	mu.Unlock()
}

func udpToTCP(ctx context.Context, tcp net.Conn, udp *LocalAcceptUDPConn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool, budget *packetBudget) {
	inactivityTimeout := 60 * time.Second // Define a 60-second inactivity timeout
	idleTimer := time.NewTimer(inactivityTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case packet, ok := <-udp.payload: // Wait for data on the UDP payload channel
			if !ok {
				return
			}
			budget.release()

			if len(packet) < 2 {
				logger.Error("invalid internally framed UDP packet")
				return
			}
			packetSize := len(packet) - 2

			totalWritten := 0
			for totalWritten < len(packet) { // Use the total packet length (header + data)
				w, err := tcp.Write(packet[totalWritten:])
				if err != nil {
					logger.Errorf("failed to write UDP payload to TCP: %v", err)
					return
				}
				totalWritten += w
			}

			logger.Tracef("received %d bytes, forwarded %d bytes from UDP to TCP", packetSize, totalWritten-2)

			if sniffer {
				usage.AddOrUpdatePort(remotePort, uint64(totalWritten))
			}
			resetTimer(idleTimer, inactivityTimeout)

		case <-idleTimer.C:
			logger.Debugf("connection with timestamp %d and address %s idle for 60 seconds, closing", udp.timeCreated, udp.clientAddr.String())
			return
		}
	}
}

func tcpToUDP(tcp net.Conn, udp *LocalAcceptUDPConn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool, rtt int64) {
	buf := make([]byte, BufferSize)
	lenBuf := make([]byte, 2)       // Buffer to store the 2-byte packet length
	timestampBuf := make([]byte, 4) // Buffer for timestamp (4 bytes)

	for {
		// First, read the 4-byte timestamp from the packet
		_, err := io.ReadFull(tcp, timestampBuf)
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP connection closed.")
			} else {
				logger.Debugf("failed to read timestamp from TCP connection: %v", err)
			}
			return
		}

		// 4-byte timestamp header
		packetTimestamp := int64(binary.BigEndian.Uint32(timestampBuf))

		// Get the current time and calculate the time difference
		timestamp := time.Now().UnixMilli()
		lastMillis := timestamp % (10 * 60 * 1000)

		packetAge := lastMillis - packetTimestamp

		// If the packet age exceeds the threshold (3x RTT), flag the connection as congested
		if packetAge > 3*rtt {
			udp.isCongested.Store(true)
		}

		// Read the 2-byte packet length header from the TCP connection
		_, err = io.ReadFull(tcp, lenBuf)
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP connection closed.")
			} else {
				logger.Errorf("failed to read packet length from TCP connection: %v", err)
			}
			return
		}

		// Convert the 2-byte length header into an integer
		packetSize := int(binary.BigEndian.Uint16(lenBuf))

		// Check if the packet size is valid
		if packetSize > len(buf) {
			logger.Errorf("packet size exceeds buffer size: %d bytes", packetSize)
			return
		}

		// Now use io.ReadFull to read the actual packet data from TCP based on the packetSize
		_, err = io.ReadFull(tcp, buf[:packetSize])
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP connection closed.")
			} else {
				logger.Errorf("failed to read from TCP connection: %v", err)
			}
			return
		}

		// Forward the data to the UDP client address
		if udp.clientAddr != nil {
			totalWritten := 0
			for totalWritten < packetSize {
				w, err := udp.listener.WriteToUDP(buf[totalWritten:packetSize], udp.clientAddr)
				if err != nil {
					logger.Errorf("failed to forward TCP response to UDP client: %v", err)
					return
				}

				totalWritten += w
			}

			if sniffer {
				usage.AddOrUpdatePort(remotePort, uint64(totalWritten))
			}

			logger.Tracef("read %d bytes from TCP, forwarded %d bytes to UDP", packetSize, totalWritten)
		}
	}
}
