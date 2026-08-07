package transport

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

type UdpTransport struct {
	config            *UdpConfig
	parentctx         context.Context
	ctx               context.Context
	cancel            context.CancelFunc
	logger            *logrus.Logger
	tunnelChannel     chan *TunnelUDPConn
	activeConnections map[string]*TunnelUDPConn
	activeMu          sync.Mutex
	reqNewConnChan    chan struct{}
	controlChannel    net.Conn
	restartOnce       sync.Once
	usageMonitor      *web.Usage
	rtt               int64 // for Fun!
	udpBudget         *packetBudget
}

type UdpConfig struct {
	BindAddr      string
	Token         string
	SnifferLog    string
	WebBindAddr   string
	WebUsername   string
	WebPassword   string
	Ports         []string
	Sniffer       bool
	Heartbeat     time.Duration // in seconds, for udp conn and control channel
	ChannelSize   int
	WebPort       int
	UDPQueueSize  int
	UDPQueueLimit int
	UDPMaxFlows   int
}

func NewUDPServer(parentCtx context.Context, config *UdpConfig, logger *logrus.Logger) *UdpTransport {
	// Create a derived context from the parent context
	ctx, cancel := context.WithCancel(parentCtx)

	// Initialize the TcpTransport struct
	server := &UdpTransport{
		config:            config,
		parentctx:         parentCtx,
		ctx:               ctx,
		cancel:            cancel,
		logger:            logger,
		tunnelChannel:     make(chan *TunnelUDPConn, config.ChannelSize),
		activeConnections: map[string]*TunnelUDPConn{},
		activeMu:          sync.Mutex{},
		reqNewConnChan:    make(chan struct{}, config.ChannelSize),
		controlChannel:    nil, // will be set when a control connection is established
		usageMonitor:      web.NewDataStore(config.WebBindAddr, config.WebPort, ctx, config.SnifferLog, config.Sniffer, "Disconnected (UDP)", config.WebUsername, config.WebPassword, logger),
		rtt:               0,
		udpBudget:         newPacketBudget(config.UDPQueueLimit),
	}

	return server
}
func (s *UdpTransport) Start() {
	s.usageMonitor.SetTunnelStatus("Disconnected (UDP)")

	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}

	go s.channelHandshake()
}

func (s *UdpTransport) Restart() {
	s.restartOnce.Do(func() {
		if s.parentctx.Err() != nil {
			return
		}
		s.usageMonitor.RecordReconnect()
		s.logger.Info("restarting server...")
		s.cancel()
		if s.controlChannel != nil {
			_ = s.controlChannel.Close()
		}
		if !utils.WaitForDelay(s.parentctx, 2*time.Second) {
			return
		}
		next := NewUDPServer(s.parentctx, s.config, s.logger)
		next.usageMonitor.InheritRuntimeMetrics(s.usageMonitor)
		go next.Start()
	})
}

func (s *UdpTransport) channelHandshake() {
	listener, err := net.Listen("tcp", s.config.BindAddr)
	if err != nil {
		s.logger.Errorf("failed to start UDP control listener on %s: %v", s.config.BindAddr, err)
		go s.Restart()
		return
	}

	s.logger.Infof("server started successfully, listening on address: %s", listener.Addr().String())

	defer listener.Close()
	stopClose := context.AfterFunc(s.ctx, func() { _ = listener.Close() })
	defer stopClose()

loop:
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				if s.ctx.Err() != nil {
					return
				}
				s.logger.Debugf("failed to accept control channel connection on %s: %v", listener.Addr().String(), err)
				continue
			}

			// Set a read deadline for the token response
			if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				s.logger.Errorf("failed to set read deadline: %v", err)
				conn.Close()
				continue
			}

			msg, transport, err := utils.ReceiveBinaryTransportString(conn)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					s.logger.Warn("timeout while waiting for control channel signal")
				} else {
					s.logger.Errorf("failed to receive control channel signal: %v", err)
				}
				conn.Close() // Close connection on error or timeout
				continue
			}
			if transport != utils.SG_Chan {
				s.logger.Errorf("invalid signal received for channel, discarding connection")
				conn.Close()
				continue
			}

			// Resetting the deadline (removes any existing deadline)
			conn.SetReadDeadline(time.Time{})

			if !utils.SecureTokenEqual(msg, s.config.Token) {
				s.logger.Warn("invalid security token received")
				conn.Close()
				continue
			}

			err = utils.SendBinaryTransportString(conn, s.config.Token, utils.SG_Chan)
			if err != nil {
				s.logger.Errorf("failed to send security token: %v", err)
				conn.Close()
				continue
			}

			s.controlChannel = conn
			s.usageMonitor.SetTunnelStatus("Connected (UDP)")

			s.logger.Info("control channel successfully established.")

			break loop
		}
	}

	go s.tunnelListener()
	go s.parsePortMappings()
	go s.channelHandler()

	<-s.ctx.Done()
}

func (s *UdpTransport) channelHandler() {
	ticker := time.NewTicker(s.config.Heartbeat)
	defer ticker.Stop()
	control := s.controlChannel
	defer control.Close()
	stopControlClose := context.AfterFunc(s.ctx, func() { _ = control.Close() })
	defer stopControlClose()

	// Channel to receive the message or error
	messageChan := make(chan byte, 1)

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				message, err := utils.ReceiveBinaryByte(control)
				if err != nil {
					if s.ctx.Err() == nil {
						s.logger.Warn("control channel closed; reconnecting: ", err)
						go s.Restart()
					}
					return
				}
				select {
				case messageChan <- message:
				case <-s.ctx.Done():
					return
				}
			}
		}
	}()

	// RTT measurment
	rtt := time.Now()
	err := utils.SendBinaryByte(control, utils.SG_RTT)
	if err != nil {
		s.logger.Error("failed to send RTT signal, attempting to restart server...")
		go s.Restart()
		return
	}

	for {
		select {
		case <-s.ctx.Done():
			_ = utils.SendBinaryByte(control, utils.SG_Closed)
			return

		case <-s.reqNewConnChan:
			err := utils.SendBinaryByte(control, utils.SG_Chan)
			if err != nil {
				s.logger.Error("failed to send request new connection signal. ", err)
				go s.Restart()
				return
			}

		case <-ticker.C:
			err := utils.SendBinaryByte(control, utils.SG_HB)
			if err != nil {
				s.logger.Error("failed to send heartbeat signal")
				go s.Restart()
				return
			}
			s.logger.Trace("heartbeat signal sent successfully")

		case message, ok := <-messageChan:
			if !ok {
				s.logger.Error("channel closed, likely due to an error in TCP read")
				return
			}

			if message == utils.SG_Closed {
				s.logger.Warn("control channel has been closed by the client")
				go s.Restart()
				return

			} else if message == utils.SG_RTT {
				measureRTT := time.Since(rtt)
				s.rtt = measureRTT.Milliseconds()
				s.logger.Infof("Round Trip Time (RTT): %d ms", s.rtt)
			}
		}
	}
}

func (s *UdpTransport) tunnelListener() {
	tunnelUDPAddr, err := net.ResolveUDPAddr("udp", s.config.BindAddr)
	if err != nil {
		s.logger.Errorf("failed to resolve UDP tunnel address: %v", err)
		go s.Restart()
		return
	}

	listener, err := net.ListenUDP("udp", tunnelUDPAddr)
	if err != nil {
		s.logger.Errorf("failed to listen on UDP tunnel port: %v", err)
		go s.Restart()
		return
	}

	defer listener.Close()

	s.logger.Infof("UDP tunnel listener started successfully, listening on address: %s", listener.LocalAddr().String())

	go s.acceptTunnelConn(listener)

	<-s.ctx.Done()
}

func (s *UdpTransport) acceptTunnelConn(listener *net.UDPConn) {
	// Buffer for UDP reads
	buf := make([]byte, 16*1024)

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
				s.logger.Errorf("failed to read from tunnel UDP listener: %v", err)
				continue
			}

			// Create a unique identifier for the connection based on IP and port
			key := addr.String()

			s.activeMu.Lock()
			// Check if the connection is already active
			if existingConn, exists := s.activeConnections[key]; exists {
				// Send the payload to the existing connection's payload channel
				if s.udpBudget.enqueue(existingConn.payload, buf[:n]) {
					s.logger.Tracef("buffered %d bytes for existing connection %s", n, addr.String())
				} else {
					recordRejected(s.usageMonitor, s.logger, "UDP tunnel per-flow queue full; dropping packet")
				}
				s.activeMu.Unlock()
				continue
			}

			s.activeMu.Unlock()

			if !utils.SecureTokenEqual(string(buf[:n]), s.config.Token) { // For new connections, validate the token
				s.logger.Warnf("invalid UDP authentication from %s", addr.String())
				continue
			}

			s.activeMu.Lock()
			if len(s.activeConnections) >= udpMaxFlows(s.config.UDPMaxFlows) {
				s.activeMu.Unlock()
				s.logger.Warn("maximum UDP tunnel flow limit reached")
				continue
			}
			s.activeMu.Unlock()

			payloadChan := make(chan []byte, udpQueueSize(s.config.UDPQueueSize))

			// Create a new TunnelUDPConn
			tunnelConn := TunnelUDPConn{
				timeCreated: time.Now().UnixNano(), // Just for debugging
				payload:     payloadChan,
				addr:        addr,
				listener:    listener,
				ping:        make(chan struct{}, 1), // Initialize the ping channel
				mu:          &sync.Mutex{},
			}

			s.activeMu.Lock()
			// Add the new connection to the active connections map
			s.activeConnections[key] = &tunnelConn
			s.activeMu.Unlock()

			// Send the new tunnel connection to the tunnel channel
			select {
			case s.tunnelChannel <- &tunnelConn:
				go s.keepAlive(&tunnelConn)
				s.logger.Debugf("accepted tunnel connection from %s", addr.String())
			default:
				recordRejected(s.usageMonitor, s.logger, "UDP tunnel channel full; dropping tunnel flow")
				s.activeMu.Lock()
				if current, ok := s.activeConnections[key]; ok && current == &tunnelConn {
					delete(s.activeConnections, key)
					s.udpBudget.closeQueue(tunnelConn.payload)
				}
				s.activeMu.Unlock()
			}
		}
	}
}

func (s *UdpTransport) parsePortMappings() {
	for _, portMapping := range s.config.Ports {
		parts := strings.Split(portMapping, "=")

		var localAddr, remoteAddr string

		// Check if only a single port or a port range is provided (no "=" present)
		if len(parts) == 1 {
			localPortOrRange := strings.TrimSpace(parts[0])
			remoteAddr = localPortOrRange // If no remote addr is provided, use the local port as the remote port

			// Check if it's a port range
			if strings.Contains(localPortOrRange, "-") {
				rangeParts := strings.Split(localPortOrRange, "-")
				if len(rangeParts) != 2 {
					s.logger.Errorf("invalid port range format: %s", localPortOrRange)
					return
				}

				// Parse and validate start and end ports
				startPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
				if err != nil || startPort < 1 || startPort > 65535 {
					s.logger.Errorf("invalid start port in range: %s", rangeParts[0])
					return
				}

				endPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
				if err != nil || endPort < 1 || endPort > 65535 || endPort < startPort {
					s.logger.Errorf("invalid end port in range: %s", rangeParts[1])
					return
				}

				// Create listeners for all ports in the range
				for port := startPort; port <= endPort; port++ {
					localAddr = fmt.Sprintf(":%d", port)
					go s.localListener(localAddr, strconv.Itoa(port)) // Use port as the remoteAddr
					time.Sleep(1 * time.Millisecond)                  // for wide port ranges
				}
				continue
			} else {
				// Handle single port case
				port, err := strconv.Atoi(localPortOrRange)
				if err != nil || port < 1 || port > 65535 {
					s.logger.Errorf("invalid port format: %s", localPortOrRange)
					return
				}
				localAddr = fmt.Sprintf(":%d", port)
			}
		} else if len(parts) == 2 {
			// Handle "local=remote" format
			localPortOrRange := strings.TrimSpace(parts[0])
			remoteAddr = strings.TrimSpace(parts[1])

			// Check if local port is a range
			if strings.Contains(localPortOrRange, "-") {
				rangeParts := strings.Split(localPortOrRange, "-")
				if len(rangeParts) != 2 {
					s.logger.Errorf("invalid port range format: %s", localPortOrRange)
					return
				}

				// Parse and validate start and end ports
				startPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
				if err != nil || startPort < 1 || startPort > 65535 {
					s.logger.Errorf("invalid start port in range: %s", rangeParts[0])
					return
				}

				endPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
				if err != nil || endPort < 1 || endPort > 65535 || endPort < startPort {
					s.logger.Errorf("invalid end port in range: %s", rangeParts[1])
					return
				}

				// Create listeners for all ports in the range
				for port := startPort; port <= endPort; port++ {
					localAddr = fmt.Sprintf(":%d", port)
					go s.localListener(localAddr, remoteAddr)
					time.Sleep(1 * time.Millisecond) // for wide port ranges
				}
				continue
			} else {
				// Handle single local port case
				port, err := strconv.Atoi(localPortOrRange)
				if err == nil && port >= 1 && port <= 65535 { // format port=remoteAddress
					localAddr = fmt.Sprintf(":%d", port)
				} else {
					localAddr = localPortOrRange // format ip:port=remoteAddress
				}
			}
		} else {
			s.logger.Errorf("invalid port mapping format: %s", portMapping)
			return
		}
		// Start listeners for single port
		go s.localListener(localAddr, remoteAddr)
	}
}

func (s *UdpTransport) localListener(localAddr, remoteAddr string) {
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

	// Buffer for UDP reads
	buf := make([]byte, 16*1024)

	// Track active connections
	activeConnections := map[string]*LocalUDPConn{}

	// mutex
	mu := &sync.Mutex{}

	// make a new channel for recieve udp packets
	udpChan := make(chan *LocalUDPConn, s.config.ChannelSize)

	// handle channel
	go s.handleLoop(udpChan, &activeConnections, mu)

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
					// If connection is active and not closed, send payload
					if s.udpBudget.enqueue(existingConn.payload, buf[:n]) {
						s.logger.Tracef("buffered %d bytes for existing connection %s", n, addr.String())
					} else {
						recordRejected(s.usageMonitor, s.logger, "UDP local per-flow queue full; dropping packet")
					}
					mu.Unlock()
					continue
				}

				if len(activeConnections) >= udpMaxFlows(s.config.UDPMaxFlows) {
					mu.Unlock()
					recordRejected(s.usageMonitor, s.logger, "maximum UDP flow limit reached; dropping new flow")
					continue
				}
				mu.Unlock()

				payloadChan := make(chan []byte, udpQueueSize(s.config.UDPQueueSize))
				if !s.udpBudget.enqueue(payloadChan, buf[:n]) {
					recordRejected(s.usageMonitor, s.logger, "global UDP queue budget exhausted; dropping new flow")
					continue
				}

				// Build the UDP connection object
				newUDPConn := LocalUDPConn{
					timeCreated: time.Now().UnixMilli(), // Just for debugging
					payload:     payloadChan,
					remoteAddr:  remoteAddr,
					listener:    listener,
					addr:        addr,
				}

				mu.Lock()
				// Store the new connection
				activeConnections[key] = &newUDPConn
				mu.Unlock()

				select {
				case udpChan <- &newUDPConn:
					s.logger.Debugf("accepted UDP connection from %s", addr.String())

					// Request a new TCP connection
					select {
					case s.reqNewConnChan <- struct{}{}:
						// Successfully requested a new TCP connection
					default:
						// The channel is full, do nothing
						recordRejected(s.usageMonitor, s.logger, "new UDP tunnel request queue full; dropping request")
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

func (s *UdpTransport) handleLoop(udpChan chan *LocalUDPConn, activeConnections *map[string]*LocalUDPConn, mu *sync.Mutex) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case localConn := <-udpChan:
			wait := 3*time.Second - time.Duration(time.Now().UnixMilli()-localConn.timeCreated)*time.Millisecond
			if wait <= 0 {
				s.removeLocalUDPFlow(localConn, activeConnections, mu)
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
					s.logger.Debugf("timed out queued UDP flow from %s", localConn.addr.String())
					s.removeLocalUDPFlow(localConn, activeConnections, mu)
					break loop

				case tunnelConn := <-s.tunnelChannel:
					timer.Stop()
					close(tunnelConn.ping)
					tunnelConn.mu.Lock()

					// Send the target addr over the connection
					if _, err := tunnelConn.listener.WriteTo([]byte(localConn.remoteAddr), tunnelConn.addr); err != nil {
						s.logger.Errorf("%v", err)
						continue loop
					}

					// Handle data exchange between connections
					go s.udpCopy(localConn, tunnelConn, activeConnections, mu)

					s.logger.Debugf("initiate new handler for connection %s with timestamp %d", localConn.addr.String(), localConn.timeCreated)
					break loop
				}
			}
		}
	}
}

func (s *UdpTransport) removeLocalUDPFlow(udp *LocalUDPConn, activeConnections *map[string]*LocalUDPConn, mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
	key := udp.addr.String()
	if current, ok := (*activeConnections)[key]; ok && current == udp {
		delete(*activeConnections, key)
		s.udpBudget.closeQueue(udp.payload)
	}
}

func (s *UdpTransport) udpCopy(udpLocal *LocalUDPConn, udpTunnel *TunnelUDPConn, activeConnections *map[string]*LocalUDPConn, mu *sync.Mutex) {
	s.usageMonitor.ConnectionOpened()
	defer s.usageMonitor.ConnectionClosed()
	flowCtx, cancelFlow := context.WithCancel(s.ctx)
	defer cancelFlow()
	done := make(chan struct{}, 2)

	// Handle data from local to tunnel
	go func() {
		s.udpLocalCopy(flowCtx, udpLocal, udpTunnel)
		done <- struct{}{}
	}()

	// Handle data from tunnel to local
	go func() {
		s.udpTunnelCopy(flowCtx, udpTunnel, udpLocal)
		done <- struct{}{}
	}()

	// Stop the peer copy as soon as either direction fails or becomes idle.
	<-done
	cancelFlow()
	<-done

	// Remove local connection from active connections and close the channel
	mu.Lock()
	s.udpBudget.closeQueue(udpLocal.payload)
	delete(*activeConnections, udpLocal.addr.String())
	mu.Unlock()

	// Remove tunnel connection from active connections and close the channel
	s.activeMu.Lock()
	s.udpBudget.closeQueue(udpTunnel.payload)
	delete(s.activeConnections, udpTunnel.addr.String())
	s.activeMu.Unlock()

}

func (s *UdpTransport) udpLocalCopy(ctx context.Context, from *LocalUDPConn, to *TunnelUDPConn) {
	inactivityTimeout := 60 * time.Second // Define a 60-second inactivity timeout
	idleTimer := time.NewTimer(inactivityTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-from.payload: // Wait for data on the UDP payload channel
			if !ok {
				return
			}
			s.udpBudget.release()

			packetSize := len(data)

			totalWritten := 0
			for totalWritten < packetSize {
				// Write the packet to the tunnel
				w, err := to.listener.WriteToUDP(data[totalWritten:], to.addr)
				if err != nil {
					s.logger.Errorf("failed to write UDP payload to tunnel: %v", err)
					return
				}
				totalWritten += w
			}

			if s.config.Sniffer {
				s.usageMonitor.AddOrUpdatePort(from.listener.LocalAddr().(*net.UDPAddr).Port, uint64(totalWritten))
			}

			s.logger.Debugf("forwarded %d bytes from local connection %s to tunnel", packetSize, from.addr.String())
			resetTimer(idleTimer, inactivityTimeout)

		case <-idleTimer.C:
			s.logger.Debugf("connection idle for 60 seconds, closing UDP connection for %s", from.addr.String())
			return
		}
	}
}

func (s *UdpTransport) udpTunnelCopy(ctx context.Context, from *TunnelUDPConn, to *LocalUDPConn) {
	inactivityTimeout := 60 * time.Second // Define a 60-second inactivity timeout
	idleTimer := time.NewTimer(inactivityTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-from.payload: // Wait for data on the UDP payload channel
			if !ok {
				return
			}
			s.udpBudget.release()

			packetSize := len(data)

			totalWritten := 0
			for totalWritten < packetSize {
				// Write the packet to the tunnel
				w, err := to.listener.WriteToUDP(data[totalWritten:], to.addr)
				if err != nil {
					s.logger.Errorf("failed to write UDP payload to tunnel: %v", err)
					return
				}
				totalWritten += w
			}

			if s.config.Sniffer {
				s.usageMonitor.AddOrUpdatePort(to.listener.LocalAddr().(*net.UDPAddr).Port, uint64(totalWritten))
			}

			s.logger.Debugf("forwarded %d bytes from local connection %s to tunnel", packetSize, from.addr.String())
			resetTimer(idleTimer, inactivityTimeout)

		case <-idleTimer.C:
			s.logger.Debugf("connection idle for 60 seconds, closing UDP connection for %s", from.addr.String())
			return
		}
	}
}

func (s *UdpTransport) keepAlive(conn *TunnelUDPConn) {
	ticker := time.NewTicker(s.config.Heartbeat) // Send periodic pings to the client

	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return

		case <-conn.ping:
			s.logger.Trace("ping channel closed")
			return
		case <-ticker.C:
			// Try to acquire the lock without blocking
			locked := conn.mu.TryLock()
			if !locked {
				// If the lock is held by another operation, stop the pingSender
				s.logger.Trace("write operation in progress, stopping pingSender")
				return
			}
			if _, err := conn.listener.WriteTo([]byte{utils.SG_Ping}, conn.addr); err != nil {
				conn.mu.Unlock()
				return
			}
			conn.mu.Unlock()
			s.logger.Trace("ping sent to the client")
		}
	}
}
