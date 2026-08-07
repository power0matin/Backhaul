package transport

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/web"

	"github.com/sirupsen/logrus"
)

type TcpTransport struct {
	config         *TcpConfig
	parentctx      context.Context
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *logrus.Logger
	tunnelChannel  chan net.Conn
	localChannel   chan LocalTCPConn
	reqNewConnChan chan struct{}
	controlChannel net.Conn
	controlMu      sync.RWMutex
	restartOnce    sync.Once
	usageMonitor   *web.Usage
	rtt            atomic.Int64 // in ms, for UDP
	udpBudget      *packetBudget
}

type TcpConfig struct {
	BindAddr      string
	Token         string
	SnifferLog    string
	WebBindAddr   string
	WebUsername   string
	WebPassword   string
	Ports         []string
	Nodelay       bool
	Sniffer       bool
	KeepAlive     time.Duration
	Heartbeat     time.Duration // in seconds
	ChannelSize   int
	WebPort       int
	AcceptUDP     bool
	MSS           int
	SO_RCVBUF     int
	SO_SNDBUF     int
	ProxyProtocol bool
	UDPQueueSize  int
	UDPQueueLimit int
	UDPMaxFlows   int
}

func NewTCPServer(parentCtx context.Context, config *TcpConfig, logger *logrus.Logger) *TcpTransport {
	// Create a derived context from the parent context
	ctx, cancel := context.WithCancel(parentCtx)

	// Initialize the TcpTransport struct
	server := &TcpTransport{
		config:         config,
		parentctx:      parentCtx,
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		tunnelChannel:  make(chan net.Conn, config.ChannelSize),
		localChannel:   make(chan LocalTCPConn, config.ChannelSize),
		reqNewConnChan: make(chan struct{}, config.ChannelSize),
		controlChannel: nil, // will be set when a control connection is established
		usageMonitor:   web.NewDataStore(config.WebBindAddr, config.WebPort, ctx, config.SnifferLog, config.Sniffer, "Disconnected (TCP)", config.WebUsername, config.WebPassword, logger),
		udpBudget:      newPacketBudget(config.UDPQueueLimit),
	}

	return server
}

func (s *TcpTransport) Start() {
	s.usageMonitor.SetTunnelStatus("Disconnected (TCP)")

	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}

	go s.tunnelListener()

	s.channelHandshake()

	if s.getControlChannel() != nil {
		s.usageMonitor.SetTunnelStatus("Connected (TCP)")

		numCPU := runtime.NumCPU()
		if numCPU > 4 {
			numCPU = 4 // Max allowed handler is 4
		}

		go s.parsePortMappings()
		go s.channelHandler()

		s.logger.Infof("starting %d handle loops on each CPU thread", numCPU)

		for i := 0; i < numCPU; i++ {
			go s.handleLoop()
		}
	}
}
func (s *TcpTransport) Restart() {
	s.restartOnce.Do(func() {
		if s.parentctx.Err() != nil {
			return
		}
		s.usageMonitor.RecordReconnect()
		s.logger.Info("restarting server...")
		s.cancel()
		if control := s.getControlChannel(); control != nil {
			_ = control.Close()
		}
		if !utils.WaitForDelay(s.parentctx, 2*time.Second) {
			return
		}
		next := NewTCPServer(s.parentctx, s.config, s.logger)
		next.usageMonitor.InheritRuntimeMetrics(s.usageMonitor)
		go next.Start()
	})
}

func (s *TcpTransport) channelHandshake() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case conn := <-s.tunnelChannel:
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

			s.setControlChannel(conn)

			s.logger.Info("control channel successfully established.")
			return
		}
	}
}

func (s *TcpTransport) channelHandler() {
	control := s.getControlChannel()
	if control == nil {
		return
	}
	defer control.Close()
	stopControlClose := context.AfterFunc(s.ctx, func() { _ = control.Close() })
	defer stopControlClose()
	ticker := time.NewTicker(s.config.Heartbeat)
	defer ticker.Stop()

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
				s.rtt.Store(measureRTT.Milliseconds())
				s.logger.Infof("Round Trip Time (RTT): %d ms", s.rtt.Load())
			}
		}
	}
}

func (s *TcpTransport) tunnelListener() {
	listener, err := network.ListenWithBuffers(
		"tcp",
		s.config.BindAddr,
		s.config.SO_RCVBUF,
		s.config.SO_SNDBUF,
		s.config.MSS,
		s.config.KeepAlive,
		!s.config.Nodelay,
	)
	if err != nil {
		s.logger.Errorf("failed to start tunnel listener on %s: %v", s.config.BindAddr, err)
		go s.Restart()
		return
	}

	defer listener.Close()

	s.logger.Infof("server started successfully, listening on address: %s", listener.Addr().String())

	go s.acceptTunnelConn(listener)

	<-s.ctx.Done()
}

func (s *TcpTransport) acceptTunnelConn(listener net.Listener) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			s.logger.Debugf("waiting for accept incoming tunnel connection on %s", listener.Addr().String())
			conn, err := listener.Accept()
			if err != nil {
				s.logger.Debugf("failed to accept tunnel connection on %s: %v", listener.Addr().String(), err)
				continue
			}

			//discard any non tcp connection
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				s.logger.Warnf("disarded non-TCP tunnel connection from %s", conn.RemoteAddr().String())
				conn.Close()
				continue
			}

			// Drop all suspicious packets from other address rather than server
			control := s.getControlChannel()
			if control != nil && control.RemoteAddr().(*net.TCPAddr).IP.String() != tcpConn.RemoteAddr().(*net.TCPAddr).IP.String() {
				s.logger.Debugf("suspicious packet from %v. expected address: %v. discarding packet...", tcpConn.RemoteAddr().(*net.TCPAddr).IP.String(), control.RemoteAddr().(*net.TCPAddr).IP.String())
				tcpConn.Close()
				continue
			}

			// trying to set tcpnodelay
			if !s.config.Nodelay {
				if err := tcpConn.SetNoDelay(s.config.Nodelay); err != nil {
					s.logger.Warnf("failed to set TCP_NODELAY for %s: %v", tcpConn.RemoteAddr().String(), err)
				} else {
					s.logger.Tracef("TCP_NODELAY disabled for %s", tcpConn.RemoteAddr().String())
				}
			}

			// Set keep-alive settings
			if err := tcpConn.SetKeepAlive(true); err != nil {
				s.logger.Warnf("failed to enable TCP keep-alive for %s: %v", tcpConn.RemoteAddr().String(), err)
			} else {
				s.logger.Tracef("TCP keep-alive enabled for %s", tcpConn.RemoteAddr().String())
			}
			if err := tcpConn.SetKeepAlivePeriod(s.config.KeepAlive); err != nil {
				s.logger.Warnf("failed to set TCP keep-alive period for %s: %v", tcpConn.RemoteAddr().String(), err)
			}

			select {
			case s.tunnelChannel <- conn:
			default: // The channel is full, do nothing
				if total, logNow := s.usageMonitor.RecordRejected(); logNow {
					s.logger.Warnf("tunnel channel full; rejecting connection from %s (total rejected/dropped=%d)", conn.RemoteAddr(), total)
				}
				conn.Close()
			}
		}
	}
}

func (s *TcpTransport) getControlChannel() net.Conn {
	s.controlMu.RLock()
	defer s.controlMu.RUnlock()
	return s.controlChannel
}

func (s *TcpTransport) setControlChannel(conn net.Conn) {
	s.controlMu.Lock()
	s.controlChannel = conn
	s.controlMu.Unlock()
}

func (s *TcpTransport) parsePortMappings() {
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
					go s.startListeners(localAddr, strconv.Itoa(port)) // Use port as the remoteAddr
					time.Sleep(1 * time.Millisecond)                   // for wide port ranges
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
					go s.startListeners(localAddr, remoteAddr)
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
		go s.startListeners(localAddr, remoteAddr)
	}
}

func (s *TcpTransport) startListeners(localAddr, remoteAddr string) {
	// Start TCP listener
	go s.localListener(localAddr, remoteAddr)

	// Start UDP listener if configured
	if s.config.AcceptUDP {
		go s.udpListener(localAddr, remoteAddr)
	}

	s.logger.Debugf("Started listening on %s, forwarding to %s", localAddr, remoteAddr)
}

func (s *TcpTransport) localListener(localAddr string, remoteAddr string) {
	listener, err := net.Listen("tcp", localAddr)
	if err != nil {
		s.logger.Errorf("failed to start forwarding listener on %s: %v", localAddr, err)
		return
	}

	defer listener.Close()

	s.logger.Infof("listener started successfully, listening on address: %s", listener.Addr().String())

	go s.acceptLocalConn(listener, remoteAddr)

	<-s.ctx.Done()
}

func (s *TcpTransport) acceptLocalConn(listener net.Listener, remoteAddr string) {
	for {
		select {
		case <-s.ctx.Done():
			return

		default:
			s.logger.Debugf("waiting for accept incoming connection on %s", listener.Addr().String())
			conn, err := listener.Accept()
			if err != nil {
				s.logger.Debugf("failed to accept connection on %s: %v", listener.Addr().String(), err)
				continue
			}

			// discard any non-tcp connection
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				s.logger.Warnf("disarded non-TCP connection from %s", conn.RemoteAddr().String())
				conn.Close()
				continue
			}

			// trying to disable tcpnodelay
			if !s.config.Nodelay {
				if err := tcpConn.SetNoDelay(s.config.Nodelay); err != nil {
					s.logger.Warnf("failed to set TCP_NODELAY for %s: %v", tcpConn.RemoteAddr().String(), err)
				} else {
					s.logger.Tracef("TCP_NODELAY disabled for %s", tcpConn.RemoteAddr().String())
				}
			}

			if enqueueLocalTCP(s.ctx, s.localChannel, LocalTCPConn{conn: conn, remoteAddr: remoteAddr, timeCreated: time.Now().UnixMilli()}) {

				select {
				case s.reqNewConnChan <- struct{}{}:
					// Successfully requested a new connection
				default:
					// The channel is full, do nothing
					recordRejected(s.usageMonitor, s.logger, "new tunnel request queue full; dropping request")
				}

				s.logger.Debugf("accepted incoming TCP connection from %s", tcpConn.RemoteAddr().String())
			} else {
				if total, logNow := s.usageMonitor.RecordRejected(); logNow {
					s.logger.Warnf("local queue for %s remained full after backpressure timeout; rejecting %s (total rejected/dropped=%d)", listener.Addr(), tcpConn.RemoteAddr(), total)
				}
				conn.Close()
			}
		}
	}
}

func (s *TcpTransport) handleLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case localConn := <-s.localChannel:
		loop:
			for {
				if time.Now().UnixMilli()-localConn.timeCreated > 3000 { // 3000ms
					s.logger.Debugf("timeouted local connection: %d ms", time.Now().UnixMilli()-localConn.timeCreated)
					localConn.conn.Close()
					break loop
				}

				select {
				case <-s.ctx.Done():
					return

				case tunnelConn := <-s.tunnelChannel:
					// Send the target addr over the connection
					if err := utils.SendBinaryTransportString(tunnelConn, localConn.remoteAddr, utils.SG_TCP); err != nil {
						s.logger.Errorf("%v", err)
						tunnelConn.Close()
						continue loop
					}

					// Handle data exchange between connections
					go handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, localConn.conn, tunnelConn, s.logger, s.usageMonitor, localConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
					break loop

				}
			}
		}
	}
}
