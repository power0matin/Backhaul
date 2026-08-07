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
	"github.com/xtaci/smux"
)

type TcpMuxTransport struct {
	config           *TcpMuxConfig
	smuxConfig       *smux.Config
	parentctx        context.Context
	ctx              context.Context
	cancel           context.CancelFunc
	logger           *logrus.Logger
	tunnelChannel    chan *smux.Session
	handshakeChannel chan net.Conn
	localChannel     chan LocalTCPConn
	reqNewConnChan   chan struct{}
	controlChannel   net.Conn
	controlMu        sync.RWMutex
	usageMonitor     *web.Usage
	restartOnce      sync.Once
	streamCounter    int32
	sessionCounter   int32
}

type TcpMuxConfig struct {
	BindAddr         string
	SnifferLog       string
	WebBindAddr      string
	WebUsername      string
	WebPassword      string
	Token            string
	Ports            []string
	Nodelay          bool
	Sniffer          bool
	ChannelSize      int
	MuxCon           int
	MuxVersion       int
	MaxFrameSize     int
	MaxReceiveBuffer int
	MaxStreamBuffer  int
	WebPort          int
	KeepAlive        time.Duration
	Heartbeat        time.Duration // in seconds
	MSS              int
	SO_RCVBUF        int
	SO_SNDBUF        int
	ProxyProtocol    bool
}

func NewTcpMuxServer(parentCtx context.Context, config *TcpMuxConfig, logger *logrus.Logger) *TcpMuxTransport {
	// Create a derived context from the parent context
	ctx, cancel := context.WithCancel(parentCtx)

	// Initialize the TcpTransport struct
	server := &TcpMuxTransport{
		smuxConfig: &smux.Config{
			Version:           config.MuxVersion,
			KeepAliveInterval: 20 * time.Second,
			KeepAliveTimeout:  40 * time.Second,
			MaxFrameSize:      config.MaxFrameSize,
			MaxReceiveBuffer:  config.MaxReceiveBuffer,
			MaxStreamBuffer:   config.MaxStreamBuffer,
		},
		config:           config,
		parentctx:        parentCtx,
		ctx:              ctx,
		cancel:           cancel,
		logger:           logger,
		tunnelChannel:    make(chan *smux.Session, config.ChannelSize),
		handshakeChannel: make(chan net.Conn),
		localChannel:     make(chan LocalTCPConn, config.ChannelSize),
		reqNewConnChan:   make(chan struct{}, config.ChannelSize),
		controlChannel:   nil, // will be set when a control connection is established
		streamCounter:    0,
		sessionCounter:   0,
		usageMonitor:     web.NewDataStore(config.WebBindAddr, config.WebPort, ctx, config.SnifferLog, config.Sniffer, "Disconnected (TCPMux)", config.WebUsername, config.WebPassword, logger),
	}

	return server
}

func (s *TcpMuxTransport) Start() {
	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}
	s.usageMonitor.SetTunnelStatus("Disconnected (TCPMux)")

	go s.tunnelListener()

	s.channelHandshake()

	if s.getControlChannel() != nil {
		s.usageMonitor.SetTunnelStatus("Connected (TCPMux)")

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
func (s *TcpMuxTransport) Restart() {
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
		next := NewTcpMuxServer(s.parentctx, s.config, s.logger)
		next.usageMonitor.InheritRuntimeMetrics(s.usageMonitor)
		go next.Start()
	})
}

func (s *TcpMuxTransport) channelHandshake() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case conn := <-s.handshakeChannel:
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

			//FORCE CONTROL CHANNEL TO BE TCP_NODELAY
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				conn.Close()
				continue
			}
			if err := tcpConn.SetNoDelay(true); err != nil {
				s.logger.Warnf("failed to set TCP_NODELAY for Control Channel %s: %v", tcpConn.RemoteAddr().String(), err)
			}

			s.setControlChannel(conn)

			s.logger.Info("control channel successfully established.")

			return
		}
	}
}

func (s *TcpMuxTransport) channelHandler() {
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
	}()

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
			}
		}
	}
}

func (s *TcpMuxTransport) tunnelListener() {
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

func (s *TcpMuxTransport) acceptTunnelConn(listener net.Listener) {
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

			// try to establish a new channel
			if s.getControlChannel() == nil {
				s.logger.Info("control channel not found, attempting to establish a new session")
				select {
				case s.handshakeChannel <- conn: // ok
				default:
					s.logger.Warnf("control channel handshake in progress...")
					conn.Close()
				}
				continue
			}

			session, err := smux.Client(conn, s.smuxConfig)
			if err != nil {
				s.logger.Errorf("failed to create MUX session for connection %s: %v", conn.RemoteAddr().String(), err)
				conn.Close()
				continue
			}

			select {
			case s.tunnelChannel <- session: // ok
			default:
				if total, logNow := s.usageMonitor.RecordRejected(); logNow {
					s.logger.Warnf("mux tunnel channel full; rejecting connection from %s (total rejected/dropped=%d)", conn.RemoteAddr(), total)
				}
				session.Close()
			}
		}
	}

}

func (s *TcpMuxTransport) getControlChannel() net.Conn {
	s.controlMu.RLock()
	defer s.controlMu.RUnlock()
	return s.controlChannel
}

func (s *TcpMuxTransport) setControlChannel(conn net.Conn) {
	s.controlMu.Lock()
	s.controlChannel = conn
	s.controlMu.Unlock()
}

func (s *TcpMuxTransport) parsePortMappings() {
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

func (s *TcpMuxTransport) localListener(localAddr string, remoteAddr string) {
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

func (s *TcpMuxTransport) acceptLocalConn(listener net.Listener, remoteAddr string) {
	for {
		select {
		case <-s.ctx.Done():
			return

		default:
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
				s.logger.Debugf("accepted incoming TCP connection from %s", tcpConn.RemoteAddr().String())

				// +1 for stream counter
				atomic.AddInt32(&s.streamCounter, 1)

				if atomic.LoadInt32(&s.streamCounter) >= atomic.LoadInt32(&s.sessionCounter)*int32(s.config.MuxCon) {
					s.logger.Tracef("stream counter: %v, session counter: %v", atomic.LoadInt32(&s.streamCounter), atomic.LoadInt32(&s.sessionCounter))

					select { // Attempt to request a new connection
					case s.reqNewConnChan <- struct{}{}:
					default:
						recordRejected(s.usageMonitor, s.logger, "new mux tunnel request queue full; dropping request")
					}
				}
			} else {
				if total, logNow := s.usageMonitor.RecordRejected(); logNow {
					s.logger.Warnf("local queue remained full after backpressure timeout; rejecting %s (total rejected/dropped=%d)", tcpConn.RemoteAddr(), total)
				}
				conn.Close()
			}

		}
	}

}

func (s *TcpMuxTransport) handleLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return

		case session := <-s.tunnelChannel:
			// +1 for session counter
			atomic.AddInt32(&s.sessionCounter, 1)

			go s.handleSession(session)
		}
	}
}

func (s *TcpMuxTransport) handleSession(session *smux.Session) {
	counter := make(chan struct{}, s.config.MuxCon)
	defer session.Close()
	defer atomic.AddInt32(&s.sessionCounter, -1)
	stopSession := context.AfterFunc(s.ctx, func() { _ = session.Close() })
	defer stopSession()

	for {
		// Acquire stream capacity without making shutdown wait for a busy session.
		select {
		case counter <- struct{}{}:
		case <-s.ctx.Done():
			return
		}

		select {
		case <-s.ctx.Done():
			<-counter
			return

		case incomingConn := <-s.localChannel:
			if time.Now().UnixMilli()-incomingConn.timeCreated > 3000 { // 3000ms
				s.logger.Debugf("timeouted local connection: %d ms", time.Now().UnixMilli()-incomingConn.timeCreated)
				incomingConn.conn.Close()

				// Decrement the counter
				atomic.AddInt32(&s.streamCounter, -1)
				<-counter
				continue
			}

			stream, err := session.OpenStream()
			if err != nil {
				<-counter
				s.handleSessionError(&incomingConn, err)
				return
			}

			// Send the target port over the tunnel connection
			if err := utils.SendBinaryString(stream, incomingConn.remoteAddr); err != nil {
				s.logger.Tracef("failed to send address over stream: %v", err)
				_ = stream.Close()
				<-counter
				s.handleSessionError(&incomingConn, err)
				return
			}

			// Handle data exchange between connections
			go func() {
				handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, incomingConn.conn, stream, s.logger, s.usageMonitor, incomingConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
				atomic.AddInt32(&s.streamCounter, -1)
				<-counter // read signal from the channel
			}()
		}
	}
}

func (s *TcpMuxTransport) handleSessionError(incomingConn *LocalTCPConn, err error) {
	s.logger.Tracef("failed to handle session: %v", err)

	if !enqueueLocalTCP(s.ctx, s.localChannel, *incomingConn) {
		_ = incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
		return
	}

	// Attempt to request a new connection
	select {
	case s.reqNewConnChan <- struct{}{}:
	default:
		recordRejected(s.usageMonitor, s.logger, "new mux tunnel request queue full; dropping request")
	}
}
