package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/config" // for mode
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/web"
	"github.com/xtaci/smux"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type WsMuxTransport struct {
	config         *WsMuxConfig
	smuxConfig     *smux.Config
	parentctx      context.Context
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *logrus.Logger
	tunnelChannel  chan *smux.Session
	localChannel   chan LocalTCPConn
	reqNewConnChan chan struct{}
	controlChannel *websocket.Conn
	controlMu      sync.RWMutex
	usageMonitor   *web.Usage
	restartOnce    sync.Once
	streamCounter  int32
	sessionCounter int32
}

type WsMuxConfig struct {
	BindAddr         string
	Token            string
	SnifferLog       string
	WebBindAddr      string
	WebUsername      string
	WebPassword      string
	TLSCertFile      string // Path to the TLS certificate file
	TLSKeyFile       string // Path to the TLS key file
	Ports            []string
	Nodelay          bool
	Sniffer          bool
	KeepAlive        time.Duration
	Heartbeat        time.Duration // in seconds
	ChannelSize      int
	MuxCon           int
	MuxVersion       int
	MaxFrameSize     int
	MaxReceiveBuffer int
	MaxStreamBuffer  int
	WebPort          int
	Mode             config.TransportType // ws or wss
	ProxyProtocol    bool
}

func NewWSMuxServer(parentCtx context.Context, config *WsMuxConfig, logger *logrus.Logger) *WsMuxTransport {
	// Create a derived context from the parent context
	ctx, cancel := context.WithCancel(parentCtx)

	// Initialize the TcpTransport struct
	server := &WsMuxTransport{
		smuxConfig: &smux.Config{
			Version:           config.MuxVersion,
			KeepAliveInterval: 20 * time.Second,
			KeepAliveTimeout:  40 * time.Second,
			MaxFrameSize:      config.MaxFrameSize,
			MaxReceiveBuffer:  config.MaxReceiveBuffer,
			MaxStreamBuffer:   config.MaxStreamBuffer,
		},
		config:         config,
		parentctx:      parentCtx,
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		tunnelChannel:  make(chan *smux.Session, config.ChannelSize),
		localChannel:   make(chan LocalTCPConn, config.ChannelSize),
		reqNewConnChan: make(chan struct{}, config.ChannelSize),
		streamCounter:  0,
		sessionCounter: 0,
		controlChannel: nil, // will be set when a control connection is established
		usageMonitor:   web.NewDataStore(config.WebBindAddr, config.WebPort, ctx, config.SnifferLog, config.Sniffer, fmt.Sprintf("Disconnected (%s)", config.Mode), config.WebUsername, config.WebPassword, logger),
	}

	return server
}

func (s *WsMuxTransport) Start() {
	// for  webui
	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}

	s.usageMonitor.SetTunnelStatus(fmt.Sprintf("Disconnected (%s)", s.config.Mode))

	go s.tunnelListener()

}

func (s *WsMuxTransport) Restart() {
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
		next := NewWSMuxServer(s.parentctx, s.config, s.logger)
		next.usageMonitor.InheritRuntimeMetrics(s.usageMonitor)
		go next.Start()
	})
}

func (s *WsMuxTransport) channelHandler() {
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
	messageChan := make(chan byte, 10)

	// Separate goroutine to continuously listen for messages
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return

			default:
				_, msg, err := control.ReadMessage()
				// Exit if there's an error
				if err != nil {
					if s.ctx.Err() == nil {
						s.logger.Warn("control channel closed; reconnecting: ", err)
						go s.Restart()
					}
					return
				}
				if len(msg) != 1 {
					s.logger.Warnf("invalid control message length %d; restarting", len(msg))
					go s.Restart()
					return
				}
				select {
				case messageChan <- msg[0]:
				case <-s.ctx.Done():
					return
				}
			}
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			_ = control.WriteMessage(websocket.BinaryMessage, []byte{utils.SG_Closed})
			return
		case <-s.reqNewConnChan:
			err := control.WriteMessage(websocket.BinaryMessage, []byte{utils.SG_Chan})
			if err != nil {
				s.logger.Error("failed to send request new connection signal. ", err)
				go s.Restart()
				return
			}

		case <-ticker.C:
			err := control.WriteMessage(websocket.BinaryMessage, []byte{utils.SG_HB})
			if err != nil {
				s.logger.Errorf("failed to send heartbeat signal. Error: %v.", err)
				go s.Restart()
				return
			}
			s.logger.Debug("heartbeat signal sent successfully")

		case msg, ok := <-messageChan:
			if !ok {
				s.logger.Error("channel closed, likely due to an error in WebSocket read")
				return
			}
			switch msg {
			case utils.SG_HB:
				s.logger.Trace("heartbeat signal received successfully")

			case utils.SG_Closed:
				s.logger.Warn("control channel has been closed by the client")
				s.Restart()
				return

			default:
				s.logger.Errorf("unexpected response from channel: %v", msg)
				go s.Restart()
				return
			}

		}
	}
}

func (s *WsMuxTransport) tunnelListener() {
	addr := s.config.BindAddr
	upgrader := websocket.Upgrader{
		ReadBufferSize:   16 * 1024,
		WriteBufferSize:  16 * 1024,
		HandshakeTimeout: 45 * time.Second,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	// Create an HTTP server
	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 * 1024,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.logger.Tracef("received http request from %s", r.RemoteAddr)

			// Read the "Authorization" header
			authHeader := r.Header.Get("Authorization")
			if !utils.SecureTokenEqual(authHeader, "Bearer "+s.config.Token) {
				s.logger.Warnf("unauthorized request from %s, closing connection", r.RemoteAddr)
				http.Error(w, "unauthorized", http.StatusUnauthorized) // Send 401 Unauthorized response
				return
			}

			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				s.logger.Errorf("failed to upgrade connection from %s: %v", r.RemoteAddr, err)
				return
			}

			if r.URL.Path == "/channel" {
				conn.SetReadLimit(1024)
				if !s.trySetControlChannel(conn) {
					s.logger.Warn("new control channel requested.")
					if control := s.getControlChannel(); control != nil {
						_ = control.Close()
					}
					conn.Close()
					go s.Restart()
					return
				}

				s.logger.Info("control channel established successfully")

				numCPU := runtime.NumCPU()
				if numCPU > 4 {
					numCPU = 4 // Max allowed handler is 4
				}

				go s.channelHandler()
				go s.parsePortMappings()

				s.logger.Infof("starting %d handle loops on each CPU thread", numCPU)

				for i := 0; i < numCPU; i++ {
					go s.handleLoop()
				}

				s.usageMonitor.SetTunnelStatus(fmt.Sprintf("Connected (%s)", s.config.Mode))

			} else if strings.HasPrefix(r.URL.Path, "/tunnel") {
				session, err := smux.Client(conn.NetConn(), s.smuxConfig)
				if err != nil {
					s.logger.Errorf("failed to create MUX session for connection %s: %v", conn.RemoteAddr().String(), err)
					conn.Close()
					return
				}
				select {
				case s.tunnelChannel <- session: // ok
				default:
					if total, logNow := s.usageMonitor.RecordRejected(); logNow {
						s.logger.Warnf("mux tunnel channel full; rejecting connection from %s (total rejected/dropped=%d)", conn.RemoteAddr(), total)
					}
					conn.Close()
				}
			} else {
				_ = conn.Close()
			}
		}),
	}

	if s.config.Mode == config.WSMUX {
		go func() {
			s.logger.Infof("%s server starting, listening on %s", s.config.Mode, addr)
			if s.getControlChannel() == nil {
				s.logger.Infof("waiting for %s control channel connection", s.config.Mode)
			}
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.logger.Errorf("failed to start websocket mux tunnel listener on %s: %v", addr, err)
				go s.Restart()
			}
		}()
	} else {
		go func() {
			s.logger.Infof("%s server starting, listening on %s", s.config.Mode, addr)
			if s.getControlChannel() == nil {
				s.logger.Infof("waiting for %s control channel connection", s.config.Mode)
			}
			if err := server.ListenAndServeTLS(s.config.TLSCertFile, s.config.TLSKeyFile); err != nil && err != http.ErrServerClosed {
				s.logger.Errorf("failed to start websocket mux TLS tunnel listener on %s: %v", addr, err)
				go s.Restart()
			}
		}()
	}

	<-s.ctx.Done()

	// close connection
	if control := s.getControlChannel(); control != nil {
		_ = control.Close()
	}

	// Gracefully shutdown the server
	s.logger.Infof("shutting down the websocket server on %s", addr)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		s.logger.Errorf("Failed to gracefully shutdown the server: %v", err)
	}
}

func (s *WsMuxTransport) getControlChannel() *websocket.Conn {
	s.controlMu.RLock()
	defer s.controlMu.RUnlock()
	return s.controlChannel
}

func (s *WsMuxTransport) trySetControlChannel(conn *websocket.Conn) bool {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if s.controlChannel != nil {
		return false
	}
	s.controlChannel = conn
	return true
}

func (s *WsMuxTransport) parsePortMappings() {
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

func (s *WsMuxTransport) localListener(localAddr string, remoteAddr string) {
	listener, err := net.Listen("tcp", localAddr)
	if err != nil {
		s.logger.Errorf("failed to start forwarding listener on %s: %v", localAddr, err)
		return
	}

	//close local listener after context cancellation
	defer listener.Close()

	go s.acceptLocalConn(listener, remoteAddr)

	s.logger.Infof("listener started successfully, listening on address: %s", listener.Addr().String())

	<-s.ctx.Done()
}

func (s *WsMuxTransport) acceptLocalConn(listener net.Listener, remoteAddr string) {
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

			// trying to enable tcpnodelay
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

			if enqueueLocalTCP(s.ctx, s.localChannel, LocalTCPConn{conn: conn, remoteAddr: remoteAddr, timeCreated: time.Now().UnixMilli()}) {
				s.logger.Debugf("accepted incoming TCP connection from %s", tcpConn.RemoteAddr().String())

				// +1 for stream counter
				atomic.AddInt32(&s.streamCounter, 1)

				if atomic.LoadInt32(&s.streamCounter) >= atomic.LoadInt32(&s.sessionCounter)*int32(s.config.MuxCon) {
					s.logger.Tracef("stream counter: %v, session counter: %v", atomic.LoadInt32(&s.streamCounter), atomic.LoadInt32(&s.sessionCounter))
					// Attempt to request a new connection
					select {
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

func (s *WsMuxTransport) handleLoop() {
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

func (s *WsMuxTransport) handleSession(session *smux.Session) {
	counter := make(chan struct{}, s.config.MuxCon)
	defer session.Close()
	defer atomic.AddInt32(&s.sessionCounter, -1)
	stopSession := context.AfterFunc(s.ctx, func() { _ = session.Close() })
	defer stopSession()

	for {
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

func (s *WsMuxTransport) handleSessionError(incomingConn *LocalTCPConn, err error) {
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
