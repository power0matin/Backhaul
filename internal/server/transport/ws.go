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
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/web"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type WsTransport struct {
	config         *WsConfig
	parentctx      context.Context
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *logrus.Logger
	tunnelChannel  chan TunnelChannel
	localChannel   chan LocalTCPConn
	reqNewConnChan chan struct{}
	controlChannel *websocket.Conn
	controlMu      sync.RWMutex
	restartOnce    sync.Once
	usageMonitor   *web.Usage
}

type WsConfig struct {
	BindAddr    string
	SnifferLog  string
	WebBindAddr string
	WebUsername string
	WebPassword string
	TLSCertFile string // Path to the TLS certificate file
	TLSKeyFile  string // Path to the TLS key file
	Token       string
	Ports       []string
	Nodelay     bool
	Sniffer     bool
	KeepAlive   time.Duration
	Heartbeat   time.Duration // in seconds
	ChannelSize int
	WebPort     int
	Mode        config.TransportType // ws or wss

}

func NewWSServer(parentCtx context.Context, config *WsConfig, logger *logrus.Logger) *WsTransport {
	// Create a derived context from the parent context
	ctx, cancel := context.WithCancel(parentCtx)

	// Initialize the TcpTransport struct
	server := &WsTransport{
		config:         config,
		parentctx:      parentCtx,
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		tunnelChannel:  make(chan TunnelChannel, config.ChannelSize),
		localChannel:   make(chan LocalTCPConn, config.ChannelSize),
		reqNewConnChan: make(chan struct{}, config.ChannelSize),
		controlChannel: nil, // will be set when a control connection is established
		usageMonitor:   web.NewDataStore(config.WebBindAddr, config.WebPort, ctx, config.SnifferLog, config.Sniffer, fmt.Sprintf("Disconnected (%s)", config.Mode), config.WebUsername, config.WebPassword, logger),
	}

	return server
}

func (s *WsTransport) Start() {
	// for  webui
	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}

	s.usageMonitor.SetTunnelStatus(fmt.Sprintf("Disconnected (%s)", s.config.Mode))

	go s.tunnelListener()

}
func (s *WsTransport) Restart() {
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
		next := NewWSServer(s.parentctx, s.config, s.logger)
		next.usageMonitor.InheritRuntimeMetrics(s.usageMonitor)
		go next.Start()
	})
}

func (s *WsTransport) channelHandler() {
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

func (s *WsTransport) tunnelListener() {
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
				// Valid Backhaul WS payload messages are emitted from 16 KiB
				// transfer buffers. Keep a generous fixed ceiling so a peer cannot
				// force ReadMessage to grow memory without bound.
				conn.SetReadLimit(64 * 1024)
				wsConn := TunnelChannel{
					conn: conn,
					ping: make(chan struct{}),
					mu:   &sync.Mutex{},
				}
				select {
				case s.tunnelChannel <- wsConn:
					go s.keepAlive(&wsConn)
					s.logger.Debugf("websocket connection accepted from %s", conn.RemoteAddr().String())
				default:
					recordRejected(s.usageMonitor, s.logger, "websocket tunnel channel full; closing excess tunnel connection")
					conn.Close()
				}
			} else {
				_ = conn.Close()
			}
		}),
	}

	if s.config.Mode == config.WS {
		go func() {
			s.logger.Infof("ws server starting, listening on %s", addr)
			if s.getControlChannel() == nil {
				s.logger.Info("waiting for ws control channel connection")
			}
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.logger.Errorf("failed to start websocket tunnel listener on %s: %v", addr, err)
				go s.Restart()
			}
		}()
	} else {
		go func() {
			s.logger.Infof("wss server starting, listening on %s", addr)
			if s.getControlChannel() == nil {
				s.logger.Info("waiting for wss control channel connection")
			}
			if err := server.ListenAndServeTLS(s.config.TLSCertFile, s.config.TLSKeyFile); err != nil && err != http.ErrServerClosed {
				s.logger.Errorf("failed to start websocket TLS tunnel listener on %s: %v", addr, err)
				go s.Restart()
			}
		}()
	}

	<-s.ctx.Done()

	// Gracefully shutdown the server
	s.logger.Infof("shutting down the webSocket server on %s", addr)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		s.logger.Errorf("Failed to gracefully shutdown the server: %v", err)
	}

	if control := s.getControlChannel(); control != nil {
		_ = control.Close()
	}

}

func (s *WsTransport) getControlChannel() *websocket.Conn {
	s.controlMu.RLock()
	defer s.controlMu.RUnlock()
	return s.controlChannel
}

func (s *WsTransport) trySetControlChannel(conn *websocket.Conn) bool {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if s.controlChannel != nil {
		return false
	}
	s.controlChannel = conn
	return true
}

func (s *WsTransport) parsePortMappings() {
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

func (s *WsTransport) localListener(localAddr string, remoteAddr string) {
	portListener, err := net.Listen("tcp", localAddr)
	if err != nil {
		s.logger.Errorf("failed to start forwarding listener on %s: %v", localAddr, err)
		return
	}

	//close local listener after context cancellation
	defer portListener.Close()

	s.logger.Infof("listener started successfully, listening on address: %s", portListener.Addr().String())

	go s.acceptLocalConn(portListener, remoteAddr)

	<-s.ctx.Done()
}

func (s *WsTransport) acceptLocalConn(listener net.Listener, remoteAddr string) {
	for {
		select {
		case <-s.ctx.Done():
			return

		default:
			s.logger.Debugf("waiting to accept incoming connection on %s", listener.Addr().String())
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

func (s *WsTransport) handleLoop() {
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
				case tunnelConnection := <-s.tunnelChannel:
					close(tunnelConnection.ping)
					tunnelConnection.mu.Lock()
					if err := tunnelConnection.conn.WriteMessage(websocket.TextMessage, []byte(localConn.remoteAddr)); err != nil {
						s.logger.Debugf("%v", err) // failed to send port number
						tunnelConnection.conn.Close()
						continue loop
					}
					// Handle data exchange between connections
					go handlers.WSConnectionHandler(s.ctx, tunnelConnection.conn, localConn.conn, s.logger, s.usageMonitor, localConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
					break loop
				}
			}
		}
	}
}

func (s *WsTransport) keepAlive(conn *TunnelChannel) {
	ticker := time.NewTicker(s.config.Heartbeat) // Send periodic pings to the client

	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			conn.conn.Close()
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

			if err := conn.conn.WriteMessage(websocket.BinaryMessage, []byte{utils.SG_Ping}); err != nil {
				conn.mu.Unlock()
				conn.conn.Close()
				return
			}
			conn.mu.Unlock()
			s.logger.Trace("ping sent to the client")
		}
	}
}
