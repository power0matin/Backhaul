package transport

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/web"
	"github.com/xtaci/smux"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type WsMuxTransport struct {
	config          *WsMuxConfig
	smuxConfig      *smux.Config
	parentctx       context.Context
	ctx             context.Context
	cancel          context.CancelFunc
	logger          *logrus.Logger
	controlChannel  *websocket.Conn
	usageMonitor    *web.Usage
	restartOnce     sync.Once
	poolConnections int32
	loadConnections int32
	controlFlow     chan struct{}
	retireRequests  chan struct{}
	muxRetire       bool
}

const maxMuxRetireBatch = 8

type WsMuxConfig struct {
	RemoteAddr       string
	Token            string
	SnifferLog       string
	WebBindAddr      string
	WebUsername      string
	WebPassword      string
	Nodelay          bool
	Sniffer          bool
	KeepAlive        time.Duration
	RetryInterval    time.Duration
	DialTimeOut      time.Duration
	MuxVersion       int
	MaxFrameSize     int
	MaxReceiveBuffer int
	MaxStreamBuffer  int
	ConnPoolSize     int
	MaxPoolSize      int
	WebPort          int
	Mode             config.TransportType
	AggressivePool   bool
	EdgeIP           string
	TLSVerify        bool
}

func NewWSMuxClient(parentCtx context.Context, config *WsMuxConfig, logger *logrus.Logger) *WsMuxTransport {
	// Create a derived context from the parent context
	ctx, cancel := context.WithCancel(parentCtx)

	// Initialize the TcpTransport struct
	client := &WsMuxTransport{
		smuxConfig: &smux.Config{
			Version:           config.MuxVersion,
			KeepAliveInterval: 20 * time.Second,
			KeepAliveTimeout:  40 * time.Second,
			MaxFrameSize:      config.MaxFrameSize,
			MaxReceiveBuffer:  config.MaxReceiveBuffer,
			MaxStreamBuffer:   config.MaxStreamBuffer,
		},
		config:          config,
		parentctx:       parentCtx,
		ctx:             ctx,
		cancel:          cancel,
		logger:          logger,
		controlChannel:  nil, // will be set when a control connection is established
		usageMonitor:    web.NewDataStore(config.WebBindAddr, config.WebPort, ctx, config.SnifferLog, config.Sniffer, fmt.Sprintf("Disconnected (%s)", config.Mode), config.WebUsername, config.WebPassword, logger),
		poolConnections: 0,
		loadConnections: 0,
		controlFlow:     make(chan struct{}, 100),
		retireRequests:  make(chan struct{}, maxMuxRetireBatch),
	}

	return client
}

func (c *WsMuxTransport) Start() {
	if c.config.WebPort > 0 {
		go c.usageMonitor.Monitor()
	}

	c.usageMonitor.SetTunnelStatus(fmt.Sprintf("Disconnected (%s)", c.config.Mode))

	go c.channelDialer()
}

func (c *WsMuxTransport) Restart() {
	c.restartOnce.Do(func() {
		if c.parentctx.Err() != nil {
			return
		}
		c.usageMonitor.RecordReconnect()
		c.logger.Info("restarting client...")
		c.cancel()
		if c.controlChannel != nil {
			_ = c.controlChannel.Close()
		}
		if !utils.WaitForDelay(c.parentctx, 2*time.Second) {
			return
		}
		next := NewWSMuxClient(c.parentctx, c.config, c.logger)
		next.usageMonitor.InheritRuntimeMetrics(c.usageMonitor)
		go next.Start()
	})
}

func (c *WsMuxTransport) channelDialer() {
	c.logger.Infof("attempting to establish a new %s control channel connection", c.config.Mode)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:

			tunnelWSConn, err := network.WebSocketDialer(c.ctx, c.config.RemoteAddr, c.config.EdgeIP, "/channel", c.config.DialTimeOut, c.config.KeepAlive, true, c.config.Token, c.config.Mode, c.config.TLSVerify, 3, 0, 0, utils.WSMUXRetireSubprotocol)
			if err != nil {
				if c.ctx.Err() != nil {
					return
				}
				c.logger.Errorf("control channel dialer: %v", err)
				if !utils.WaitForDelay(c.ctx, c.config.RetryInterval) {
					return
				}
				continue
			}
			c.controlChannel = tunnelWSConn
			c.muxRetire = tunnelWSConn.Subprotocol() == utils.WSMUXRetireSubprotocol
			c.logger.Info("control channel established successfully")
			if c.muxRetire {
				c.logger.Debug("negotiated graceful WSMUX idle-session retirement")
			}

			c.usageMonitor.SetTunnelStatus(fmt.Sprintf("Connected (%s)", c.config.Mode))

			go c.poolMaintainer()
			go c.channelHandler()

			return
		}
	}
}

func (c *WsMuxTransport) poolMaintainer() {
	for i := 0; i < c.config.ConnPoolSize; i++ { //initial pool filling
		go c.tunnelDialer()
	}

	// factors
	a := 4
	b := 5
	x := 3
	y := 4.0

	if c.config.AggressivePool {
		c.logger.Info("aggressive pool management enabled")
		a = 1
		b = 2
		x = 0
		y = 0.75
	}

	tickerPool := time.NewTicker(time.Second * 1)
	defer tickerPool.Stop()

	tickerLoad := time.NewTicker(time.Second * 10)
	defer tickerLoad.Stop()

	newPoolSize := c.config.ConnPoolSize // intial value
	var poolConnectionsSum int32 = 0

	for {
		select {
		case <-c.ctx.Done():
			return

		case <-tickerPool.C:
			// Accumulate pool connections over time (every second)
			poolConnections := atomic.LoadInt32(&c.poolConnections)
			c.usageMonitor.SetPoolConnections(int64(poolConnections))
			atomic.AddInt32(&poolConnectionsSum, poolConnections)

		case <-tickerLoad.C:
			// Calculate the loadConnections over the last 10 seconds
			loadConnections := (int(atomic.LoadInt32(&c.loadConnections)) + 9) / 10 // +9 for ceil-like logic
			atomic.StoreInt32(&c.loadConnections, 0)                                // Reset

			// Calculate the average pool connections over the last 10 seconds
			poolConnectionsAvg := (int(atomic.LoadInt32(&poolConnectionsSum)) + 9) / 10 // +9 for ceil-like logic
			atomic.StoreInt32(&poolConnectionsSum, 0)                                   // Reset

			// Dynamically adjust the pool size based on current connections
			if (loadConnections+a) > poolConnectionsAvg*b && newPoolSize < c.config.MaxPoolSize {
				c.logger.Debugf("increasing pool size: %d -> %d, avg pool conn: %d, avg load conn: %d", newPoolSize, newPoolSize+1, poolConnectionsAvg, loadConnections)
				newPoolSize++

				// Add a new connection to the pool
				go c.tunnelDialer()
			} else if float64(loadConnections+x) < float64(poolConnectionsAvg)*y {
				decreasedTarget := false
				if newPoolSize > c.config.ConnPoolSize {
					c.logger.Debugf("decreasing pool target: %d -> %d, avg pool conn: %d, avg load conn: %d", newPoolSize, newPoolSize-1, poolConnectionsAvg, loadConnections)
					newPoolSize--
					decreasedTarget = true
				}

				currentPool := int(atomic.LoadInt32(&c.poolConnections))
				if c.muxRetire && currentPool > newPoolSize {
					c.queueMuxRetirements(currentPool - newPoolSize)
				} else if !c.muxRetire && decreasedTarget {
					// Preserve the legacy shrink hint when the peer does not support
					// graceful retirement: suppress one future server pool request.
					select {
					case c.controlFlow <- struct{}{}:
					default:
					}
				}
			}
		}
	}

}

func (c *WsMuxTransport) queueMuxRetirements(excess int) {
	if excess > maxMuxRetireBatch {
		excess = maxMuxRetireBatch
	}
	for i := 0; i < excess; i++ {
		select {
		case c.retireRequests <- struct{}{}:
		default:
			return
		}
	}
}

func (c *WsMuxTransport) channelHandler() {
	control := c.controlChannel
	defer control.Close()
	stopControlClose := context.AfterFunc(c.ctx, func() { _ = control.Close() })
	defer stopControlClose()
	msgChan := make(chan byte, 1000)

	// Goroutine to handle the blocking ReceiveBinaryString
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return

			default:
				_, msg, err := control.ReadMessage()
				if err != nil {
					if c.ctx.Err() == nil {
						c.logger.Warn("control channel closed; reconnecting: ", err)
						go c.Restart()
					}
					return
				}
				if len(msg) != 1 {
					c.logger.Warnf("invalid control message length %d; restarting", len(msg))
					go c.Restart()
					return
				}
				select {
				case msgChan <- msg[0]:
				case <-c.ctx.Done():
					return
				}
			}
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			_ = control.WriteMessage(websocket.BinaryMessage, []byte{utils.SG_Closed})
			return

		case <-c.retireRequests:
			if err := control.WriteMessage(websocket.BinaryMessage, []byte{utils.SG_MuxRetire}); err != nil {
				if c.ctx.Err() == nil {
					c.logger.Warn("failed to request WSMUX session retirement; reconnecting: ", err)
					go c.Restart()
				}
				return
			}

		case msg := <-msgChan:
			switch msg {
			case utils.SG_Chan:
				atomic.AddInt32(&c.loadConnections, 1)
				select {
				case <-c.controlFlow: // Do nothing

				default:
					c.logger.Debug("channel signal received, initiating tunnel dialer")
					go c.tunnelDialer()
				}

			case utils.SG_HB:
				c.logger.Debug("heartbeat received successfully")
				err := control.WriteMessage(websocket.BinaryMessage, []byte{utils.SG_HB})
				if err != nil {
					c.logger.Errorf("failed to send heartbeat: %v", msg)
					go c.Restart()
					return
				}
				c.logger.Trace("heartbeat signal sent successfully")

			case utils.SG_Closed:
				c.logger.Warn("control channel has been closed by the server")
				go c.Restart()
				return

			default:
				c.logger.Errorf("unexpected response from control channel: %v", msg)
				go c.Restart()
				return
			}

		}
	}
}

func (c *WsMuxTransport) tunnelDialer() {
	c.logger.Debugf("initiating new %s tunnel connection to address %s", c.config.Mode, c.config.RemoteAddr)

	// Dial to the tunnel server
	tunnelWSConn, err := network.WebSocketDialer(c.ctx, c.config.RemoteAddr, c.config.EdgeIP, "/tunnel", c.config.DialTimeOut, c.config.KeepAlive, c.config.Nodelay, c.config.Token, c.config.Mode, c.config.TLSVerify, 3, 2*1024*1024, 2*1024*1024)
	if err != nil {
		if c.ctx.Err() != nil {
			return
		}
		c.logger.Errorf("tunnel server dialer: %v", err)

		return
	}
	stopClose := context.AfterFunc(c.ctx, func() { _ = tunnelWSConn.Close() })
	defer stopClose()
	defer tunnelWSConn.Close()

	// Increment active connections counter
	atomic.AddInt32(&c.poolConnections, 1)

	c.handleSession(tunnelWSConn)
}

func (c *WsMuxTransport) handleSession(tunnelConn *websocket.Conn) {
	defer func() {
		atomic.AddInt32(&c.poolConnections, -1)
	}()

	// SMUX server
	session, err := smux.Server(tunnelConn.NetConn(), c.smuxConfig)
	if err != nil {
		c.logger.Errorf("failed to create mux session: %v", err)
		return
	}
	stopSession := context.AfterFunc(c.ctx, func() { _ = session.Close() })
	defer stopSession()
	defer session.Close()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			stream, err := session.AcceptStream()
			if err != nil {
				c.logger.Debug("session is closed: ", err)
				session.Close()
				return
			}

			remoteAddr, err := utils.ReceiveBinaryString(stream)
			if err != nil {
				c.logger.Errorf("unable to get port from stream connection %s: %v", tunnelConn.RemoteAddr().String(), err)
				stream.Close()
				continue
			}

			go c.localDialer(stream, remoteAddr)
		}
	}
}

func (c *WsMuxTransport) localDialer(stream *smux.Stream, remoteAddr string) {
	// Extract the port from the received address
	port, resolvedAddr, err := network.ResolveRemoteAddr(remoteAddr)
	if err != nil {
		c.logger.Infof("failed to resolve remote port: %v", err)
		stream.Close()
		return
	}

	var sendBuf, recvBuf int

	if strings.Contains(resolvedAddr, "127.0.0.1") {
		// Use 32 KB for localhost
		sendBuf = 32 * 1024
		recvBuf = 32 * 1024
	} else {
		// Use your custom buffer sizes
		sendBuf = 0
		recvBuf = 0
	}

	localConnection, err := network.TcpDialer(c.ctx, resolvedAddr, "", c.config.DialTimeOut, c.config.KeepAlive, true, 1, recvBuf, sendBuf, 0)
	if err != nil {
		c.logger.Errorf("local dialer: %v", err)
		stream.Close()
		return
	}

	c.logger.Debugf("connected to local address %s successfully", remoteAddr)

	handlers.TCPConnectionHandler(c.ctx, false, stream, localConnection, c.logger, c.usageMonitor, int(port), c.config.Sniffer)
}
