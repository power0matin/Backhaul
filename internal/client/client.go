package client

import (
	"context"
	"time"

	"github.com/musix/backhaul/internal/utils"

	"github.com/musix/backhaul/config"

	"github.com/musix/backhaul/internal/client/transport"

	"github.com/sirupsen/logrus"
)

// Client encapsulates the client configuration and state
type Client struct {
	config *config.ClientConfig
	ctx    context.Context
	cancel context.CancelFunc
	logger *logrus.Logger
}

func NewClient(cfg *config.ClientConfig, parentCtx context.Context) *Client {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Client{
		config: cfg,
		ctx:    ctx,
		cancel: cancel,
		logger: utils.NewLogger(cfg.LogLevel),
	}
}

// Run starts the client and begins dialing the tunnel server
func (c *Client) Start() {
	// for pprof
	if c.config.PPROF {
		if err := utils.StartPprof(c.ctx, "127.0.0.1:6061", c.logger); err != nil {
			c.logger.Errorf("failed to start pprof: %v", err)
		}
	}

	c.logger.Infof("client with remote address %s started successfully", c.config.RemoteAddr)
	if (c.config.Transport == config.WSS || c.config.Transport == config.WSSMUX) && !c.config.TLSVerify {
		c.logger.Warn("TLS certificate verification is disabled for backward compatibility; set tls_verify = true when using a trusted certificate")
	}

	switch c.config.Transport {
	case config.TCP:
		tcpConfig := &transport.TcpConfig{
			RemoteAddr:     c.config.RemoteAddr,
			Nodelay:        c.config.Nodelay,
			KeepAlive:      time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:  time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:    time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:   c.config.ConnectionPool,
			MaxPoolSize:    effectiveMaxPoolSize(c.config.ConnectionPool, c.config.MaxPoolSize),
			Token:          c.config.Token,
			Sniffer:        c.config.Sniffer,
			WebPort:        c.config.WebPort,
			WebBindAddr:    c.config.WebBindAddr,
			WebUsername:    c.config.WebUsername,
			WebPassword:    c.config.WebPassword,
			SnifferLog:     c.config.SnifferLog,
			AggressivePool: c.config.AggressivePool,
			MSS:            c.config.MSS,
			SO_RCVBUF:      c.config.SO_RCVBUF,
			SO_SNDBUF:      c.config.SO_SNDBUF,
		}
		tcpClient := transport.NewTCPClient(c.ctx, tcpConfig, c.logger)
		go tcpClient.Start()

	case config.TCPMUX:
		tcpMuxConfig := &transport.TcpMuxConfig{
			RemoteAddr:       c.config.RemoteAddr,
			Nodelay:          c.config.Nodelay,
			KeepAlive:        time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:    time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:      time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:     c.config.ConnectionPool,
			MaxPoolSize:      effectiveMaxPoolSize(c.config.ConnectionPool, c.config.MaxPoolSize),
			Token:            c.config.Token,
			MuxVersion:       c.config.MuxVersion,
			MaxFrameSize:     c.config.MaxFrameSize,
			MaxReceiveBuffer: c.config.MaxReceiveBuffer,
			MaxStreamBuffer:  c.config.MaxStreamBuffer,
			Sniffer:          c.config.Sniffer,
			WebPort:          c.config.WebPort,
			WebBindAddr:      c.config.WebBindAddr,
			WebUsername:      c.config.WebUsername,
			WebPassword:      c.config.WebPassword,
			SnifferLog:       c.config.SnifferLog,
			AggressivePool:   c.config.AggressivePool,
			MSS:              c.config.MSS,
			SO_RCVBUF:        c.config.SO_RCVBUF,
			SO_SNDBUF:        c.config.SO_SNDBUF,
		}
		tcpMuxClient := transport.NewMuxClient(c.ctx, tcpMuxConfig, c.logger)
		go tcpMuxClient.Start()

	case config.WS, config.WSS:
		WsConfig := &transport.WsConfig{
			RemoteAddr:     c.config.RemoteAddr,
			Nodelay:        c.config.Nodelay,
			KeepAlive:      time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:  time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:    time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:   c.config.ConnectionPool,
			MaxPoolSize:    effectiveMaxPoolSize(c.config.ConnectionPool, c.config.MaxPoolSize),
			Token:          c.config.Token,
			Sniffer:        c.config.Sniffer,
			WebPort:        c.config.WebPort,
			WebBindAddr:    c.config.WebBindAddr,
			WebUsername:    c.config.WebUsername,
			WebPassword:    c.config.WebPassword,
			SnifferLog:     c.config.SnifferLog,
			Mode:           c.config.Transport,
			AggressivePool: c.config.AggressivePool,
			EdgeIP:         c.config.EdgeIP,
			TLSVerify:      c.config.TLSVerify,
		}
		WsClient := transport.NewWSClient(c.ctx, WsConfig, c.logger)
		go WsClient.Start()

	case config.WSMUX, config.WSSMUX:
		wsMuxConfig := &transport.WsMuxConfig{
			RemoteAddr:       c.config.RemoteAddr,
			Nodelay:          c.config.Nodelay,
			KeepAlive:        time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:    time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:      time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:     c.config.ConnectionPool,
			MaxPoolSize:      effectiveMaxPoolSize(c.config.ConnectionPool, c.config.MaxPoolSize),
			Token:            c.config.Token,
			MuxVersion:       c.config.MuxVersion,
			MaxFrameSize:     c.config.MaxFrameSize,
			MaxReceiveBuffer: c.config.MaxReceiveBuffer,
			MaxStreamBuffer:  c.config.MaxStreamBuffer,
			Sniffer:          c.config.Sniffer,
			WebPort:          c.config.WebPort,
			WebBindAddr:      c.config.WebBindAddr,
			WebUsername:      c.config.WebUsername,
			WebPassword:      c.config.WebPassword,
			SnifferLog:       c.config.SnifferLog,
			Mode:             c.config.Transport,
			AggressivePool:   c.config.AggressivePool,
			EdgeIP:           c.config.EdgeIP,
			TLSVerify:        c.config.TLSVerify,
		}
		wsMuxClient := transport.NewWSMuxClient(c.ctx, wsMuxConfig, c.logger)
		go wsMuxClient.Start()

	case config.UDP:
		udpConfig := &transport.UdpConfig{
			RemoteAddr:     c.config.RemoteAddr,
			RetryInterval:  time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:    time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:   c.config.ConnectionPool,
			MaxPoolSize:    effectiveMaxPoolSize(c.config.ConnectionPool, c.config.MaxPoolSize),
			Token:          c.config.Token,
			Sniffer:        c.config.Sniffer,
			WebPort:        c.config.WebPort,
			WebBindAddr:    c.config.WebBindAddr,
			WebUsername:    c.config.WebUsername,
			WebPassword:    c.config.WebPassword,
			SnifferLog:     c.config.SnifferLog,
			AggressivePool: c.config.AggressivePool,
		}
		udpClient := transport.NewUDPClient(c.ctx, udpConfig, c.logger)
		go udpClient.Start()

	default:
		c.logger.Error("invalid transport type: ", c.config.Transport)
		c.cancel()
	}

	<-c.ctx.Done()

	c.logger.Info("all workers stopped successfully")

	// suppress other logs
	c.logger.SetLevel(logrus.FatalLevel)
}

// effectiveMaxPoolSize also protects callers that construct ClientConfig
// directly instead of going through cmd.LoadConfig. max_pool_size did not
// exist in v0.7.2, so a zero value must retain adaptive pool behavior while
// still imposing the new upper bound.
func effectiveMaxPoolSize(base, configured int) int {
	if configured > 0 {
		return configured
	}
	maximum := base * 4
	if minimum := base + 16; maximum < minimum {
		maximum = minimum
	}
	return maximum
}

func (c *Client) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
}
