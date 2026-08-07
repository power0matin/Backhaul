package transport

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

const localQueueWait = 250 * time.Millisecond

const (
	defaultUDPQueueSize  = 64
	defaultUDPQueueLimit = 4096
	defaultUDPMaxFlows   = 2048
)

// packetBudget caps the number of retained UDP packet payloads across all
// flows owned by one transport. A per-flow channel bound alone is not enough:
// an attacker can multiply that capacity by creating many source addresses.
type packetBudget struct {
	slots chan struct{}
}

func newPacketBudget(limit int) *packetBudget {
	if limit <= 0 {
		limit = defaultUDPQueueLimit
	}
	return &packetBudget{slots: make(chan struct{}, limit)}
}

func udpQueueSize(size int) int {
	if size <= 0 {
		return defaultUDPQueueSize
	}
	return size
}

func udpMaxFlows(limit int) int {
	if limit <= 0 {
		return defaultUDPMaxFlows
	}
	return limit
}

func (b *packetBudget) enqueue(queue chan []byte, data []byte) bool {
	select {
	case b.slots <- struct{}{}:
	default:
		return false
	}

	payload := append([]byte(nil), data...)
	select {
	case queue <- payload:
		return true
	default:
		<-b.slots
		return false
	}
}

// enqueueFramedUDP reserves the TCP length header in the same allocation used
// to take ownership of an incoming UDP datagram. accept_udp otherwise copied
// every payload a second time solely to prepend this two-byte header.
func (b *packetBudget) enqueueFramedUDP(queue chan []byte, data []byte) bool {
	if len(data) > int(^uint16(0)) {
		return false
	}
	select {
	case b.slots <- struct{}{}:
	default:
		return false
	}

	packet := make([]byte, 2+len(data))
	binary.BigEndian.PutUint16(packet[:2], uint16(len(data)))
	copy(packet[2:], data)
	select {
	case queue <- packet:
		return true
	default:
		<-b.slots
		return false
	}
}

func (b *packetBudget) release() {
	<-b.slots
}

// closeQueue must be called while the flow map lock prevents concurrent
// producers from sending to queue.
func (b *packetBudget) closeQueue(queue chan []byte) {
	close(queue)
	for range queue {
		b.release()
	}
}

// enqueueLocalTCP applies bounded backpressure to short userspace queue
// bursts. It deliberately does not create a goroutine per accepted socket and
// never grows the queue beyond its configured capacity.
func enqueueLocalTCP(ctx context.Context, queue chan LocalTCPConn, conn LocalTCPConn) bool {
	// Do not allocate/register a timer on the normal path. Backpressure only
	// needs a timer after the bounded queue is actually saturated.
	select {
	case queue <- conn:
		return true
	case <-ctx.Done():
		return false
	default:
	}

	timer := time.NewTimer(localQueueWait)
	defer timer.Stop()

	select {
	case queue <- conn:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

func recordRejected(usage *web.Usage, logger *logrus.Logger, category string) {
	if total, logNow := usage.RecordRejected(); logNow {
		logger.Warnf("%s (total rejected/dropped=%d)", category, total)
	}
}

type TunnelChannel struct { // for websocket
	conn *websocket.Conn
	ping chan struct{}
	mu   *sync.Mutex
}

type LocalTCPConn struct {
	conn        net.Conn
	remoteAddr  string
	timeCreated int64
}

type LocalAcceptUDPConn struct {
	timeCreated int64
	payload     chan []byte
	remoteAddr  string
	listener    *net.UDPConn
	clientAddr  *net.UDPAddr
	isCongested atomic.Bool // for congested tcp connection
}

type LocalUDPConn struct {
	timeCreated int64
	payload     chan []byte
	remoteAddr  string
	listener    *net.UDPConn
	addr        *net.UDPAddr
}

type TunnelUDPConn struct {
	timeCreated int64
	payload     chan []byte
	addr        *net.UDPAddr
	listener    *net.UDPConn
	ping        chan struct{}
	mu          *sync.Mutex //mutex for ping channel
}
