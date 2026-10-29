package handlers

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

type proxyProtocolTestConn struct {
	localAddr  net.Addr
	remoteAddr net.Addr
	written    []byte
}

func (c *proxyProtocolTestConn) Read(p []byte) (int, error) { return 0, nil }
func (c *proxyProtocolTestConn) Write(p []byte) (int, error) {
	c.written = append(c.written, p...)
	return len(p), nil
}
func (c *proxyProtocolTestConn) Close() error                       { return nil }
func (c *proxyProtocolTestConn) LocalAddr() net.Addr                { return c.localAddr }
func (c *proxyProtocolTestConn) RemoteAddr() net.Addr               { return c.remoteAddr }
func (c *proxyProtocolTestConn) SetDeadline(time.Time) error        { return nil }
func (c *proxyProtocolTestConn) SetReadDeadline(time.Time) error   { return nil }
func (c *proxyProtocolTestConn) SetWriteDeadline(time.Time) error  { return nil }

func TestWriteProxyProtocolUsesOriginalForwardingDestination(t *testing.T) {
	from := &proxyProtocolTestConn{
		localAddr:  &net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 443},
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 54321},
	}
	// The tunnel peer address must not be reported as the PROXY destination.
	to := &proxyProtocolTestConn{
		localAddr:  &net.TCPAddr{IP: net.ParseIP("192.0.2.20"), Port: 40000},
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("198.51.100.20"), Port: 3080},
	}
	if err := WriteProxyProtocol(from, to); err != nil {
		t.Fatalf("WriteProxyProtocol: %v", err)
	}
	header := to.written
	if len(header) != 28 {
		t.Fatalf("header length = %d, want 28", len(header))
	}
	if header[12] != 0x21 || header[13] != 0x11 {
		t.Fatalf("unexpected PROXY v2 command/family bytes: %x %x", header[12], header[13])
	}
	if got, want := net.IP(header[16:20]).String(), "203.0.113.7"; got != want {
		t.Fatalf("source IP = %s, want %s", got, want)
	}
	if got, want := net.IP(header[20:24]).String(), "192.0.2.10"; got != want {
		t.Fatalf("destination IP = %s, want original local endpoint %s", got, want)
	}
	if got, want := binary.BigEndian.Uint16(header[24:26]), uint16(54321); got != want {
		t.Fatalf("source port = %d, want %d", got, want)
	}
	if got, want := binary.BigEndian.Uint16(header[26:28]), uint16(443); got != want {
		t.Fatalf("destination port = %d, want original local port %d", got, want)
	}
}
