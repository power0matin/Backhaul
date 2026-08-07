package utils

import (
	"bytes"
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"
)

type shortWriteConn struct {
	bytes.Buffer
	maxWrite int
}

func (c *shortWriteConn) Write(p []byte) (int, error) {
	if len(p) > c.maxWrite {
		p = p[:c.maxWrite]
	}
	return c.Buffer.Write(p)
}

func (c *shortWriteConn) Read(p []byte) (int, error)       { return c.Buffer.Read(p) }
func (c *shortWriteConn) Close() error                     { return nil }
func (c *shortWriteConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (c *shortWriteConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (c *shortWriteConn) SetDeadline(time.Time) error      { return nil }
func (c *shortWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (c *shortWriteConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }

func TestSendBinaryStringHandlesShortWrites(t *testing.T) {
	conn := &shortWriteConn{maxWrite: 1}
	if err := SendBinaryString(conn, "backhaul"); err != nil {
		t.Fatalf("SendBinaryString: %v", err)
	}
	got := conn.Bytes()
	if len(got) != 2+len("backhaul") {
		t.Fatalf("framed length = %d, want %d", len(got), 2+len("backhaul"))
	}
	if length := binary.BigEndian.Uint16(got[:2]); length != uint16(len("backhaul")) {
		t.Fatalf("encoded length = %d", length)
	}
}

func TestBinaryFramingRejectsOversizedMessage(t *testing.T) {
	conn := &shortWriteConn{maxWrite: 1024}
	message := strings.Repeat("x", 65536)
	if err := SendBinaryString(conn, message); err == nil {
		t.Fatal("SendBinaryString accepted a message that cannot fit uint16 framing")
	}
	if err := SendBinaryTransportString(conn, message, SG_TCP); err == nil {
		t.Fatal("SendBinaryTransportString accepted a message that cannot fit uint16 framing")
	}
}

func TestSecureTokenEqual(t *testing.T) {
	if !SecureTokenEqual("correct horse", "correct horse") {
		t.Fatal("equal tokens did not match")
	}
	if SecureTokenEqual("correct horse", "wrong") {
		t.Fatal("different tokens matched")
	}
}
