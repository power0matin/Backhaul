package utils

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	dataChannelChallengeSize = sha256.Size
	dataChannelServerTimeout = 500 * time.Millisecond
	dataChannelClientTimeout = 3 * time.Second
)

var dataChannelProofDomain = []byte("backhaul/data-channel-auth/v1\x00")

type bufferedDataConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedDataConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

// AuthenticateDataChannelClient proves possession of the configured tunnel
// token on a newly established TCP data connection. The token itself is never
// sent on pooled data connections.
func AuthenticateDataChannelClient(conn net.Conn, token string) error {
	if conn == nil {
		return errors.New("data channel connection is nil")
	}
	if err := conn.SetDeadline(time.Now().Add(dataChannelClientTimeout)); err != nil {
		return fmt.Errorf("set data-channel authentication deadline: %w", err)
	}
	defer conn.SetDeadline(time.Time{})

	if err := writeAll(conn, []byte{SG_DataAuth}); err != nil {
		return fmt.Errorf("send data-channel authentication marker: %w", err)
	}

	challenge := make([]byte, dataChannelChallengeSize)
	if _, err := io.ReadFull(conn, challenge); err != nil {
		return fmt.Errorf("read data-channel challenge: %w", err)
	}
	proof := dataChannelProof(token, challenge)
	if err := writeAll(conn, proof); err != nil {
		return fmt.Errorf("send data-channel proof: %w", err)
	}
	return nil
}

// AuthenticateDataChannelServer authenticates a pooled TCP connection with a
// one-time server challenge and a token-derived HMAC proof. When legacy mode is
// explicitly enabled, a connection whose first byte is not the auth marker is
// returned through a buffered wrapper so a legacy TCPMUX SMUX preamble is not
// consumed. A silent legacy TCP connection is also accepted only in that mode.
func AuthenticateDataChannelServer(conn net.Conn, token string, allowLegacy bool) (net.Conn, bool, error) {
	if conn == nil {
		return nil, false, errors.New("data channel connection is nil")
	}
	if err := conn.SetDeadline(time.Now().Add(dataChannelServerTimeout)); err != nil {
		return nil, false, fmt.Errorf("set data-channel authentication deadline: %w", err)
	}
	defer conn.SetDeadline(time.Time{})

	reader := bufio.NewReader(conn)
	marker, err := reader.Peek(1)
	if err != nil {
		var netErr net.Error
		if allowLegacy && errors.As(err, &netErr) && netErr.Timeout() {
			return conn, true, nil
		}
		return nil, false, fmt.Errorf("read data-channel authentication marker: %w", err)
	}
	if marker[0] != SG_DataAuth {
		if allowLegacy {
			return &bufferedDataConn{Conn: conn, reader: reader}, true, nil
		}
		return nil, false, fmt.Errorf("unexpected data-channel authentication marker %d", marker[0])
	}
	if _, err := reader.ReadByte(); err != nil {
		return nil, false, fmt.Errorf("consume data-channel authentication marker: %w", err)
	}

	challenge := make([]byte, dataChannelChallengeSize)
	if _, err := rand.Read(challenge); err != nil {
		return nil, false, fmt.Errorf("generate data-channel challenge: %w", err)
	}
	if err := writeAll(conn, challenge); err != nil {
		return nil, false, fmt.Errorf("send data-channel challenge: %w", err)
	}

	proof := make([]byte, dataChannelChallengeSize)
	if _, err := io.ReadFull(reader, proof); err != nil {
		return nil, false, fmt.Errorf("read data-channel proof: %w", err)
	}
	expected := dataChannelProof(token, challenge)
	if !hmac.Equal(proof, expected) {
		return nil, false, errors.New("invalid data-channel authentication proof")
	}
	return conn, false, nil
}

func dataChannelProof(token string, challenge []byte) []byte {
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write(dataChannelProofDomain)
	_, _ = mac.Write(challenge)
	return mac.Sum(nil)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
