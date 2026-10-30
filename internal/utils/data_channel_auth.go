package utils

import (
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
// one-time server challenge and a token-derived HMAC proof. It returns
// legacy=true only when no authentication marker arrives before the deadline
// and the operator has explicitly enabled legacy data connections.
//
// Legacy mode exists only to support a controlled rolling upgrade. It must be
// disabled again after every client has been upgraded.
func AuthenticateDataChannelServer(conn net.Conn, token string, allowLegacy bool) (legacy bool, err error) {
	if conn == nil {
		return false, errors.New("data channel connection is nil")
	}
	if err := conn.SetDeadline(time.Now().Add(dataChannelServerTimeout)); err != nil {
		return false, fmt.Errorf("set data-channel authentication deadline: %w", err)
	}
	defer conn.SetDeadline(time.Time{})

	var marker [1]byte
	if _, err := io.ReadFull(conn, marker[:]); err != nil {
		var netErr net.Error
		if allowLegacy && errors.As(err, &netErr) && netErr.Timeout() {
			return true, nil
		}
		return false, fmt.Errorf("read data-channel authentication marker: %w", err)
	}
	if marker[0] != SG_DataAuth {
		return false, fmt.Errorf("unexpected data-channel authentication marker %d", marker[0])
	}

	challenge := make([]byte, dataChannelChallengeSize)
	if _, err := rand.Read(challenge); err != nil {
		return false, fmt.Errorf("generate data-channel challenge: %w", err)
	}
	if err := writeAll(conn, challenge); err != nil {
		return false, fmt.Errorf("send data-channel challenge: %w", err)
	}

	proof := make([]byte, dataChannelChallengeSize)
	if _, err := io.ReadFull(conn, proof); err != nil {
		return false, fmt.Errorf("read data-channel proof: %w", err)
	}
	expected := dataChannelProof(token, challenge)
	if !hmac.Equal(proof, expected) {
		return false, errors.New("invalid data-channel authentication proof")
	}
	return false, nil
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
