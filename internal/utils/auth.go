package utils

import (
	"crypto/sha256"
	"crypto/subtle"
)

// SecureTokenEqual compares authentication tokens without data-dependent early
// exit. Hashing first also avoids crypto/subtle's early return on unequal input
// lengths.
func SecureTokenEqual(a, b string) bool {
	aHash := sha256.Sum256([]byte(a))
	bHash := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(aHash[:], bHash[:]) == 1
}
