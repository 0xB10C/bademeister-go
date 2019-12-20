package types

import (
	"encoding/hex"
	"fmt"
)

// Hash32 is a 32 byte / 256 bit hash.
// This hash is used for block header hashes and transaction IDs.
type Hash32 [32]byte

func (h Hash32) String() string {
	return hex.EncodeToString(h[:])
}

// NewHashFromBytes returns a new Hash32 from a 32-length byte slice.
// Panics on length mismatch.
func NewHashFromBytes(bytes []byte) (res Hash32) {
	if len(bytes) != 32 {
		// for ergonomics, we do not return an error here
		panic(fmt.Errorf("invalid hash length"))
	}
	copy(res[:], bytes)
	return
}

// NewHashFromArray returns a new Hash32 from a 32-length byte array.
func NewHashFromArray(bytes [32]byte) Hash32 {
	return NewHashFromBytes(bytes[:])
}
