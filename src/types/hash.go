package types

import "encoding/hex"

// Hash32 is a 32 byte / 256 bit hash.
// This hash is used for block header hashes and transaction IDs.
type Hash32 [32]byte

func (h Hash32) String() string {
	return hex.EncodeToString(h[:])
}
