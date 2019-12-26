package test

import (
	"crypto/sha256"

	"github.com/0xb10c/bademeister-go/src/types"
)

// GenerateHash32 returns the hash of a provided preimage.
func GenerateHash32(in string) types.Hash32 {
	return sha256.Sum256([]byte(in))
}
