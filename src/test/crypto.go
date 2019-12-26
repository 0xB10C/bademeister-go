package test

import (
	"crypto/sha256"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/hdkeychain"

	"github.com/0xb10c/bademeister-go/src/types"
)

// GenerateHash32 returns the hash of a provided preimage.
func GenerateHash32(seed string) types.Hash32 {
	return sha256.Sum256([]byte(seed))
}

// GetKeychain returns a keychain for a fixed seed
func GetKeychain(seed string) *hdkeychain.ExtendedKey {
	// NewMaster has a 16-byte limit, so we stretch it a bit here
	extendedSeed := GenerateHash32(seed)
	keychain, err := hdkeychain.NewMaster(extendedSeed[:], &chaincfg.RegressionNetParams)
	if err != nil {
		panic(err)
	}
	return keychain
}

// GetPrivateKeyWIF returns the private key in WIF (wallet import format)
func GetPrivateKeyWIF(seed string) *btcutil.WIF {
	keychain := GetKeychain(seed)
	privKey, err := keychain.ECPrivKey()
	if err != nil {
		panic(nil)
	}
	wif, err := btcutil.NewWIF(privKey, &chaincfg.RegressionNetParams, true)
	if err != nil {
		panic(nil)
	}
	return wif
}

// GetAddress returns the address for a keychain
func GetAddress(seed string) btcutil.Address {
	keychain := GetKeychain(seed)
	addr, err := keychain.Address(&chaincfg.RegressionNetParams)
	if err != nil {
		panic(err)
	}
	return addr
}

// MiningSeed is the seed used for deriving the mining address
var MiningSeed = "mining"

// MiningAddress is the address that collects block rewards
var MiningAddress = GetAddress(MiningSeed)
