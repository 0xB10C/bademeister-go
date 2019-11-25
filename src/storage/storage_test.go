package storage

import (
	"crypto/sha256"
	"fmt"
	"github.com/0xb10c/bademeister-go/src/types"
	"os"
	"time"
)

// GenerateHash32 returns the hash of a provided preimage.
func GenerateHash32(in string) types.Hash32 {
	return sha256.Sum256([]byte(in))
}

// The nanoseconds are truncated, because the precision is lost
// when writing the firstSeen unix timestamp to database.
// Not truncating would result in unequal transactions when
// comparing with assert.Equal().
var startTime = time.Unix(0, 0).UTC().Truncate(time.Second)

func GetTime(offsetSeconds int) time.Time {
	return startTime.Add(time.Duration(offsetSeconds) * time.Second)
}

func NewTxAtOffset(offsetSeconds int) *types.Transaction {
	return &types.Transaction{
		TxID:      GenerateHash32(fmt.Sprintf("tx-%d", offsetSeconds)),
		FirstSeen: GetTime(offsetSeconds),
		Fee:       uint64(100 + offsetSeconds),
		Weight:    100 + offsetSeconds,
	}
}

func StoragePath() string {
	// The environment variable `TEST_INTEGRATION_DIR` is set to a temporary
	// directory created by the Makefile in the target `test-integration`.
	integrationTestDir := os.Getenv("TEST_INTEGRATION_DIR")
	return integrationTestDir + "/mempool.db"
}

func NewTestStorage() (*Storage, error) {
	if err := os.Remove(StoragePath()); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf(`could not reset mempool.db: %s`, err)
	}

	return NewStorage(StoragePath())
}
