package storage

import (
	"fmt"
	"os"
	"time"

	"github.com/0xb10c/bademeister-go/src/test"

	"github.com/0xb10c/bademeister-go/src/types"
)

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
		TxID:      test.GenerateHash32(fmt.Sprintf("tx-%d", offsetSeconds)),
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

type TestChain struct {
	transactions []types.Transaction
	blocks       []types.Block
}

func txidsFromStrings(names ...string) (res []types.Hash32) {
	for _, n := range names {
		res = append(res, test.GenerateHash32(n))
	}
	return
}

func NewTestChainReorg() TestChain {
	txs := []types.Transaction{
		*NewTxAtOffset(10),
		*NewTxAtOffset(20),
		*NewTxAtOffset(30),

		*NewTxAtOffset(100),
		*NewTxAtOffset(110),
		*NewTxAtOffset(120),

		*NewTxAtOffset(200),
		*NewTxAtOffset(210),
		*NewTxAtOffset(220),
	}

	blocks := []types.Block{
		{
			Hash:      test.GenerateHash32("1"),
			FirstSeen: GetTime(100),
			TxIDs:     txidsFromStrings("tx-10"),
			IsBest:    true,
			Height:    0,
		},

		// this block will be reorged
		{
			Parent:    test.GenerateHash32("1"),
			Hash:      test.GenerateHash32("2"),
			FirstSeen: GetTime(200),
			TxIDs:     txidsFromStrings("tx-20", "tx-100"),
			IsBest:    true,
			Height:    1,
		},

		// this block will be reorged
		{
			Parent:    test.GenerateHash32("2"),
			Hash:      test.GenerateHash32("3"),
			FirstSeen: GetTime(300),
			TxIDs:     txidsFromStrings("tx-30", "tx-110"),
			IsBest:    true,
			Height:    2,
		},

		// Minority chain - since `IsBest=false` this does not affect `last_removed` yet
		{
			Parent:    test.GenerateHash32("1"),
			Hash:      test.GenerateHash32("1.1"),
			FirstSeen: GetTime(400),
			TxIDs:     txidsFromStrings("tx-20", "tx-200"),
			IsBest:    false,
			Height:    1,
		},

		// Adding this block will reorg the chain (`IsBest=true` and parent is not current best)
		{
			Parent:    test.GenerateHash32("1.1"),
			Hash:      test.GenerateHash32("1.2"),
			FirstSeen: GetTime(500),
			TxIDs:     txidsFromStrings("tx-30", "tx-210"),
			IsBest:    true,
			Height:    2,
		},
	}

	return TestChain{
		transactions: txs,
		blocks:       blocks,
	}
}
