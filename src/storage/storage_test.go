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
	transactions     []types.Transaction
	blocks           []types.Block
	expectedMempools map[time.Time]([]types.Hash32)
}

func insertTestChain(st *Storage, testChain *TestChain) error {
	for _, tx := range testChain.transactions {
		if _, err := st.InsertTransaction(&tx); err != nil {
			return err
		}
	}

	for _, b := range testChain.blocks {
		if _, err := st.InsertBlock(&b); err != nil {
			return err
		}
	}

	return nil
}

func (t TestChain) ExpectedTransactionsForTime(at time.Time) []types.Hash32 {
	var maxBefore time.Time
	for k := range t.expectedMempools {
		k := k
		if !k.After(at) && k.After(maxBefore) {
			maxBefore = k
		}
	}
	return t.expectedMempools[maxBefore]
}

func txidsFromStrings(names ...string) (res []types.Hash32) {
	for _, n := range names {
		res = append(res, test.GenerateHash32(n))
	}
	return
}

func transactionIdsFromTxs(txs []types.Transaction) (res []types.Hash32) {
	for _, tx := range txs {
		res = append(res, tx.TxID)
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

	mempoolAtTime := map[time.Time]([]types.Hash32){
		GetTime(0):  {},
		GetTime(10): txidsFromStrings("tx-10"),
		GetTime(20): txidsFromStrings("tx-10", "tx-20"),
		GetTime(30): txidsFromStrings("tx-10", "tx-20", "tx-30"),

		// first block confirms tx-10
		GetTime(100): txidsFromStrings("tx-20", "tx-30", "tx-100"),
		GetTime(110): txidsFromStrings("tx-20", "tx-30", "tx-100", "tx-110"),
		GetTime(120): txidsFromStrings("tx-20", "tx-30", "tx-100", "tx-110", "tx-120"),

		// second block confirms tx-20, tx-100
		// transaction tx-200 gets added
		GetTime(200): txidsFromStrings("tx-30", "tx-110", "tx-120", "tx-200"),

		// transaction tx-210 gets added
		GetTime(210): txidsFromStrings("tx-30", "tx-110", "tx-120", "tx-200", "tx-210"),

		// transaction tx-220 gets added
		GetTime(220): txidsFromStrings("tx-30", "tx-110", "tx-120", "tx-200", "tx-210", "tx-220"),

		// third block confirms tx-30, tx-110
		GetTime(300): txidsFromStrings("tx-120", "tx-200", "tx-210", "tx-220"),

		// fourth block on minority chain -- doesn't change mempool yet.
		// GetSeconds(400):

		// Fifth block reorgs the chain.
		// Transactions that were removed in block 2 and 3 are back in the pool:
		// 		tx-20, tx-100, tx-30, tx-110
		// Transactions confirmed by block 1.1 and 1.2 will be removed
		//		tx-20, tx-200, tx-30, tx-210
		GetTime(500): txidsFromStrings("tx-100", "tx-110", "tx-120", "tx-220"),
	}

	return TestChain{
		transactions:     txs,
		blocks:           blocks,
		expectedMempools: mempoolAtTime,
	}
}
