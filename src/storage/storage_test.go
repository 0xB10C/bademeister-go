package storage

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The nanoseconds are truncated, because the precision is lost
// when writing the firstSeen unix timestamp to database.
// Not truncating would result in unequal transactions when
// comparing with assert.Equal().
var startTime = time.Now().UTC().Truncate(time.Second)

func getTime(offsetSeconds int) time.Time {
	return startTime.Add(time.Duration(offsetSeconds) * time.Second)
}

func newTxAtOffset(offsetSeconds int) *types.Transaction {
	return &types.Transaction{
		TxID:      generateHash32([]byte(fmt.Sprintf("tx-%d", offsetSeconds))),
		FirstSeen: getTime(offsetSeconds),
		Fee:       uint64(100 + offsetSeconds),
		Weight:    100 + offsetSeconds,
	}
}

func testQueryTransactions(t *testing.T, st *Storage, firstSeen time.Time, txs []types.Transaction) {
	// test query all txs
	{
		txIter, err := st.QueryTransactions(Query{})
		require.NoError(t, err)

		recoverTx := txIter.Next()
		require.NotNil(t, recoverTx)
		assert.Equal(t, txs[0], *recoverTx)

		recoverTx = txIter.Next()
		require.NotNil(t, recoverTx)
		assert.Equal(t, txs[1], *recoverTx)

		assert.Nil(t, txIter.Next())
	}

	// test query with FirstSeen
	{
		txIter, err := st.QueryTransactions(Query{
			FirstSeen: &firstSeen,
		})
		require.NoError(t, err)

		recoverTx := txIter.Next()
		require.NotNil(t, recoverTx)
		assert.Equal(t, txs[1], *recoverTx)

		assert.Nil(t, txIter.Next())
	}
}

// generateHash32 returns the hash of a provided preimage.
func generateHash32(preimage []byte) types.Hash32 {
	return sha256.Sum256(preimage)
}

func testStoragePath() string {
	// The environment variable `TEST_INTEGRATION_DIR` is set to a temporary
	// directory created by the Makefile in the target `test-integration`.
	integrationTestDir := os.Getenv("TEST_INTEGRATION_DIR")
	return integrationTestDir + "/mempool.db"
}

func newTestStorage() (*Storage, error) {
	if err := os.Remove(testStoragePath()); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf(`could not reset mempool.db: %s`, err)
	}

	return NewStorage(testStoragePath())
}

func TestStorage_InsertTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	st, err := newTestStorage()
	require.NoError(t, err)

	txs := []types.Transaction{
		*newTxAtOffset(0),
		*newTxAtOffset(10),
	}

	for _, tx := range txs {
		err := st.InsertTransaction(&tx)
		require.NoError(t, err)
	}

	testQueryTransactions(t, st, getTime(0), txs)
	err = st.Close()

	// re-open, req-run query tests
	st, err = NewStorage(testStoragePath())
	require.NoError(t, err)
	defer st.Close()
	testQueryTransactions(t, st, getTime(0), txs)

	// repeated insertion with same txid upserts iff FirstSeen is lower
	{
		err = st.InsertTransaction(&txs[0])
		require.NoError(t, err)
		testQueryTransactions(t, st, getTime(0), txs)

		txLater := txs[0]
		txLater.FirstSeen = txLater.FirstSeen.Add(10 * time.Second)
		err = st.InsertTransaction(&txLater)
		require.NoError(t, err)
		testQueryTransactions(t, st, getTime(0), txs)
	}

	{
		txEarlier := txs[0]
		txEarlier.FirstSeen = txEarlier.FirstSeen.Add(-10 * time.Second)
		err = st.InsertTransaction(&txEarlier)
		require.NoError(t, err)
		testQueryTransactions(t, st, getTime(0), []types.Transaction{txEarlier, txs[1]})
	}

	count, err := st.TxCount()
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestStorage_InsertBlock(t *testing.T) {
	st, err := newTestStorage()
	require.NoError(t, err)

	txs := []types.Transaction{
		*newTxAtOffset(10),
		*newTxAtOffset(20),
	}

	blocks := []types.Block{
		{
			Hash:         generateHash32([]byte("hash 1")),
			FirstSeen:    getTime(0),
			Transactions: []types.Transaction{txs[0]},
			Height:       0,
		},
		{
			Hash:         generateHash32([]byte("hash 2")),
			FirstSeen:    getTime(10),
			Transactions: []types.Transaction{txs[0], txs[1]},
			Height:       1,
		},
	}

	{
		err = st.InsertBlock(&blocks[0])
		require.NoError(t, err);
		err = st.InsertBlock(&blocks[1])
		require.NoError(t, err);
	}

	{
		txIter, err := st.TransactionsInBlock(blocks[0].Hash)
		require.NoError(t, err)
		require.Equal(t, blocks[0].Transactions, txIter.Collect())
	}

	{
		txIter, err := st.TransactionsInBlock(blocks[1].Hash)
		require.NoError(t, err)
		require.Equal(t, blocks[1].Transactions, txIter.Collect())
	}
}
