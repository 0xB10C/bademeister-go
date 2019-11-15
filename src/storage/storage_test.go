package storage

import (
	"crypto/sha256"
	"os"
	"testing"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var startTime = time.Now().UTC().Truncate(time.Second)

func getTime(offsetSeconds int) time.Time {
	return startTime.Add(time.Duration(offsetSeconds) * time.Second)
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

// generateTestTxID returns the hash of a provided preimage.
func generateTestTxID(preimage []byte) types.Hash32 {
	return sha256.Sum256(preimage)
}

func TestStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	// The environment variable `TEST_INTEGRATION_DIR` is set to a temporary
	// directory created by the Makefile in the target `test-integration`.
	integrationTestDir := os.Getenv("TEST_INTEGRATION_DIR")
	path := integrationTestDir + "/mempool.db"

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatal(`could not reset mempool.db`)
	}

	st, err := NewStorage(path)
	require.NoError(t, err)

	// The nanoseconds are truncated, because the precision is lost
	// when writing the firstSeen unix timestamp to database.
	// Not truncating would result in unequal transactions when
	// comparing with assert.Equal().
	txs := []types.Transaction{
		{
			TxID:      generateTestTxID([]byte("tx 1")),
			FirstSeen: getTime(0),
			Fee:       232,
			Weight:    489,
		},
		{
			TxID:      generateTestTxID([]byte("tx 2")),
			FirstSeen: getTime(10),
			Fee:       1234567890,
			Weight:    12345,
		},
	}

	for _, tx := range txs {
		err := st.InsertTransaction(&tx)
		require.NoError(t, err)
	}

	testQueryTransactions(t, st, getTime(0), txs)
	err = st.Close()

	// re-open, req-run query tests
	st, err = NewStorage(path)
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
		testQueryTransactions(t, st, tm, []types.Transaction{txEarlier, txs[1]})
	}

	count, err := st.TxCount()
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}
