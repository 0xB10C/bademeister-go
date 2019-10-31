package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/0xb10c/bademeister-go/src/test"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestStorage(t *testing.T) {
	path := fmt.Sprintf("%s/storageTest.db", test.DataDir)

	if err := os.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			t.Fail()
		}
	}

	// create from empty file
	st, err := NewStorage(path)
	require.NoError(t, err)

	// The nanoseconds are truncated, because the precision is lost
	// when writing the firstSeen unix timestamp to database.
	// Not truncating would result in unequal transactions when
	// comparing with assert.Equal().
	tm := time.Now().UTC().Truncate(time.Second)
	txs := []types.Transaction{
		{
			TxID:      test.NewTestTxId(nil),
			FirstSeen: tm,
			Fee:       232,
			Size:      123,
		},
		{
			TxID:      test.NewTestTxId(nil),
			FirstSeen: tm.Add(10 * time.Second),
			Fee:       1234567890,
			Size:      12345,
		},
	}

	for _, tx := range txs {
		err := st.InsertTransaction(&tx)
		require.NoError(t, err)
	}

	testQueryTransactions(t, st, tm, txs)
	err = st.Close()

	// re-open, req-run query tests
	st, err = NewStorage(path)
	require.NoError(t, err)
	defer st.Close()
	testQueryTransactions(t, st, tm, txs)

	// repeated insertion with same txid upserts iff FirstSeen is lower
	{
		err = st.InsertTransaction(&txs[0])
		require.NoError(t, err)
		testQueryTransactions(t, st, tm, txs)

		txLater := txs[0]
		txLater.FirstSeen = txLater.FirstSeen.Add(10 * time.Second)
		err = st.InsertTransaction(&txLater)
		require.NoError(t, err)
		testQueryTransactions(t, st, tm, txs)
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
