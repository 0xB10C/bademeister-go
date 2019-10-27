package storage

import (
	"fmt"
	"github.com/0xb10c/bademeister-go/src/test"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
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
	tm := time.Now().UTC()

	txs := []types.Transaction{
		{
			TxID:      test.NewTestTxId(nil),
			FirstSeen: tm,
		},
		{
			TxID:      test.NewTestTxId(nil),
			FirstSeen: tm.Add(10 * time.Second),
		},
	}

	for _, tx := range txs {
		err := st.AddTransaction(&tx)
		require.NoError(t, err)
	}

	testQueryTransactions(t, st, tm, txs)
	err = st.Close()

	// re-open, req-run query tests
	st, err = NewStorage(path)
	require.NoError(t, err)
	defer st.Close()
	testQueryTransactions(t, st, tm, txs)
}
