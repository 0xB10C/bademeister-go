package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testQueryTransactions(t *testing.T, st *Storage, txs []types.Transaction) {
	// test query all txs
	txIter, err := st.QueryTransactions(StaticQuery{})
	require.NoError(t, err)
	defer txIter.Close()

	recoverTx := txIter.Next()
	require.NotNil(t, recoverTx)
	assert.Equal(t, txs[0], recoverTx.Transaction)

	recoverTx = txIter.Next()
	require.NotNil(t, recoverTx)
	assert.Equal(t, txs[1], recoverTx.Transaction)

	recoverTx = txIter.Next()
	assert.Nil(t, recoverTx)
}

func TestStorage_InsertTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	st, err := NewTestStorage()
	require.NoError(t, err)

	txs := []types.Transaction{
		*NewTxAtOffset(0),
		*NewTxAtOffset(10),
	}

	for _, tx := range txs {
		_, err := st.InsertTransaction(&tx)
		require.NoError(t, err)
	}

	testQueryTransactions(t, st, txs)
	err = st.Close()
	require.NoError(t, err)

	// re-open, req-run query tests
	st, err = NewStorage(StoragePath())
	require.NoError(t, err)
	defer st.Close()
	testQueryTransactions(t, st, txs)

	// repeated insertion with same txid upserts iff FirstSeen is lower
	{
		_, err = st.InsertTransaction(&txs[0])
		require.NoError(t, err)
		testQueryTransactions(t, st, txs)

		txLater := txs[0]
		txLater.FirstSeen = txLater.FirstSeen.Add(10 * time.Second)
		_, err = st.InsertTransaction(&txLater)
		require.NoError(t, err)
		testQueryTransactions(t, st, txs)
	}

	{
		txEarlier := txs[0]
		txEarlier.FirstSeen = txEarlier.FirstSeen.Add(-10 * time.Second)
		_, err = st.InsertTransaction(&txEarlier)
		require.NoError(t, err)
		testQueryTransactions(t, st, []types.Transaction{txEarlier, txs[1]})
	}

	count, err := st.TxCount()
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestStorage_NextTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	st, err := NewTestStorage()
	require.NoError(t, err)
	defer st.Close()

	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			tx := types.Transaction{
				TxID:      GenerateHash32(fmt.Sprintf("tx-%d-%d", i, j)),
				FirstSeen: GetTime(i),
			}
			dbid, err := st.InsertTransaction(&tx)
			require.Equal(t, int64(i*8+j+1), dbid)
			require.NoError(t, err)
		}
	}

	tx := &types.StoredTransaction{}
	tm := GetTime(0)
	for i := 1; ; i++ {
		txIter, err := st.NextTransactions(tm, tx.DBID, 1)
		require.NoError(t, err)
		defer txIter.Close()

		tx = txIter.Next()
		if tx == nil {
			break
		}
		tm = tx.FirstSeen

		assert.Equal(t, int64(i), tx.DBID)

		if i > 8*8 {
			t.Logf("invalid result count")
			t.Fail()
		}
	}
}

