package storage

import (
	"github.com/stretchr/testify/require"
	"log"
	"testing"
)

func TestMempool(t *testing.T) {
	st, err := NewTestStorage()
	require.NoError(t, err)
	defer st.Close()

	testChain := NewTestChainReorg()

	for _, tx := range testChain.transactions {
		_, err := st.InsertTransaction(&tx)
		require.NoError(t, err)
	}

	for _, b := range testChain.blocks {
		_, err := st.InsertBlock(&b)
		require.NoError(t, err)
	}

	txIter, err := st.QueryTransactions(StaticQuery{})
	require.NoError(t, err)
	txs := txIter.Collect()
	for _, tx := range txs {
		r := tx.LastRemoved
		switch tx.TxID {
		case GenerateHash32("tx-10"):
			require.Equal(t, GetTime(100), *r)
		case GenerateHash32("tx-20"):
			require.Equal(t, GetTime(500), *r)
		case GenerateHash32("tx-30"):
			require.Equal(t, GetTime(500), *r)
		case GenerateHash32("tx-100"):
			require.Nil(t, r)
		case GenerateHash32("tx-110"):
			require.Nil(t, r)
		case GenerateHash32("tx-120"):
			require.Nil(t, r)
		case GenerateHash32("tx-200"):
			require.Equal(t, GetTime(500), *r)
		case GenerateHash32("tx-210"):
			require.Equal(t, GetTime(500), *r)
		case GenerateHash32("tx-220"):
			require.Nil(t, r)
		default:
			t.Errorf("unknown TxID %s", tx.TxID)
		}
	}

	memZero, err := NewMempoolAtTime(st, GetTime(0))
	require.NoError(t, err)
	err = memZero.Seek(GetTime(500))
	require.NoError(t, err)
	require.ElementsMatch(
		t, testChain.ExpenctedTransactionsAt(GetTime(500)), transactionIdsFromTxs(memZero.Transactions()),
	)

	for targetSeconds := 0; targetSeconds < 600; targetSeconds++ {
		log.Printf("targetSeconds=%d", targetSeconds)
		for startSeconds := 0; startSeconds <= targetSeconds; startSeconds += 10 {
			mem, err := NewMempoolAtTime(st, GetTime(startSeconds))
			require.NoError(t, err)
			require.NotNil(t, mem)

			if startSeconds != targetSeconds {
				require.NoError(t, mem.Seek(GetTime(targetSeconds)))
			}

			expectedTxs := testChain.ExpenctedTransactionsAt(GetTime(targetSeconds))
			mempoolTxs := transactionIdsFromTxs(mem.Transactions())
			require.Equal(t, len(expectedTxs), len(mempoolTxs))
			require.ElementsMatch(t, expectedTxs, mempoolTxs)
		}
	}
}
