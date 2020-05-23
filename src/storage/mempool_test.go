package storage

import (
	"github.com/0xb10c/bademeister-go/src/test"

	"testing"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testMempoolEvent(t *testing.T, mem *Mempool, testChain TestChain, event *Event, nEvent int) {
	require.NotNil(t, event)

	storedBlocks := []types.StoredBlock{}
	for _, b := range testChain.blocks {
		storedBlock, err := mem.storage.BlockByHash(b.Hash)
		require.NoError(t, err)
		storedBlocks = append(storedBlocks, *storedBlock)
	}

	switch event.Type {
	case EnterMempool:
		assert.Len(t, event.AddTransactions, 1)
		assert.Equal(t, event.Time, event.AddTransactions[0].FirstSeen)
		assert.Equal(t, []int64{}, event.RemoveTransactions)
		assert.Equal(t, event.AddTransactions[0], *event.NewTransaction)
	case BlockConfirmation:
		dbids, err := mem.storage.TransactionDBIDsInBlock(event.NewBlock.DBID)
		require.NoError(t, err)
		assert.Len(t, event.AddTransactions, 0)
		assert.ElementsMatch(t, dbids, event.RemoveTransactions)
	}

	switch nEvent {
	case 0:
		assert.Equal(t, GetTime(10), event.Time)
		assert.Equal(t, EnterMempool, event.Type)
	case 1:
		assert.Equal(t, GetTime(20), event.Time)
		assert.Equal(t, EnterMempool, event.Type)
	case 2:
		assert.Equal(t, GetTime(30), event.Time)
		assert.Equal(t, EnterMempool, event.Type)
	case 3:
		assert.Equal(t, GetTime(100), event.Time)
		assert.Equal(t, EnterMempool, event.Type)
	case 4:
		assert.Equal(t, GetTime(100), event.Time)
		assert.Equal(t, BlockConfirmation, event.Type)
	case 5:
		assert.Equal(t, GetTime(110), event.Time)
		assert.Equal(t, EnterMempool, event.Type)
	case 6:
		assert.Equal(t, GetTime(120), event.Time)
		assert.Equal(t, EnterMempool, event.Type)
	case 7:
		assert.Equal(t, GetTime(200), event.Time)
		assert.Equal(t, EnterMempool, event.Type)
	case 8:
		assert.Equal(t, GetTime(200), event.Time)
		assert.Equal(t, BlockConfirmation, event.Type)
	case 9:
		assert.Equal(t, GetTime(210), event.Time)
		assert.Equal(t, EnterMempool, event.Type)
	case 10:
		assert.Equal(t, GetTime(220), event.Time)
		assert.Equal(t, EnterMempool, event.Type)
	case 11:
		assert.Equal(t, GetTime(300), event.Time)
		assert.Equal(t, BlockConfirmation, event.Type)
	case 12:
		assert.Equal(t, GetTime(500), event.Time)
		assert.Equal(t, BlockReorg, event.Type)

		addTxs := []types.StoredTransaction{}
		block1Txs, err := mem.storage.TransactionsInBlock(storedBlocks[1].DBID)
		require.NoError(t, err)
		addTxs = append(addTxs, block1Txs.Collect()...)

		block2Txs, err := mem.storage.TransactionsInBlock(storedBlocks[2].DBID)
		require.NoError(t, err)
		addTxs = append(addTxs, block2Txs.Collect()...)

		assert.ElementsMatch(t, addTxs, event.AddTransactions)

		removeTxs := []int64{}
		blocks3TxIDs, err := mem.storage.TransactionDBIDsInBlock(storedBlocks[3].DBID)
		require.NoError(t, err)
		removeTxs = append(removeTxs, blocks3TxIDs...)

		blocks4TxIDs, err := mem.storage.TransactionDBIDsInBlock(storedBlocks[4].DBID)
		require.NoError(t, err)
		removeTxs = append(removeTxs, blocks4TxIDs...)

		assert.ElementsMatch(t, removeTxs, event.RemoveTransactions)
	default:
		t.Errorf("unexpected nEvent %d", nEvent)
	}
}

func TestMempool_NextEvent(t *testing.T) {
	test.SkipIfShort(t)

	st, err := NewTestStorage()
	require.NoError(t, err)
	defer st.Close()

	testChain := NewTestChainReorg()
	require.NoError(t, insertTestChain(st, &testChain))

	mem, err := NewMempoolAtTime(st, GetTime(0))
	require.NoError(t, err)

	for i := 0; ; i++ {
		event, err := mem.NextEvent()
		require.NoError(t, err)
		if event == nil {
			break
		}
		testMempoolEvent(t, mem, testChain, event, i)
		require.NoError(t, mem.ApplyEvent(event))
	}
}

func TestMempool(t *testing.T) {
	test.SkipIfShort(t)

	st, err := NewTestStorage()
	require.NoError(t, err)
	defer st.Close()

	testChain := NewTestChainReorg()
	require.NoError(t, insertTestChain(st, &testChain))

	txIter, err := st.QueryTransactions(StaticQuery{})
	require.NoError(t, err)
	txs := txIter.Collect()
	for _, tx := range txs {
		r := tx.LastRemoved
		switch tx.TxID {
		case test.GenerateHash32("tx-10"):
			require.Equal(t, GetTime(100), *r)
		case test.GenerateHash32("tx-20"):
			require.Equal(t, GetTime(500), *r)
		case test.GenerateHash32("tx-30"):
			require.Equal(t, GetTime(500), *r)
		case test.GenerateHash32("tx-100"):
			require.Nil(t, r)
		case test.GenerateHash32("tx-110"):
			require.Nil(t, r)
		case test.GenerateHash32("tx-120"):
			require.Nil(t, r)
		case test.GenerateHash32("tx-200"):
			require.Equal(t, GetTime(500), *r)
		case test.GenerateHash32("tx-210"):
			require.Equal(t, GetTime(500), *r)
		case test.GenerateHash32("tx-220"):
			require.Nil(t, r)
		default:
			t.Errorf("unknown TxID %s", tx.TxID)
		}
	}

	for targetSeconds := 0; targetSeconds < 600; targetSeconds++ {
		t.Logf("targetSeconds=%d", targetSeconds)
		for startSeconds := 0; startSeconds <= targetSeconds; startSeconds += 10 {
			mem, err := NewMempoolAtTime(st, GetTime(startSeconds))
			require.NoError(t, err)
			require.NotNil(t, mem)

			if startSeconds != targetSeconds {
				require.NoError(t, mem.Seek(GetTime(targetSeconds)))
			}

			expectedTxs := testChain.ExpectedTransactionsForTime(GetTime(targetSeconds))
			mempoolTxs := transactionIdsFromTxs(mem.Transactions())
			require.Equal(t, len(expectedTxs), len(mempoolTxs))
			require.ElementsMatch(t, expectedTxs, mempoolTxs)
		}
	}
}
