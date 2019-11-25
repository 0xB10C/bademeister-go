package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorage_InsertBlock(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	st, err := NewTestStorage()
	require.NoError(t, err)
	defer st.Close()

	testChain := NewTestChainReorg()
	blocks := testChain.blocks

	{
		_, err := st.InsertBlock(&blocks[0])
		require.Error(t, err)
		require.True(t, IsErrorMissingTransactions(err), err)
		block, err := st.BestBlockNow()
		require.NoError(t, err)
		require.Nil(t, block)
	}

	for _, tx := range testChain.transactions {
		_, err := st.InsertTransaction(&tx)
		require.NoError(t, err)
	}

	testLastRemoved := func(txids []types.Hash32, tm time.Time) {
		for _, txid := range txids {
			tx, err := st.TransactionById(txid)
			require.NoError(t, err)
			require.NotNil(t, tx.LastRemoved)
			assert.Equal(t, tm, *tx.LastRemoved, fmt.Sprintf(`txid=%s`, txid))
		}
	}

	for n, block := range blocks {
		_, err := st.InsertBlock(&block)
		require.NoError(t, err)

		// the first block remains unaffected by the reorg
		testLastRemoved(blocks[0].TxIDs, blocks[0].FirstSeen)

		if n < 1 {
			continue
		}

		if n < 4 {
			storedBlock, err := st.queryBlock(StaticQuery{where: fmt.Sprintf(`hash = x'%s'`, block.Hash)})
			require.NoError(t, err)
			txIter, err := st.TransactionsInBlock(storedBlock.DBID)
			require.NoError(t, err)
			txsInBlock := txIter.Collect()
			require.Len(t, txsInBlock, 2)
			testLastRemoved(blocks[1].TxIDs, blocks[1].FirstSeen)

			if n < 2 {
				continue
			}

			testLastRemoved(blocks[2].TxIDs, blocks[2].FirstSeen)
			bestBlock, err := st.BestBlockNow()
			require.NoError(t, err)

			if n < 3 {
				assert.Equal(t, blocks[n].Hash, bestBlock.Hash)
			}
		} else {
			// This is after the reorg
			// Only the transactions confirmed in blocks {1, 3, 4} are confirmed.
			testLastRemoved(blocks[3].TxIDs, blocks[4].FirstSeen)
			testLastRemoved(blocks[4].TxIDs, blocks[4].FirstSeen)

			confirmedTxs := map[types.Hash32]struct{}{}
			chain := []types.Block{blocks[0], blocks[3], blocks[4]}
			for _, b := range chain {
				for _, txid := range b.TxIDs {
					confirmedTxs[txid] = struct{}{}
				}
			}

			for _, tx := range testChain.transactions {
				storedTx, err := st.TransactionById(tx.TxID)
				require.NoError(t, err)
				if _, ok := confirmedTxs[tx.TxID]; ok {
					assert.NotNil(t, storedTx.LastRemoved)
				} else {
					assert.Nil(t, storedTx.LastRemoved)
				}
			}
		}
	}
}

func chainedBlocks(startHeight int, parentId string, blockIds []string) (res []types.Block) {
	// The first block has the zero has as the parent id.
	// Allow the empty string as a special value to be able to insert a block without a parent.
	var prevId types.Hash32
	if parentId != "" {
		prevId = GenerateHash32(parentId)
	}
	for i, blockId := range blockIds {
		block := types.Block{
			Hash:      GenerateHash32(blockId),
			Parent:    prevId,
			Height:    uint32(startHeight + i),
			FirstSeen: GetTime(i * 100),
		}
		res = append(res, block)
		prevId = block.Hash
	}
	return res
}

func insertBlocks(st *Storage, blocks []types.Block) error {
	for _, block := range blocks {
		if _, err := st.InsertBlock(&block); err != nil {
			return err
		}
	}
	return nil
}

func TestStorage_commonAncestor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	st, err := NewTestStorage()
	require.NoError(t, err)
	defer st.Close()

	commonAncestorById := func(a, b string) (*types.Block, error) {
		block1, err := st.blockByHash(GenerateHash32(a))
		if err != nil {
			return nil, err
		}
		block2, err := st.blockByHash(GenerateHash32(b))
		if err != nil {
			return nil, err
		}
		storedBlock, err := st.CommonAncestor(block1, block2)
		if err != nil {
			return nil, err
		}
		return &storedBlock.Block, nil
	}
	require.NoError(t,
		insertBlocks(st, chainedBlocks(0, "", []string{"1", "2", "3", "4", "5"})),
	)
	require.NoError(t,
		insertBlocks(st, chainedBlocks(3, "3", []string{"3.1", "3.2", "3.3", "3.4"})),
	)

	testCommonAncestor := func(a, b, expected string) {
		ancestor, err := commonAncestorById(a, b)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, ancestor.Hash, GenerateHash32(expected))

		revAncestor, err := commonAncestorById(b, a)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, ancestor.Hash, revAncestor.Hash)
	}

	testCommonAncestor("2", "1", "1")
	testCommonAncestor("3", "1", "1")
	testCommonAncestor("4", "1", "1")

	testCommonAncestor("3", "2", "2")
	testCommonAncestor("4", "2", "2")

	testCommonAncestor("3.1", "3", "3")
	testCommonAncestor("3.1", "4", "3")
	testCommonAncestor("3.1", "5", "3")

	testCommonAncestor("3.2", "3", "3")
	testCommonAncestor("3.2", "4", "3")
	testCommonAncestor("3.2", "5", "3")
}

func TestStorage_ReorgBase(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	st, err := NewTestStorage()
	require.NoError(t, err)
	defer st.Close()

	blocks1 := chainedBlocks(0, "", []string{"1", "2", "3", "4", "5"})
	for _, b := range blocks1 {
		b.IsBest = true
	}

	blocks2 := chainedBlocks(3, "3", []string{"3.1", "3.2", "3.3", "3.4"})
	for _, b := range blocks2 {
		// starting with block 3.3 we reorg the blocks 4 and 5
		b.IsBest = b.Height > 4
	}

	require.NoError(t, insertBlocks(st, blocks1))
	require.NoError(t, insertBlocks(st, blocks2))

	for _, block := range blocks1 {
		if !block.IsBest {
			_, err := st.ReorgBase(&block)
			require.Error(t, err)
			continue
		}

		reorgBase, err := st.ReorgBase(&block)
		require.NoError(t, err)

		// reorgBase is idempotent
		reorgBase2, err := st.ReorgBase(&block)
		require.NoError(t, err)
		assert.Equal(t, reorgBase, reorgBase2)

		if block.Height < 3 {
			// since there is no parallel chain for blocks[0:3], the reorgBase is the block itself
			assert.Equal(t, block, reorgBase)
		} else {
			// blocks 4 and 5 will get reorged later by block "3.3", the common base is "3"
			assert.Equal(t, block, blocks1[2])
		}
	}

	for _, block := range blocks2 {
		if !block.IsBest {
			_, err := st.ReorgBase(&block)
			require.Error(t, err)
			continue
		}

		require.GreaterOrEqual(t, 4, block.Height)

		// from block 3.3 on the blocks are on the consensus chain
		reorgBase, err := st.ReorgBase(&block)
		require.NoError(t, err)
		assert.Equal(t, block, reorgBase)
	}
}
