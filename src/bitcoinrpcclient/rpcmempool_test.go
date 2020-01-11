package bitcoinrpcclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/0xb10c/bademeister-go/src/types"
)

func TestBitcoinRPCClient_GetRawMempoolVerbose(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	nTransactions := 32

	rpcClient, err := NewBitcoinRPCClientForIntegrationTest()
	require.NoError(t, err)

	_, err = rpcClient.GetRawMempoolVerbose()
	require.NoError(t, err)

	mempool, err := rpcClient.GetRawMempoolVerbose()
	require.NoError(t, err)
	require.Len(t, mempool, 0)

	addressSendTo, err := rpcClient.GetNewAddress("addressSendTo")
	require.NoError(t, err)

	start := time.Now().Add(-time.Second)

	generatedTxIDs := map[types.Hash32]struct{}{}
	for i := 0; i < nTransactions; i++ {
		txid, err := rpcClient.SendSimpleTransaction(addressSendTo)
		require.NoError(t, err)
		generatedTxIDs[types.NewHashFromArray(*txid).Reversed()] = struct{}{}
	}

	end := time.Now().Add(time.Second)
	require.Len(t, generatedTxIDs, nTransactions)

	mempool, err = rpcClient.GetRawMempoolVerbose()
	require.Len(t, mempool, nTransactions)

	decodedTxs, err := RawMempoolToTransactions(mempool)
	require.NoError(t, err)

	assert.Equal(t, len(mempool), len(decodedTxs))

	for _, tx := range decodedTxs {
		assert.True(t, tx.FirstSeen.After(start))
		assert.True(t, tx.FirstSeen.Before(end))
		assert.Greater(t, int(tx.Fee), 100)
		assert.Less(t, int(tx.Fee), 1000)
		assert.Greater(t, int(tx.Weight), 100)
		assert.Less(t, int(tx.Weight), 1000)
		assert.Contains(t, generatedTxIDs, tx.TxID)
	}

	_, err = rpcClient.GenerateToFixedAddress(1)
	require.NoError(t, err)

	mempool, err = rpcClient.GetRawMempoolVerbose()
	require.NoError(t, err)
	require.Len(t, mempool, 0)
}
