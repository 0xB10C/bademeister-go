package bitcoinrpcclient

import (
	"testing"

	"github.com/0xb10c/bademeister-go/src/test"

	"github.com/stretchr/testify/require"
)

func TestBitcoinRPCClient_GetBestBlockHash(t *testing.T) {
	test.SkipIfShort(t)

	rpcClient, err := NewBitcoinRPCClientForIntegrationTest()
	require.NoError(t, err)

	hashes, err := rpcClient.GenerateToFixedAddress(2)
	require.NoError(t, err)
	require.Len(t, hashes, 2)

	bestHash, err := rpcClient.GetBestBlockHash()
	require.Equal(t, hashes[1], bestHash)
}
