package zmqsubscriber

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/0xb10c/bademeister-go/src/bitcoinrpcclient"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/require"
)

// TestMain is called by `go test` and is the entry point for this tests file.
func TestMain(m *testing.M) {
	result := m.Run()
	os.Exit(result)
}

// waitForZMQTransaction waits for a new transaction to come in over ZMQ. It
// returns the transaction as soon as it comes in. Otherwise the function times
// out after the specified `timeout` and returns nil.
func waitForZMQTransaction(t *testing.T, z *ZMQSubscriber, timeout time.Duration) *types.Transaction {
	select {
	case tx := <-z.IncomingTx:
		return &tx
	case <-time.After(timeout):
		t.Logf("Timed out while waiting for a transaction (timeout %s)", timeout)
		return nil
	}
}

// waitForZMQBlock waits for a new blocks to come in over ZMQ. It returns the
// block as soon as it comes in. Otherwise the function times out after the
// specified `timeout` and returns nil.
func waitForZMQBlock(t *testing.T, z *ZMQSubscriber, timeout time.Duration) *types.Block {
	select {
	case block := <-z.IncomingBlocks:
		return &block
	case <-time.After(timeout):
		t.Logf("Timed out while waiting for a block (timeout %s)", timeout)
		return nil
	}
}

func setupAndRunZMQSubscriber(t *testing.T, zmqHost string, zmqPort string) (*ZMQSubscriber, error) {
	z, err := NewZMQSubscriber(zmqHost, zmqPort)
	if err != nil {
		return nil, errors.Wrap(err, "could not create a new ZMQ Subscriber")
	}

	go func() {
		if err := z.Run(); err != nil {
			log.Fatalf("ZMQSubscriber exited with error: %s", err)
		}
	}()

	return z, nil
}

func TestZMQSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	const zmqWaitTimeout = 5 * time.Second

	// The environment variables `TEST_INTEGRATION_*` are set in the Makefile.
	zmqHost := os.Getenv("TEST_INTEGRATION_ZMQ_HOST")
	zmqPort := os.Getenv("TEST_INTEGRATION_ZMQ_PORT")
	rpcHost := os.Getenv("TEST_INTEGRATION_RPC_HOST")
	rpcPort := os.Getenv("TEST_INTEGRATION_RPC_PORT")
	rpcUser := os.Getenv("TEST_INTEGRATION_RPC_USER")
	rpcPass := os.Getenv("TEST_INTEGRATION_RPC_PASS")

	rpcClient, err := bitcoinrpcclient.NewBitcoinRPCClient(rpcUser, rpcPass, rpcHost, rpcPort)
	require.NoError(t, err)

	addressMineTo, err := rpcClient.GetNewAddress("addressMineTo")
	require.NoError(t, err)

	// Generate 101 blocks to have spendable UTXOs.
	_, err = rpcClient.GenerateToAddress(101, addressMineTo)
	require.NoError(t, err)

	addressSendTo, err := rpcClient.GetNewAddress("addressSendTo")
	require.NoError(t, err)

	z, err := setupAndRunZMQSubscriber(t, zmqHost, zmqPort)
	require.NoError(t, err)
	defer z.Stop()

	{
		hashes, err := rpcClient.GenerateToAddress(1, addressMineTo)
		require.NoError(t, err)
		require.Equal(t, 1, len(hashes))
		block1 := waitForZMQBlock(t, z, zmqWaitTimeout)
		require.NotNil(t, block1)
		assert.Equal(t, hashes[0][:], block1.Hash[:])
		assert.Greater(t, int(block1.Height), 100)
		assert.Equal(t, 1, len(block1.TxIDs))

		_, err = rpcClient.GenerateToAddress(1, addressMineTo)
		require.NoError(t, err)
		block2 := waitForZMQBlock(t, z, zmqWaitTimeout)
		require.NotNil(t, block2)
		assert.Equal(t, block1.Height+1, block2.Height)
		assert.Equal(t, block1.Hash, block2.Parent)
	}

	_, err = rpcClient.SendSimpleTransaction(addressSendTo)
	tx := waitForZMQTransaction(t, z, zmqWaitTimeout)
	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.InDelta(t, 250, int(tx.Fee), 250)
	assert.InDelta(t, 500, int(tx.Weight), 500)

	_, err = rpcClient.GenerateToAddress(1, addressMineTo)
	require.NoError(t, err)
	require.NotNil(t, waitForZMQBlock(t, z, zmqWaitTimeout))

	_, err = rpcClient.SendSimpleTransaction(addressSendTo)
	require.NoError(t, err)
	require.NotNil(t, waitForZMQTransaction(t, z, zmqWaitTimeout))

	// Test second subscriber.
	// This demonstrates that it should be possible to connect multiples instance of
	// Bademeisterd and have continuous capture.
	{
		z2, err := setupAndRunZMQSubscriber(t, zmqHost, zmqPort)
		require.NoError(t, err)
		defer z2.Stop()

		_, err = rpcClient.GenerateToAddress(1, addressMineTo)
		require.NoError(t, err)
		require.NotNil(t, waitForZMQBlock(t, z, zmqWaitTimeout))
		require.NotNil(t, waitForZMQBlock(t, z2, zmqWaitTimeout))

		_, err = rpcClient.SendSimpleTransaction(addressSendTo)
		require.NoError(t, err)
		require.NotNil(t, waitForZMQTransaction(t, z, zmqWaitTimeout))
		require.NotNil(t, waitForZMQTransaction(t, z2, zmqWaitTimeout))

	}
}
