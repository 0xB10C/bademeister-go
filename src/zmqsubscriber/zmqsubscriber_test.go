package zmqsubscriber

import (
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/0xb10c/bademeister-go/src/bitcoinrpcclient"
	"github.com/0xb10c/bademeister-go/src/types"
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

func setupAndRunZMQSubscriber(t *testing.T, zmqAddress string) (*ZMQSubscriber, error) {
	z, err := NewZMQSubscriber(zmqAddress)
	if err != nil {
		return nil, errors.Wrap(err, "could not create a new ZMQ Subscriber")
	}

	go func() {
		if err := z.Run(); err != nil {
			t.Fatalf("ZMQSubscriber exited with error: %s", err)
		}
	}()

	return z, nil
}

// drainZMQChannels empties any pending messages on a ZMQSubscriber
func drainZMQChannels(z *ZMQSubscriber) {
	for {
		time.Sleep(time.Second)
		// receive any Blocks or Transactions and discard them
		// if no messages are pending, return
		select {
		case <-z.IncomingBlocks:
		case <-z.IncomingTx:
		default:
			return
		}
	}

}

func TestZMQSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}

	const zmqWaitTimeout = 10 * time.Second

	// The environment variables `TEST_INTEGRATION_*` are set in the Makefile.
	zmqAddress := os.Getenv("TEST_INTEGRATION_ZMQ_ADDRESS")

	rpcClient, err := bitcoinrpcclient.NewBitcoinRPCClientForIntegrationTest()
	require.NoError(t, err)

	addressSendTo, err := rpcClient.GetNewAddress("addressSendTo")
	require.NoError(t, err)

	z, err := setupAndRunZMQSubscriber(t, zmqAddress)
	require.NoError(t, err)
	defer z.Stop()

	// Other tests can put events on the ZMQ socket which are not related to these tests.
	// Remove them from the channel.
	drainZMQChannels(z)

	{
		hashes, err := rpcClient.GenerateToFixedAddress(1)
		require.NoError(t, err)
		require.Equal(t, 1, len(hashes))
		block1 := waitForZMQBlock(t, z, zmqWaitTimeout)
		require.NotNil(t, block1)
		assert.Equal(t, hashes[0][:], block1.Hash[:])
		assert.Greater(t, int(block1.Height), 100)
		assert.Equal(t, 1, len(block1.TxIDs))

		_, err = rpcClient.GenerateToFixedAddress(1)
		require.NoError(t, err)
		block2 := waitForZMQBlock(t, z, zmqWaitTimeout)
		require.NotNil(t, block2)
		assert.Equal(t, block1.Height+1, block2.Height)
		assert.Equal(t, block1.Hash, block2.Parent)
		assert.Equal(t, 1, len(block1.TxIDs))
	}

	_, err = rpcClient.SendSimpleTransaction(addressSendTo)
	tx := waitForZMQTransaction(t, z, zmqWaitTimeout)
	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.InDelta(t, 250, int(tx.Fee), 250)
	assert.InDelta(t, 500, int(tx.Weight), 500)

	_, err = rpcClient.GenerateToFixedAddress(1)
	require.NoError(t, err)
	require.NotNil(t, waitForZMQBlock(t, z, zmqWaitTimeout))

	_, err = rpcClient.SendSimpleTransaction(addressSendTo)
	require.NoError(t, err)
	require.NotNil(t, waitForZMQTransaction(t, z, zmqWaitTimeout))

	// Test second subscriber.
	// This demonstrates that it should be possible to connect multiples instance of
	// Bademeisterd and have continuous capture.
	{
		z2, err := setupAndRunZMQSubscriber(t, zmqAddress)
		require.NoError(t, err)
		defer z2.Stop()

		_, err = rpcClient.GenerateToFixedAddress(1)
		require.NoError(t, err)
		require.NotNil(t, waitForZMQBlock(t, z, zmqWaitTimeout))
		require.NotNil(t, waitForZMQBlock(t, z2, zmqWaitTimeout))

		_, err = rpcClient.SendSimpleTransaction(addressSendTo)
		require.NoError(t, err)
		require.NotNil(t, waitForZMQTransaction(t, z, zmqWaitTimeout))
		require.NotNil(t, waitForZMQTransaction(t, z2, zmqWaitTimeout))

	}
}
