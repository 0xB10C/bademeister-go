package zmqsubscriber

import (
	"log"
	"testing"
	"time"

	"github.com/0xb10c/bademeister-go/src/test"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/require"
)

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
		t.Logf("Timed out while waiting for a transaction (timeout %s)", timeout)
		return nil
	}
}

func getTestZMQSubscriber(t *testing.T, env test.TestEnv) *ZMQSubscriber {
	z, err := NewZMQSubscriber(env.ZmqHost, env.ZmqPort)
	require.NoError(t, err)

	go func() {
		if err := z.Run(); err != nil {
			log.Fatalf("ZMQSubscriber exited with error: %s", err)
		}
	}()

	return z
}

func TestZMQSubscriber(t *testing.T) {
	const waitTimeout = 5 * time.Second

	env := test.NewTestEnv()
	defer env.Quit()

	z := getTestZMQSubscriber(t, env)
	defer z.Stop()

	env.GenerateBlocks(1)
	require.NotNil(t, waitForZMQTransaction(t, z, waitTimeout))
	require.NotNil(t, waitForZMQBlock(t, z, waitTimeout))

	env.GenerateBlocks(1)
	require.NotNil(t, waitForZMQTransaction(t, z, waitTimeout))
	require.NotNil(t, waitForZMQBlock(t, z, waitTimeout))

	// Test second subscriber.
	// This demonstrates that it should be possible to connect multiples instance of
	// Bademeisterd and have continuous capture.
	{
		z2 := getTestZMQSubscriber(t, env)
		defer z2.Stop()

		env.GenerateBlocks(1)
		require.NotNil(t, waitForZMQTransaction(t, z, waitTimeout))
		require.NotNil(t, waitForZMQBlock(t, z, waitTimeout))

		require.NotNil(t, waitForZMQTransaction(t, z2, waitTimeout))
		require.NotNil(t, waitForZMQBlock(t, z2, waitTimeout))
	}
}
