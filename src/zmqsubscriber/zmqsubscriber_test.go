package zmqsubscriber

import (
	"github.com/0xb10c/bademeister-go/src/test"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
)

func captureTxs(z *ZMQSubscriber) *[]types.Transaction {
	txs := []types.Transaction{}
	go func() {
		for t := range z.IncomingTx {
			txs = append(txs, t)
		}
	}()
	return &txs
}

func captureBlocks(z *ZMQSubscriber) *[]types.Block {
	blocks := []types.Block{}
	go func() {
		for b := range z.IncomingBlocks {
			blocks = append(blocks, b)
		}
	}()
	return &blocks
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
	env := test.NewTestEnv()
	defer env.Quit()

	z := getTestZMQSubscriber(t, env)
	defer z.Stop()

	txs := captureTxs(z)
	blocks := captureBlocks(z)

	env.GenerateBlocks(1)
	assert.Equal(t, 1, len(*txs))
	assert.Equal(t, 1, len(*blocks))

	env.GenerateBlocks(1)
	assert.Equal(t, 2, len(*txs))
	assert.Equal(t, 2, len(*blocks))

	// Test second subscriber.
	// This demonstrates that it should be possible to connect multiples instance of
	// Bademeisterd and have continuous capture.
	{
		z2 := getTestZMQSubscriber(t, env)
		defer z2.Stop()

		txs2 := captureTxs(z2)
		blocks2 := captureBlocks(z2)

		env.GenerateBlocks(1)

		assert.Equal(t, 3, len(*txs))
		assert.Equal(t, 3, len(*blocks))
		assert.Equal(t, 1, len(*txs2))
		assert.Equal(t, 1, len(*blocks2))
	}
}
