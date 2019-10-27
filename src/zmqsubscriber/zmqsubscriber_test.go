package zmqsubscriber

import (
	"github.com/0xb10c/bademeister-go/src/test"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestZMQSubscriber(t *testing.T) {
	env := test.NewTestEnv()
	defer env.Quit()

	z, err := NewZMQSubscriber(env.ZmqHost, env.ZmqPort)
	if err != nil {
		t.Error(err)
	}

	defer func() {
		err := z.Quit()
		if err != nil {
			panic(err)
		}
	}()

	txs := []types.Transaction{}
	go func() {
		for t := range z.IncomingTx {
			txs = append(txs, t)
		}
	}()

	blocks := []types.Block{}
	go func() {
		for b := range z.IncomingBlocks {
			blocks = append(blocks, b)
		}
	}()

	env.GenerateBlocks(1)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, 1, len(blocks))

	env.GenerateBlocks(1)
	assert.Equal(t, 2, len(txs))
	assert.Equal(t, 2, len(blocks))
}
