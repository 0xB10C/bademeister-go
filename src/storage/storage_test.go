package storage

import (
	"fmt"
	"github.com/0xb10c/bademeister-go/src/test"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestStorage(t *testing.T) {
	path := fmt.Sprintf("%s/storageTest.db", test.DataDir)

	if err := os.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			t.Fail()
		}
	}

	st, err := NewStorage(path, 1)
	require.NoError(t, err)

	tx := types.Transaction{
		TxID: test.NewTestTxId(nil),
		FirstSeen: time.Now().UTC(),
	}

	err = st.AddTransaction(&tx)
	require.NoError(t, err)

	txIter, err := st.QueryTransactions(Query{})
	require.NoError(t, err)

	recoverTx := txIter.Next()
	require.NotNil(t, tx)
	assert.Equal(t, tx, *recoverTx)
}
