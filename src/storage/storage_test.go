package storage

import (
	"fmt"
	"github.com/0xb10c/bademeister-go/src/test"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
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
	_ = st
}
