package bitcoinrpcclient

import (
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/0xb10c/bademeister-go/src/types"
)

// GetRawMempoolVerboseResult implements the current version of `getrawmempool`.
// https://bitcoin.org/en/developer-reference#getrawmempool
// The version provided by btcsuite uses a deprecated format.
type GetRawMempoolVerboseResult struct {
	Weight            int32    `json:"weight"`
	Time              int64    `json:"time"`
	Height            int64    `json:"height"`
	Depends           []string `json:"depends"`
	Bip125Replaceable bool     `json:"bip125-replaceable"`
	Fees              struct {
		Base float64 `json:"base"`
	} `json:"fees"`
}

// GetRawMempoolVerbose returns the transactions in the mempool
func (rpcClient *BitcoinRPCClient) GetRawMempoolVerbose() (map[string]GetRawMempoolVerboseResult, error) {
	jsonArgVerbose, err := json.Marshal(true)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	rawResult, err := rpcClient.RawRequest("getrawmempool", []json.RawMessage{jsonArgVerbose})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var mempoolItems map[string]GetRawMempoolVerboseResult
	err = json.Unmarshal(rawResult, &mempoolItems)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return mempoolItems, nil
}

// RawMempoolToTransactions converts the result of GetRawMempoolVerbose to a list of types.Transaction
func RawMempoolToTransactions(
	rpcMempool map[string]GetRawMempoolVerboseResult,
) (res []types.Transaction, err error) {
	for txHashStr, txInfo := range rpcMempool {
		bytes, err := hex.DecodeString(txHashStr)
		if err != nil {
			return nil, errors.Wrapf(err, "error decoding tx hash %s", txHashStr)
		}

		if len(bytes) != 32 {
			return nil, errors.Wrapf(err, "invalid txhash size %d", len(bytes))
		}

		firstSeen := time.Unix(txInfo.Time, 0)

		tx := types.Transaction{
			TxID:        types.NewHashFromBytes(bytes),
			FirstSeen:   firstSeen,
			LastRemoved: nil,
			Fee:         uint64(txInfo.Fees.Base * 1e8),
			Weight:      int(txInfo.Weight),
		}

		res = append(res, tx)
	}

	return res, nil
}
