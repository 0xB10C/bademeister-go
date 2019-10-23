package types

import (
	"time"
)

// Transaction represents a Bitcoin transaction
type Transaction struct {
	TxID         Hash32    `json:"txid"`
	FirstSeen    time.Time `json:"firstSeen"`
	OutputValue  uint64    `json:"outputValue"`
	Fee          uint64    `json:"fee"`
	BlockHeight  int32     `json:"blockHeight"`
	IndexInBlock int32     `json:"indexInBlock"`
}
