package types

import (
	"time"
)

// Transaction represents a Bitcoin transaction
type Transaction struct {
	TxID         Hash32    `json:"txid"`
	FirstSeen    time.Time `json:"firstSeen"`
	Fee          uint64    `json:"fee"`
	Size         int       `json:"vsize"`
	BlockHeight  int32     `json:"blockHeight"`
	IndexInBlock int32     `json:"indexInBlock"`
}
