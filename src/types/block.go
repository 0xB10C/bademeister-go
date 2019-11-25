package types

import "time"

type Block struct {
	Hash        Hash32    `json:"hash"`
	Parent      Hash32    `json:"parent"`
	FirstSeen   time.Time `json:"firstSeen"`
	Height      uint32    `json:"height"`
	IsBest      bool      `json:"isBest"`
	TxIDs       []Hash32  `json:"txids"`
	EncodedTime time.Time `json:"encodedTime"` // TODO: find a better name for this?
	// TODO: Size ?
}

type StoredBlock struct {
	DBID int64
	Block
}
