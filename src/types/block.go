package types

import "time"

type Block struct {
	Hash         Hash32        `json:"hash"`
	FirstSeen    time.Time     `json:"firstSeen"`
	Height       uint32        `json:"height"`
	Transactions []Transaction `json:"transactions"`
	EncodedTime  time.Time     `json:"encodedTime"` // TODO: find a better name for this?
	// TODO: Size ?

}
