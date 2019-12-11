package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/wire"
)

// Block contains the block data required for mempool reconstruction
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

// https://bitcoin.org/en/developer-reference#coinbase
func parseHeight(txin wire.TxIn) int {
	// taken from https://github.com/0xB10C/memo/blob/39c5c5/memod/processor/zmq_handler.go#L54-L76

	// To get the block height we look into the coinbase transaction
	// (the first transaction in a block). The scriptsig of the coin-
	// base transaction starts with the height. **The first byte sets
	// height length**. (only true for blocks with BIP34, but this can
	// be ignored here, since we probably don't work with older blocks)
	heightLength := txin.SignatureScript[0]

	// we get the bytes from pos 1 till pos heightLength + 1 since the
	// second parameter is exclusive in Go
	heightLittleEndian := txin.SignatureScript[1 : heightLength+1]

	// since we want the block height in a int32 (4 byte) and big
	// endian we first add padding (at the end, since it's little
	// endian) and then convert it to big endian.
	for len(heightLittleEndian) < 4 {
		heightLittleEndian = append(heightLittleEndian, 0)
	}

	return int(binary.LittleEndian.Uint32(heightLittleEndian))
}

// NewBlock creates a new Block from serialized bytes
func NewBlock(firstSeen time.Time, rawblock []byte) (*Block, error) {
	reader := bytes.NewReader(rawblock)

	var wireBlock wire.MsgBlock
	err := wireBlock.BtcDecode(reader, 0, wire.LatestEncoding)
	if err != nil {
		return nil, fmt.Errorf("error during BtcDecode: %s", err)
	}

	height := -1
	txHashes := []Hash32{}

	for _, t := range wireBlock.Transactions {
		if blockchain.IsCoinBaseTx(t) {
			height = parseHeight(*t.TxIn[0])
		}
		txHashes = append(txHashes, NewHashFromArray(t.TxHash()))
	}

	if height < 0 {
		return nil, fmt.Errorf("height not found")
	}

	// FIXME: the default zmq rawblock only provides the current best block.
	//        In a reorg, we will not be able to find the parent of a new best block.
	isBest := true

	return &Block{
		FirstSeen:   firstSeen,
		EncodedTime: wireBlock.Header.Timestamp,
		Hash:        NewHashFromArray(wireBlock.BlockHash()),
		Parent:      NewHashFromArray(wireBlock.Header.PrevBlock),
		TxIDs:       txHashes,
		Height:      uint32(height),
		IsBest:      isBest,
	}, nil
}

// StoredBlock extends Block with Database ID
type StoredBlock struct {
	// Internal database ID
	DBID int64
	Block
}
