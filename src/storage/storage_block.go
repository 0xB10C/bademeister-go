package storage

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/0xb10c/bademeister-go/src/types"
)

// BlockIterator helps fetching blocks row-by-row
type BlockIterator struct {
	rows *sql.Rows
}

// Next returns next block
func (i *BlockIterator) Next() *types.StoredBlock {
	if !i.rows.Next() {
		return nil
	}

	var blockHashBytes []byte
	var parentHashBytes []byte
	var firstSeen int64
	var block types.StoredBlock
	err := i.rows.Scan(
		&block.DBID,
		&blockHashBytes,
		&parentHashBytes,
		&firstSeen,
		&block.Height,
		&block.IsBest,
	)
	if err != nil {
		panic(err)
	}
	block.Hash = types.NewHashFromBytes(blockHashBytes)
	block.Parent = types.NewHashFromBytes(parentHashBytes)
	block.FirstSeen = time.Unix(firstSeen, 0).UTC()
	return &block
}

// Close underlying cursor
func (i *BlockIterator) Close() error {
	return i.rows.Close()
}

// Collect returns remaining blocks as list and closes cursor
func (i *BlockIterator) Collect() (res []types.StoredBlock) {
	defer i.Close()
	for b := i.Next(); b != nil; b = i.Next() {
		res = append(res, *b)
	}
	return res
}

func (s *Storage) queryBlocks(q Query) (*BlockIterator, error) {
	fields := []string{"id", "hash", "parent", "first_seen", "height", "is_best"}
	table := "block"
	rows, err := s.db.Query(formatQuery(fields, table, q))

	if err != nil {
		return nil, err
	}

	return &BlockIterator{rows}, nil
}

func (s *Storage) queryBlock(q Query) (*types.StoredBlock, error) {
	blockIter, err := s.queryBlocks(q)
	if err != nil {
		return nil, err
	}
	defer blockIter.Close()
	return blockIter.Next(), nil
}

func (s *Storage) blockByHash(h types.Hash32) (*types.StoredBlock, error) {
	return s.queryBlock(StaticQuery{
		where: fmt.Sprintf(`hash = x'%s'`, h),
		order: "",
		limit: 1,
	})
}

// BestBlockNow returns the most recent best block
func (s *Storage) BestBlockNow() (*types.StoredBlock, error) {
	return s.queryBlock(StaticQuery{
		where: `is_best = 1`,
		order: "first_seen DESC",
		limit: 1,
	})
}

// BestBlockAtTime returns latest best block before or at provided time
func (s *Storage) BestBlockAtTime(t time.Time) (*types.StoredBlock, error) {
	return s.queryBlock(StaticQuery{
		where: fmt.Sprintf(`(is_best = 1) AND (first_seen <= %d)`, t.Unix()),
		order: "first_seen DESC",
		limit: 1,
	})
}

// NextBestBlocks returns best blocks after time `t`.
// If multiple blocks at `t` exist, return block with higher `dbid`
func (s *Storage) NextBestBlocks(t time.Time, dbid int64, limit int) (*BlockIterator, error) {
	return s.queryBlocks(StaticQuery{
		where: fmt.Sprintf(
			`((first_seen > %d) OR ((first_seen == %d) AND (id > %d))) AND (is_best = 1)`,
			t.Unix(), t.Unix(), dbid),
		order: "first_seen ASC, id ASC",
		limit: limit,
	})
}

// CommonAncestor returns closest block that is a parent of both `a` and `b`.
// If no parent can be found, returns error.
func (s *Storage) CommonAncestor(a, b *types.StoredBlock) (*types.StoredBlock, error) {
	if a.Height < b.Height {
		// make sure that b is never ahead of a
		a, b = b, a
	}

	if a.Height == b.Height {
		if a.Hash == b.Hash {
			return a, nil
		}
	}

	if a.Height == b.Height+1 {
		if a.Parent == b.Hash {
			return b, nil
		}
	}

	aParent, err := s.blockByHash(a.Parent)
	if err != nil {
		return nil, errors.Errorf("error retrieving parent: %s", err)
	}
	if aParent == nil {
		return nil, errors.Errorf("parent not found: %s", a.Parent)
	}

	return s.CommonAncestor(aParent, b)
}

// ReorgBase finds the reorg base for the given block.
// If the block is on final the consensus chain, this will simply return the current block.
// If the block is on a minority chain that will be reorged, returns the common ancestor.
func (s *Storage) ReorgBase(block *types.Block) (*types.StoredBlock, error) {
	if !block.IsBest {
		return nil, errors.Errorf("must provide block that was chainhead at some point")
	}

	storedBlock, err := s.blockByHash(block.Hash)
	if err != nil {
		return nil, err
	}
	if storedBlock == nil {
		return nil, errors.Errorf("could not load block %s", block.Hash)
	}

	lastBest := storedBlock

	// Scan block in increasing height and keep track of the best block.
	// If there is no parallel chain at a given height, abort.
	// In the regular case where there is no minority chain, this terminates immediately
	// and returns _block_
	for height := block.Height; ; height++ {
		blockIter, err := s.queryBlocks(StaticQuery{
			where: fmt.Sprintf("height = %d", height),
			order: "first_seen ASC",
		})
		if err != nil {
			return nil, err
		}

		blocks := blockIter.Collect()

		for _, b := range blocks {
			b := b
			if b.IsBest {
				lastBest = &b
			}
		}

		// if there are no parallel chains at a given height, we are done
		if len(blocks) < 2 {
			break
		}
	}

	return s.CommonAncestor(storedBlock, lastBest)
}

// WalkBlocks runs function `f` up the parent chain from blocks `start` to `end` (not including `end`)
// If end is nil, called once for `start`
func (s *Storage) WalkBlocks(start, end *types.StoredBlock, f func(*types.StoredBlock) error) error {
	current := start
	for end == nil || (current.Hash != end.Hash) {
		if err := f(current); err != nil {
			return err
		}

		parentBlock, err := s.blockByHash(current.Parent)
		if err != nil {
			return err
		}
		current = parentBlock

		if end == nil {
			return nil
		}
	}
	return nil
}

// Updates the last_removed timestamps of transactions.
// In the default case, the new best block has current best block as parent,
// and we set `last_removed` of the contained transactions to `newBest.FirstSeen`.
// In case of a reorg, we traverse back to the common ancestor of the current best block
// and the new best block and clear `last_removed` of the contained transactions.
// We then traverse from the new best block to the common ancestor and set
// `last_removed = newBest.FirstSeen` for the transactions contained in these blocks.
func (s *Storage) updateBestBlock(lastBest, newBest *types.StoredBlock) error {
	log.Printf("updateBestBlock() newBest=%s height=%d", newBest.Hash, newBest.Height)

	// the first block is a special case
	if lastBest == nil {
		log.Println("WARNING: lastBest=nil, assuming this is the first block")
		return s.updateLastRemoved(newBest, &newBest.FirstSeen)
	}

	// In the default case, the common ancestor is simply `currentBest`
	commonAncestor, err := s.CommonAncestor(newBest, lastBest)
	if err != nil {
		return err
	}

	if commonAncestor.Hash != lastBest.Hash {

		LogReorg(
			lastBest,
			newBest,
			commonAncestor,
		)
	}

	// Clear old `last_removed` values.
	// In the default case, currentBest == CommonAncestor and the func is not called.
	// In case of a reorg, this clears the values up to the common ancestor
	err = s.WalkBlocks(lastBest, commonAncestor, func(block *types.StoredBlock) error {
		log.Printf("REORG: clearing last_removed for block %s heigth %d", block.Hash, block.Height)
		return s.updateLastRemoved(block, nil)
	})
	if err != nil {
		return err
	}

	// Set `last_removed` for the new best chain to the new value.
	// In the default case, this only updates the values of the transactions contained
	// in newBest.
	return s.WalkBlocks(newBest, commonAncestor, func(block *types.StoredBlock) error {
		return s.updateLastRemoved(block, &newBest.FirstSeen)
	})
}

// inserts new block with some basic sanity checks
func (s *Storage) insertBlock(block *types.Block, firstBlock bool) (int64, error) {
	var zeroHash types.Hash32

	// sanity check height and parent
	if block.Parent != zeroHash && !firstBlock {
		parentBlock, err := s.blockByHash(block.Parent)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Printf("warning: could not find parent block %s for block %s", block.Parent, block.Hash)
			} else {
				return 0, err
			}
		}

		if parentBlock == nil {
			return 0, errors.Errorf("parentBlock==nil")
		}

		if block.Height != (parentBlock.Height + 1) {
			return 0, errors.Errorf(
				"invalid block height %d for block %s (parent %s height=%d)",
				block.Height, block.Hash, parentBlock.Hash, parentBlock.Height,
			)
		}
	}

	const insertBlock string = `
	INSERT INTO
	 	"block" (hash, first_seen, parent, height, is_best) 
 	VALUES
 		(?, ?, ?, ?, ?)
	ON CONFLICT(hash) DO
		UPDATE SET
			first_seen = excluded.first_seen
		WHERE
			first_seen > excluded.first_seen
	`
	res, err := s.db.Exec(
		insertBlock,
		block.Hash[:],
		block.FirstSeen.UTC().Unix(),
		block.Parent[:],
		block.Height,
		block.IsBest,
	)
	if err != nil {
		return 0, errors.Errorf("could not insert a block into table `block`: %s", err)
	}

	return res.LastInsertId()
}

func (s *Storage) insertTransactionBlock(blockID int64, dbids []int64) error {
	if len(dbids) == 0 {
		return nil
	}

	valueTuples := []string{}
	for blockIndex, dbid := range dbids {
		valueTuples = append(
			valueTuples,
			fmt.Sprintf("(%d, %d, %d)", dbid, blockID, blockIndex),
		)
	}

	insertTransactionBlock := fmt.Sprintf(`
		INSERT INTO 
			"transaction_block"
			(transaction_id, block_id, block_index)
		VALUES
			%s
	`, strings.Join(valueTuples, ","))

	_, err := s.db.Exec(insertTransactionBlock)
	if err != nil {
		return errors.Errorf(`error inserting to table "transaction_block": %s`, err)
	}

	return nil
}

func (s *Storage) updateLastRemoved(block *types.StoredBlock, lastRemoved *time.Time) error {
	log.Printf("updateLastRemoved() block=%s lastRemoved=%s", block.Hash, lastRemoved)

	var lastRemovedSeconds interface{}
	if lastRemoved == nil {
		lastRemovedSeconds = nil
	} else {
		lastRemovedSeconds = lastRemoved.Unix()
	}

	_, err := s.db.Exec(`
		UPDATE
			"transaction"
		SET
			last_removed = :last_removed
		WHERE
			id IN (
				SELECT
					transaction_id
				FROM
					"transaction_block"
				WHERE
					block_id = :block_id
			)
		`,
		sql.Named("last_removed", lastRemovedSeconds),
		sql.Named("block_id", block.DBID),
	)

	return err
}

// InsertBlock inserts new block and update `last_removed` transactions.
func (s *Storage) InsertBlock(block *types.Block) (int64, error) {
	txDbIds, err := s.transactionDBIDs(block.TxIDs)
	if err != nil {
		if IsErrorMissingTransactions(err) {
			return 0, err
		}
		return 0, errors.Errorf("error getting tx database ids: %s", err)
	}

	currentBest, errBestBlock := s.BestBlockNow()
	isFirstBlock := errBestBlock == sql.ErrNoRows
	if isFirstBlock {
		// In the beginning we do not have any best blocks
		currentBest = nil
	} else if errBestBlock != nil {
		return 0, err
	}

	blockID, err := s.insertBlock(block, isFirstBlock)
	if err != nil {
		return 0, errors.Errorf("error in insertBlock(): %s", err)
	}

	err = s.insertTransactionBlock(blockID, *txDbIds)
	if err != nil {
		return 0, errors.Errorf("error in insertTransactionBlock(): %s", err)
	}

	if block.IsBest {
		storedBlock := types.StoredBlock{
			DBID:  blockID,
			Block: *block,
		}
		if err := s.updateBestBlock(currentBest, &storedBlock); err != nil {
			return 0, err
		}
	}

	return blockID, nil
}
