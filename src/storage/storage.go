package storage

import (
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"os"
	"strings"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	_ "github.com/mattn/go-sqlite3"
)

const currentVersion = 4

func LogReorg(lastBest, newBest, commonAncestor *types.StoredBlock) {
	log.Printf(
		"REORG: newBest.Hash=%s newBest.Height=%d lastBest.Height=%d CommonAncestor.Height=%d",
		newBest.Hash, newBest.Height, lastBest.Height, commonAncestor.Height,
	)
}

// Storage represents a SQL database.
type Storage struct {
	db *sql.DB
}

type ErrorLookupTransactionDBIDs struct {
	MissingTxs []types.Hash32
	Total      int
}

func (e *ErrorLookupTransactionDBIDs) Error() string {
	return fmt.Sprintf("could not find %d of %d transactions", len(e.MissingTxs), e.Total)
}

func IsErrorMissingTransactions(err error) bool {
	_, ok := err.(*ErrorLookupTransactionDBIDs)
	return ok
}

type Query interface {
	Where() string
	Order() string
	Limit() int
}

type StaticQuery struct {
	where string
	order string
	limit int
}

func (q StaticQuery) Where() string {
	return q.where
}

func (q StaticQuery) Order() string {
	return q.order
}

func (q StaticQuery) Limit() int {
	return q.limit
}

func formatQuery(fields []string, table string, q Query) string {
	query := fmt.Sprintf(`SELECT %s FROM "%s"`, strings.Join(fields, ","), table)

	if q.Where() != "" {
		query = fmt.Sprintf("%s WHERE %s", query, q.Where())
	}

	if q.Order() != "" {
		query = fmt.Sprintf("%s ORDER BY %s", query, q.Order())
	}

	if q.Limit() > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, q.Limit())
	}

	return query
}

type TransactionQueryByTime struct {
	FirstSeenBeforeOrAt *time.Time
	LastRemovedAfter    *time.Time
}

func (r TransactionQueryByTime) Where() string {
	clause := []string{}
	if r.FirstSeenBeforeOrAt != nil {
		clause = append(clause, fmt.Sprintf(
			"(first_seen <= %d)", r.FirstSeenBeforeOrAt.Unix(),
		))
	}
	if r.LastRemovedAfter != nil {
		clause = append(clause, fmt.Sprintf(
			"((last_removed IS NULL) OR (last_removed > %d))",
			r.LastRemovedAfter.Unix(),
		))
	}
	return strings.Join(clause, " AND ")
}

func (q TransactionQueryByTime) Order() string {
	return ""
}

func (q TransactionQueryByTime) Limit() int {
	return 0
}

type TxIterator struct {
	rows *sql.Rows
}

func (i *TxIterator) Next() *types.StoredTransaction {
	if !i.rows.Next() {
		return nil
	}

	var txidBytes []byte
	var firstSeenSeconds int64
	var lastRemovedSeconds *int64
	var tx types.StoredTransaction
	err := i.rows.Scan(
		&tx.DBID,
		&txidBytes,
		&firstSeenSeconds,
		&lastRemovedSeconds,
		&tx.Fee,
		&tx.Weight,
	)

	tx.TxID = types.NewHashFromBytes(txidBytes)
	tx.FirstSeen = time.Unix(firstSeenSeconds, 0).UTC()
	if lastRemovedSeconds != nil {
		lastRemoved := time.Unix(*lastRemovedSeconds, 0).UTC()
		tx.LastRemoved = &lastRemoved
	}

	if err != nil {
		panic(err)
	}

	return &tx
}

func (i TxIterator) Collect() (res []types.StoredTransaction) {
	defer i.Close()
	for tx := i.Next(); tx != nil; tx = i.Next() {
		res = append(res, *tx)
	}
	return res
}

func (i *TxIterator) Close() error {
	return i.rows.Close()
}

type BlockIterator struct {
	rows *sql.Rows
}

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

func (i *BlockIterator) Close() error {
	return i.rows.Close()
}

func (i *BlockIterator) Collect() (res []types.StoredBlock) {
	defer i.Close()
	for b := i.Next(); b != nil; b = i.Next() {
		res = append(res, *b)
	}
	return res
}

// reference: https://github.com/mattn/go-sqlite3/blob/master/_example/simple/simple.go
func NewStorage(path string) (*Storage, error) {
	_, err := os.Stat(path)
	init := false

	if err != nil {
		if os.IsNotExist(err) {
			init = true
		} else {
			return nil, err
		}
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	s := Storage{db}

	if init {
		if err := s.initialize(currentVersion); err != nil {
			return nil, errors.Wrapf(err, "could not initialize the database at path %s", path)
		}
	} else {
		if err := s.migrate(s.getVersion()); err != nil {
			return nil, errors.Errorf("could not migrate the database: %s", err)
		}
	}

	return &s, nil
}

// initialize creates tables for a new database and fills in the configuration.
// The caller must make sure that the database isn't initialized already.
func (s *Storage) initialize(version int) error {
	log.Printf("Initializing a new database with version %d.\n", version)

	const createConfigTable string = `
		CREATE TABLE config (
			version INTEGER
		)`

	if _, err := s.db.Exec(createConfigTable); err != nil {
		return errors.Errorf("could not create the `config` table: %s", err)
	}

	const fillConfigTable string = `
	INSERT INTO config (version) VALUES (?)
	`

	if _, err := s.db.Exec(fillConfigTable, version); err != nil {
		return errors.Errorf("could not fill the `config` table: %s", err)
	}

	const createTransactionTable string = `
		CREATE TABLE "transaction" (
			id             INTEGER PRIMARY KEY UNIQUE NOT NULL,
			txid           BLOB UNIQUE NOT NULL,
			first_seen     INTEGER,
			last_removed   INTEGER,
			fee            INTEGER,
			weight         INTEGER
		)
	`

	if _, err := s.db.Exec(createTransactionTable); err != nil {
		return errors.Errorf("could not create the table `transaction`: %s", err)
	}

	const createBlockTable string = `
		CREATE TABLE "block" (
			 id         INTEGER PRIMARY KEY UNIQUE NOT NULL, 
			 hash       BLOB (32) UNIQUE NOT NULL, 
			 parent     BLOB (32),
			 first_seen INTEGER, 
			 height     INTEGER,
			 is_best	INTEGER
		)
	`
	if _, err := s.db.Exec(createBlockTable); err != nil {
		return errors.Errorf("could not create the table `block`: %s", err)
	}

	const createTransactionBlockTabe string = `
		CREATE TABLE transaction_block (
			-- internal transaction id
			transaction_id INTEGER REFERENCES "transaction" (id) NOT NULL, 
			-- internal block id
			block_id       INTEGER REFERENCES "block" (id) NOT NULL,
			-- position of tx in block
			block_index    INTEGER NOT NULL
  		)
	`
	if _, err := s.db.Exec(createTransactionBlockTabe); err != nil {
		return errors.Errorf("could not create the table `transaction_block`: %s", err)
	}

	return nil
}

func (s *Storage) getVersion() (version int) {
	row := s.db.QueryRow(`SELECT version FROM config`)
	if row == nil {
		panic(errors.Errorf("could not query version"))
	}
	if err := row.Scan(&version); err != nil {
		panic(err)
	}
	return
}

func (s *Storage) TxCount() (count int, err error) {
	row := s.db.QueryRow(`SELECT COUNT(txid) FROM "transaction"`)
	if err := row.Scan(&count); err != nil {
		return 0, errors.Errorf("could not get count from table `transaction`: %s", err)
	}
	return
}

func (s *Storage) migrate(fromVersion int) error {
	if fromVersion == currentVersion {
		// nothing to do
		return nil
	}

	// TODO: implement

	return errors.Errorf("cannot migrate from version %d", fromVersion)
}

func (s *Storage) InsertTransaction(tx *types.Transaction) (int64, error) {
	log.Printf("Transaction=%#v", tx)
	const insertTransaction string = `
	INSERT INTO "transaction" (txid, first_seen, fee, weight) VALUES(?, ?, ?, ?)
	ON CONFLICT(txid) DO
		UPDATE SET
			first_seen = excluded.first_seen
		WHERE
			first_seen > excluded.first_seen
	`

	// The firstSeen timestamp might not be to be monotonic, since transactions
	// can be inserted from multiple sources (ZMQ and getrawmempool RPC).
	// https://www.sqlite.org/lang_UPSERT.html
	res, err := s.db.Exec(insertTransaction, tx.TxID[:], tx.FirstSeen.UTC().Unix(), tx.Fee, tx.Weight)
	if err != nil {
		return 0, errors.Errorf("could not insert a transaction into table `transaction`: %s", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Storage) transactionDBIDs(txids []types.Hash32) (*[]int64, error) {
	inClause := []string{}
	for _, txid := range txids {
		inClause = append(inClause, fmt.Sprintf("x'%s'", txid))
	}

	selectTransactionIds := fmt.Sprintf(`
		SELECT 
			id, txid
		FROM 
			"transaction" 
		WHERE 
			txid IN (%s)
		`, strings.Join(inClause, ","),
	)

	rows, err := s.db.Query(selectTransactionIds)
	if err != nil {
		return nil, errors.Errorf("error getting database ids from transactions: %s", err)
	}

	dbidByTxId := map[types.Hash32]int64{}

	for rows.Next() {
		var dbid int64
		var txidBytes []byte

		err := rows.Scan(&dbid, &txidBytes)
		if err != nil {
			return nil, errors.Errorf("error reading row: %s", err)
		}

		dbidByTxId[types.NewHashFromBytes(txidBytes)] = dbid
	}

	missing := []types.Hash32{}
	res := []int64{}
	for _, txid := range txids {
		if dbid, ok := dbidByTxId[txid]; ok {
			res = append(res, dbid)
		} else {
			missing = append(missing, txid)
		}
	}

	if len(missing) > 0 {
		return nil, &ErrorLookupTransactionDBIDs{
			MissingTxs: missing,
			Total:      len(txids),
		}
	}

	return &res, nil
}

func (s *Storage) TransactionsInBlock(blockId int64) (*TxIterator, error) {
	rows, err := s.db.Query(`
		SELECT
			id, txid, first_seen, last_removed, fee, weight
		FROM
			"transaction"
		WHERE
			id IN (
				SELECT
					transaction_id
				FROM
					"transaction_block"
				WHERE
					block_id = ?
			)`, blockId)
	if err != nil {
		return nil, errors.Errorf("error querying transactions: %s", err)
	}

	return &TxIterator{rows}, nil
}

func (s Storage) TransactionDBIDsInBlock(blockId int64) (res []int64, err error) {
	rows, err := s.db.Query(`
		SELECT
			transaction_id
		FROM
			"transaction_block"
		WHERE
			block_id = ?
	`, blockId)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		res = append(res, id)
	}

	return
}

func (s *Storage) QueryTransactions(q Query) (*TxIterator, error) {
	var rows *sql.Rows
	var err error

	rows, err = s.db.Query(formatQuery(
		[]string{"id", "txid", "first_seen", "last_removed", "fee", "weight"},
		"transaction",
		q,
	))

	if err != nil {
		return nil, errors.Wrapf(err, "error in transaction query %v", q)
	}

	return &TxIterator{rows}, nil
}

func (s *Storage) TransactionById(txid types.Hash32) (*types.StoredTransaction, error) {
	txIter, err := s.QueryTransactions(StaticQuery{
		where: fmt.Sprintf("txid = x'%s'", txid),
		limit: 1,
	})
	if err != nil {
		return nil, err
	}
	defer txIter.Close()
	return txIter.Next(), nil
}

func (s *Storage) NextTransactions(t time.Time, dbid int64, limit int) (*TxIterator, error) {
	return s.QueryTransactions(StaticQuery{
		// since there can be multiple txs with the same timestamp, we must use the dbid to query as well
		where: fmt.Sprintf("(first_seen >= %d) AND (id > %d)", t.Unix(), dbid),
		order: "first_seen ASC, id ASC",
		limit: limit,
	})
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

func (s *Storage) BestBlockNow() (*types.StoredBlock, error) {
	return s.queryBlock(StaticQuery{
		where: `is_best = 1`,
		order: "first_seen DESC",
		limit: 1,
	})
}

func (s *Storage) BestBlockAtTime(t time.Time) (*types.StoredBlock, error) {
	return s.queryBlock(StaticQuery{
		where: fmt.Sprintf(`(is_best = 1) AND (first_seen <= %d)`, t.Unix()),
		order: "first_seen DESC",
		limit: 1,
	})
}

func (s *Storage) NextBestBlocks(t time.Time, dbid int64, limit int) (*BlockIterator, error) {
	return s.queryBlocks(StaticQuery{
		where: fmt.Sprintf("(first_seen >= %d) AND (id > %d) AND (is_best = 1)", t.Unix(), dbid),
		order: "first_seen ASC, id ASC",
		limit: limit,
	})
}

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

// Finds the reorg base for the given block.
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

// Run function `f` up the parent chain from blocks `start` to `end` (not including `end`)
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
// In the default case, the new best blocks has current best block as parent,
// and set `last_removed` of the contained transaction to the `newBest.FirstSeen`.
// In case of a reorg, we traverse back to the common ancestor of the current best block
// and the new best block, and update all transaction of the new best chain to have
// `last_removed = newBest.FirstSeen`.
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

func (s *Storage) insertTransactionBlock(blockId int64, dbids []int64) error {
	if len(dbids) == 0 {
		return nil
	}

	valueTuples := []string{}
	for blockIndex, dbid := range dbids {
		valueTuples = append(
			valueTuples,
			fmt.Sprintf("(%d, %d, %d)", dbid, blockId, blockIndex),
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

	blockId, err := s.insertBlock(block, isFirstBlock)
	if err != nil {
		return 0, errors.Errorf("error in insertBlock(): %s", err)
	}

	err = s.insertTransactionBlock(blockId, *txDbIds)
	if err != nil {
		return 0, errors.Errorf("error in insertTransactionBlock(): %s", err)
	}

	if block.IsBest {
		storedBlock := types.StoredBlock{
			DBID:  blockId,
			Block: *block,
		}
		if err := s.updateBestBlock(currentBest, &storedBlock); err != nil {
			return 0, err
		}
	}

	return blockId, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}
