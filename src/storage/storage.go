package storage

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	_ "github.com/mattn/go-sqlite3"
)

const currentVersion = 4

// Storage represents a SQL database.
type Storage struct {
	db *sql.DB
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
			return nil, fmt.Errorf("could not initialize the database: %s", err)
		}
	} else {
		if err := s.migrate(s.getVersion()); err != nil {
			return nil, fmt.Errorf("could not migrate the database: %s", err)
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
		return fmt.Errorf("could not create the `config` table: %s", err)
	}

	const fillConfigTable string = `
	INSERT INTO config (version) VALUES (?)
	`

	if _, err := s.db.Exec(fillConfigTable, version); err != nil {
		return fmt.Errorf("could not fill the `config` table: %s", err)
	}

	const createTransactionTable string = `
		CREATE TABLE "transaction" (
			id          INTEGER PRIMARY KEY UNIQUE NOT NULL,
			txid        BLOB UNIQUE NOT NULL,
			first_seen  INTEGER,
			fee         INTEGER,
			weight      INTEGER
		)
	`

	if _, err := s.db.Exec(createTransactionTable); err != nil {
		return fmt.Errorf("could not create the table `transaction`: %s", err)
	}

	const createBlockTable string = `
		CREATE TABLE "block" (
			 id         INTEGER PRIMARY KEY UNIQUE NOT NULL, 
			 hash       BLOB (32) UNIQUE, 
			 first_seen INTEGER, 
			 height     INTEGER 
		)
	`
	if _, err := s.db.Exec(createBlockTable); err != nil {
		return fmt.Errorf("could not create the table `block`: %s", err)
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
		return fmt.Errorf("could not create the table `transaction_block`: %s", err)
	}

	return nil
}

func (s *Storage) getVersion() (version int) {
	row := s.db.QueryRow(`SELECT version FROM config`)
	if row == nil {
		panic(fmt.Errorf("could not query version"))
	}
	if err := row.Scan(&version); err != nil {
		panic(err)
	}
	return
}

func (s *Storage) TxCount() (count int, err error) {
	row := s.db.QueryRow(`SELECT COUNT(txid) FROM "transaction"`)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("could not get count from table `transaction`: %s", err)
	}
	return
}

func (s *Storage) migrate(fromVersion int) error {
	if fromVersion == currentVersion {
		// nothing to do
		return nil
	}

	// TODO: implement

	return fmt.Errorf("cannot migrate from version %d", fromVersion)
}

func (s *Storage) InsertTransaction(tx *types.Transaction) error {
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
	_, err := s.db.Exec(insertTransaction, tx.TxID[:], tx.FirstSeen.UTC().Unix(), tx.Fee, tx.Weight)
	if err != nil {
		return fmt.Errorf("could not insert a transaction into table `transaction`: %s", err)
	}
	return nil
}

func (s *Storage) getTransactionDBIDs(txs *[]types.Transaction) (*[]int64, error) {
	inClause := []string{}
	for _, tx := range *txs {
		inClause = append(inClause, fmt.Sprintf("x'%s'", tx.TxID))
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
		return nil, fmt.Errorf("error getting database ids from transactions: %s", err)
	}

	dbidByTxId := map[types.Hash32]int64{}

	for rows.Next() {
		var dbid int64
		var txidBytes []byte

		err := rows.Scan(&dbid, &txidBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading row: %s", err)
		}

		var txid types.Hash32
		copy(txid[:], txidBytes)
		dbidByTxId[txid] = dbid
	}

	res := []int64{}
	for _, tx := range *txs {
		if dbid, ok	:= dbidByTxId[tx.TxID]; ok {
			res = append(res, dbid)
		} else {
			return nil, fmt.Errorf("could not lookup dbid for %s", tx.TxID)
		}
	}
	return &res, nil
}

func (s *Storage) insertTransactionBlock(blockId int64, txs *[]types.Transaction) error {
	if len(*txs) == 0 {
		return nil
	}

	dbids, err := s.getTransactionDBIDs(txs)
	if err != nil {
		return fmt.Errorf("error getting tx database ids: %s", err)
	}

	valueTuples := []string{}
	for n, dbid := range *dbids {
		valueTuples = append(
			valueTuples,
			fmt.Sprintf("(%d, %d, %d)", dbid, blockId, n),
		)
	}

	insertTransactionBlock := fmt.Sprintf(`
		INSERT INTO 
			"transaction_block"
			(transaction_id, block_id, block_index)
		VALUES
			%s
	`, strings.Join(valueTuples, ","))

	// fmt.Println(insertTransactionBlock)

	_, err = s.db.Exec(insertTransactionBlock)
	if err != nil {
		return fmt.Errorf(`error inserting to table "transaction_block": %s`, err)
	}

	return nil
}

func (s *Storage) InsertBlock(block *types.Block) error {
	for _, tx := range block.Transactions {
		if err := s.InsertTransaction(&tx); err != nil {
			return err
		}
	}

	const insertBlock string = `
	INSERT INTO "block" (hash, first_seen, height) VALUES(?, ?, ?)
	ON CONFLICT(hash) DO
		UPDATE SET
			first_seen = excluded.first_seen
		WHERE
			first_seen > excluded.first_seen
	`
	res, err := s.db.Exec(insertBlock, block.Hash[:], block.FirstSeen.UTC().Unix(), block.Height)
	if err != nil {
		return fmt.Errorf("could not insert a block into table `block`: %s", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("error in LastInsertId(): %s", err)
	}

	err = s.insertTransactionBlock(id, &block.Transactions)
	if err != nil {
		return fmt.Errorf("error in insertTransactionBlock(): %s", err)
	}

	return nil
}

func (s *Storage) TransactionsInBlock(blockHash types.Hash32) (*TxIterator, error) {
	row := s.db.QueryRow(`
		SELECT 
			id 
		FROM 
			"block"
		WHERE
			hash = ?
	`, blockHash[:])
	var blockId int64
	if err := row.Scan(&blockId); err != nil {
		return nil, fmt.Errorf("error reading blockId: %s", err)
	}

	log.Printf("blockHash=%s blockId=%d", blockHash, blockId)

	rows, err := s.db.Query(`
		SELECT
			txid, first_seen, fee, weight
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
		return nil, fmt.Errorf("error querying transactions: %s", err)
	}

	return &TxIterator{rows}, nil
}

type Query struct {
	FirstSeen *time.Time
}

type TxIterator struct {
	rows *sql.Rows
}

func (i *TxIterator) Next() *types.Transaction {
	if !i.rows.Next() {
		return nil
	}

	var txidBytes []byte
	var firstSeen int64
	var fee uint64
	var weight int
	if err := i.rows.Scan(&txidBytes, &firstSeen, &fee, &weight); err != nil {
		panic(err)
	}

	var txid types.Hash32
	copy(txid[:], txidBytes)
	return &types.Transaction{
		TxID:      txid,
		FirstSeen: time.Unix(firstSeen, 0).UTC(),
		Fee:       fee,
		Weight:    weight,
	}
}

func (i TxIterator) Collect() []types.Transaction {
	defer i.Close()
	res := []types.Transaction{}
	for tx := i.Next(); tx != nil; tx = i.Next() {
		res = append(res, *tx)
	}
	return res
}

func (i *TxIterator) Close() error {
	return i.rows.Close()
}

func (s *Storage) QueryTransactions(q Query) (*TxIterator, error) {
	var rows *sql.Rows
	var err error

	baseQuery := `SELECT txid, first_seen, fee, weight FROM "transaction"`

	if q.FirstSeen == nil {
		rows, err = s.db.Query(baseQuery)
	} else {
		rows, err = s.db.Query(
			fmt.Sprintf("%s where first_seen > ?", baseQuery),
			q.FirstSeen.Unix(),
		)
	}

	if err != nil {
		return nil, err
	}

	return &TxIterator{rows}, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}
