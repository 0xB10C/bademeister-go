package storage

import (
	"database/sql"
	"fmt"
	"log"
	"os"
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
			version INT
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

	const createMempoolTxTable string = `
		CREATE TABLE mempool_tx (
			txid 				BLOB 	UNIQUE NOT NULL,
			first_seen 	INT,
			fee 				INT,
			weight 			INT
		)
	`

	if _, err := s.db.Exec(createMempoolTxTable); err != nil {
		return fmt.Errorf("could not create the `mempool_tx` table: %s", err)
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
	row := s.db.QueryRow(`SELECT COUNT(txid) FROM mempool_tx`)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("could not get count from table `mempool_tx`: %s", err)
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
	INSERT INTO mempool_tx (txid, first_seen, fee, weight) VALUES(?, ?, ?, ?)
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
		return fmt.Errorf("could not insert a transaction into table `mempool_tx`: %s", err)
	}
	return nil
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

func (i *TxIterator) Close() error {
	return i.rows.Close()
}

func (s *Storage) QueryTransactions(q Query) (*TxIterator, error) {
	var rows *sql.Rows
	var err error

	baseQuery := `SELECT txid, first_seen, fee, weight FROM mempool_tx`

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
