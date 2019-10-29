package storage

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	_ "github.com/mattn/go-sqlite3"
)

const VERSION = 2

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
		if err := s.init(VERSION); err != nil {
			return nil, err
		}
	} else {
		if err := s.migrate(s.getVersion()); err != nil {
			return nil, err
		}
	}

	return &s, nil
}

func (s *Storage) init(version int) error {
	sqlStmts := []string{
		`CREATE TABLE config (version int)`,
		`CREATE TABLE transactions (
			txid BLOB UNIQUE NULL,
			first_seen INT,
			confirmed_block_height INT
		)`,
	}
	for _, stmt := range sqlStmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}

	_, err := s.db.Exec(
		`INSERT INTO config (version) VALUES (?);`, version,
	)
	return err
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

func (s *Storage) TxCount() (count int) {
	row := s.db.QueryRow(`SELECT COUNT(*) FROM transactions`)
	if row == nil {
		panic(fmt.Errorf("could not query tx count"))
	}
	if err := row.Scan(&count); err != nil {
		panic(err)
	}
	return
}

func (s *Storage) migrate(fromVersion int) error {
	if fromVersion == VERSION {
		// nothing to do
		return nil
	}

	// TODO: implement

	return fmt.Errorf("cannot migrate from version %d", fromVersion)
}

func (s *Storage) AddTransaction(tx *types.Transaction) error {
	// We cannot expect FirstSeen to be monotonic since we might add a tx data
	// from multiple sources (`getrawmempool`).
	// https://www.sqlite.org/lang_UPSERT.html
	_, err := s.db.Exec(`
		INSERT INTO transactions (txid, first_seen) VALUES(?, ?)
		ON CONFLICT(txid) DO
			UPDATE SET
				first_seen = excluded.first_seen
			WHERE
				first_seen > excluded.first_seen
	`, tx.TxID[:], tx.FirstSeen.UTC().Unix())
	return err
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
	var firstSeenTimestamp int64
	if err := i.rows.Scan(&txidBytes, &firstSeenTimestamp); err != nil {
		panic(err)
	}
	var txid types.Hash32
	copy(txid[:], txidBytes)
	return &types.Transaction{
		TxID:      txid,
		FirstSeen: time.Unix(firstSeenTimestamp, 0).UTC(),
	}
}

func (i *TxIterator) Close() error {
	return i.rows.Close()
}

func (s *Storage) QueryTransactions(q Query) (*TxIterator, error) {
	var rows *sql.Rows
	var err error

	baseQuery := `SELECT txid, first_seen FROM transactions`

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
