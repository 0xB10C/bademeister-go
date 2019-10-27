package storage

import (
	"database/sql"
	"fmt"
	"github.com/0xb10c/bademeister-go/src/types"
	_ "github.com/mattn/go-sqlite3"
	"time"
)

type Storage struct {
	db *sql.DB
}

// reference: https://github.com/mattn/go-sqlite3/blob/master/_example/simple/simple.go
func NewStorage(path string, version int) (*Storage, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	s := Storage{db}

	if err := s.init(); err != nil {
		return nil, err
	}

	return &s, nil
}

func (s *Storage) init() error {
	// TODO add other values
	sqlStmt := `
	create table transactions (
		txid blob,
		first_seen date,
		confirmed_block_height integer
	);`
	_, err := s.db.Exec(sqlStmt)
	return err
}

func (s *Storage) AddTransaction(tx *types.Transaction) error {
	if tx.FirstSeen.Location() != time.UTC {
		return fmt.Errorf("time must be UTC")
	}
	_, err := s.db.Exec(`
		insert into transactions (txid, first_seen) values(?, ?)
	`, tx.TxID[:], tx.FirstSeen)
	return err
}

type Query struct {
	FirstSeen *time.Duration
}

type TxIterator struct {
	rows *sql.Rows
}

func (i *TxIterator) Next() *types.Transaction {
	if !i.rows.Next() {
		return nil
	}
	var txidBytes []byte
	var firstSeen time.Time
	if err := i.rows.Scan(&txidBytes, &firstSeen); err != nil {
		panic(err)
	}
	var txid types.Hash32
	copy(txid[:], txidBytes)
	return &types.Transaction{
		TxID: txid,
		FirstSeen: firstSeen.UTC(),
	}
}

func (i *TxIterator) Close() error {
	return i.rows.Close()
}

func (s *Storage) QueryTransactions(q Query) (*TxIterator, error) {
	var rows *sql.Rows
	var err error

	baseQuery := `select txid, first_seen from transactions`;

	if q.FirstSeen == nil {
		rows, err = s.db.Query(baseQuery)
	} else {
		rows, err = s.db.Query(
			fmt.Sprintf("%s where first_seen > ?", baseQuery),
			q.FirstSeen,
		)
	}

	if err != nil {
		return nil, err
	}

	return &TxIterator{rows}, nil
}
