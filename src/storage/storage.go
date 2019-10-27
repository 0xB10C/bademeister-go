package storage

import (
	"database/sql"
	"fmt"
	"github.com/0xb10c/bademeister-go/src/types"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"time"
)

const VERSION = 1

type Storage struct {
	db *sql.DB
}

// reference: https://github.com/mattn/go-sqlite3/blob/master/_example/simple/simple.go
func NewStorage(path string) (*Storage, error) {
	_, err := os.Stat(path)
	init := false

	if err != nil && os.IsNotExist(err) {
		init = true
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
		`create table config (version int);`,
		`create table transactions (
			txid blob unique,
			first_seen date,
			confirmed_block_height integer
		);`,
	}
	for _, stmt := range sqlStmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}

	_, err := s.db.Exec(
		`insert into config (version) values (?);`, version,
	);
	return err
}

func (s *Storage) getVersion() (version int) {
	row := s.db.QueryRow(`select version from config`);
	if row == nil {
		panic(fmt.Errorf("could not query version"))
	}
	if err := row.Scan(&version); err != nil {
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
	if tx.FirstSeen.Location() != time.UTC {
		return fmt.Errorf("time must be UTC")
	}
	_, err := s.db.Exec(`
		insert into transactions (txid, first_seen) values(?, ?)
	`, tx.TxID[:], tx.FirstSeen)
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

func (s *Storage) Close() error {
	return s.db.Close()
}
