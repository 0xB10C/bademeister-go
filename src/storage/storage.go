package storage

import (
	"database/sql"
	"github.com/0xb10c/bademeister-go/src/types"
	_ "github.com/mattn/go-sqlite3"
	"log"
)

type Storage struct {
	db *sql.DB
}

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
	sqlStmt := `
	create table txs (
		txid blob,
		confirmed_block_height integer,
		confirmed_block_index integer,
		first_seen date
	);`
	_, err := s.db.Exec(sqlStmt)
	return err
}

func (s *Storage) AddTransaction(tx types.Transaction) error {
	dbtx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := dbtx.Prepare(`
		insert into txs (txid, first_seem) values(?, ?)
	`)
	if err != nil {
		return err
	}

	defer func() {
		if err := stmt.Close(); err != nil {
			log.Printf("error in smt.Close()=%v", err)
		}
	}()

	if _, err = stmt.Exec(tx.TxID, tx.FirstSeen); err != nil {
		return err
	}

	if err := dbtx.Commit(); err != nil {
		return err
	}

	return nil
}
