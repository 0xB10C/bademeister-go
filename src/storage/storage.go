package storage

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/0xb10c/bademeister-go/src/types"
	_ "github.com/mattn/go-sqlite3"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
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

func (s *Storage) Close() error {
	return s.db.Close()
}
