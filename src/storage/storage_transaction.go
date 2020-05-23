package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/0xb10c/bademeister-go/src/types"
)

// ErrorLookupTransactionDBIDs is returned if transactions confirmed by a block are not found
type ErrorLookupTransactionDBIDs struct {
	MissingTxs []types.Hash32
	Total      int
}

// Return error string
func (e *ErrorLookupTransactionDBIDs) Error() string {
	return fmt.Sprintf("could not find %d of %d transactions", len(e.MissingTxs), e.Total)
}

// IsErrorMissingTransactions returns true iff err is of type ErrorLookupTransactionDBIDs
func IsErrorMissingTransactions(err error) bool {
	_, ok := err.(*ErrorLookupTransactionDBIDs)
	return ok
}

// TransactionQueryByTime implements the Query interface.
type TransactionQueryByTime struct {
	// Upper limit (inclusive) of transaction first_seen time
	FirstSeenBeforeOrAt *time.Time
	// Lower limit (exclusive) for last_removed time
	LastRemovedAfter *time.Time
}

// Where implements the Query interface
func (q TransactionQueryByTime) Where() string {
	clause := []string{}
	if q.FirstSeenBeforeOrAt != nil {
		clause = append(clause, fmt.Sprintf(
			"(first_seen <= %d)", q.FirstSeenBeforeOrAt.Unix(),
		))
	}
	if q.LastRemovedAfter != nil {
		clause = append(clause, fmt.Sprintf(
			"((last_removed IS NULL) OR (last_removed > %d))",
			q.LastRemovedAfter.Unix(),
		))
	}
	return strings.Join(clause, " AND ")
}

// Order implements the Query interface. Empty by default.
func (q TransactionQueryByTime) Order() string {
	return ""
}

// Limit implements the Query interface. Empty by default.
func (q TransactionQueryByTime) Limit() int {
	return 0
}

// TxIterator helps fetching transactions row-by-row.
type TxIterator struct {
	rows *sql.Rows
}

// Next returns the next transaction or nil.
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

// Close underlying cursor
func (i *TxIterator) Close() error {
	return i.rows.Close()
}

// Collect returns remaining transactions as list and closes cursor
func (i TxIterator) Collect() (res []types.StoredTransaction) {
	defer i.Close()
	for tx := i.Next(); tx != nil; tx = i.Next() {
		res = append(res, *tx)
	}
	return res
}

// InsertTransactions inserts transactions into storage.
// If same transaction already exists, update `first_seen` to smaller of both values.
func (s *Storage) InsertTransactions(txs []types.Transaction) (int64, error) {
	// The firstSeen timestamp might not be to be monotonic, since transactions
	// can be inserted from multiple sources (ZMQ and getrawmempool RPC).
	// https://www.sqlite.org/lang_UPSERT.html
	const insertTransaction string = `
	INSERT INTO
	 	"transaction" 
	 	(txid, first_seen, fee, weight) 
	VALUES
		%s
	ON CONFLICT(txid) DO
		UPDATE SET
			first_seen = excluded.first_seen
		WHERE
			first_seen > excluded.first_seen
	`

	values := []string{}
	for _, tx := range txs {
		values = append(values, fmt.Sprintf(
			`(x'%s', %d, %d, %d)`,
			tx.TxID, tx.FirstSeen.UTC().Unix(), tx.Fee, tx.Weight,
		))
	}

	smt := fmt.Sprintf(insertTransaction, strings.Join(values, ","))
	res, err := s.db.Exec(smt)
	if err != nil {
		return 0, errors.Errorf("could not insert transactions into table `transaction`: %s", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

// InsertTransaction inserts a single transaction.
// See InsertTransactions for more info.
func (s *Storage) InsertTransaction(tx *types.Transaction) (int64, error) {
	return s.InsertTransactions([]types.Transaction{*tx})
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

	dbidByTXID := map[types.Hash32]int64{}

	for rows.Next() {
		var dbid int64
		var txidBytes []byte

		err := rows.Scan(&dbid, &txidBytes)
		if err != nil {
			return nil, errors.Errorf("error reading row: %s", err)
		}

		dbidByTXID[types.NewHashFromBytes(txidBytes)] = dbid
	}

	missing := []types.Hash32{}
	res := []int64{}
	for _, txid := range txids {
		if dbid, ok := dbidByTXID[txid]; ok {
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

// TransactionsInBlock returns transactions that are confirmed in block specified by `blockId`
func (s *Storage) TransactionsInBlock(blockID int64) (*TxIterator, error) {
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
			)`, blockID)
	if err != nil {
		return nil, errors.Errorf("error querying transactions: %s", err)
	}

	return &TxIterator{rows}, nil
}

// TransactionDBIDsInBlock returns the transaction database ids of the transactions confirmed in block
func (s Storage) TransactionDBIDsInBlock(blockID int64) (res []int64, err error) {
	rows, err := s.db.Query(`
		SELECT
			transaction_id
		FROM
			"transaction_block"
		WHERE
			block_id = ?
	`, blockID)
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

// QueryTransactions returns transactions satisfying query
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

// TransactionByID returns the transaction with `txid`
func (s *Storage) TransactionByID(txid types.Hash32) (*types.StoredTransaction, error) {
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

// NextTransactions returns transactions after `t`.
// If multiple transactions exist for `t`, return transaction with higher `dbid`.
func (s *Storage) NextTransactions(t time.Time, dbid int64, limit int) (*TxIterator, error) {
	return s.QueryTransactions(StaticQuery{
		// since there can be multiple txs with the same timestamp, we must use the dbid to query as well
		where: fmt.Sprintf(
			"(first_seen > %d) OR ((first_seen == %d) AND (id > %d))",
			t.Unix(), t.Unix(), dbid,
		),
		order: "first_seen ASC, id ASC",
		limit: limit,
	})
}
