package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/pkg/errors"
)

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

func (s *Storage) InsertTransaction(tx *types.Transaction) (int64, error) {
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
