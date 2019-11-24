package storage

import (
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/pkg/errors"
	"time"
)

type EventType int

const (
	Unknown EventType = iota
	EnterMempool
	BlockConfirmation
	BlockReorg
	// replaced?
	// expired?
)

func (e EventType) String() string {
	return []string{
		"Unknown",
		"EnterMempool",
		"BlockConfirmation",
		"BlockReorg",
	}[e]
}

type Event struct {
	Time               time.Time
	Type               EventType
	NewBlock           *types.StoredBlock
	NewTransaction     *types.StoredTransaction
	AddTransactions    []types.StoredTransaction
	RemoveTransactions []int64
}

func transactionsByDBID(txs []types.StoredTransaction) map[int64]types.StoredTransaction {
	res := map[int64]types.StoredTransaction{}
	for _, tx := range txs {
		res[tx.DBID] = tx
	}
	return res
}

type Mempool struct {
	time            time.Time
	lastEvent       *Event
	lastBlock       *types.StoredBlock
	lastTransaction *types.StoredTransaction
	storage         *Storage
	transactions    map[int64]types.StoredTransaction
}

// Get all transactions that are potentially in mempool at given time.
// Note that due to reorgs, this is a superset of transactions.
func newMempoolWithoutBlock(st *Storage, t time.Time) (*Mempool, error) {
	txIter, err := st.QueryTransactions(TransactionQueryByTime{
		FirstSeenBeforeOrAt: &t,
		LastRemovedAfter:    &t,
	})

	if err != nil {
		return nil, err
	}

	txs := txIter.Collect()
	var lastTransaction *types.StoredTransaction
	for _, tx := range txs {
		if lastTransaction == nil || tx.FirstSeen.After(lastTransaction.FirstSeen) {
			tx := tx
			lastTransaction = &tx
		}
	}

	tm := time.Unix(0, 0)
	if lastTransaction != nil {
		tm = lastTransaction.FirstSeen
	}

	return &Mempool{
		time:            tm,
		storage:         st,
		lastTransaction: lastTransaction,
		transactions:    transactionsByDBID(txs),
	}, nil
}

// Return mempool that results with confirmation of _block_.
// Block must be on the consensus chain.
// Use `st.ReorgBase(block)` to find a suitable block that is on the consensus chain.
func NewMempoolAtBlock(st *Storage, block *types.StoredBlock) (*Mempool, error) {
	reorgBase, err := st.ReorgBase(&block.Block)
	if err != nil {
		return nil, err
	}

	if reorgBase.Hash != block.Hash {
		return nil, errors.Errorf(
			"block %s is not on eventual consensus chain %s",
			block.Hash, reorgBase.Hash,
		)
	}

	m, err := newMempoolWithoutBlock(st, block.FirstSeen)
	if err != nil {
		return nil, err
	}

	blockEvent, err := m.newBlockEvent(block)
	if err != nil {
		return nil, err
	}
	if err := m.ApplyEvent(blockEvent); err != nil {
		return nil, err
	}

	return m, nil
}

func NewMempoolAtTime(st *Storage, time time.Time) (*Mempool, error) {
	// In a reorg, some transactions that were previously confirmed come back into the mempool.
	// For this reason, we cannot query transactions with `first_seen < t < last_removed`,
	// since this would include transactions that are temporarily confirmed.
	// In order to properly reflect the mempool state, we must start from a block that will
	// not be reorged later and seek to the time.

	bestBlock, err := st.BestBlockAtTime(time)
	if err != nil {
		return nil, errors.Wrapf(err, "error finding best block at %s", time)
	}

	// if we query the mempool before the first block record
	if bestBlock == nil {
		return newMempoolWithoutBlock(st, time)
	}

	// If bestBlock is on the final consensus chain, bestBlock == reorgBase
	reorgBase, err := st.ReorgBase(&bestBlock.Block)
	if err != nil {
		return nil, err
	}

	mempool, err := NewMempoolAtBlock(st, reorgBase)
	if err != nil {
		return nil, err
	}
	if err := mempool.Seek(time); err != nil {
		return nil, err
	}

	return mempool, nil
}

func (m *Mempool) Clone() *Mempool {
	newMempool := *m
	transactions := map[int64]types.StoredTransaction{}
	for k, v := range m.transactions {
		transactions[k] = v
	}
	newMempool.transactions = transactions
	return &newMempool
}

func (m *Mempool) NextTransaction() (*types.StoredTransaction, error) {
	// FIXME: this could use a buffer so that we don't have to query the DB every time
	dbid := int64(0)
	if m.lastTransaction != nil {
		dbid = m.lastTransaction.DBID
	}
	txIter, err := m.storage.NextTransactions(m.time, dbid, 1)
	if err != nil {
		return nil, err
	}
	defer txIter.Close()
	return txIter.Next(), nil
}

func (m *Mempool) NextBlock() (*types.StoredBlock, error) {
	// FIXME: use buffer here as well (see NextTransaction())
	dbid := int64(0)
	if m.lastBlock != nil {
		dbid = m.lastBlock.DBID
	}
	blockIter, err := m.storage.NextBestBlocks(m.time, dbid, 1)
	if err != nil {
		return nil, err
	}
	defer blockIter.Close()
	return blockIter.Next(), nil
}

func (m *Mempool) newBlockEvent(newBest *types.StoredBlock) (*Event, error) {
	event := Event{
		Time:               newBest.FirstSeen,
		Type:               BlockConfirmation,
		NewBlock:           newBest,
		NewTransaction:     nil,
		AddTransactions:    []types.StoredTransaction{},
		RemoveTransactions: []int64{},
	}

	var commonAncestor *types.StoredBlock

	if m.lastBlock != nil {
		var err error
		commonAncestor, err = m.storage.CommonAncestor(m.lastBlock, newBest)
		if err != nil {
			return nil, err
		}

		if commonAncestor.Hash != m.lastBlock.Hash {
			event.Type = BlockReorg
			// LogReorg(m.lastBlock, newBest, commonAncestor)
		}

		// this will only be called if there is a reorg
		err = m.storage.WalkBlocks(m.lastBlock, commonAncestor, func(b *types.StoredBlock) error {
			txIter, err := m.storage.TransactionsInBlock(b.DBID)
			if err != nil {
				return err
			}
			event.AddTransactions = append(event.AddTransactions, txIter.Collect()...)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Unless there is a reorg, this will be called once with `newBest`.
	err := m.storage.WalkBlocks(newBest, commonAncestor, func(b *types.StoredBlock) error {
		dbids, err := m.storage.TransactionDBIDsInBlock(b.DBID)
		if err != nil {
			return err
		}
		event.RemoveTransactions = append(event.RemoveTransactions, dbids...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &event, nil
}

func (m *Mempool) newTransactionEvent(newTransaction *types.StoredTransaction) (*Event, error) {
	return &Event{
		Time:               newTransaction.FirstSeen,
		Type:               EnterMempool,
		NewBlock:           nil,
		NewTransaction:     newTransaction,
		AddTransactions:    []types.StoredTransaction{*newTransaction},
		RemoveTransactions: []int64{},
	}, nil
}

func (m *Mempool) NextEvent() (*Event, error) {
	nextTx, err := m.NextTransaction()
	if err != nil {
		return nil, err
	}

	nextBlock, err := m.NextBlock()
	if err != nil {
		return nil, err
	}

	if nextBlock == nil && nextTx == nil {
		return nil, nil
	}

	if nextBlock == nil {
		return m.newTransactionEvent(nextTx)
	}

	if nextTx == nil || nextBlock.FirstSeen.Before(nextTx.FirstSeen) {
		return m.newBlockEvent(nextBlock)
	}

	return m.newTransactionEvent(nextTx)
}

func (m *Mempool) ApplyEvent(e *Event) error {
	m.time = e.Time
	if e.NewBlock != nil {
		m.lastBlock = e.NewBlock
	}
	if e.NewTransaction != nil {
		m.lastTransaction = e.NewTransaction
	}
	for _, tx := range e.AddTransactions {
		m.transactions[tx.DBID] = tx
	}
	for _, dbid := range e.RemoveTransactions {
		if _, ok := m.transactions[dbid]; !ok {
			// return errors.Errorf("error removing %d: not in mempool", dbid)
		}
		delete(m.transactions, dbid)
	}
	return nil
}

func (m *Mempool) Seek(t time.Time) error {
	if t.Before(m.time) {
		return errors.Errorf("cannot seek backwards")
	}
	for {
		nextEvent, err := m.NextEvent()
		if err != nil {
			return err
		}

		if nextEvent == nil || nextEvent.Time.After(t) {
			return nil
		}

		if err := m.ApplyEvent(nextEvent); err != nil {
			return err
		}
	}
}

func (m *Mempool) TransactionMap() map[int64]types.StoredTransaction {
	return m.transactions
}

func (m *Mempool) Transactions() (res []types.Transaction) {
	for _, tx := range m.transactions {
		res = append(res, tx.Transaction)
	}
	return
}
