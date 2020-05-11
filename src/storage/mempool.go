package storage

import (
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/pkg/errors"
)

// EventType describes the types of events that can change the mempool
type EventType string

const (
	transactionBufferSize = 4096
	blockBufferSize       = 4
	// EnterMempool is emitted when a single transaction enters the mempool
	EnterMempool EventType = "EnterMempool"
	// BlockConfirmation is emitted when a block is confirmed
	BlockConfirmation EventType = "BlockConfirmation"
	// BlockReorg is emitted when a block is confirmed and the parent is not the current best block
	BlockReorg EventType = "BlockReorg"
	// replaced?
	// expired?
)

// Event describes a change to the mempool
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

// Mempool describes the unconfirmed transactions at a point in time
type Mempool struct {
	Time      time.Time
	LastEvent *Event

	storage *Storage

	lastBlock           *types.StoredBlock
	nextBlocks          []*types.StoredBlock
	nextBlockBufferSize int

	lastTransaction            *types.StoredTransaction
	nextTransactions           []*types.StoredTransaction
	nextTransactionsBufferSize int

	transactions map[int64]types.StoredTransaction
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
		Time:      tm,
		LastEvent: nil,
		storage:   st,

		lastBlock:           nil,
		nextBlocks:          []*types.StoredBlock{},
		nextBlockBufferSize: blockBufferSize,

		lastTransaction:            lastTransaction,
		nextTransactions:           []*types.StoredTransaction{},
		nextTransactionsBufferSize: transactionBufferSize,

		transactions: transactionsByDBID(txs),
	}, nil
}

// NewMempoolAtBlock returns the Mempool that results with confirmation of _block_.
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

// NewMempoolAtTime returns the Mempool where the last event is before or at `time`
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

// Clone returns a copy of the Mempool
func (m *Mempool) Clone() *Mempool {
	newMempool := *m
	transactions := map[int64]types.StoredTransaction{}
	for k, v := range m.transactions {
		transactions[k] = v
	}
	newMempool.transactions = transactions
	return &newMempool
}

// NextTransaction returns the next Transaction that will enter the Mempool
func (m *Mempool) NextTransaction() (*types.StoredTransaction, error) {
	dbid := int64(0)
	if m.lastTransaction != nil {
		dbid = m.lastTransaction.DBID
	}

	if len(m.nextTransactions) == 0 {
		// the buffer is empty, fill it up
		m.nextTransactions = make([]*types.StoredTransaction, m.nextTransactionsBufferSize)
		txIter, err := m.storage.NextTransactions(m.Time, dbid, len(m.nextTransactions))
		if err != nil {
			return nil, err
		}

		for i, tx := range txIter.Collect() {
			tx := tx
			m.nextTransactions[i] = &tx
		}
	}

	for _, tx := range m.nextTransactions {
		// sentinel value `nil` means that we reached the end of the collection
		if tx == nil {
			return nil, nil
		}

		if tx.FirstSeen.After(m.Time) || (tx.FirstSeen == m.Time && tx.DBID > dbid) {
			return tx, nil
		}
	}

	// reset and reload buffer
	m.nextTransactions = nil
	return m.NextTransaction()
}

// NextBlock returns the next Block that will alter the Mempool
func (m *Mempool) NextBlock() (*types.StoredBlock, error) {
	dbid := int64(0)
	if m.lastBlock != nil {
		dbid = m.lastBlock.DBID
	}

	if len(m.nextBlocks) == 0 {
		// the buffer is empty, fill it up
		m.nextBlocks = make([]*types.StoredBlock, m.nextBlockBufferSize)
		blockIter, err := m.storage.NextBestBlocks(m.Time, dbid, len(m.nextBlocks))
		if err != nil {
			return nil, err
		}

		for i, block := range blockIter.Collect() {
			block := block
			m.nextBlocks[i] = &block
		}
	}

	// find closest next item in buffer
	for _, block := range m.nextBlocks {
		// sentinel value `nil` means that we reached the end of the collection
		if block == nil {
			return nil, nil
		}
		if block.FirstSeen.After(m.Time) || (block.FirstSeen == m.Time && block.DBID > dbid) {
			return block, nil
		}
	}

	// reset and reload buffer
	m.nextBlocks = nil
	return m.NextBlock()
}

// newBlockEvent creates and returns a new Block event.
//
// If the new best block has the current block as parent, returns BlockConfirmation event where
// _AddTransactions_ is empty and _RemoveTransactions_ has the transactions confirmed in the block.
//
// If the new best block does not have the current block as parent, returns BlockReorg event
// where _AddTransactions_ contains transactions that were contained in the reorged chain and
// _RemoveTransactions_ contains transactions that are confirmed in the new chain. The same
// transactions can appear in AddTransactions and RemoveTransactions, so the latter should be applied
// after the former for an accurate result.
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

// NextEvent returns the next event that will alter the mempool
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

// ApplyEvent applies the provided Event
func (m *Mempool) ApplyEvent(e *Event) error {
	m.Time = e.Time
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
		delete(m.transactions, dbid)
	}
	return nil
}

// Seek applies all events that are before or at time `t` to the current mempool
func (m *Mempool) Seek(t time.Time) error {
	if t.Before(m.Time) {
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

// TransactionMap returns a map `DBID -> StoredTransaction`
func (m *Mempool) TransactionMap() map[int64]types.StoredTransaction {
	return m.transactions
}

// Transactions returns the list of Transactions
func (m *Mempool) Transactions() (res []types.Transaction) {
	for _, tx := range m.transactions {
		res = append(res, tx.Transaction)
	}
	return
}
