package daemon

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/0xb10c/bademeister-go/src/bitcoinrpcclient"
	"github.com/0xb10c/bademeister-go/src/storage"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"

	log "github.com/sirupsen/logrus"
)

// ErrMaxBackfill is returned when InitBlocksRPC needs to fetch too many blocks
var ErrMaxBackfill = errors.New("maxBackfill exceeded")

// BademeisterDaemon reads data off ZMQSubscriber and inserts it to Storage
type BademeisterDaemon struct {
	zmqSub    *zmqsubscriber.ZMQSubscriber
	rpcClient *bitcoinrpcclient.BitcoinRPCClient
	storage   *storage.Storage
	quit      chan struct{}
}

// NewBademeisterDaemon initiates a new BademeisterDaemon.
func NewBademeisterDaemon(
	zmqSub *zmqsubscriber.ZMQSubscriber,
	rpcClient *bitcoinrpcclient.BitcoinRPCClient,
	dbPath string,
) (*BademeisterDaemon, error) {
	if zmqSub == nil {
		return nil, fmt.Errorf("zmqSub must not be nil")
	}

	store, err := storage.NewStorage(dbPath)
	if err != nil {
		return nil, fmt.Errorf("could not initialize storage: %s", err)
	}

	quit := make(chan struct{}, 1)
	return &BademeisterDaemon{
		zmqSub:    zmqSub,
		rpcClient: rpcClient,
		storage:   store,
		quit:      quit,
	}, nil
}

func (b *BademeisterDaemon) processTransaction(tx *types.Transaction) error {
	log.Debug("Received transaction, adding to storage")
	_, err := b.storage.InsertTransaction(tx)
	return err
}

func (b *BademeisterDaemon) processBlock(block *types.Block) error {
	log.Debugf("Received block %s height=%d, updating database", block.Hash, block.Height)
	_, err := b.storage.InsertBlock(block)
	return err
}

type stats struct {
	date  time.Time
	count int
}

// dumpStats shows daemon stats.
// Can contain cpu-intensive calls.
func (b *BademeisterDaemon) dumpStatsRel(prevStats *stats) *stats {
	count, err := b.storage.TxCount()
	if err != nil {
		log.Errorf("Can not dump stats: %s", err)
		return nil
	}
	date := time.Now()
	msg := fmt.Sprintf("Transaction count: %d", count)
	if prevStats != nil {
		msg = fmt.Sprintf(
			"%s (%.2f/sec)", msg,
			float64(count-prevStats.count)/date.Sub(prevStats.date).Seconds(),
		)
	}
	log.Infof(msg)
	return &stats{date, count}
}

func (b *BademeisterDaemon) dumpStats() *stats {
	return b.dumpStatsRel(nil)
}

func (b *BademeisterDaemon) dumpStatsLoop() {
	var s *stats
	for {
		select {
		case <-b.quit:
			b.quit <- struct{}{}
			return
		case <-time.After(10 * time.Second):
			s = b.dumpStatsRel(s)
		}
	}
}

// RunParams describes run parameters for BademeisterDaemon
type RunParams struct {
	InitMempoolRPC bool
	InitBlocksRPC  bool
}

// Run starts the zmqSub loop which feeds zmqSub channels.
// Wait on zmqSub channels and call `processBlock`, `processTransaction`.
// Stop on quit signal or errors.
func (b *BademeisterDaemon) Run(params RunParams) error {
	var zmqSubErr error
	go func() {
		zmqSubErr = b.zmqSub.Run()
		b.Stop()
	}()

	go b.dumpStatsLoop()

	if params.InitMempoolRPC {
		// it is OK to block here since IncomingTx will be queued
		if err := b.InitMempoolRPC(); err != nil {
			log.Printf("error initializing mempool from rpc: %s", err)
			return err
		}
	}

	if params.InitBlocksRPC {
		hasBlocks, err := b.storage.HasBlocks()
		if err != nil {
			return errors.WithStack(err)
		}
		if hasBlocks {
			if err := b.InitBlocksRPC(); err != nil {
				log.Printf("error backfilling blocks from rpc: %s", err)
				return err
			}
		} else {
			log.Printf("no blocks in database, skipping InitBlocksRPC")
		}
	}
	b.dumpStats()

	for {
		select {
		case <-b.quit:
			log.Printf("Received quit signal")
			b.quit <- struct{}{}
			return zmqSubErr
		case tx := <-b.zmqSub.IncomingTx:
			if err := b.processTransaction(&tx); err != nil {
				log.Errorf("Error in processTransaction(): %s", err)
				return err
			}
		case block := <-b.zmqSub.IncomingBlocks:
			if err := b.processBlock(&block); err != nil {
				log.Errorf("Error in processBlock(): %s", err)
				return err
			}
			b.dumpStats()
		}
	}
}

// InitMempoolRPC uses the bitcoind rpc command `getrawmemmpool` to get the current mempool snapshot
func (b *BademeisterDaemon) InitMempoolRPC() error {
	if b.rpcClient == nil {
		return errors.New("no rpcClient")
	}

	log.Printf("Fetching raw mempool...")
	mempool, err := b.rpcClient.GetRawMempoolVerbose()
	if err != nil {
		return errors.Wrap(err, "error getting raw mempool")
	}

	log.Printf("Inserting %d transactions...", len(mempool))

	mempoolTxs, err := bitcoinrpcclient.RawMempoolToTransactions(mempool)
	if err != nil {
		return err
	}

	for _, tx := range mempoolTxs {
		err = b.processTransaction(&tx)
		if err != nil {
			return err
		}
	}

	log.Printf("Initial mempool insertion complete.")

	return nil
}

func (b *BademeisterDaemon) findMissingBlocks(maxBackfill int) (res []types.Block, err error) {
	if b.rpcClient == nil {
		return nil, errors.New("no rpcClient")
	}

	current, err := b.rpcClient.GetBestBlockHash()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for {
		if len(res) > maxBackfill {
			return nil, ErrMaxBackfill
		}

		h := types.NewHashFromArray(*current)
		log.Printf("fetching block %s n=%d limit=%d", h, len(res), maxBackfill)

		dbBlock, err := b.storage.BlockByHash(h)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if dbBlock != nil {
			break
		}

		wireBlock, err := b.rpcClient.GetBlock(current)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		block, err := types.NewBlockFromWireBlock(wireBlock.Header.Timestamp, wireBlock)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		res = append([]types.Block{*block}, res...)

		current = &wireBlock.Header.PrevBlock
	}

	return res, nil
}

// InitBlocksRPC fetches missing blocks that were dropped while BademeisterDaemon was not running
func (b *BademeisterDaemon) InitBlocksRPC() error {
	bestBlock, err := b.storage.BestBlockNow()
	if err != nil {
		return errors.WithStack(err)
	}
	if bestBlock == nil {
		return nil
	}

	maxBackfill := 100
	missingBlocks, err := b.findMissingBlocks(maxBackfill)
	if err != nil {
		return errors.WithStack(err)
	}

	log.Printf("inserting %d missing blocks...", len(missingBlocks))
	for _, block := range missingBlocks {
		if err := b.processBlock(&block); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

// Stop makes Run() return
func (b *BademeisterDaemon) Stop() {
	b.quit <- struct{}{}
}

// Close shuts down the storage
func (b *BademeisterDaemon) Close() error {
	errors := false

	errStorage := b.storage.Close()
	if errStorage != nil {
		log.Errorf("error closing db: %v", errStorage)
		errors = true
	}

	if errors {
		return fmt.Errorf("there were errors, see logs for details")
	}

	return nil
}
