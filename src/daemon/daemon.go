package daemon

import (
	"fmt"
	"log"

	"github.com/0xb10c/bademeister-go/src/bitcoinrpcclient"
	"github.com/0xb10c/bademeister-go/src/storage"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"
)

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
	log.Printf("Received transaction, adding to storage")
	_, err := b.storage.InsertTransaction(tx)
	return err
}

func (b *BademeisterDaemon) processBlock(block *types.Block) error {
	log.Printf("Received block %s height=%d, updating database", block.Hash, block.Height)
	_, err := b.storage.InsertBlock(block)
	return err
}

// dumpStats shows daemon stats.
// Can contain cpu-intensive calls.
func (b *BademeisterDaemon) dumpStats() {
	count, err := b.storage.TxCount()
	if err != nil {
		log.Printf("Can not dump stats: %s", err)
		return
	}
	log.Printf("Current transaction count: %d", count)
}

// RunParams describes run parameters for BademeisterDaemon
type RunParams struct {
	InitMempoolRPC bool
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

	if params.InitMempoolRPC {
		// it is OK to block here since IncomingTx will be queued
		if err := b.InitMempoolRPC(); err != nil {
			log.Printf("error initializing mempool from rpc: %s", err)
		}
	}
	b.dumpStats()

	for {
		select {
		case <-b.quit:
			log.Printf("Received quit signal")
			return zmqSubErr
		case tx := <-b.zmqSub.IncomingTx:
			if err := b.processTransaction(&tx); err != nil {
				log.Printf("Error in processTransaction(): %s", err)
				return err
			}
		case block := <-b.zmqSub.IncomingBlocks:
			if err := b.processBlock(&block); err != nil {
				log.Printf("Error in processBlock(): %s", err)
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

// Stop makes Run() return
func (b *BademeisterDaemon) Stop() {
	b.quit <- struct{}{}
}

// Close shuts down the storage
func (b *BademeisterDaemon) Close() error {
	errors := false

	errStorage := b.storage.Close()
	if errStorage != nil {
		log.Printf("error closing db: %v", errStorage)
		errors = true
	}

	if errors {
		return fmt.Errorf("there were errors, see logs for details")
	}

	return nil
}
