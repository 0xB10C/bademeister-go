package daemon

import (
	"fmt"
	"log"

	"github.com/0xb10c/bademeister-go/src/storage"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"
)

// BademeisterDaemon reads data off ZMQSubscriber and inserts it to Storage
type BademeisterDaemon struct {
	zmqSub  *zmqsubscriber.ZMQSubscriber
	storage *storage.Storage
	quit    chan struct{}
}

// NewBademeisterDaemon initiates a new BademeisterDaemon.
func NewBademeisterDaemon(
	zmqSub *zmqsubscriber.ZMQSubscriber,
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
	return &BademeisterDaemon{zmqSub, store, quit}, nil
}

func (b *BademeisterDaemon) processTransaction(tx *types.Transaction) error {
	log.Printf("Received transaction, adding to storage")
	_, err := b.storage.InsertTransaction(tx)
	return err
}

func (b *BademeisterDaemon) processBlock(block *types.Block) error {
	log.Printf("Received block, updating database")
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

// Run starts the zmqSub loop which feeds zmqSub channels.
// Wait on zmqSub channels and call `processBlock`, `processTransaction`.
// Stop on quit signal or errors.
func (b *BademeisterDaemon) Run() error {
	var zmqSubErr error
	go func() {
		zmqSubErr = b.zmqSub.Run()
		b.Stop()
	}()

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
