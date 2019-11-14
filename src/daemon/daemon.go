package daemon

import (
	"fmt"
	"log"

	"github.com/0xb10c/bademeister-go/src/storage"
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"
)

type BademeisterDaemon struct {
	zmqSub  *zmqsubscriber.ZMQSubscriber
	storage *storage.Storage
	quit    chan struct{}
}

// NewBademeisterDaemon initiates a new BademeisterDaemon.
func NewBademeisterDaemon(host, port, dbPath string) (*BademeisterDaemon, error) {
	zmqSub, err := zmqsubscriber.NewZMQSubscriber(host, port)
	if err != nil {
		return nil, fmt.Errorf("Could not setup ZMQ subscriber: %s", err)
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
	return b.storage.InsertTransaction(tx)
}

func (b *BademeisterDaemon) processBlock(block *types.Block) error {
	log.Printf("Received block, updating transactions")
	// TODO update storage
	return nil
}

func (b *BademeisterDaemon) dumpStats() {
	count, err := b.storage.TxCount()
	if err != nil {
		log.Printf("Can not dump stats: %s", err)
		return
	}
	log.Printf("Current transaction count: %d", count)
}

func (b *BademeisterDaemon) Run() error {
	var zmqSubErr error
	go func() {
		zmqSubErr = b.zmqSub.Run()
		b.Stop()
	}()

	for {
		select {
		case <-b.quit:
			log.Printf("Received quit signal")
			return zmqSubErr
		case tx := <-b.zmqSub.IncomingTx:
			if err := b.processTransaction(&tx); err != nil {
				log.Printf("Error in processTransaction()")
				return err
			}
		case block := <-b.zmqSub.IncomingBlocks:
			if err := b.processBlock(&block); err != nil {
				log.Printf("Error in processBlock()")
				return err
			}
		}

		b.dumpStats()
	}
}

func (b *BademeisterDaemon) Stop() {
	b.quit <- struct{}{}
}

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
