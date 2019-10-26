package daemon

import (
	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"
	"log"
)

type BademeisterDaemon struct {
	zmqSub         zmqsubscriber.ZMQSubscriber
	incomingTx     chan types.Transaction
	incomingBlocks chan types.Block
}

// NewBademeisterDaemon initiates a new BademeisterDaemon.
func NewBademeisterDaemon() (d *BademeisterDaemon) {
	txChan := make(chan types.Transaction)
	blockChan := make(chan types.Block)

	d.incomingTx = txChan
	d.incomingBlocks = blockChan
	return d
}

// Start starts the BademeisterDaemon
func (daemon *BademeisterDaemon) Start(host, port string) {
	daemon.zmqSub = zmqsubscriber.NewZMQSubscriber(host, port, []string{"rawtx", "rawblock"})
	err := daemon.zmqSub.Setup()
	if err != nil {
		log.Fatalf("Could not setup ZMQ subscriber: %s", err)
	}
	daemon.zmqSub.Loop()
}
