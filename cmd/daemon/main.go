package main

import (
	"log"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"
)

type BademeisterDaemon struct {
	zmqSub         zmqsubscriber.ZMQSubscriber
	incomingTx     chan types.Transaction
	incomingBlocks chan types.Block
}

func main() {
	d := NewBademeisterDaemon()
	d.Start()
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
func (daemon *BademeisterDaemon) Start() {
	log.Println("Starting Bademeister Daemon")

	// TODO: parameterize host and port
	daemon.zmqSub = zmqsubscriber.NewZMQSubscriber("127.0.0.1", "28332", []string{"rawtx", "rawblock"})
	err := daemon.zmqSub.Setup()
	if err != nil {
		log.Fatalf("Could not setup ZMQ subscriber: %s", err)
	}
	daemon.zmqSub.Loop()
}
