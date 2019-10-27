package daemon

import (
	"fmt"
	"github.com/0xb10c/bademeister-go/src/storage"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"
)

type BademeisterDaemon struct {
	zmqSub     	*zmqsubscriber.ZMQSubscriber
	storage 	*storage.Storage
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
	return &BademeisterDaemon{zmqSub, store}, nil
}
