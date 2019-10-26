package daemon

import (
	"fmt"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"
)

type BademeisterDaemon struct {
	zmqSub         *zmqsubscriber.ZMQSubscriber
}

// NewBademeisterDaemon initiates a new BademeisterDaemon.
func NewBademeisterDaemon(host, port string) (*BademeisterDaemon, error) {
	zmqSub, err := zmqsubscriber.NewZMQSubscriber(host, port)
	if err != nil {
		return nil, fmt.Errorf("Could not setup ZMQ subscriber: %s", err)
	}
	return &BademeisterDaemon{zmqSub}, nil
}
