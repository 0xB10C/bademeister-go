package zmqsubscriber

import (
	"github.com/0xb10c/bademeister-go/src/types"
	"log"

	"github.com/pebbe/zmq4"
)

type ZMQSubscriber struct {
	Port    string
	Host    string
	Topics  []string
	socket  *zmq4.Socket
	IncomingTx     chan types.Transaction
	IncomingBlocks chan types.Block
}

func parseTransaction(msg [][]byte) types.Transaction {
	return types.Transaction{}
}

func NewZMQSubscriber(host string, port string) (*ZMQSubscriber, error) {
	socket, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		return nil, err
	}

	topics := []string{"hashtx", "rawblock"}
	for _, topic := range topics {
		err := socket.SetSubscribe(topic)
		if err != nil {
			return nil, err
		}
	}

	connectionString := "tcp://" + host + ":" + port
	err = socket.Connect(connectionString)
	if err != nil {
		log.Printf("Could not connect ZMQ subscriber to '%s': %s\n", connectionString, err)
		return nil, err
	}

	incomingTx := make(chan types.Transaction)
	incomingBlocks := make(chan types.Block)

	go func () {
		for {
			msg, err := socket.RecvMessageBytes(0)
			if err != nil {
				log.Fatalf("Could not receive ZMQ message: %s\n", err)
			}
			log.Printf("%v", msg)
			incomingTx <- parseTransaction(msg)
		}
	}()

	return &ZMQSubscriber{
		Host:       host,
		Port:       port,
		Topics:     topics,
		socket:     socket,
		IncomingTx: incomingTx,
		IncomingBlocks: incomingBlocks,
	}, nil
}

func (z *ZMQSubscriber) Quit() error {
	return z.socket.Close()
}
