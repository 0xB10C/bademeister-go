package zmqsubscriber

import (
	"fmt"
	"github.com/0xb10c/bademeister-go/src/types"
	"log"
	"time"

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

func parseTransaction(firstSeen time.Time, msg [][]byte) types.Transaction {
	if len(msg) != 2 {
		panic(fmt.Sprintf("unknown message format: len(msg)=%d", len(msg)))
	}
	txhash, ctr := msg[0], msg[1]
	_ = ctr
	var txid types.Hash32
	copy(txid[:], txhash)
	// TODO: provider other values
	return types.Transaction{
		FirstSeen: firstSeen,
		TxID: txid,
	}
}

func parseBlock(firstSeen time.Time, msg [][]byte) types.Block {
	// TODO
	return types.Block{}
}

const TOPIC_HASHTX = "hashtx"
const TOPIC_RAWBLOCK = "rawblock"

func NewZMQSubscriber(host string, port string) (*ZMQSubscriber, error) {
	socket, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		return nil, err
	}

	topics := []string{TOPIC_HASHTX, TOPIC_RAWBLOCK}
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
			log.Printf("waiting for msg")
			msg, err := socket.RecvMessageBytes(0)
			if err != nil {
				panic(fmt.Errorf("Could not receive ZMQ message: %s", err))
			}
			// TODO: use GetTime() and allow other time sources (eg NTP-corrected)
			t := time.Now()
			topic, payload := string(msg[0]), msg[1:]
			log.Printf("%s: %d parts", topic, len(payload))
			switch topic {
			case TOPIC_HASHTX:
				incomingTx <- parseTransaction(t, payload)
			case TOPIC_RAWBLOCK:
				incomingBlocks <- parseBlock(t, payload)
			default:
				panic(fmt.Sprintf("unknown topic %v", topic))
			}
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
