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
	IncomingTx     chan types.Transaction
	IncomingBlocks chan types.Block
	socket  *zmq4.Socket
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

	zmqSub := &ZMQSubscriber{
		Host:       host,
		Port:       port,
		Topics:     topics,
		socket:     socket,
		IncomingTx: incomingTx,
		IncomingBlocks: incomingBlocks,
	}

	go func () {
		for {
			log.Printf("waiting for msg")
			// FIXME(#11): sometimes we panic when socket.Close() is called
			// maybe we should Recv with a timeout and quit after reading
			msg, err := socket.RecvMessageBytes(0)
			if err != nil {
				fmt.Printf("%#v\n", err)
				panic(fmt.Errorf("Could not receive ZMQ message: %s", err))
			}
			// TODO: use GetTime() and allow other time sources (eg NTP-corrected)
			t := time.Now().UTC()
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

	return zmqSub, nil
}

func (z *ZMQSubscriber) Quit() error {
	log.Printf("closing socket...")
	err := z.socket.Close()
	if err != nil {
		z.socket = nil
	}
	return err
}
