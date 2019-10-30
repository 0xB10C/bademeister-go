package zmqsubscriber

import (
	"fmt"
	"log"
	"syscall"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"

	"github.com/pebbe/zmq4"
)

// ZMQSubscriber represents a ZMQ subscriber for the Bitcoin Core ZMQ interface
type ZMQSubscriber struct {
	IncomingTx     chan types.Transaction
	IncomingBlocks chan types.Block
	topics         []string
	socket         *zmq4.Socket
	cancel         bool
}

const topicHashTx = "hashtx"
const topicRawBlock = "rawblock"

// NewZMQSubscriber creates and returns a new ZMQSubscriber,
// which subscribes and connect to a Bitcoin Core ZMQ interface.
func NewZMQSubscriber(host string, port string) (*ZMQSubscriber, error) {
	socket, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		return nil, err
	}

	topics := []string{topicHashTx, topicRawBlock} //topicRawTxWithFee
	for _, topic := range topics {
		err := socket.SetSubscribe(topic)
		if err != nil {
			return nil, err
		}
	}

	connectionString := "tcp://" + host + ":" + port
	if err = socket.Connect(connectionString); err != nil {
		return nil, fmt.Errorf("could not connect ZMQ subscriber to '%s': %s", connectionString, err)
	}

	log.Printf("ZMQ subscriber successfully connected to %s", connectionString)

	incomingTx := make(chan types.Transaction)
	incomingBlocks := make(chan types.Block)

	return &ZMQSubscriber{
		topics:         topics,
		IncomingTx:     incomingTx,
		IncomingBlocks: incomingBlocks,
		socket:         socket,
		cancel:         false,
	}, nil
}

// Run starts receiving new ZMQ messages. These messages are parsed according to
// their topic and passed as native data types into the corresponding channels
// (`IncomingTx` or `IncomingBlocks). Run returns an error if an error occurs
// while parsing. On normal stops with `Stop()` `nil` is returned.
func (z *ZMQSubscriber) Run() error {
	defer func() {
		if err := z.socket.Close(); err != nil {
			log.Printf("ZMQ subscriber socket closed with error (ignored): %s\n", err)
		}
	}()

	parseErrors := make(chan error)

	if err := z.socket.SetRcvtimeo(time.Second); err != nil {
		return fmt.Errorf("could not set a receive timeout: %s", err)
	}

	// FIXME(#11):
	// Workaround for a zmq crash when Close() is called from a different
	// goroutine. Instead of permanently blocking on Recv(), a timeout is set and
	// we check for `z.cancel`.
	for !z.cancel {
		select {
		case err := <-parseErrors:
			return err
		default:
		}

		msg, err := z.socket.RecvMessageBytes(0)
		if err != nil {
			if err == zmq4.Errno(syscall.EAGAIN) {
				log.Println("No ZMQ message received in the last second.")
				continue
			} else if err == zmq4.Errno(syscall.EINTR) {
				continue
			}
			return fmt.Errorf("could not receive ZMQ message: %s", err)
		}

		topic, payload := string(msg[0]), msg[1:]
		log.Printf("ZMQ subscriber received topic %s", topic)

		// received messages are processed asynchronously so that the queue does not
		// stall while parsing
		go func() {
			if err := z.processMessage(topic, payload); err != nil {
				parseErrors <- err
			}
		}()
	}

	return nil
}

func (z *ZMQSubscriber) processMessage(topic string, payload [][]byte) error {
	// TODO: use GetTime() and allow other time sources (eg NTP-corrected)
	t := time.Now().UTC()

	switch topic {
	case topicHashTx:
		tx, err := parseTransactionHash(t, payload)
		if err != nil {
			return err
		}
		z.IncomingTx <- *tx
	case topicRawBlock:
		block, err := parseBlock(t, payload)
		if err != nil {
			return err
		}
		z.IncomingBlocks <- *block
	default:
		return fmt.Errorf("unknown topic %s", topic)
	}

	return nil
}

// Stop sets the cancel flag to true. The ZMQSubscriber is stopped after it
// finishes receiving a message or reaches the timeout.
func (z *ZMQSubscriber) Stop() {
	z.cancel = true
}

func parseTransactionHash(firstSeen time.Time, payload [][]byte) (*types.Transaction, error) {
	if len(payload) != 2 {
		return nil, fmt.Errorf("unexpected payload length: expected len((tx hash, sequence) == 2 but got len(payload) == %d", len(payload))
	}

	// payload[1] contains a 16bit LE sequence number provided by Bitcoin Core,
	// which is not used here, but noted for completeness.
	txhash, _ := payload[0], payload[1]

	var txid types.Hash32
	copy(txid[:], txhash)

	return &types.Transaction{
		FirstSeen: firstSeen,
		TxID:      txid,
	}, nil
}

func parseBlock(firstSeen time.Time, msg [][]byte) (*types.Block, error) {
	// TODO
	return &types.Block{}, nil
}
