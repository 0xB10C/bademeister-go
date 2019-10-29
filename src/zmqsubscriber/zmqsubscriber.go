package zmqsubscriber

import (
	"fmt"
	"github.com/0xb10c/bademeister-go/src/types"
	"log"
	"syscall"
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
	cancel  bool
}

func parseTransaction(firstSeen time.Time, msg [][]byte) (*types.Transaction, error) {
	if len(msg) != 2 {
		panic(fmt.Sprintf("unknown message format: len(msg)=%d", len(msg)))
	}
	txhash, ctr := msg[0], msg[1]
	_ = ctr
	var txid types.Hash32
	copy(txid[:], txhash)
	// TODO: provider other values
	return &types.Transaction{
		FirstSeen: firstSeen,
		TxID: txid,
	}, nil
}

func parseBlock(firstSeen time.Time, msg [][]byte) (*types.Block, error) {
	// TODO
	return &types.Block{}, nil
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

	log.Printf("[ZMQ] successfully connected to %s", connectionString)

	incomingTx := make(chan types.Transaction)
	incomingBlocks := make(chan types.Block)

	return &ZMQSubscriber{
		Host:           host,
		Port:           port,
		Topics:         topics,
		IncomingTx:     incomingTx,
		IncomingBlocks: incomingBlocks,
		socket:         socket,
		cancel:         false,
	}, nil
}

func (z *ZMQSubscriber) processMessage(topic string, payload [][]byte) error {
	// TODO: use GetTime() and allow other time sources (eg NTP-corrected)
	t := time.Now().UTC()

	switch topic {
	case TOPIC_HASHTX:
		tx, err := parseTransaction(t, payload);
		if err != nil {
			return err
		}
		z.IncomingTx <- *tx
	case TOPIC_RAWBLOCK:
		block, err := parseBlock(t, payload);
		if err != nil {
			return err
		}
		z.IncomingBlocks <- *block
	default:
		return fmt.Errorf("unknown topic %s", topic)
	}

	return nil
}

// Listen for new messages, parse messages into native data types and put them into channels
// `IncomingTx` and `IncomingBlocks`.
// Returns error if something goes wrong and `nil` if stopped using `Stop()`
func (z *ZMQSubscriber) Run() error {
	defer func () {
		if err := z.socket.Close(); err != nil {
			log.Printf("[ZMQ] socket closed with error %q (ignored)", err)
		}
	}()

	parseErrors := make(chan error)

	// FIXME(#11):  Workaround for bug where zmq crashes when Close() is called from a different
	// 				goroutine. Instead of permanently blocking on Recv(), we set a timeout and check
	// 				for `z.cancel`.
	for !z.cancel {
		// if a parse error happened, exit the loop
		select {
		case err := <-parseErrors:
			return err
		default:
		}

		if err := z.socket.SetRcvtimeo(time.Second); err != nil {
			return fmt.Errorf("error setting timeout: %s", err)
		}

		msg, err := z.socket.RecvMessageBytes(0)
		if err != nil {
			if err == zmq4.ETIMEDOUT || err == zmq4.Errno(syscall.EAGAIN) {
				continue
			}

			return fmt.Errorf("Could not receive ZMQ message: %s %v", err, err)
		}

		topic, payload := string(msg[0]), msg[1:]
		log.Printf(`[ZMQ] received topic "%s"`, topic)

		// process received messages asynchronously so that we do not stall the queue while parsing
		go func() {
			if err := z.processMessage(topic, payload); err != nil {
				parseErrors <- err
			}
		}()
	}

	return nil
}

func (z *ZMQSubscriber) Stop() {
	z.cancel = true
}
