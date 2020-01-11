package zmqsubscriber

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/wire"
	"github.com/pebbe/zmq4"
	"github.com/pkg/errors"

	"github.com/0xb10c/bademeister-go/src/types"
)

// ZMQSubscriber represents a ZMQ subscriber for the Bitcoin Core ZMQ interface
type ZMQSubscriber struct {
	// Deserialized transactions
	IncomingTx chan types.Transaction
	// Deserialized blocks
	IncomingBlocks chan types.Block
	topics         []string
	socket         *zmq4.Socket
	cancel         bool
}

const topicRawTxWithFee = "rawtxwithfee"
const topicRawBlock = "rawblock"

// In order to allow non-blocking writes to channels, initialize them
// with a certain capacity. During long-running synchronous calls (GetRawMempoolVerbose()),
// the channel readers can be stalled for a while.
const channelSizeTx = 256
const channelSizeBlock = 256

// ErrChannelCapacityExceeded is returned when channel write is blocked
type ErrChannelCapacityExceeded string

func (e ErrChannelCapacityExceeded) Error() string {
	return fmt.Sprintf("channel capacity exceeded (%s)", e)
}

// NewZMQSubscriber creates and returns a new ZMQSubscriber,
// which subscribes and connect to a Bitcoin Core ZMQ interface.
func NewZMQSubscriber(zmqAddress string) (*ZMQSubscriber, error) {
	socket, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		return nil, err
	}

	topics := []string{topicRawTxWithFee, topicRawBlock}
	for _, topic := range topics {
		err := socket.SetSubscribe(topic)
		if err != nil {
			return nil, err
		}
	}

	if err = socket.Connect(zmqAddress); err != nil {
		return nil, errors.Errorf("could not connect ZMQ subscriber to '%s': %s", zmqAddress, err)
	}

	log.Printf("ZMQ subscriber successfully connected to %s", zmqAddress)

	incomingTx := make(chan types.Transaction, channelSizeTx)
	incomingBlocks := make(chan types.Block, channelSizeBlock)

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
// (`IncomingTx` or `IncomingBlocks`). Run returns an error if an error occurs
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
	firstSeen := time.Now().UTC()

	switch topic {
	case topicRawTxWithFee:
		tx, err := parseTransaction(firstSeen, payload)
		if err != nil {
			return err
		}

		if len(z.IncomingTx) > (channelSizeTx / 2) {
			fmt.Printf("warning: chan IncomingTx at %d/%d", len(z.IncomingTx), channelSizeTx)
		}

		select {
		case z.IncomingTx <- *tx:
		default:
			return ErrChannelCapacityExceeded("IncomingTx")
		}
	case topicRawBlock:
		block, err := parseBlock(firstSeen, payload)
		if err != nil {
			return err
		}

		if len(z.IncomingBlocks) > (channelSizeBlock / 2) {
			fmt.Printf("warning: chan IncomingBlocks at %d/%d", len(z.IncomingBlocks), channelSizeBlock)
		}

		select {
		case z.IncomingBlocks <- *block:
		default:
			return ErrChannelCapacityExceeded("IncomingBlocks")
		}
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

func parseTransaction(firstSeen time.Time, payload [][]byte) (*types.Transaction, error) {
	if len(payload) != 2 {
		return nil, fmt.Errorf("unexpected payload length: expected len(tx hash, sequence) == 2 but got len(payload) == %d", len(payload))
	}

	// payload[1] contains a 16bit LE sequence number provided by Bitcoin Core,
	// which is not used here, but noted for completeness.
	// rawtxwithfee is the rawtx by Bitcoin Core concatinated with the 8 byte LE
	// transaction fee. This is a patch from the branch
	// https://github.com/0xB10C/bitcoin/tree/2019-10-rawtxwithfee-zmq-publisher
	rawtxwithfee, _ := payload[0], payload[1]

	length := len(rawtxwithfee)
	if length <= 8 {
		return nil, errors.New("unexpected rawtxwithfee length")
	}
	rawtx, feeBytes := rawtxwithfee[:length-8], rawtxwithfee[length-8:]

	wireTx := wire.NewMsgTx(wire.TxVersion)
	if err := wireTx.Deserialize(bytes.NewReader(rawtx)); err != nil {
		return nil, fmt.Errorf("could not deserialize the rawtx as wire.MsgTx: %s", err)
	}

	txid := types.NewHashFromArray(wireTx.TxHash())

	fee := binary.LittleEndian.Uint64(feeBytes)
	weight := wireTx.SerializeSizeStripped()*3 + wireTx.SerializeSize()

	return &types.Transaction{
		FirstSeen: firstSeen,
		TxID:      txid,
		Fee:       fee,
		Weight:    weight,
	}, nil
}

func parseBlock(firstSeen time.Time, msg [][]byte) (*types.Block, error) {
	rawblock, ctr := msg[0], msg[1]
	_ = ctr
	return types.NewBlock(firstSeen, rawblock)
}
