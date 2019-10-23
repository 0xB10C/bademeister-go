package zmqsubscriber

import (
	"errors"
	"log"

	"github.com/pebbe/zmq4"
)

type ZMQSubscriber struct {
	Port    string
	Host    string
	Topics  []string
	isSetup bool
	socket  *zmq4.Socket
}

func NewZMQSubscriber(host string, port string, topics []string) (zmqSub ZMQSubscriber) {
	zmqSub = ZMQSubscriber{Host: host, Port: port, Topics: topics, isSetup: false}
	return
}

func (zmqSub *ZMQSubscriber) Setup() error {
	if zmqSub.isSetup {
		return errors.New("subscriber is already setup")
	}

	socket, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		return err
	}

	for _, topic := range zmqSub.Topics {
		err := socket.SetSubscribe(topic)
		if err != nil {
			return err
		}
	}

	zmqSub.socket = socket
	zmqSub.isSetup = true

	return nil
}

func (zmqSub *ZMQSubscriber) Loop() {
	if !zmqSub.isSetup {
		log.Fatalln("Could not loop ZMQ subscriber since it's not set up.")
		return
	}

	connectionString := "tcp://" + zmqSub.Host + ":" + zmqSub.Port
	err := zmqSub.socket.Connect(connectionString)
	if err != nil {
		log.Fatalf("Could not connect ZMQ subscriber to '%s': %s\n", connectionString, err)
	}

	defer func() {
		err := zmqSub.socket.Close()
		if err != nil {
			log.Fatalf("Could not close ZMQ subscriber: %s\n", err)
		}
	}()

	for {
		msg, err := zmqSub.socket.RecvMessage(0)
		if err != nil {
			log.Fatalf("Could not receive ZMQ message: %s\n", err)
		}
		// TODO: pass to message processing
		log.Println(msg)
	}
}
