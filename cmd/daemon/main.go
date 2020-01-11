package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/0xb10c/bademeister-go/src/bitcoinrpcclient"
	"github.com/0xb10c/bademeister-go/src/daemon"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"
)

var zmqAddress = flag.String("zmq-address", "tcp://127.0.0.1:28332", "zmq adddress")
var rpcAddress = flag.String("rpc-address", "http://127.0.0.1:18443", "rpc address")
var initMempoolRPC = flag.Bool("init-mempool-rpc", true, "fetch initial mempool via getrawmempool")
var dbPath = flag.String("db", "transactions.db", "path to transactions database")

func main() {
	flag.Parse()

	log.Println("Starting Bademeister Daemon")

	zmqSub, err := zmqsubscriber.NewZMQSubscriber(*zmqAddress)
	if err != nil {
		log.Fatalf("Could not setup ZMQ subscriber: %s", err)
	}

	var rpcClient *bitcoinrpcclient.BitcoinRPCClient
	if *rpcAddress != "" {
		rpcClient, err = bitcoinrpcclient.NewBitcoinRPCClient(*rpcAddress)
		if err != nil {
			log.Fatalf("could not initialize rpcClient: %s", err)
		}
	}

	d, err := daemon.NewBademeisterDaemon(zmqSub, rpcClient, *dbPath)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		s := <-c
		log.Printf("Received signal %s, shutting down", s)
		d.Stop()
	}()

	errRun := d.Run(daemon.RunParams{InitMempoolRPC: *initMempoolRPC})
	if errRun != nil {
		log.Printf("Error during operation, shutting down: %s", errRun)
	}

	errClose := d.Close()
	if errClose != nil {
		log.Printf("Error during shutdown: %s", errClose)
	}

	if errRun != nil || errClose != nil {
		os.Exit(1)
	}

	os.Exit(0)
}
