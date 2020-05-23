package main

import (
	"flag"
	"os"
	"os/signal"
	"time"

	"github.com/0xb10c/bademeister-go/src/bitcoinrpcclient"
	"github.com/0xb10c/bademeister-go/src/daemon"
	"github.com/0xb10c/bademeister-go/src/zmqsubscriber"
	log "github.com/sirupsen/logrus"
)

var zmqAddress = flag.String("zmq-address", "tcp://127.0.0.1:28332", "zmq adddress")
var rpcAddress = flag.String("rpc-address", "http://127.0.0.1:18443", "rpc address")
var initBlocksRPC = flag.Bool("init-blocks-rpc", true, "backfill missed blocks via rpc")
var initMempoolRPC = flag.Bool("init-mempool-rpc", true, "fetch initial mempool via getrawmempool")
var dbPath = flag.String("db", "transactions.db", "path to transactions database")
var logLevel = flag.String("log", "info", "log level (info,debug,trace)")

func main() {
	flag.Parse()

	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: time.RFC3339,
		FullTimestamp:   true,
	})
	switch *logLevel {
	case "info":
		log.SetLevel(log.InfoLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "trace":
		log.SetLevel(log.TraceLevel)
	default:
		log.Fatalf("invalid log level %q", *logLevel)
	}

	log.Println("Starting Bademeister Daemon")
	log.Printf("log level %s", *logLevel)

	zmqSub, err := zmqsubscriber.NewZMQSubscriber(*zmqAddress)
	if err != nil {
		log.Fatalf("Could not setup ZMQ subscriber: %s", err)
	}

	var rpcClient *bitcoinrpcclient.BitcoinRPCClient
	if *rpcAddress != "" {
		log.Debugf("connecting to %s...", *rpcAddress)
		rpcClient, err = bitcoinrpcclient.NewBitcoinRPCClient(*rpcAddress)
		if err != nil {
			log.Fatalf("could not initialize rpcClient: %s", err)
		}
		log.Debugf("connected to %s", *rpcAddress)
	}

	d, err := daemon.NewBademeisterDaemon(zmqSub, rpcClient, *dbPath)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		s := <-c
		log.Errorf("Received signal %s, shutting down", s)
		d.Stop()
	}()

	errRun := d.Run(daemon.RunParams{
		InitMempoolRPC: *initMempoolRPC,
		InitBlocksRPC:  *initBlocksRPC,
	})
	if errRun != nil {
		log.Errorf("Error during operation, shutting down: %s", errRun)
	}

	errClose := d.Close()
	if errClose != nil {
		log.Errorf("Error during shutdown: %s", errClose)
	}

	if errRun != nil || errClose != nil {
		os.Exit(1)
	}

	os.Exit(0)
}
