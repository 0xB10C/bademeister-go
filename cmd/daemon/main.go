package main

import (
	"github.com/0xb10c/bademeister-go/src/daemon"
	"log"
	"flag"
)

var zmqHost = flag.String("zmq-host", "127.0.0.1", "zmq host")
var zmqPort = flag.String("zmq-port", "28332", "zmq port")
var dbPath = flag.String("db", "transactions.db", "path to transactions database")

func main() {
	flag.Parse()

	log.Println("Starting Bademeister Daemon")

	_, err := daemon.NewBademeisterDaemon(*zmqHost, *zmqPort, *dbPath)
	if err != nil {
		log.Fatal(err)
	}
}
