package main

import (
	"github.com/0xb10c/bademeister-go/src/daemon"
	"log"
	"flag"
	"os"
	"os/signal"
)

var zmqHost = flag.String("zmq-host", "127.0.0.1", "zmq host")
var zmqPort = flag.String("zmq-port", "28332", "zmq port")
var dbPath = flag.String("db", "transactions.db", "path to transactions database")

func main() {
	flag.Parse()

	log.Println("Starting Bademeister Daemon")

	d, err := daemon.NewBademeisterDaemon(*zmqHost, *zmqPort, *dbPath)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		s := <- c
		log.Printf("Received signal %s, shutting down", s)
		d.Stop()
	}()

	if err := d.Run(); err != nil {
		log.Printf("Error %v, shutting down", err)
	}

	if err := d.Close(); err != nil {
		log.Fatalln("Error during shutdown")
	}

	os.Exit(0)
}
