package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/0xb10c/bademeister-go/src/daemon"
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
		s := <-c
		log.Printf("Received signal %s, shutting down", s)
		d.Stop()
	}()

	errRun := d.Run()
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
