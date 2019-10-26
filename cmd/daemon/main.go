package main

import (
	"github.com/0xb10c/bademeister-go/src/daemon"
	"log"
)

func main() {
	log.Println("Starting Bademeister Daemon")

	_, err := daemon.NewBademeisterDaemon("127.0.0.1", "28332")
	if err != nil {
		log.Fatal(err)
	}
}
