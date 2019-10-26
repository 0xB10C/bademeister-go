package main

import (
	"github.com/0xb10c/bademeister-go/src/daemon"
	"log"
)

func main() {
	log.Println("Starting Bademeister Daemon")

	d := daemon.NewBademeisterDaemon()
	d.Start("127.0.0.1", "28332")
}
