package test

import (
	"fmt"
	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

const dataDir = "/tmp/test-bademeister-go"
const zmqHost = "127.0.0.1"
const zmqPort = "28334"
const rpcPort = 18333

type TestEnv struct {
	cmd *exec.Cmd
	rpc *rpcclient.Client
	ZmqHost string
	ZmqPort string
}

func newBitcoindRpc() (*rpcclient.Client, error) {
	var cookie []byte
	var err error
	// wait 1s for cookie file to appear
	for i := 0; i < 10; i++ {
		cookie, err = ioutil.ReadFile(fmt.Sprintf("%s/regtest/.cookie", dataDir))
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			panic(err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if len(cookie) == 0 {
		panic("could not get cookie")
	}

	parts := strings.Split(string(cookie), ":")
	cfg := &rpcclient.ConnConfig{
		Host: fmt.Sprintf("127.0.0.1:%d", rpcPort),
		User: parts[0],
		Pass: parts[1],
		HTTPPostMode: true,
		DisableTLS: true,
	}
	rpc, err := rpcclient.New(cfg, nil)
	if err != nil {
		return nil, err
	}

	for {
		n, err := rpc.GetBlockCount()

		if err == nil {
			log.Printf("blockCount=%d", n)
			break
		}

		if rpcErr, ok := err.(*btcjson.RPCError); ok {
			if rpcErr.Code == -28 {
				time.Sleep(100* time.Millisecond)
				continue
			}
		}

		fmt.Printf("err=%v", err)
		panic(err)
	}

	return rpc, nil
}

func NewTestEnv() TestEnv {
	err := os.RemoveAll(dataDir)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}

	err = os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		panic(err)
	}

	zmqUrl := fmt.Sprintf("tcp://%s:%s", zmqHost, zmqPort)
	args := []string{
		"-regtest=1",
		fmt.Sprintf("-datadir=%s", dataDir),
		"-rpcallowip=127.0.0.1",
		fmt.Sprintf("-rpcbind=127.0.0.1:%d", rpcPort),
		fmt.Sprintf("-zmqpubrawtx=%s", zmqUrl),
		fmt.Sprintf("-zmqpubrawblock=%s", zmqUrl),
		fmt.Sprintf("-zmqpubhashtx=%s", zmqUrl),
		fmt.Sprintf("-zmqpubhashblock=%s", zmqUrl),
		// unfortunately the bitcoind rpc lib doesn't support generatetoaddr yet
		// https://github.com/btcsuite/btcd/pull/845
		fmt.Sprintf("-deprecatedrpc=generate"),
	}
	cmd := exec.Command("bitcoind", args...)
	if err = cmd.Start(); err != nil {
		panic(err)
	}

	rpc, err := newBitcoindRpc()
	if err != nil {
		panic(err)
	}

	return TestEnv{
		cmd,
		rpc,
		zmqHost,
		zmqPort,
	}
}

func (e *TestEnv) GenerateBlocks(n uint32) []*chainhash.Hash {
	hashes, err := e.rpc.Generate(n)
	if err != nil {
		panic(err)
	}
	// allow txs to propagate to zmq
	time.Sleep(200 * time.Millisecond)
	return hashes
}

// Kill with SIGTERM, panic if process exited for some other reason
func (e *TestEnv) Quit() {
	err := e.cmd.Process.Signal(syscall.SIGINT)
	if err != nil {
		panic(err)
	}

	err = e.cmd.Wait()
	if err == nil {
		return
	}

	exitErr, ok := err.(*exec.ExitError);
	if !ok {
		panic(err)
	}

	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok {
		panic(err)
	}

	if status.ExitStatus() == 1 {
		return
	}

	if status.Signaled() && status.Signal() == syscall.SIGINT {
		return
	}

	panic(err)
}

