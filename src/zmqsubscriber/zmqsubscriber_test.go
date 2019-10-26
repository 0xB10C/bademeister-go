package zmqsubscriber

import (
	"github.com/0xb10c/bademeister-go/src/types"
	"os"
	"syscall"
)
import "fmt"
import "os/exec"
import "testing"

const dataDir = "/tmp/test-bademeister-go"
const zmqHost = "127.0.0.1"
const zmqPort = "28334"
const rpcPort = 18333

type bitcoindProc struct {
	cmd *exec.Cmd
}

func newBitcoindProc() bitcoindProc {
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
	}
	cmd := exec.Command("bitcoind", args...)
	if err = cmd.Start(); err != nil {
		panic(err)
	}
	return bitcoindProc{
		cmd,
	}
}

// Kill with SIGTERM, panic if process exited for some other reason
func (p *bitcoindProc) quit() {
	err := p.cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		panic(err)
	}
	err = p.cmd.Wait()
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

	if !status.Signaled() {
		panic(err)
	}

	if status.Signal() != syscall.SIGTERM {
		panic(err)
	}
}

type CaptureTransactions struct {
	txs []types.Transaction
}

func newTxCapture(z *ZMQSubscriber) *CaptureTransactions {
	txs := []types.Transaction{}
	capture := CaptureTransactions{txs}
	go func() {
		for t := range z.IncomingTx {
			capture.txs = append(txs, t)
		}
	}()
	return &capture
}

func TestZMQSubscriber(t *testing.T) {
	proc := newBitcoindProc()
	defer proc.quit()
	z, err := NewZMQSubscriber(zmqHost, zmqPort)
	if err != nil {
		t.Error(err)
	}

	capture := newTxCapture(z)

	generateBlocks(100)
}
