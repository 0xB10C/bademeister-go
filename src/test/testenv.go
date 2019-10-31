package test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/0xb10c/bademeister-go/src/types"
	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcutil"
)

const DataDir = "/tmp/test-bademeister-go/"
const zmqHost = "127.0.0.1"
const zmqPort = "28334"
const rpcPort = "18333"

type TestEnv struct {
	cmd     *exec.Cmd
	rpc     *rpcclient.Client
	ZMQHost string
	ZMQPort string
}

func newBitcoindRPC() (*rpcclient.Client, error) {
	var cookie []byte
	var err error
	// wait 1s for cookie file to appear
	for i := 0; i < 10; i++ {
		cookie, err = ioutil.ReadFile(DataDir + "regtest/.cookie")
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
		time.Sleep(100 * time.Millisecond)
	}

	if len(cookie) == 0 {
		return nil, err
	}

	parts := strings.Split(string(cookie), ":")
	cfg := &rpcclient.ConnConfig{
		Host:         "127.0.0.1:" + rpcPort,
		User:         parts[0],
		Pass:         parts[1],
		HTTPPostMode: true,
		DisableTLS:   true,
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
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		fmt.Printf("err=%v", err)
	}

	return rpc, nil
}

func NewTestEnv() (testEnv *TestEnv, err error) {
	err = os.RemoveAll(DataDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("Could remove the data directory (%s): %s", DataDir, err)
	}

	err = os.MkdirAll(DataDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("Could not create the data directory (%s): %s", DataDir, err)
	}

	zmqURL := fmt.Sprintf("tcp://%s:%s", zmqHost, zmqPort)
	args := []string{
		"-regtest",
		"-datadir=" + DataDir,
		"-rpcallowip=127.0.0.1",
		"-rpcbind=127.0.0.1:" + rpcPort,
		"-zmqpubrawblock=" + zmqURL,
		"-zmqpubrawtxwithfee=" + zmqURL,
		// It takes a while until the fee estimation is ready on regtest. To send
		// wallet transactions the `-fallbackfee` must be set.
		"-fallbackfee=0.00001",
	}
	cmd := exec.Command("./../../testdata/bin/bitcoind/bitcoind", args...)
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("could not start bitcoind: %s", err)
	}

	rpc, err := newBitcoindRPC()
	if err != nil {
		return nil, fmt.Errorf("could not start a new bitcoind RPC: %s", err)
	}

	return &TestEnv{
		cmd:     cmd,
		rpc:     rpc,
		ZMQHost: zmqHost,
		ZMQPort: zmqPort,
	}, nil
}

// GetNewAddress returns a new address via the `getnewaddress` RPC.
func (e *TestEnv) GetNewAddress() (btcutil.Address, error) {
	rawResponse, err := e.rpc.RawRequest("getnewaddress", []json.RawMessage{})
	if err != nil {
		return nil, fmt.Errorf("the rawRequest '%s' failed: %s", "generatetoaddress", err)
	}

	var result string
	err = json.Unmarshal(rawResponse, &result)
	if err != nil {
		return nil, err
	}

	address, err := btcutil.DecodeAddress(result, &chaincfg.RegressionNetParams)
	if err != nil {
		return nil, fmt.Errorf("could not decode the returned address: %s", err)
	}

	return address, nil
}

// GenerateToAddress mines `nBlocks` to the passed address and returns the block
// hashes.
func (e *TestEnv) GenerateToAddress(nBlocks int, address btcutil.Address) ([]*chainhash.Hash, error) {
	// Unfortunately the bitcoind RPC package doesn't support `generatetoaddress`
	// https://github.com/btcsuite/btcd/pull/845. In Bitcoin Core version v0.19.0
	// the `generate` RPC is removed after being deprecated in v0.18.0. This
	// methods provides a workaround by using the RawRequest() function.

	jsonNBlocks, err := json.Marshal(nBlocks)
	if err != nil {
		return nil, fmt.Errorf("could not JSON marshal nBlocks (%d): %s", nBlocks, err)
	}

	jsonAddress, err := json.Marshal(address.String())
	if err != nil {
		return nil, fmt.Errorf("could not JSON marshal address (%s): %s", address.String(), err)
	}

	res, err := e.rpc.RawRequest("generatetoaddress", []json.RawMessage{jsonNBlocks, jsonAddress})
	if err != nil {
		return nil, fmt.Errorf("the rawRequest '%s' failed: %s", "generatetoaddress", err)
	}

	var result []string
	err = json.Unmarshal(res, &result)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal the response as JSON: %s", err)
	}

	// Convert each block hash to a chainhash.Hash and store a pointer to each.
	chainhashes := make([]*chainhash.Hash, len(result))
	for i, hashString := range result {
		chainhashes[i], err = chainhash.NewHashFromStr(hashString)
		if err != nil {
			return nil, fmt.Errorf("could not create a new chainhash from '%s': %s", hashString, err)
		}
	}

	return chainhashes, nil
}

// SendSimpleTransaction sends 0.1 BTC to the passed address via the
// `sendtoaddress` RPC. The caller must make sure enough spendable funds are
// avaliable in the wallet.
func (e *TestEnv) SendSimpleTransaction(address btcutil.Address) (*chainhash.Hash, error) {
	amount, err := btcutil.NewAmount(0.1)
	if err != nil {
		return nil, fmt.Errorf("could not create a new amount from %f: %s", 0.1, err)
	}

	txid, err := e.rpc.SendToAddress(address, amount)
	if err != nil {
		return nil, err
	}
	return txid, nil
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

	exitErr, ok := err.(*exec.ExitError)
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

func NewTestTxId(seed *[]byte) types.Hash32 {
	if seed != nil {
		return sha256.Sum256(*seed)
	}
	var res types.Hash32
	if _, err := rand.Read(res[:]); err != nil {
		panic(err)
	}
	return res
}

/* This is a function draft to send custom transactions.

func (e *TestEnv) SendCustomTransaction(address btcutil.Address) (*chainhash.Hash, error) {

	// list unspend with at least 100 confirmations
	unspend, err := e.rpc.ListUnspentMin(100)

	if len(unspend) == 0 {
		return nil, fmt.Errorf("no spendable inputs avaliable: %s", err)
	}

	first := unspend[1]
	input := btcjson.TransactionInput{
		Txid: first.TxID,
		Vout: first.Vout,
	}

	amount, err := btcutil.NewAmount(first.Amount)
	if err != nil {
		return nil, fmt.Errorf("could not create a new amount from %f: %s", first.Amount, err)
	}
	recipients := make(map[btcutil.Address]btcutil.Amount)

	recipients[address] = amount
	var locktime int64 = 0

	unsignedTx, err := e.rpc.CreateRawTransaction([]btcjson.TransactionInput{input}, recipients, &locktime)
	if err != nil {
		return nil, fmt.Errorf("could not create a raw transaction: %s", err)
	}

	signedTx, _, err := e.rpc.SignRawTransaction(unsignedTx)
	if err != nil {
		return nil, fmt.Errorf("could not sign the raw transaction: %s", err)
	}

	txid, err := e.rpc.SendRawTransaction(signedTx, true)
	if err != nil {
		return nil, fmt.Errorf("could not send the raw transaction: %s", err)
	}
	return txid, nil
}

*/
