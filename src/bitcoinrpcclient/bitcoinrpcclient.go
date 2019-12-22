package bitcoinrpcclient

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcutil"
	"github.com/pkg/errors"
)

// BitcoinRPCClient represents a Bitcoin Core RPC Client.
type BitcoinRPCClient struct {
	*rpcclient.Client
	addressMineTo btcutil.Address
}

// NewBitcoinRPCClient returns a new Bitcoin Core RPC Client. This functions
// waits for a maximum of 10 seconds for the corresponding RPC server to be
// ready. Otherwise it returns a timeout.
func NewBitcoinRPCClient(rpcAddress string) (*BitcoinRPCClient, error) {
	if len(rpcAddress) == 0 {
		return nil, errors.Errorf("rpcAddress is empty")
	}

	u, err := url.Parse(rpcAddress)
	if err != nil {
		return nil, errors.Wrap(err, "invalid rpc address")
	}

	pass, ok := u.User.Password()
	if !ok {
		return nil, errors.Errorf("password not set")
	}

	cfg := &rpcclient.ConnConfig{
		Host:         u.Host,
		User:         u.User.Username(),
		Pass:         pass,
		HTTPPostMode: true,
		DisableTLS:   true,
	}

	rpc, err := rpcclient.New(cfg, nil)
	if err != nil {
		return nil, err
	}

	client := &BitcoinRPCClient{Client: rpc}

	err = client.waitTillRPCServerReady(10 * time.Second)
	if err != nil {
		return nil, err
	}

	addressMineTo, err := rpc.GetNewAddress("addressMineTo")
	if err != nil {
		return nil, errors.Wrapf(err, "error creating mining address")
	}

	client.addressMineTo = addressMineTo
	return client, nil
}

// NewBitcoinRPCClientForIntegrationTest creates a new BitcoinRPCClient based on envvar
func NewBitcoinRPCClientForIntegrationTest() (*BitcoinRPCClient, error) {
	return NewBitcoinRPCClient(os.Getenv("TEST_INTEGRATION_RPC_ADDRESS"))
}

// waitTillRPCServerReady tries every `checkInterval` if the RPC Server the
// client is connected to is ready. It returns nil if the server is ready, or
// otherwise times out after `timeout`.
func (rpcClient *BitcoinRPCClient) waitTillRPCServerReady(timeout time.Duration) error {
	const checkInterval = 100 * time.Millisecond
	currentWaitTime := 0 * time.Second

	for currentWaitTime < timeout {
		_, err := rpcClient.GetBlockCount()
		if err != nil {
			log.Printf("Waiting for RPC Server: %s", err)
			currentWaitTime += checkInterval
			time.Sleep(checkInterval)
		} else {
			log.Printf("Waited %s for the RPCServer.", currentWaitTime)
			return nil
		}
	}
	return fmt.Errorf("timed out after %s while waiting for the Bitcoin Core RPC Server to be ready", timeout)
}

// Generate is deprecated, see error message
func (rpcClient *BitcoinRPCClient) Generate(nBlocks int) ([]*chainhash.Hash, error) {
	return nil, fmt.Errorf("deprecated, use GenerateToAddress(int, Address) or GenerateToFixedAddress(int)")
}

// GenerateToAddress mines `nBlocks` to the passed address and returns the block
// hashes.
func (rpcClient *BitcoinRPCClient) GenerateToAddress(nBlocks int, address btcutil.Address) ([]*chainhash.Hash, error) {
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

	res, err := rpcClient.RawRequest("generatetoaddress", []json.RawMessage{jsonNBlocks, jsonAddress})
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

// GenerateToFixedAddress mines blocks to `addressMineTo`
func (rpcClient *BitcoinRPCClient) GenerateToFixedAddress(nBlocks int) ([]*chainhash.Hash, error) {
	return rpcClient.GenerateToAddress(nBlocks, rpcClient.addressMineTo)
}

// SendSimpleTransaction sends 0.1 BTC to the passed address via the
// `sendtoaddress` RPC. The caller must make sure enough spendable funds are
// avaliable in the wallet.
func (rpcClient *BitcoinRPCClient) SendSimpleTransaction(address btcutil.Address) (*chainhash.Hash, error) {
	amount, err := btcutil.NewAmount(0.1)
	if err != nil {
		return nil, fmt.Errorf("could not create a new amount from %f: %s", 0.1, err)
	}

	txid, err := rpcClient.SendToAddress(address, amount)
	if err != nil {
		return nil, err
	}
	return txid, nil
}

/* This is a function draft to send custom transactions.

func (c *BitcoinRPCClient) SendCustomTransaction(address btcutil.Address) (*chainhash.Hash, error) {

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
