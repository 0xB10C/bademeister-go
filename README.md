# bademeister-go

Bademeister implementation in Golang.


## What is bademeister?

Bademeister watches and records the bitcoind mempool.

It stores the time when a transaction was first received by a bitcoin node, including
transactions that do not end up in the blockchain (double-spends).

### Motivation

While the bitcoin blockchain contains all transactions that get eventually confirmed,
the confirmation time (time between a transaction first appearing on the
network and being included into a block by a miner) is not recorded on-chain.

Having a record of confirmation time allows research and benchmark of fee estimation
algorithms and other applications.

### Design Goals

* Easy to run: requirement is a patched version of `bitcoind`
* Easy to interface with
  * REST API, Golang API
  * Support simple query types like
    * `getFirstSeen(txid, nodeId): Date`
    * `getMempool(Date, nodeId): Transaction[]`
* Easy to combine multiple sources of data to have a robust data collection network
