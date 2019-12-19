export SHELL:=/bin/bash
export SHELLOPTS:=$(if $(SHELLOPTS),$(SHELLOPTS):)pipefail:errexit
.ONESHELL:

GOFILES=$(shell git ls-files | grep -E 'go$$')

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOIMPORTS=goimports
GOVET=$(GOCMD) vet

# binary names
BINARY_NAME_DAEMON=bademeisterd
BINARY_NAME_API=bademeister-api

# integration test constants
TEST_INTEGRATION_DOCKER_IMAGE_TAG="v0.19.99.0-gf03785b4"
TEST_INTEGRATION_DOCKER_CONTAINER_NAME="bitcoind-bademeister-ci"

TEST_INTEGRATION_RPC_HOST="127.0.0.1"
TEST_INTEGRATION_RPC_PORT="18443"
TEST_INTEGRATION_RPC_USER="use-for-regtest-and-ci-only"
TEST_INTEGRATION_RPC_PASS="0rU3Doo7M6FCPc8SmBB2aLqUEd6b1Jgcl-o-c86JJ9k="
TEST_INTEGRATION_ZMQ_HOST="0.0.0.0"
TEST_INTEGRATION_ZMQ_PORT="28334"


all: go-fmt go-vet test-unit build
ci: go-fmt-check go-vet test build
build: build-daemon build-api
build-daemon:
	$(GOBUILD) -o $(BINARY_NAME_DAEMON) -v cmd/daemon/main.go
build-api:
	$(GOBUILD) -o $(BINARY_NAME_API) -v cmd/api/main.go
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME_DAEMON)
	rm -f $(BINARY_NAME_API)
run-daemon: build-daemon
	./$(BINARY_NAME_DAEMON)
run-api: build-api
	./$(BINARY_NAME_API)
go-fmt:
	@$(GOIMPORTS) -w $(GOFILES)
go-fmt-check:
	@unformatted=$(shell @$(GOIMPORTS) -l $(GOFILES))
	if [ ! -z "$$unformatted" ]; then \
		echo "Unformatted files: $$unformatted"; \
		exit 1
	fi
go-vet:
	@$(GOVET) ./...
test: test-unit test-integration
test-unit:
	$(GOTEST) -v -short ./...
test-bitcoind-start:
	@echo "starting bitcoind docker"
	docker run --rm -it \
		--name ${TEST_INTEGRATION_DOCKER_CONTAINER_NAME} \
		--publish ${TEST_INTEGRATION_RPC_PORT}:${TEST_INTEGRATION_RPC_PORT} \
		--publish ${TEST_INTEGRATION_ZMQ_PORT}:${TEST_INTEGRATION_ZMQ_PORT} \
		--detach \
		b10c/bitcoind-patched-zmq:${TEST_INTEGRATION_DOCKER_IMAGE_TAG} \
			-server=1 \
			-regtest=1 \
			-printtoconsole \
			-rpcbind=0.0.0.0 \
			-rpcallowip=172.17.0.0/16 \
			-rpcuser=${TEST_INTEGRATION_RPC_USER} \
			-rpcpassword=${TEST_INTEGRATION_RPC_PASS} \
			-zmqpubrawblock="tcp://${TEST_INTEGRATION_ZMQ_HOST}:${TEST_INTEGRATION_ZMQ_PORT}" \
			-zmqpubrawtxwithfee="tcp://${TEST_INTEGRATION_ZMQ_HOST}:${TEST_INTEGRATION_ZMQ_PORT}" \
			-fallbackfee=0.00001 \
			-debug=rpc
test-bitcoind-stop:
	@echo "bitcoind shutdown initiated..."
	@docker kill ${TEST_INTEGRATION_DOCKER_CONTAINER_NAME}
	@echo "bitcoind stopped"
test-integration:
	@echo ""
	$(eval TEST_INTEGRATION_DIR := $(shell mktemp -d -t bademeister-test-XXXXXX))
	@echo "Using TEST_INTEGRATION_DIR = $(TEST_INTEGRATION_DIR)"
	make test-bitcoind-start TEST_INTEGRATION_DIR=$(TEST_INTEGRATION_DIR)
	function cleanup {
		make test-bitcoind-stop TEST_INTEGRATION_DIR=$(TEST_INTEGRATION_DIR)
		echo "removing $(TEST_INTEGRATION_DIR)"
		rm -rf $(TEST_INTEGRATION_DIR)
	}
	trap cleanup EXIT

	@echo "running integration tests"
	TEST_INTEGRATION_DIR=${TEST_INTEGRATION_DIR} \
	TEST_INTEGRATION_ZMQ_HOST=${TEST_INTEGRATION_ZMQ_HOST} \
	TEST_INTEGRATION_ZMQ_PORT=${TEST_INTEGRATION_ZMQ_PORT} \
	TEST_INTEGRATION_RPC_HOST=${TEST_INTEGRATION_RPC_HOST} \
	TEST_INTEGRATION_RPC_PORT=${TEST_INTEGRATION_RPC_PORT} \
	TEST_INTEGRATION_RPC_USER=${TEST_INTEGRATION_RPC_USER} \
	TEST_INTEGRATION_RPC_PASS=${TEST_INTEGRATION_RPC_PASS} \
		$(GOTEST) -v ./...
