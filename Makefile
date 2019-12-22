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
GOLINT=golint

# binary names
BINARY_NAME_DAEMON=bademeisterd
BINARY_NAME_API=bademeister-api

# integration test constants
TEST_INTEGRATION_DOCKER_IMAGE_TAG="v0.19.99.0-gf03785b4"
TEST_INTEGRATION_DOCKER_CONTAINER_NAME="bitcoind-bademeister-ci"
TEST_INTEGRATION_DOCKER_DETACH?="false"

TEST_INTEGRATION_RPC_USER=use-for-regtest-and-ci-only
TEST_INTEGRATION_RPC_PASS=0ru3doo7m6fcpc8smbb2alqued6b1jgcl-o-c86jj9k=
TEST_INTEGRATION_RPC_PORT=18443
TEST_INTEGRATION_RPC_CREDS=$(TEST_INTEGRATION_RPC_USER):$(TEST_INTEGRATION_RPC_PASS)
TEST_INTEGRATION_RPC_ADDRESS="http://$(TEST_INTEGRATION_RPC_CREDS)@127.0.0.1:$(TEST_INTEGRATION_RPC_PORT)"

TEST_INTEGRATION_ZMQ_PORT=28334
TEST_INTEGRATION_ZMQ_ADDRESS=tcp://0.0.0.0:$(TEST_INTEGRATION_ZMQ_PORT)

all: go-fmt go-vet go-lint test-unit build
ci: go-fmt-check go-vet go-lint test build
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
go-lint:
	@$(GOLINT) -set_exit_status ./...
test: test-unit test-integration
test-unit:
	$(GOTEST) -v -short ./...
test-bitcoind-start:
	@echo "starting bitcoind docker"
	docker run --rm -it \
		--name ${TEST_INTEGRATION_DOCKER_CONTAINER_NAME} \
		--publish ${TEST_INTEGRATION_RPC_PORT}:${TEST_INTEGRATION_RPC_PORT} \
		--publish ${TEST_INTEGRATION_ZMQ_PORT}:${TEST_INTEGRATION_ZMQ_PORT} \
		--detach=$(TEST_INTEGRATION_DOCKER_DETACH) \
		b10c/bitcoind-patched-zmq:${TEST_INTEGRATION_DOCKER_IMAGE_TAG} \
			-server=1 \
			-regtest=1 \
			-printtoconsole \
			-rpcbind=0.0.0.0 \
			-rpcallowip=172.17.0.0/16 \
			-rpcuser=${TEST_INTEGRATION_RPC_USER} \
			-rpcpassword=${TEST_INTEGRATION_RPC_PASS} \
			-zmqpubrawblock="$(TEST_INTEGRATION_ZMQ_ADDRESS)" \
			-zmqpubrawtxwithfee="$(TEST_INTEGRATION_ZMQ_ADDRESS)" \
			-fallbackfee=0.00001 \
			-debug=rpc
test-bitcoind-stop:
	@echo "bitcoind shutdown initiated..."
	@docker kill ${TEST_INTEGRATION_DOCKER_CONTAINER_NAME}
	@echo "bitcoind stopped"
test-integration:
	@echo ""
	$(eval TEST_INTEGRATION_DIR := $(shell mktemp -d -t bademeister-test-XXXXXX))
	@echo "TEST_INTEGRATION_DIR=$(TEST_INTEGRATION_DIR)"
	@echo "TEST_INTEGRATION_RPC_ADDRESS=${TEST_INTEGRATION_RPC_ADDRESS}"
	@echo "TEST_INTEGRATION_ZMQ_ADDRESS=${TEST_INTEGRATION_ZMQ_ADDRESS}"

	make test-bitcoind-start \
	    TEST_INTEGRATION_DIR=$(TEST_INTEGRATION_DIR) \
	    TEST_INTEGRATION_DOCKER_DETACH=true

	function cleanup {
		make test-bitcoind-stop TEST_INTEGRATION_DIR=$(TEST_INTEGRATION_DIR)
		echo "removing $(TEST_INTEGRATION_DIR)"
		rm -rf $(TEST_INTEGRATION_DIR)
	}
	trap cleanup EXIT

	@echo "running integration tests"
	TEST_INTEGRATION_DIR=${TEST_INTEGRATION_DIR} \
	TEST_INTEGRATION_ZMQ_ADDRESS=${TEST_INTEGRATION_ZMQ_ADDRESS} \
	TEST_INTEGRATION_RPC_ADDRESS=${TEST_INTEGRATION_RPC_ADDRESS} \
		$(GOTEST) -v ./...
