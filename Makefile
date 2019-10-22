# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test

BINARY_NAME_DAEMON=bademeisterd
BINARY_NAME_API=bademeister-api

all: test build
build: build-daemon build-api
build-daemon:
				$(GOBUILD) -o $(BINARY_NAME_DAEMON) -v cmd/daemon/main.go
build-api:
				$(GOBUILD) -o $(BINARY_NAME_API) -v cmd/api/main.go
test:
				$(GOTEST) -v ./...
clean:
				$(GOCLEAN)
				rm -f $(BINARY_NAME_DAEMON)
				rm -f $(BINARY_NAME_API)
run-daemon: build-daemon
				./$(BINARY_NAME_DAEMON)
run-api: build-api
				./$(BINARY_NAME_API)
