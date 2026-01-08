.PHONY: build build-linux-amd64 test test-e2e test-all lint clean

# Use Docker host from current context for e2e tests
E2E_ENV = DOCKER_HOST=$(shell docker context inspect --format '{{.Endpoints.docker.Host}}')

build:
	go build -o dupedog ./cmd/dupedog

build-linux-amd64:
	GOOS=linux GOARCH=amd64 go build -o dupedog-linux-amd64 ./cmd/dupedog

# Build binaries for E2E tests (cross-compiled for Linux containers)
build-e2e:
	GOOS=linux GOARCH=$(shell go env GOARCH) CGO_ENABLED=0 go build -o .build/e2e/dupedog ./cmd/dupedog
	GOOS=linux GOARCH=$(shell go env GOARCH) CGO_ENABLED=0 go build -tags=linux -o .build/e2e/testfs-helper ./internal/testfs/cmd/testfs-helper

test:
	go test ./...

test-e2e: build-e2e
	DUPEDOG_E2E_BINDIR=$(CURDIR)/.build/e2e $(E2E_ENV) go test -tags=e2e -v ./internal/...

test-all: build-e2e
	DUPEDOG_E2E_BINDIR=$(CURDIR)/.build/e2e $(E2E_ENV) go test -tags=e2e ./...

test-race:
	go test -race ./...

test-cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run

fmt:
	gofmt -w .

vet:
	go vet ./...

clean:
	rm -f dupedog dupedog-linux-amd64 coverage.out coverage.html
	rm -rf dist/ .build/

snapshot:
	goreleaser build --snapshot --clean

all: fmt lint test build
