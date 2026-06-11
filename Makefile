PROTO_FILES = $(wildcard pkg/proto/*.proto)

.PHONY: proto build lint format test docker-up docker-down

proto:
	protoc --go_out=pkg/proto --go-grpc_out=pkg/proto \
	       --go_opt=paths=source_relative \
	       --go-grpc_opt=paths=source_relative \
	       -I pkg/proto \
	       $(PROTO_FILES)

build:
	mkdir -p dist
	go build -o dist/ ./cmd/...

lint:
	go vet ./...
	golangci-lint run

format:
	gofmt -s -w .

test:
	go test ./... -race -count=1

N ?= 3

docker-up:
	docker compose up --build -d --scale node=$(N)

docker-down:
	docker compose down -v
