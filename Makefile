PROTO_KV        = 

.PHONY: proto build test docker-up docker-down

proto:
	protoc --go_out=gen --go-grpc_out=gen \
	       --go_opt=paths=source_relative \
	       --go-grpc_opt=paths=source_relative \
	       -I proto \
	       $(PROTO_KV)


build:
	mkdir -p dist
	go build -o dist/ ./cmd/...

lint:
	go vet ./...

format:
	gofmt -s -w .

test:
	go test ./... -race -count=1

N ?= 2

docker-up:
	docker compose up --build -d --scale worker=$(N)

docker-down:
	docker compose down -v
