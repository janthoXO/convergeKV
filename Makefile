PROTO_KV        = proto/kv/kv.proto
PROTO_REP       = proto/replication/replication.proto

.PHONY: proto build test docker-up docker-down

proto:
	protoc --go_out=gen --go-grpc_out=gen \
	       --go_opt=paths=source_relative \
	       --go-grpc_opt=paths=source_relative \
	       -I proto \
	       $(PROTO_KV) $(PROTO_REP)

build:
	go build ./...

test:
	go test ./... -race -count=1

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v
