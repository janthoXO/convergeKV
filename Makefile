PROTO_KV        = proto/kv/kv.proto
PROTO_REP       = proto/replication/replication.proto

.PHONY: proto build test docker-up docker-down

proto:
	mkdir -p gen
	protoc -I=proto --go_out=gen --go-grpc_out=gen \
	       --go_opt=paths=source_relative \
	       --go-grpc_opt=paths=source_relative \
	       kv/kv.proto replication/replication.proto

build:
	go build ./...

test:
	go test ./... -race -count=1

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v
