PROTO_KV        = proto/kv/kv.proto
PROTO_REP       = proto/replication/replication.proto
PROTO_FWD       = proto/forward/forward.proto

.PHONY: proto build test docker-up docker-down

proto:
	protoc --go_out=gen --go-grpc_out=gen \
	       --go_opt=paths=source_relative \
	       --go-grpc_opt=paths=source_relative \
	       -I proto \
	       $(PROTO_KV) $(PROTO_REP) $(PROTO_FWD)


build:
	mkdir -p dist
	go build -o dist/ ./cmd/...

lint:
	go vet ./...

format:
	gofmt -s -w .

test:
	go test ./... -race -count=1

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v
