# ── Build stage ────────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Cache dependencies first
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /convergekv ./cmd/server

# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /convergekv /convergekv
ENTRYPOINT ["/convergekv"]
