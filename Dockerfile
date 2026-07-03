# syntax=docker/dockerfile:1.6
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/autoscaler ./cmd/autoscaler

FROM alpine:3.20
RUN apk add --no-cache docker-cli ca-certificates tzdata
COPY --from=builder /out/autoscaler /usr/local/bin/autoscaler
# Runs as root to access /var/run/docker.sock; the container itself is the
# security boundary. Spawned runner containers do not inherit this scope.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -q -O- http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/autoscaler"]
CMD ["-config", "/etc/autoscaler/config.json"]
