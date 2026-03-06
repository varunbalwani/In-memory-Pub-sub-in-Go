# pubsub-system

An in-memory publish/subscribe broker written in Go. Clients interact via WebSocket (`/ws`) using a JSON protocol. Topics and system health are managed via HTTP REST endpoints. All state is in-process — no external dependencies.

## Build

```bash
go build ./cmd/server
```

## Test

```bash
# All unit and property-based tests
go test ./...

# With race detector (recommended)
go test -race ./...

# With more property-based test iterations
go test -rapid.checks=500 ./...

# Integration tests
go test -race -tags integration ./...
```

## Docker

```bash
# Build image
docker build -t pubsub-system .

# Run with defaults
docker run -p 8080:8080 pubsub-system

# Run with custom config
docker run -p 9090:9090 \
  -e ADDR=:9090 \
  -e QUEUE_SIZE=512 \
  -e BACKPRESSURE_POLICY=disconnect \
  pubsub-system
```

## Configuration

All options can be set via CLI flags or environment variables. Flags take precedence over env vars.

| Flag                  | Env var               | Default | Description                              |
|-----------------------|-----------------------|---------|------------------------------------------|
| `-addr`               | `ADDR`                | `:8080` | HTTP/WebSocket listen address            |
| `-queue-size`         | `QUEUE_SIZE`          | `256`   | Per-subscriber outbound queue depth      |
| `-heartbeat`          | `HEARTBEAT_INTERVAL`  | `30s`   | Server-initiated heartbeat interval      |
| `-backpressure`       | `BACKPRESSURE_POLICY` | `drop`  | Slow consumer policy: `drop` or `disconnect` |
| `-history-size`       | `HISTORY_SIZE`        | `100`   | Per-topic message history buffer size    |
| `-shutdown-timeout`   | `SHUTDOWN_TIMEOUT`    | `10s`   | Graceful shutdown deadline               |

## Backpressure Policies

When a subscriber's outbound queue is full and a new message arrives, the broker applies the configured backpressure policy.

| Policy | Behaviour |
|--------|-----------|
| `drop` (default) | Drops the **oldest** message in the queue and enqueues the new one. The subscriber stays connected but loses the oldest message. Best for use cases where latest data matters more (e.g. sensor readings, order updates). |
| `disconnect` | Sends a `SLOW_CONSUMER` error to the subscriber and closes the connection. Use this when message loss is unacceptable and you prefer the client to reconnect and replay via `last_n`. |

Run with `disconnect` policy:
```bash
./server -backpressure disconnect
# or
BACKPRESSURE_POLICY=disconnect ./server
```

## HTTP API

| Method | Path              | Description                        |
|--------|-------------------|------------------------------------|
| POST   | /topics           | Create a topic                     |
| DELETE | /topics/{name}    | Delete a topic                     |
| GET    | /topics           | List all topics with subscriber counts |
| GET    | /health           | Health check (uptime, counts)      |
| GET    | /stats            | Per-topic message and subscriber counts |
| GET    | /ws               | WebSocket endpoint                 |

## WebSocket Testing

Install [websocat](https://github.com/vi/websocat):
```bash
brew install websocat
```

> Always use `--no-close` with piped input so the connection stays open long enough to receive the response. Add `-1` when you only expect one response and want to exit automatically.

### Ping

```bash
echo '{"type":"ping","request_id":"r1"}' | websocat --no-close -1 ws://localhost:8080/ws
```

### Subscribe (stays open to receive deliveries)

```bash
echo '{"type":"subscribe","topic":"orders","client_id":"sub-1","request_id":"r2"}' \
  | websocat --no-close ws://localhost:8080/ws
```

### Subscribe with last_n replay

```bash
echo '{"type":"subscribe","topic":"orders","client_id":"sub-1","last_n":5,"request_id":"r3"}' \
  | websocat --no-close ws://localhost:8080/ws
```

### Publish

```bash
printf '{"type":"publish","topic":"orders","message":{"id":"550e8400-e29b-41d4-a716-446655440000","payload":{"item":"widget"}},"request_id":"r4"}\n' \
  | websocat --no-close -1 ws://localhost:8080/ws
```

### Unsubscribe

```bash
echo '{"type":"unsubscribe","topic":"orders","request_id":"r5"}' \
  | websocat --no-close -1 ws://localhost:8080/ws
```

### Error cases

Malformed JSON:
```bash
echo 'not json' | websocat --no-close -1 ws://localhost:8080/ws
```

Unknown message type:
```bash
echo '{"type":"unknown"}' | websocat --no-close -1 ws://localhost:8080/ws
```

Subscribe without client_id:
```bash
echo '{"type":"subscribe","topic":"orders"}' | websocat --no-close -1 ws://localhost:8080/ws
```

Publish to non-existent topic:
```bash
printf '{"type":"publish","topic":"ghost","message":{"id":"550e8400-e29b-41d4-a716-446655440000","payload":{}}}\n' \
  | websocat --no-close -1 ws://localhost:8080/ws
```

Publish with invalid UUID:
```bash
printf '{"type":"publish","topic":"orders","message":{"id":"not-a-uuid","payload":{}}}\n' \
  | websocat --no-close -1 ws://localhost:8080/ws
```

### Fan-out test (two terminals)

First make sure the topic exists:
```bash
curl -X POST http://localhost:8080/topics -H "Content-Type: application/json" -d '{"name":"orders"}'
```

Terminal 1 — subscriber (keep open, will receive deliveries):
```bash
echo '{"type":"subscribe","topic":"orders","client_id":"sub-1"}' \
  | websocat --no-close ws://localhost:8080/ws
```

Terminal 2 — publisher:
```bash
printf '{"type":"publish","topic":"orders","message":{"id":"550e8400-e29b-41d4-a716-446655440000","payload":{"item":"widget"}},"request_id":"r4"}\n' \
  | websocat --no-close -1 ws://localhost:8080/ws
```

Terminal 2 gets the ack, Terminal 1 receives the delivered message.

## WebSocket Protocol Reference

### Message types (client → server)

| type | Required fields | Description |
|------|----------------|-------------|
| `subscribe` | `topic`, `client_id` | Subscribe to a topic |
| `unsubscribe` | `topic` | Unsubscribe from a topic |
| `publish` | `topic`, `message.id` (UUID), `message.payload` | Publish a message |
| `ping` | — | Liveness check |

### Response types (server → client)

| type | When |
|------|------|
| `ack` | Successful subscribe / unsubscribe / publish |
| `pong` | Response to ping |
| `message` | Delivered message to a subscriber |
| `info` | Server-initiated (heartbeat, topic_deleted) |
| `error` | Any failure |

Error codes: `BAD_REQUEST`, `TOPIC_NOT_FOUND`, `SLOW_CONSUMER`, `INTERNAL`
