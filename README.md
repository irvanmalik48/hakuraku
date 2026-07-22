# 伯楽 (Hakuraku) Monitoring System

伯楽 (Hakuraku) is a high-throughput system monitoring and telemetry collection suite
written in Rust. It consists of a lightweight daemon (agent) that collects host
metrics and a central server that ingests telemetry via gRPC, stores it in an
optimized PostgreSQL 18 database, and exposes data through a REST API and a real-time
WebSocket broadcast channel.

The system uses a size-optimized workspace design, compiles static binaries with
Full Link-Time Optimization (LTO), and runs inside minimal container images.

---

## Workspace Architecture

The workspace is divided into three crates:

1. **pulse-core**: Shared protobuf definitions (`telemetry.proto`), auto-generated
   gRPC service interfaces, and the HMAC-SHA256 authentication interceptors.
2. **pulse-agent**: Lightweight monitoring daemon that reads metrics from host
   mounts of `/proc` and `/sys` filesystems, handles offline telemetry holdback,
   and executes network latency probes.
3. **pulse-server**: Ingestion and API server running Tonic (gRPC) and Axum (HTTP/
   WebSockets) concurrently. Features automatic data retention cleanup,
   in-memory caching, and request rate limiting.

```mermaid
graph LR
    subgraph Client ["Client Layer"]
        WC["Web Client"]
    end

    subgraph Core ["Hakuraku Central Server"]
        SV["Hakuraku Server"]
        DB[("PostgreSQL 18 DB")]
    end

    subgraph Daemon ["Hakuraku Daemon"]
        AG["Hakuraku Agent"]
    end

    AG -- "gRPC Telemetry" --> SV
    SV -- "Server Commands" --> AG
    SV -- "WebSockets / REST" --> WC
    SV -- "SQL Write/Read" --> DB
```

---

## Key Features

- **Ingestion**: Bidirectional streaming gRPC with sub-millisecond overhead.
- **Authentication**: HMAC-SHA256 payload signing with timestamp drift checking.
- **Reliability**: A bounded circular holdback buffer in the agent that caches
  telemetry during network dropouts and flushes it upon reconnection.
- **Rate Limiting**: IP-based rate limiting on the REST API and WebSocket routes.
- **Security**:
  - Request body size limits (restricted to a maximum of 64 KB).
  - Global request timeout handling (10-second timeout returning 408).
  - Concurrency limits on gRPC connections (maximum 32 concurrent requests per
    connection).
  - Configured keepalives and keepalive timeouts for HTTP/2 & gRPC.
  - Scratch-based agent container and non-root Alpine server container.
- **Data Retention**: Background worker that purges PostgreSQL snapshots older than
  7 days.
- **Probing**: Concurrent TCP handshake latency measurements.

---

## Configuration

伯楽 (Hakuraku) services are configured via environment variables. Create a `.env` file
based on the provided `.env.example`:

```bash
# Shared secret for HMAC-SHA256 authentication (64-character hex string)
PULSE_AUTH_SECRET=change-me-to-a-random-64-char-hex-string

# Target domain for Caddy reverse proxy HTTPS configuration
PULSE_DOMAIN=localhost

# Agent configuration
PULSE_NODE_ID=node-01
PULSE_SERVER_ADDR=http://pulse-server:50051
PULSE_INTERVAL_MS=1000

# Server configuration
DATABASE_URL=postgres://pulse:password@localhost:54321/pulse
PULSE_GRPC_PORT=50051
PULSE_HTTP_PORT=3000
```

---

## Deployment

### Docker Compose Quickstart

The Server and Agent can be built and run using Docker Compose. The configuration
expects an external Caddy proxy running on a shared network named `caddy_mesh`
(you can edit this later if you want to):

```bash
# Clone the repository and configure environment variables
cp .env.example .env

# Start the stack in detached mode
docker compose up -d
```

### Manual Compilation

To compile the binaries directly on a host machine, execute:

```bash
# Build both binaries in release mode
cargo build --release

# The compiled binaries will be located at:
# target/release/pulse-server
# target/release/pulse-agent
```

---

## API Documentation

### gRPC Ingestion Service

Pushed by agents on port `50051` (proxied under path `/pulse.MonitoringService/`
via HTTP/2 cleartext `h2c`).

- **Authentication Headers**:
  - `x-pulse-node-id`: The identifier of the reporting agent.
  - `x-pulse-timestamp`: Current Unix timestamp in milliseconds.
  - `x-pulse-signature`: Hex-encoded HMAC-SHA256 signature of the node ID and
    timestamp.

### REST API (Port `3000`)

Requests are rate-limited to 2 requests per second with a burst capacity of 10.

#### `GET /api/v1/nodes`
Returns a list of all monitored nodes with their latest telemetry snapshots.

Response:
```json
{
  "nodes": [
    {
      "node_id": "node-01",
      "hostname": "node-01",
      "last_seen_ms": 1721634839000,
      "status": "online",
      "latest_stats": {
        "cpu_percent": 1.25,
        "mem_used": 2048000,
        "mem_total": 8192000
      }
    }
  ],
  "count": 1
}
```

#### `GET /api/v1/nodes/{id}`
Returns state details for a single node. If the node is not in the in-memory
cache, the database is queried as a fallback.

Response:
```json
{
  "node_id": "node-01",
  "hostname": "node-01",
  "last_seen_ms": 1721634839000,
  "status": "online",
  "latest_stats": {
    "cpu_percent": 1.25,
    "mem_used": 2048000,
    "mem_total": 8192000
  }
}
```

#### `GET /api/v1/nodes/{id}/history?range={range}&limit={limit}`
Queries historical snapshots for a node from the database.
- `range`: Time range as a string: `1h`, `6h`, `24h`, `7d`. Defaults to `1h`.
- `limit`: Maximum number of data points to return. Defaults to `360`.

Response:
```json
{
  "node_id": "node-01",
  "range": "1h",
  "from_ms": 1721631239000,
  "to_ms": 1721634839000,
  "count": 1,
  "snapshots": [
    {
      "node_id": "node-01",
      "timestamp": 1721634839000,
      "stats_json": {
        "cpu_percent": 1.25,
        "mem_used": 2048000,
        "mem_total": 8192000
      }
    }
  ]
}
```

#### `GET /health`
Returns service availability status. Bypasses standard logs.

Response:
```json
{
  "status": "ok",
  "service": "pulse-server",
  "version": "0.1.0"
}
```

---

### WebSocket API (Port `3000`)

Subscribes to real-time metric broadcasts.

- **Endpoint**: `/ws` (supports optional filtering: `/ws?node_id=node-01`).
- **Initial Payload**: On connection, the server sends an `init` event containing
  snapshots of all nodes.
- **Update Payload**: Subsequent real-time metrics are broadcast as `update`
  events.

#### Event Format

Initialization:
```json
{
  "type": "init",
  "nodes": [
    {
      "type": "snapshot",
      "node_id": "node-01",
      "hostname": "node-01",
      "last_seen_ms": 1721634839000,
      "status": "online",
      "stats": { ... }
    }
  ]
}
```

Real-time Update:
```json
{
  "type": "update",
  "node_id": "node-01",
  "timestamp_ms": 1721634840000,
  "stats": { ... }
}
```

---

## Development

### Running Tests

Execute the workspace-wide unit test suite:

```bash
cargo test --workspace
```

### Static Analysis

Ensure code conforms to formatting and safety rules:

```bash
# Format check
cargo fmt --all -- --check

# Linter analysis
cargo clippy --workspace --all-targets -- -D warnings
```
