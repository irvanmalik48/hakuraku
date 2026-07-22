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
   WebSockets) concurrently. Features a bounded queue database worker pool, automatic
   data retention cleanup, in-memory caching, rate limiting, and an optional VictoriaMetrics time-series storage path.

```mermaid
graph TD
    subgraph Client ["Client / Dashboard"]
        WC["Web Client"]
        PR["Prometheus Scraper"]
    end

    subgraph Server ["Hakuraku Central Server"]
        AX["Axum Router (REST & WS)"]
        GR["Tonic gRPC Receiver"]
        CH[("Bounded Ingestion Channel (MPSC)")]
        WP["Fixed Worker Pool (DB Writers)"]
        LC["Liveness Checker Loop"]
        MS["Prometheus Registry"]
    end

    subgraph Storage ["Storage Targets"]
        DB[("PostgreSQL 18 DB")]
        VM[("VictoriaMetrics (Optional)")]
    end

    GR -- "Enforce Auth Node ID" --> CH
    CH --> WP
    WP -- "Write Relational & Probes" --> DB
    WP -- "Write Numeric Telemetry" --> VM
    AX -- "Metrics Scrape" --> PR
    AX -- "Query Nodes / History" --> DB
    LC -- "Mark Offline" --> DB
```

---

## Key Features

- **Ingestion & Backpressure**: Bounded worker pool ingestion queue (`mpsc::channel`) on the server prevents connection pool starvation. Sub-millisecond overhead.
- **Authentication**: HMAC-SHA256 payload signing with timestamp drift checking on gRPC. Bearer token / apiKey header verification on all REST/WebSocket API routes.
- **Reliability**: A bounded circular holdback buffer in the agent that caches telemetry during network dropouts and flushes it upon reconnection.
- **Node Liveness & Lifecycle**: Background liveness thread checks node heartbeats and marks inactive agents `offline` in the database and broadcasts the event.
- **Rate Limiting**: IP-based rate limiting on the REST API and WebSocket routes.
- **Graceful Shutdown**: Servers, checkers, and database worker threads coordinate shutdown using cancellation tokens to cleanly drain the ingestion queues.
- **Observability**: Prometheus-compatible `/metrics` endpoint measuring HTTP requests, gRPC streams, DB queries, queue depths, and connection states.
- **Data Retention**: Background worker that purges PostgreSQL snapshots and probe results older than 7 days based on telemetry timestamps.

---

## Configuration

伯楽 (Hakuraku) services are configured via environment variables. Create a `.env` file
based on the provided `.env.example`:

```bash
# Shared secret for HMAC-SHA256 authentication (64-character hex string)
PULSE_AUTH_SECRET=change-me-to-a-random-64-char-hex-string

# Target domain for Caddy reverse proxy HTTPS configuration
PULSE_DOMAIN=localhost

# CORS configuration (comma-separated origins list, e.g. "http://localhost:3000")
PULSE_CORS_ALLOWED_ORIGINS=http://localhost:3000

# Optional VictoriaMetrics URL for numeric scalar telemetry (e.g. "http://localhost:8428")
# If omitted, server runs in PostgreSQL-only mode.
VICTORIAMETRICS_URL=http://localhost:8428

# Agent configuration
PULSE_NODE_ID=node-01
PULSE_SERVER_ADDR=http://pulse-server:50051
PULSE_INTERVAL_MS=1000

# Server configuration
DATABASE_URL=postgres://pulse:password@localhost:5432/pulse
PULSE_GRPC_PORT=50051
PULSE_HTTP_PORT=3000
```

---

## Deployment

### Docker Compose Quickstart

The Server and Agent can be built and run using Docker Compose:

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

Pushed by agents on port `50051`.

- **Authentication Headers**:
  - `x-pulse-node-id`: The identifier of the reporting agent. Must match the payload node_id.
  - `x-pulse-timestamp`: Current Unix timestamp in seconds.
  - `x-pulse-signature`: Hex-encoded HMAC-SHA256 signature of the node ID and timestamp.

### REST API (Port `3000`)

Requests are rate-limited and require Bearer Token authentication (`Authorization: Bearer <secret>` or header `x-pulse-auth-token`).

#### `GET /api/v1/nodes`
Returns a list of all monitored nodes with their latest telemetry snapshots (joins relational node tables with latest stats).

#### `GET /api/v1/nodes/{id}`
Returns state details for a single node.

#### `GET /api/v1/nodes/{id}/history?range={range}&limit={limit}`
Queries historical snapshots for a node from the database.
- `range`: Time range (e.g. `1h`, `6h`, `24h`, `7d`). Defaults to `1h`.
- `limit`: Maximum number of data points (capped at `1000`).

#### `GET /health` / `GET /healthz`
Liveness check endpoints. Returns `200 OK`.

#### `GET /readyz`
Readiness check endpoint. Verifies database connectivity. Returns `200 OK` or `503 Service Unavailable`.

#### `GET /metrics`
Prometheus metrics scrape endpoint. Requires Bearer Token authentication. Exposes server statistics.

---

### WebSocket API (Port `3000`)

Subscribes to real-time metric broadcasts. Requires authentication token via `token` query parameter (e.g. `/ws?token=<secret>`).

- **Endpoint**: `/ws` (supports optional filtering: `/ws?token=<secret>&node_id=node-01`).
- **Initial Payload**: On connection, the server sends an `init` event containing snapshots of all nodes.
- **Update Payload**: Subsequent real-time metrics are broadcast as `update` events.

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
