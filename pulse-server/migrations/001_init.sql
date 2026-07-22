-- 伯楽 (Hakuraku) server initial schema for PostgreSQL 18

CREATE TABLE IF NOT EXISTS nodes (
    id          VARCHAR PRIMARY KEY NOT NULL,
    hostname    VARCHAR NOT NULL DEFAULT '',
    last_seen   BIGINT NOT NULL DEFAULT 0,  -- Unix timestamp ms
    status      VARCHAR NOT NULL DEFAULT 'unknown'  -- 'online', 'offline', 'unknown'
);

CREATE TABLE IF NOT EXISTS snapshots (
    id          BIGSERIAL PRIMARY KEY,
    node_id     VARCHAR NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    timestamp   BIGINT NOT NULL,  -- Unix timestamp ms
    stats_json  JSONB NOT NULL,    -- Full NodeStats serialized as JSON
    created_at  BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000)::BIGINT
);

CREATE TABLE IF NOT EXISTS probe_results (
    id            BIGSERIAL PRIMARY KEY,
    node_id       VARCHAR NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    target        VARCHAR NOT NULL,
    success       BOOLEAN NOT NULL,
    latency_us    BIGINT NOT NULL,
    error_message VARCHAR NOT NULL DEFAULT '',
    timestamp     BIGINT NOT NULL
);

-- Index for time-range queries per node
CREATE INDEX IF NOT EXISTS idx_snapshots_node_time
    ON snapshots(node_id, timestamp DESC);

-- Index for snapshot retention cleanup by telemetry timestamp
CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp
    ON snapshots(timestamp);

-- Index for probe history queries per node
CREATE INDEX IF NOT EXISTS idx_probes_node_time
    ON probe_results(node_id, timestamp DESC);
