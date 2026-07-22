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

-- Index for time-range queries per node
CREATE INDEX IF NOT EXISTS idx_snapshots_node_time
    ON snapshots(node_id, timestamp DESC);

-- Index for cleanup of old snapshots
CREATE INDEX IF NOT EXISTS idx_snapshots_created
    ON snapshots(created_at);
