-- 伯楽 (Hakuraku) server initial schema
-- SQLite with WAL mode (configured at connection level, not here)

CREATE TABLE IF NOT EXISTS nodes (
    id          TEXT PRIMARY KEY NOT NULL,
    hostname    TEXT NOT NULL DEFAULT '',
    last_seen   INTEGER NOT NULL DEFAULT 0,  -- Unix timestamp ms
    status      TEXT NOT NULL DEFAULT 'unknown'  -- 'online', 'offline', 'unknown'
);

CREATE TABLE IF NOT EXISTS snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id     TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    timestamp   INTEGER NOT NULL,  -- Unix timestamp ms
    stats_json  TEXT NOT NULL,     -- Full NodeStats serialized as JSON
    created_at  INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000)
);

-- Index for time-range queries per node
CREATE INDEX IF NOT EXISTS idx_snapshots_node_time
    ON snapshots(node_id, timestamp DESC);

-- Index for cleanup of old snapshots
CREATE INDEX IF NOT EXISTS idx_snapshots_created
    ON snapshots(created_at);
