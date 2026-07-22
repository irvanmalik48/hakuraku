-- 伯楽 (Hakuraku) server initial schema for PostgreSQL 18
-- Idempotent: safe to run multiple times.

DO $$
BEGIN
    -- Nodes table
    CREATE TABLE IF NOT EXISTS nodes (
        id          VARCHAR PRIMARY KEY NOT NULL,
        hostname    VARCHAR NOT NULL DEFAULT '',
        last_seen   BIGINT NOT NULL DEFAULT 0,  -- Unix timestamp ms
        status      VARCHAR NOT NULL DEFAULT 'unknown'  -- 'online', 'offline', 'unknown'
    );

    -- Snapshots table (using IDENTITY instead of SERIAL to avoid type conflicts)
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'snapshots') THEN
        CREATE TABLE snapshots (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            node_id     VARCHAR NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            timestamp   BIGINT NOT NULL,  -- Unix timestamp ms
            stats_json  JSONB NOT NULL,    -- Full NodeStats serialized as JSON
            created_at  BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000)::BIGINT
        );
    END IF;

    -- Probe results table
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'probe_results') THEN
        CREATE TABLE probe_results (
            id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            node_id       VARCHAR NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            target        VARCHAR NOT NULL,
            success       BOOLEAN NOT NULL,
            latency_us    BIGINT NOT NULL,
            error_message VARCHAR NOT NULL DEFAULT '',
            timestamp     BIGINT NOT NULL
        );
    END IF;
END $$;

-- Indexes (IF NOT EXISTS is natively safe)
CREATE INDEX IF NOT EXISTS idx_snapshots_node_time
    ON snapshots(node_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp
    ON snapshots(timestamp);

CREATE INDEX IF NOT EXISTS idx_probes_node_time
    ON probe_results(node_id, timestamp DESC);
