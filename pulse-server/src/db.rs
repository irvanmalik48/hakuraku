//! Database access layer with repository pattern.
//!
//! Currently implements SQLite via `sqlx`. The `NodeRepository` trait
//! provides an abstraction point for future PostgreSQL support.

use anyhow::Result;
use sqlx::SqlitePool;

use crate::state::{NodeInfo, NodeStatus};

/// Abstract repository for node and snapshot persistence.
#[allow(async_fn_in_trait)]
pub trait NodeRepository {
    /// Upsert a node record (update last_seen and status).
    async fn upsert_node(&self, node_id: &str, hostname: &str, status: &str)
        -> Result<()>;

    /// Insert a telemetry snapshot.
    async fn insert_snapshot(
        &self,
        node_id: &str,
        timestamp_ms: i64,
        stats_json: &str,
    ) -> Result<()>;

    /// Retrieve all nodes.
    async fn get_all_nodes(&self) -> Result<Vec<NodeInfo>>;

    /// Retrieve a single node by ID.
    async fn get_node(&self, node_id: &str) -> Result<Option<NodeInfo>>;

    /// Retrieve snapshot history for a node within a time range.
    async fn get_snapshots(
        &self,
        node_id: &str,
        from_ms: i64,
        to_ms: i64,
        limit: i64,
    ) -> Result<Vec<SnapshotRecord>>;

    /// Delete snapshots older than the given timestamp.
    async fn cleanup_old_snapshots(&self, before_ms: i64) -> Result<u64>;
}

/// A raw snapshot record from the database.
#[derive(Debug, serde::Serialize)]
pub struct SnapshotRecord {
    pub node_id: String,
    pub timestamp: i64,
    pub stats_json: serde_json::Value,
}

/// SQLite implementation of `NodeRepository`.
#[derive(Clone)]
pub struct SqliteNodeRepository {
    pool: SqlitePool,
}

impl SqliteNodeRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Run embedded migrations.
    pub async fn migrate(&self) -> Result<()> {
        // Read and execute the migration SQL directly
        let migration_sql = include_str!("../migrations/001_init.sql");
        sqlx::raw_sql(migration_sql)
            .execute(&self.pool)
            .await?;
        tracing::info!("database migrations applied");
        Ok(())
    }
}

impl NodeRepository for SqliteNodeRepository {
    async fn upsert_node(
        &self,
        node_id: &str,
        hostname: &str,
        status: &str,
    ) -> Result<()> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        sqlx::query(
            r#"
            INSERT INTO nodes (id, hostname, last_seen, status)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                hostname = excluded.hostname,
                last_seen = excluded.last_seen,
                status = excluded.status
            "#,
        )
        .bind(node_id)
        .bind(hostname)
        .bind(now_ms)
        .bind(status)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn insert_snapshot(
        &self,
        node_id: &str,
        timestamp_ms: i64,
        stats_json: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO snapshots (node_id, timestamp, stats_json)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(node_id)
        .bind(timestamp_ms)
        .bind(stats_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_all_nodes(&self) -> Result<Vec<NodeInfo>> {
        let rows = sqlx::query_as::<_, NodeRow>("SELECT id, hostname, last_seen, status FROM nodes")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    async fn get_node(&self, node_id: &str) -> Result<Option<NodeInfo>> {
        let row = sqlx::query_as::<_, NodeRow>(
            "SELECT id, hostname, last_seen, status FROM nodes WHERE id = ?",
        )
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.into()))
    }

    async fn get_snapshots(
        &self,
        node_id: &str,
        from_ms: i64,
        to_ms: i64,
        limit: i64,
    ) -> Result<Vec<SnapshotRecord>> {
        let rows = sqlx::query_as::<_, SnapshotRow>(
            r#"
            SELECT node_id, timestamp, stats_json
            FROM snapshots
            WHERE node_id = ? AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp DESC
            LIMIT ?
            "#,
        )
        .bind(node_id)
        .bind(from_ms)
        .bind(to_ms)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let records = rows
            .into_iter()
            .map(|r| SnapshotRecord {
                node_id: r.node_id,
                timestamp: r.timestamp,
                stats_json: serde_json::from_str(&r.stats_json).unwrap_or_default(),
            })
            .collect();

        Ok(records)
    }

    async fn cleanup_old_snapshots(&self, before_ms: i64) -> Result<u64> {
        let result =
            sqlx::query("DELETE FROM snapshots WHERE created_at < ?")
                .bind(before_ms)
                .execute(&self.pool)
                .await?;

        Ok(result.rows_affected())
    }
}

// ── Internal row types for sqlx ────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct NodeRow {
    id: String,
    hostname: String,
    last_seen: i64,
    status: String,
}

impl From<NodeRow> for NodeInfo {
    fn from(row: NodeRow) -> Self {
        let status = match row.status.as_str() {
            "online" => NodeStatus::Online,
            "offline" => NodeStatus::Offline,
            _ => NodeStatus::Unknown,
        };
        NodeInfo {
            node_id: row.id,
            hostname: row.hostname,
            last_seen_ms: row.last_seen,
            status,
            latest_stats: None,
        }
    }
}

#[derive(sqlx::FromRow)]
struct SnapshotRow {
    node_id: String,
    timestamp: i64,
    stats_json: String,
}
