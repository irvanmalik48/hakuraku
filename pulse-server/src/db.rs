//! Database access layer with repository pattern.
//!
//! Implements PostgreSQL 18 via `sqlx`. The `NodeRepository` trait
//! provides an abstraction point for future database backends.

use anyhow::Result;
use sqlx::PgPool;

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

/// PostgreSQL implementation of `NodeRepository`.
#[derive(Clone)]
pub struct PostgresNodeRepository {
    pool: PgPool,
}

impl PostgresNodeRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Run embedded migrations.
    pub async fn migrate(&self) -> Result<()> {
        let migration_sql = include_str!("../migrations/001_init.sql");
        sqlx::raw_sql(migration_sql)
            .execute(&self.pool)
            .await?;
        tracing::info!("database migrations applied");
        Ok(())
    }
}

impl NodeRepository for PostgresNodeRepository {
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
            VALUES ($1, $2, $3, $4)
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
        let stats_val: serde_json::Value = serde_json::from_str(stats_json).unwrap_or_default();

        sqlx::query(
            r#"
            INSERT INTO snapshots (node_id, timestamp, stats_json)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(node_id)
        .bind(timestamp_ms)
        .bind(stats_val)
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
            "SELECT id, hostname, last_seen, status FROM nodes WHERE id = $1",
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
            WHERE node_id = $1 AND timestamp BETWEEN $2 AND $3
            ORDER BY timestamp DESC
            LIMIT $4
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
                stats_json: r.stats_json,
            })
            .collect();

        Ok(records)
    }

    async fn cleanup_old_snapshots(&self, before_ms: i64) -> Result<u64> {
        let result =
            sqlx::query("DELETE FROM snapshots WHERE created_at < $1")
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
    stats_json: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    async fn get_test_pool() -> Option<PgPool> {
        let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://pulse:password@localhost:54321/pulse".to_string()
        });
        PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .ok()
    }

    #[tokio::test]
    async fn test_node_upsert_and_retrieve() {
        let pool = match get_test_pool().await {
            Some(p) => p,
            None => {
                eprintln!("skipping database test: postgres not running or DATABASE_URL not set");
                return;
            }
        };

        let repo = PostgresNodeRepository::new(pool);
        repo.migrate().await.unwrap();

        sqlx::query("DELETE FROM snapshots WHERE node_id = $1").bind("test-node-1").execute(&repo.pool).await.unwrap();
        sqlx::query("DELETE FROM nodes WHERE id = $1").bind("test-node-1").execute(&repo.pool).await.unwrap();

        repo.upsert_node("test-node-1", "test-host", "online").await.unwrap();

        let node = repo.get_node("test-node-1").await.unwrap().unwrap();
        assert_eq!(node.node_id, "test-node-1");
        assert_eq!(node.hostname, "test-host");
        assert_eq!(node.status, NodeStatus::Online);

        let nodes = repo.get_all_nodes().await.unwrap();
        assert!(nodes.iter().any(|n| n.node_id == "test-node-1"));
    }

    #[tokio::test]
    async fn test_snapshot_insert_and_cleanup() {
        let pool = match get_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let repo = PostgresNodeRepository::new(pool);
        repo.migrate().await.unwrap();

        sqlx::query("DELETE FROM snapshots WHERE node_id = $1").bind("test-node-2").execute(&repo.pool).await.unwrap();
        sqlx::query("DELETE FROM nodes WHERE id = $1").bind("test-node-2").execute(&repo.pool).await.unwrap();

        repo.upsert_node("test-node-2", "test-host", "online").await.unwrap();

        let stats_json = r#"{"cpu_percent": 12.5}"#;
        repo.insert_snapshot("test-node-2", 1721634839000, stats_json).await.unwrap();

        let snapshots = repo.get_snapshots("test-node-2", 1721634830000, 1721634845000, 10).await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].node_id, "test-node-2");
        assert_eq!(snapshots[0].timestamp, 1721634839000);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let deleted = repo.cleanup_old_snapshots(now_ms + 10_000).await.unwrap();
        assert!(deleted >= 1);
    }
}
