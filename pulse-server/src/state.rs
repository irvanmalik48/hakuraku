//! Shared application state for the 伯楽 (Hakuraku) server.

use std::sync::Arc;

use dashmap::DashMap;
use sqlx::PgPool;
use tokio::sync::broadcast;

/// Unique node identifier.
pub type NodeId = String;

/// Real-time node update broadcast to WebSocket subscribers.
#[derive(Clone, Debug, serde::Serialize)]
pub struct NodeUpdate {
    pub node_id: String,
    pub timestamp_ms: i64,
    pub stats: serde_json::Value,
}

/// Per-node in-memory state.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub node_id: String,
    pub hostname: String,
    pub last_seen_ms: i64,
    pub status: NodeStatus,
    pub latest_stats: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    Online,
    Offline,
    Unknown,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Online => write!(f, "online"),
            Self::Offline => write!(f, "offline"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum IngestionItem {
    Stats {
        node_id: String,
        timestamp_ms: i64,
        stats_str: String,
        stats_json: serde_json::Value,
    },
    ProbeResult {
        node_id: String,
        target: String,
        success: bool,
        latency_us: i64,
        error_message: String,
        timestamp: i64,
    },
}

/// Shared application state accessible from both gRPC and Axum handlers.
#[derive(Clone)]
pub struct AppState {
    /// PostgreSQL connection pool.
    pub db: PgPool,
    /// Broadcast channel for real-time WebSocket fan-out.
    pub broadcast_tx: broadcast::Sender<NodeUpdate>,
    /// In-memory node registry for fast lookups.
    pub nodes: Arc<DashMap<NodeId, NodeInfo>>,
    /// Sharded ingestion worker senders (partitioned by node_id hash).
    pub worker_txs: Arc<Vec<tokio::sync::mpsc::Sender<IngestionItem>>>,
}

impl AppState {
    pub fn new(
        db: PgPool,
        worker_txs: Arc<Vec<tokio::sync::mpsc::Sender<IngestionItem>>>,
    ) -> Self {
        // Buffer up to 256 updates in the broadcast channel
        let (broadcast_tx, _) = broadcast::channel(256);
        Self {
            db,
            broadcast_tx,
            nodes: Arc::new(DashMap::new()),
            worker_txs,
        }
    }

    /// Route an ingestion item to the appropriate sharded worker by node_id hash.
    pub fn send_to_worker(&self, node_id: &str, item: IngestionItem) {
        let shard = self.shard(node_id);
        if self.worker_txs[shard].try_send(item).is_err() {
            tracing::error!(node_id = %node_id, shard = shard, "ingestion queue full, dropping item");
            crate::metrics::INGESTION_DROPS.inc();
        }
    }

    /// Compute the worker shard index for a given node_id.
    fn shard(&self, node_id: &str) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        node_id.hash(&mut hasher);
        (hasher.finish() as usize) % self.worker_txs.len()
    }
}
