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
    /// Bounded ingestion queue sender for database operations.
    pub ingestion_tx: tokio::sync::mpsc::Sender<IngestionItem>,
}

impl AppState {
    pub fn new(db: PgPool, ingestion_tx: tokio::sync::mpsc::Sender<IngestionItem>) -> Self {
        // Buffer up to 256 updates in the broadcast channel
        let (broadcast_tx, _) = broadcast::channel(256);
        Self {
            db,
            broadcast_tx,
            nodes: Arc::new(DashMap::new()),
            ingestion_tx,
        }
    }
}
