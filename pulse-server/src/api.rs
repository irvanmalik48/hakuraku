//! REST API routes for the dashboard.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;

use crate::db::{NodeRepository, SqliteNodeRepository};
use crate::state::AppState;

/// Query parameters for the history endpoint.
#[derive(Deserialize)]
pub struct HistoryQuery {
    /// Time range as a human-readable string: "1h", "6h", "24h", "7d"
    #[serde(default = "default_range")]
    pub range: String,
    /// Maximum number of data points to return.
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_range() -> String {
    "1h".to_string()
}

fn default_limit() -> i64 {
    360
}

/// `GET /api/v1/nodes` — List all monitored nodes with latest stats.
pub async fn list_nodes(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let nodes: Vec<serde_json::Value> = state
        .nodes
        .iter()
        .map(|entry| {
            let node = entry.value();
            serde_json::json!({
                "node_id": node.node_id,
                "hostname": node.hostname,
                "last_seen_ms": node.last_seen_ms,
                "status": node.status,
                "latest_stats": node.latest_stats,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "nodes": nodes,
        "count": nodes.len(),
    })))
}

/// `GET /api/v1/nodes/:id` — Get detailed stats for a specific node.
pub async fn get_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(entry) = state.nodes.get(&node_id) {
        let n = entry.value();
        return Ok(Json(serde_json::json!({
            "node_id": n.node_id,
            "hostname": n.hostname,
            "last_seen_ms": n.last_seen_ms,
            "status": n.status,
            "latest_stats": n.latest_stats,
        })));
    }

    // Fallback: Check the database directly
    let repo = SqliteNodeRepository::new(state.db.clone());
    match repo.get_node(&node_id).await {
        Ok(Some(n)) => {
            // Populate the in-memory cache
            state.nodes.insert(n.node_id.clone(), n.clone());
            Ok(Json(serde_json::json!({
                "node_id": n.node_id,
                "hostname": n.hostname,
                "last_seen_ms": n.last_seen_ms,
                "status": n.status,
                "latest_stats": n.latest_stats,
            })))
        }
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!(error = %e, "failed to query node from database");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// `GET /api/v1/nodes/:id/history` — Historical time-series data.
pub async fn get_node_history(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Query(params): Query<HistoryQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let repo = SqliteNodeRepository::new(state.db.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let from_ms = now_ms - parse_range_to_ms(&params.range);

    let snapshots = repo
        .get_snapshots(&node_id, from_ms, now_ms, params.limit)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to query snapshots");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(serde_json::json!({
        "node_id": node_id,
        "range": params.range,
        "from_ms": from_ms,
        "to_ms": now_ms,
        "count": snapshots.len(),
        "snapshots": snapshots,
    })))
}

/// `GET /health` — Health check endpoint.
pub async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "pulse-server",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// Parse a human-readable time range into milliseconds.
fn parse_range_to_ms(range: &str) -> i64 {
    let range = range.trim().to_lowercase();
    let (num_str, unit) = range.split_at(range.len().saturating_sub(1));
    let num: i64 = num_str.parse().unwrap_or(1);

    match unit {
        "m" => num * 60 * 1000,
        "h" => num * 3600 * 1000,
        "d" => num * 86400 * 1000,
        _ => 3600 * 1000, // Default to 1 hour
    }
}
