//! REST API routes for the dashboard.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;

use crate::db::{NodeRepository, PostgresNodeRepository};
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
    let repo = PostgresNodeRepository::new(state.db.clone());
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
    let repo = PostgresNodeRepository::new(state.db.clone());

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;
    use crate::state::{NodeInfo, NodeStatus};

    fn get_mock_state() -> AppState {
        let db = sqlx::PgPool::connect_lazy("postgres://localhost/test").unwrap();
        AppState::new(db)
    }

    #[tokio::test]
    async fn test_api_list_nodes() {
        let state = get_mock_state();
        state.nodes.insert(
            "node-test".to_string(),
            NodeInfo {
                node_id: "node-test".to_string(),
                hostname: "node-test-host".to_string(),
                last_seen_ms: 1000,
                status: NodeStatus::Online,
                latest_stats: Some(serde_json::json!({"cpu": 10.0})),
            },
        );

        let app = Router::new()
            .route("/api/v1/nodes", get(list_nodes))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/nodes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        
        assert_eq!(body_json["count"], 1);
        assert_eq!(body_json["nodes"][0]["node_id"], "node-test");
    }

    #[tokio::test]
    async fn test_api_get_node_cache_hit() {
        let state = get_mock_state();
        state.nodes.insert(
            "node-test-2".to_string(),
            NodeInfo {
                node_id: "node-test-2".to_string(),
                hostname: "node-test-host-2".to_string(),
                last_seen_ms: 2000,
                status: NodeStatus::Online,
                latest_stats: Some(serde_json::json!({"cpu": 20.0})),
            },
        );

        let app = Router::new()
            .route("/api/v1/nodes/{id}", get(get_node))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/nodes/node-test-2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        
        assert_eq!(body_json["node_id"], "node-test-2");
        assert_eq!(body_json["hostname"], "node-test-host-2");
    }
}
