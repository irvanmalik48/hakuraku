//! REST API routes for the dashboard.

use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::Json;
use prometheus::Encoder;
use serde::Deserialize;

use crate::db::{NodeRepository, PostgresNodeRepository};
use crate::state::AppState;

/// Authenticated user credentials extractor.
pub struct Claims;

impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
    AppState: axum::extract::FromRef<S>,
{
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let secret = match std::env::var("PULSE_AUTH_SECRET") {
            Ok(s) => s,
            Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        };

        // 1. Authorization: Bearer <token>
        if let Some(token) = parts.headers.get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .filter(|tok| validate_token(tok.trim(), &secret))
        {
            let _ = token;
            return Ok(Claims);
        }

        // 2. x-pulse-auth-token header
        if let Some(token_str) = parts.headers.get("x-pulse-auth-token")
            .and_then(|h| h.to_str().ok())
            .filter(|tok| validate_token(tok.trim(), &secret))
        {
            let _ = token_str;
            return Ok(Claims);
        }

        // 3. Query Parameter: token=<value> (parse manually to avoid dependency)
        let query_str = parts.uri.query().unwrap_or("");
        let mut token = None;
        for pair in query_str.split('&') {
            if let Some(val) = pair.strip_prefix("token=") {
                token = Some(val.to_string());
                break;
            }
        }
        if let Some(tok) = token.filter(|t| validate_token(t.trim(), &secret)) {
            let _ = tok;
            return Ok(Claims);
        }

        crate::metrics::AUTH_FAILURES
            .with_label_values(&["http", "unauthorized"])
            .inc();
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn validate_token(token: &str, expected: &str) -> bool {
    if token.len() != expected.len() {
        return false;
    }
    let mut equal = 0;
    for (a, b) in token.bytes().zip(expected.bytes()) {
        equal |= a ^ b;
    }
    equal == 0
}

fn is_valid_node_id(node_id: &str) -> bool {
    !node_id.is_empty()
        && node_id.len() <= 64
        && node_id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

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
    _claims: Claims,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let start = std::time::Instant::now();
    let res: Result<Json<serde_json::Value>, StatusCode> = async {
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
    .await;

    let status = match &res {
        Ok(_) => "200",
        Err(code) => code.as_str(),
    };
    crate::metrics::HTTP_REQUESTS
        .with_label_values(&["/api/v1/nodes", "GET", status])
        .inc();
    crate::metrics::HTTP_DURATION
        .with_label_values(&["/api/v1/nodes", "GET"])
        .observe(start.elapsed().as_secs_f64());
    res
}

/// `GET /api/v1/nodes/:id` — Get detailed stats for a specific node.
pub async fn get_node(
    _claims: Claims,
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let start = std::time::Instant::now();
    let res: Result<Json<serde_json::Value>, StatusCode> = async {
        if !is_valid_node_id(&node_id) {
            return Err(StatusCode::BAD_REQUEST);
        }

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
    .await;

    let status = match &res {
        Ok(_) => "200",
        Err(code) => code.as_str(),
    };
    crate::metrics::HTTP_REQUESTS
        .with_label_values(&["/api/v1/nodes/:id", "GET", status])
        .inc();
    crate::metrics::HTTP_DURATION
        .with_label_values(&["/api/v1/nodes/:id", "GET"])
        .observe(start.elapsed().as_secs_f64());
    res
}

/// `GET /api/v1/nodes/:id/history` — Historical time-series data.
pub async fn get_node_history(
    _claims: Claims,
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Query(params): Query<HistoryQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let start = std::time::Instant::now();
    let res: Result<Json<serde_json::Value>, StatusCode> = async {
        if !is_valid_node_id(&node_id) {
            return Err(StatusCode::BAD_REQUEST);
        }

        if !params.range.chars().all(|c| c.is_alphanumeric()) || params.range.len() > 8 {
            return Err(StatusCode::BAD_REQUEST);
        }

        let limit = params.limit.clamp(1, 1000);
        let repo = PostgresNodeRepository::new(state.db.clone());

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let from_ms = now_ms - parse_range_to_ms(&params.range);

        let snapshots = repo
            .get_snapshots(&node_id, from_ms, now_ms, limit)
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
    .await;

    let status = match &res {
        Ok(_) => "200",
        Err(code) => code.as_str(),
    };
    crate::metrics::HTTP_REQUESTS
        .with_label_values(&["/api/v1/nodes/:id/history", "GET", status])
        .inc();
    crate::metrics::HTTP_DURATION
        .with_label_values(&["/api/v1/nodes/:id/history", "GET"])
        .observe(start.elapsed().as_secs_f64());
    res
}

/// `GET /health` — Health check endpoint.
pub async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "pulse-server",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// `GET /healthz` — Liveness check.
pub async fn liveness_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "pulse-server",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// `GET /readyz` — Readiness check.
pub async fn readiness_check(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match sqlx::query("SELECT 1").execute(&state.db).await {
        Ok(_) => Ok(Json(serde_json::json!({
            "status": "ready",
            "database": "connected",
        }))),
        Err(e) => {
            tracing::error!(error = %e, "readiness check failed: database unreachable");
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

/// `GET /metrics` — Prometheus metrics scrape endpoint.
pub async fn metrics_handler(_claims: Claims) -> impl axum::response::IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = crate::metrics::REGISTRY.gather();
    let mut buffer = Vec::new();

    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(error = %e, "failed to encode prometheus metrics");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain; charset=utf-8")],
            Vec::new(),
        );
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        buffer,
    )
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
    use crate::state::{NodeInfo, NodeStatus};
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;

    fn get_mock_state() -> AppState {
        unsafe {
            std::env::set_var("PULSE_AUTH_SECRET", "testsecret");
        }
        let db = sqlx::PgPool::connect_lazy("postgres://localhost/test").unwrap();
        let (tx, _) = tokio::sync::mpsc::channel(100);
        AppState::new(db, tx)
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
                    .header("Authorization", "Bearer testsecret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
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
                    .header("Authorization", "Bearer testsecret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(body_json["node_id"], "node-test-2");
        assert_eq!(body_json["hostname"], "node-test-host-2");
    }
}
