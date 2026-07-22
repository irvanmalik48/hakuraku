//! WebSocket fan-out handler for real-time dashboard updates.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::state::AppState;

/// Optional query parameters for WebSocket filtering.
#[derive(Deserialize)]
pub struct WsQuery {
    /// Filter updates to a specific node ID. If omitted, all updates are sent.
    pub node_id: Option<String>,
}

/// `GET /ws` — Upgrade to WebSocket for real-time telemetry updates.
pub async fn ws_handler(
    _claims: crate::api::Claims,
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(params): Query<WsQuery>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, params.node_id))
}

struct WsGuard;
impl Drop for WsGuard {
    fn drop(&mut self) {
        crate::metrics::WS_CONNECTIONS.dec();
    }
}

/// Process a single WebSocket connection.
async fn handle_socket(socket: WebSocket, state: AppState, filter_node_id: Option<String>) {
    crate::metrics::WS_CONNECTIONS.inc();
    let _guard = WsGuard;
    let (mut sender, mut receiver) = socket.split();

    // Subscribe to the broadcast channel
    let mut rx = state.broadcast_tx.subscribe();

    debug!(
        filter = ?filter_node_id,
        "WebSocket client connected"
    );

    // Send initial state: current snapshot of all nodes
    let initial_state: Vec<serde_json::Value> = state
        .nodes
        .iter()
        .filter(|entry| filter_node_id.as_ref().is_none_or(|f| f == entry.key()))
        .map(|entry| {
            let node = entry.value();
            serde_json::json!({
                "type": "snapshot",
                "node_id": node.node_id,
                "hostname": node.hostname,
                "last_seen_ms": node.last_seen_ms,
                "status": node.status,
                "stats": node.latest_stats,
            })
        })
        .collect();

    let init_msg = serde_json::json!({
        "type": "init",
        "nodes": initial_state,
    });

    if let Ok(json) = serde_json::to_string(&init_msg)
        && sender.send(Message::Text(json.into())).await.is_err()
    {
        return;
    }

    // Spawn a task to forward broadcast updates to the WebSocket
    let filter_clone = filter_node_id.clone();
    let send_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(update) => {
                    // Apply node filter if specified
                    if let Some(ref filter) = filter_clone
                        && &update.node_id != filter
                    {
                        continue;
                    }

                    let msg = serde_json::json!({
                        "type": "update",
                        "node_id": update.node_id,
                        "timestamp_ms": update.timestamp_ms,
                        "stats": update.stats,
                    });

                    match serde_json::to_string(&msg) {
                        Ok(json) => {
                            if sender.send(Message::Text(json.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to serialize WebSocket update");
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "WebSocket client lagging, skipped messages");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    });

    // Listen for client messages (ping/pong, close)
    let recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(Message::Ping(data)) => {
                    // Axum handles pong automatically
                    debug!(len = data.len(), "received ping from WebSocket client");
                }
                _ => {
                    // Ignore other messages from client
                }
            }
        }
    });

    // Wait for either task to complete (client disconnect or broadcast end)
    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }

    debug!(filter = ?filter_node_id, "WebSocket client disconnected");
}
