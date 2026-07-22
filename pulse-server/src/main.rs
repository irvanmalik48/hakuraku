//! `pulse-server` — High-throughput ingestion API and dashboard server.
//!
//! Runs two protocol servers concurrently:
//! - **gRPC** (`:50051`): Agent telemetry ingestion via `tonic`
//! - **HTTP** (`:3000`): REST API + WebSocket fan-out via `axum`

mod api;
mod db;
mod grpc;
mod metrics;
mod state;
mod ws;

#[cfg(test)]
mod integrity_tests;

use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::error_handling::HandleErrorLayer;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::routing::get;
use sqlx::postgres::PgPoolOptions;
use tonic::transport::Server as TonicServer;
use tower::BoxError;
use tower::timeout::TimeoutLayer;
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use pulse_core::MonitoringServiceServer;
use pulse_core::auth::AuthInterceptor;

use crate::db::{NodeRepository, PostgresNodeRepository};
use crate::grpc::MonitoringServiceImpl;
use crate::state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pulse_server=info,tower_http=debug".parse().unwrap()),
        )
        .compact()
        .init();

    info!("pulse-server v{} starting", env!("CARGO_PKG_VERSION"));

    // ── Database Setup ──────────────────────────────────────────────────────

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://pulse:password@localhost:5432/pulse".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url)
        .await
        .context("failed to connect to PostgreSQL database")?;

    info!("database connected successfully");

    // Run migrations
    let repo = PostgresNodeRepository::new(pool.clone());
    repo.migrate()
        .await
        .context("failed to run database migrations")?;

    let cancel_token = tokio_util::sync::CancellationToken::new();

    // Spawn background task to clean up old snapshots (older than 7 days, checked every hour)
    let repo_cleanup = repo.clone();
    let cancel_token_cleanup = cancel_token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    let seven_days_ms = 7 * 24 * 3600 * 1000;
                    let before_ms = now_ms - seven_days_ms;
                    match repo_cleanup.cleanup_old_snapshots(before_ms).await {
                        Ok(count) => {
                            if count > 0 {
                                tracing::info!(count = count, "cleaned up old snapshots and probe results");
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "failed to clean up old snapshots");
                        }
                    }
                }
                _ = cancel_token_cleanup.cancelled() => {
                    tracing::info!("snapshot cleanup loop stopped");
                    break;
                }
            }
        }
    });

    // ── Rate Limiting Config ─────────────────────────────────────────────────
    let governor_conf = GovernorConfigBuilder::default()
        .per_second(2)
        .burst_size(10)
        .finish()
        .context("failed to build governor config")?;

    // ── Ingestion Channel & Worker Pool ──────────────────────────────────────
    let vm_url = std::env::var("VICTORIAMETRICS_URL").ok().map(|url| {
        let trimmed = url.trim().trim_end_matches('/');
        format!("{}/api/v1/import", trimmed)
    });

    let vm_client = if vm_url.is_some() {
        tracing::info!(url = ?vm_url, "VictoriaMetrics integration enabled");
        Some(reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to build reqwest client for VictoriaMetrics");
                reqwest::Client::new()
            }))
    } else {
        None
    };

    let (ingestion_tx, ingestion_rx) =
        tokio::sync::mpsc::channel::<crate::state::IngestionItem>(4096);
    let shared_rx = std::sync::Arc::new(tokio::sync::Mutex::new(ingestion_rx));
    let worker_count = 4;
    let mut worker_handles = Vec::new();

    for i in 0..worker_count {
        let rx = shared_rx.clone();
        let repo = repo.clone();
        let vm_url = vm_url.clone();
        let vm_client = vm_client.clone();
        let handle = tokio::spawn(async move {
            tracing::info!(worker_id = i, "ingestion worker started");
            loop {
                let item = {
                    let mut lock = rx.lock().await;
                    lock.recv().await
                };

                let Some(item) = item else {
                    break;
                };

                match item {
                    crate::state::IngestionItem::Stats {
                        node_id,
                        timestamp_ms,
                        stats_str,
                        stats_json,
                    } => {
                        if let Err(e) = repo.upsert_node(&node_id, &node_id, "online").await {
                            tracing::error!(error = %e, node_id = %node_id, "worker failed to upsert node");
                        }
                        if let Err(e) = repo
                            .insert_snapshot(&node_id, timestamp_ms, &stats_str)
                            .await
                        {
                            tracing::error!(error = %e, node_id = %node_id, "worker failed to insert snapshot");
                        }

                        // Optional VictoriaMetrics push
                        if let Some(ref client) = vm_client {
                            let url = vm_url.clone().unwrap();
                            let client = client.clone();
                            let payload =
                                serialize_to_vm_jsonl(&node_id, timestamp_ms, &stats_json);
                            tokio::spawn(async move {
                                let res = client
                                    .post(&url)
                                    .header("content-type", "application/json")
                                    .body(payload)
                                    .send()
                                    .await;
                                if let Err(e) = res {
                                    tracing::error!(error = %e, "failed to send metrics to VictoriaMetrics");
                                    crate::metrics::DB_ERRORS
                                        .with_label_values(&["victoriametrics_post"])
                                        .inc();
                                }
                            });
                        }
                    }
                    crate::state::IngestionItem::ProbeResult {
                        node_id,
                        target,
                        success,
                        latency_us,
                        error_message,
                        timestamp,
                    } => {
                        if let Err(e) = repo
                            .insert_probe_result(
                                &node_id,
                                &target,
                                success,
                                latency_us,
                                &error_message,
                                timestamp,
                            )
                            .await
                        {
                            tracing::error!(error = %e, node_id = %node_id, "worker failed to insert probe result");
                        }
                    }
                }
            }
            tracing::info!(worker_id = i, "ingestion worker shut down");
        });
        worker_handles.push(handle);
    }

    // ── Shared State ────────────────────────────────────────────────────────

    let app_state = AppState::new(pool.clone(), ingestion_tx);

    // Load existing nodes from database to populate in-memory state
    match repo.get_all_nodes().await {
        Ok(nodes) => {
            let count = nodes.len();
            for node in nodes {
                app_state.nodes.insert(node.node_id.clone(), node);
            }
            info!(count = count, "loaded existing nodes from database");
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to load nodes from database");
        }
    }

    // Spawn background task to check node liveness (offline transitions, checked every 10 seconds)
    let liveness_state = app_state.clone();
    let liveness_repo = repo.clone();
    let cancel_token_liveness = cancel_token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;

                    let mut transitions = Vec::new();
                    let mut online = 0;
                    let mut offline = 0;

                    for mut entry in liveness_state.nodes.iter_mut() {
                        let node = entry.value_mut();
                        if node.status == crate::state::NodeStatus::Online && now_ms - node.last_seen_ms > 30_000 {
                            node.status = crate::state::NodeStatus::Offline;
                            tracing::info!(node_id = %node.node_id, "node went offline due to timeout");
                            transitions.push((node.node_id.clone(), node.last_seen_ms, node.latest_stats.clone()));
                        }

                        match node.status {
                            crate::state::NodeStatus::Online => online += 1,
                            crate::state::NodeStatus::Offline => offline += 1,
                            _ => {}
                        }
                    }

                    crate::metrics::NODES_ONLINE.set(online as f64);
                    crate::metrics::NODES_OFFLINE.set(offline as f64);
                    crate::metrics::INGESTION_DEPTH.set((4096 - liveness_state.ingestion_tx.capacity()) as f64);

                    for (node_id, last_seen, latest_stats) in transitions {
                        // Persist offline status to database
                        if let Err(e) = liveness_repo.upsert_node(&node_id, &node_id, "offline").await {
                            tracing::error!(error = %e, node_id = %node_id, "failed to mark node offline in database");
                        }

                        // Broadcast WebSocket update
                        let _ = liveness_state.broadcast_tx.send(crate::state::NodeUpdate {
                            node_id: node_id.clone(),
                            timestamp_ms: last_seen,
                            stats: latest_stats.unwrap_or(serde_json::Value::Null),
                        });
                    }
                }
                _ = cancel_token_liveness.cancelled() => {
                    tracing::info!("liveness checker loop stopped");
                    break;
                }
            }
        }
    });

    // ── Auth Secret ─────────────────────────────────────────────────────────

    let auth_secret =
        std::env::var("PULSE_AUTH_SECRET").context("PULSE_AUTH_SECRET must be set")?;

    // ── gRPC Server ─────────────────────────────────────────────────────────

    let grpc_port: u16 = std::env::var("PULSE_GRPC_PORT")
        .unwrap_or_else(|_| "50051".to_string())
        .parse()
        .context("PULSE_GRPC_PORT must be a valid port number")?;

    let grpc_addr = format!("0.0.0.0:{grpc_port}").parse()?;
    let monitoring_service = MonitoringServiceImpl::new(app_state.clone());
    let auth_secret_bytes = auth_secret.into_bytes();
    let auth_interceptor = move |req: tonic::Request<()>| {
        let mut interceptor = AuthInterceptor::new(auth_secret_bytes.clone());
        use tonic::service::Interceptor;
        match interceptor.call(req) {
            Ok(r) => Ok(r),
            Err(status) => {
                crate::metrics::AUTH_FAILURES
                    .with_label_values(&["grpc", "unauthenticated"])
                    .inc();
                Err(status)
            }
        }
    };

    let grpc_server = TonicServer::builder()
        .concurrency_limit_per_connection(32)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .http2_keepalive_interval(Some(Duration::from_secs(60)))
        .http2_keepalive_timeout(Some(Duration::from_secs(20)))
        .add_service(MonitoringServiceServer::with_interceptor(
            monitoring_service,
            auth_interceptor,
        ));

    info!(port = grpc_port, "gRPC server listening");

    // ── Axum HTTP Server ────────────────────────────────────────────────────

    let http_port: u16 = std::env::var("PULSE_HTTP_PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .context("PULSE_HTTP_PORT must be a valid port number")?;

    let cors_origins_env = std::env::var("PULSE_CORS_ALLOWED_ORIGINS").ok();
    let cors = if let Some(ref origins_str) = cors_origins_env {
        let mut origins = Vec::new();
        for s in origins_str.split(',') {
            if let Ok(parsed) = s.trim().parse() {
                origins.push(parsed);
            }
        }
        if origins.is_empty() {
            CorsLayer::permissive()
        } else {
            CorsLayer::new()
                .allow_origin(origins)
                .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
                .allow_headers([
                    axum::http::header::AUTHORIZATION,
                    axum::http::header::CONTENT_TYPE,
                    axum::http::HeaderName::from_static("x-pulse-auth-token"),
                ])
        }
    } else {
        CorsLayer::new()
            .allow_origin([
                "http://localhost:3000".parse().unwrap(),
                "http://127.0.0.1:3000".parse().unwrap(),
            ])
            .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
            .allow_headers([
                axum::http::header::AUTHORIZATION,
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderName::from_static("x-pulse-auth-token"),
            ])
    };

    let app = Router::new()
        // REST API routes
        .route("/api/v1/nodes", get(api::list_nodes))
        .route("/api/v1/nodes/{id}", get(api::get_node))
        .route("/api/v1/nodes/{id}/history", get(api::get_node_history))
        // WebSocket endpoint
        .route("/ws", get(ws::ws_handler))
        // Health check
        .route("/health", get(api::health_check))
        .route("/healthz", get(api::liveness_check))
        .route("/readyz", get(api::readiness_check))
        .route("/metrics", get(api::metrics_handler))
        // Middleware layers
        .layer(cors)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(GovernorLayer::new(governor_conf))
        .layer(
            tower::ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|err: BoxError| async move {
                    (
                        StatusCode::REQUEST_TIMEOUT,
                        format!("Request timed out: {}", err),
                    )
                }))
                .layer(TimeoutLayer::new(Duration::from_secs(10))),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(app_state.clone());

    let http_addr = format!("0.0.0.0:{http_port}");
    let listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .context("failed to bind HTTP listener")?;

    info!(port = http_port, "HTTP/WebSocket server listening");

    // ── Run Both Servers Concurrently ───────────────────────────────────────

    let token_for_ctrl_c = cancel_token.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Ctrl-C received, triggering graceful shutdown...");
        token_for_ctrl_c.cancel();
    });

    let token_grpc = cancel_token.clone();
    let grpc_handle = tokio::spawn(async move {
        grpc_server
            .serve_with_shutdown(grpc_addr, token_grpc.cancelled())
            .await
            .context("gRPC server error")
    });

    let token_axum = cancel_token.clone();
    let axum_handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                token_axum.cancelled().await;
            })
            .await
            .context("HTTP server error")
    });

    // Wait for servers to stop
    let _ = grpc_handle.await;
    let _ = axum_handle.await;

    tracing::info!("Servers stopped. Draining ingestion queue workers...");

    // Drop all sender references to close the channel and terminate workers
    drop(app_state);

    for handle in worker_handles {
        let _ = handle.await;
    }

    tracing::info!("pulse-server shut down cleanly");
    Ok(())
}

fn serialize_to_vm_jsonl(node_id: &str, timestamp_ms: i64, stats: &serde_json::Value) -> String {
    let mut lines = Vec::new();

    let mut add_metric = |name: &str, val: f64| {
        let line = serde_json::json!({
            "metric": {
                "__name__": name,
                "node_id": node_id,
            },
            "values": [val],
            "timestamps": [timestamp_ms]
        })
        .to_string();
        lines.push(line);
    };

    if let Some(obj) = stats.as_object() {
        if let Some(cpu) = obj.get("cpu_percent").and_then(|v| v.as_f64()) {
            add_metric("hakuraku_cpu_percent", cpu);
        }
        if let Some(mem_total) = obj.get("mem_total").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_mem_total_bytes", mem_total as f64);
        }
        if let Some(mem_used) = obj.get("mem_used").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_mem_used_bytes", mem_used as f64);
        }
        if let Some(mem_free) = obj.get("mem_free").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_mem_free_bytes", mem_free as f64);
        }
        if let Some(mem_available) = obj.get("mem_available").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_mem_available_bytes", mem_available as f64);
        }
        if let Some(swap_total) = obj.get("swap_total").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_swap_total_bytes", swap_total as f64);
        }
        if let Some(swap_used) = obj.get("swap_used").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_swap_used_bytes", swap_used as f64);
        }
        if let Some(disk_read) = obj.get("disk_read_bytes_sec").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_disk_read_bytes_sec", disk_read as f64);
        }
        if let Some(disk_write) = obj.get("disk_write_bytes_sec").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_disk_write_bytes_sec", disk_write as f64);
        }
        if let Some(tcp) = obj.get("tcp_connections").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_tcp_connections", tcp as f64);
        }
        if let Some(udp) = obj.get("udp_connections").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_udp_connections", udp as f64);
        }
        if let Some(uptime) = obj.get("uptime_seconds").and_then(|v| v.as_u64()) {
            add_metric("hakuraku_uptime_seconds", uptime as f64);
        }
    }

    lines.join("\n") + "\n"
}
