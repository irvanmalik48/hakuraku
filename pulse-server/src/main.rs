//! `pulse-server` — High-throughput ingestion API and dashboard server.
//!
//! Runs two protocol servers concurrently:
//! - **gRPC** (`:50051`): Agent telemetry ingestion via `tonic`
//! - **HTTP** (`:3000`): REST API + WebSocket fan-out via `axum`

mod api;
mod db;
mod grpc;
mod state;
mod ws;

use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::error_handling::HandleErrorLayer;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::routing::get;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tonic::transport::Server as TonicServer;
use tower::BoxError;
use tower::timeout::TimeoutLayer;
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use pulse_core::MonitoringServiceServer;
use pulse_core::auth::AuthInterceptor;

use crate::db::{NodeRepository, SqliteNodeRepository};
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

    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://pulse.db".to_string());

    // Strip the sqlite:// prefix to get the file path for SqliteConnectOptions
    let db_path = database_url
        .strip_prefix("sqlite://")
        .unwrap_or(&database_url);

    let connect_opts = SqliteConnectOptions::new()
        .filename(db_path)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5))
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(connect_opts)
        .await
        .context("failed to connect to SQLite database")?;

    info!(url = %database_url, "database connected (WAL mode)");

    // Run migrations
    let repo = SqliteNodeRepository::new(pool.clone());
    repo.migrate()
        .await
        .context("failed to run database migrations")?;

    // Spawn background task to clean up old snapshots (older than 7 days, checked every hour)
    let repo_cleanup = repo.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let seven_days_ms = 7 * 24 * 3600 * 1000;
            let before_ms = now_ms - seven_days_ms;
            match repo_cleanup.cleanup_old_snapshots(before_ms).await {
                Ok(count) => {
                    if count > 0 {
                        tracing::info!(count = count, "cleaned up old snapshots");
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to clean up old snapshots");
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

    // ── Shared State ────────────────────────────────────────────────────────

    let app_state = AppState::new(pool.clone());

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

    // ── Auth Secret ─────────────────────────────────────────────────────────

    let auth_secret =
        std::env::var("PULSE_AUTH_SECRET").context("PULSE_AUTH_SECRET must be set")?;

    // ── gRPC Server ─────────────────────────────────────────────────────────

    let grpc_port: u16 = std::env::var("PULSE_GRPC_PORT")
        .unwrap_or_else(|_| "50051".to_string())
        .parse()
        .context("PULSE_GRPC_PORT must be a valid port number")?;

    let grpc_addr = format!("0.0.0.0:{grpc_port}").parse()?;
    let monitoring_service = MonitoringServiceImpl::new(app_state.clone(), repo.clone());
    let auth_interceptor = AuthInterceptor::new(auth_secret.into_bytes());

    let grpc_server = TonicServer::builder()
        .concurrency_limit_per_connection(32)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .http2_keepalive_interval(Some(Duration::from_secs(60)))
        .http2_keepalive_timeout(Some(Duration::from_secs(20)))
        .add_service(MonitoringServiceServer::with_interceptor(
            monitoring_service,
            auth_interceptor,
        ))
        .serve(grpc_addr);

    info!(port = grpc_port, "gRPC server listening");

    // ── Axum HTTP Server ────────────────────────────────────────────────────

    let http_port: u16 = std::env::var("PULSE_HTTP_PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .context("PULSE_HTTP_PORT must be a valid port number")?;

    let cors = CorsLayer::permissive();

    let app = Router::new()
        // REST API routes
        .route("/api/v1/nodes", get(api::list_nodes))
        .route("/api/v1/nodes/{id}", get(api::get_node))
        .route("/api/v1/nodes/{id}/history", get(api::get_node_history))
        // WebSocket endpoint
        .route("/ws", get(ws::ws_handler))
        // Health check
        .route("/health", get(api::health_check))
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
        .with_state(app_state);

    let http_addr = format!("0.0.0.0:{http_port}");
    let listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .context("failed to bind HTTP listener")?;

    info!(port = http_port, "HTTP/WebSocket server listening");

    // ── Run Both Servers Concurrently ───────────────────────────────────────

    tokio::select! {
        result = grpc_server => {
            result.context("gRPC server error")?;
        }
        result = axum::serve(listener, app) => {
            result.context("HTTP server error")?;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received, stopping servers");
        }
    }

    info!("pulse-server shut down cleanly");
    Ok(())
}
