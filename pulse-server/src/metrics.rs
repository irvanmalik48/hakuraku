//! Prometheus instrumentation and metrics registry.

use prometheus::{Counter, CounterVec, Gauge, HistogramOpts, HistogramVec, Opts, Registry};
use std::sync::LazyLock;

/// The central Prometheus metrics registry.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

// ── HTTP Metrics ─────────────────────────────────────────────────────────────

pub static HTTP_REQUESTS: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new(
        "http_requests_total",
        "Total number of HTTP requests processed",
    );
    let c = CounterVec::new(opts, &["path", "method", "status"]).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

pub static HTTP_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new(
        "http_request_duration_seconds",
        "HTTP request durations in seconds",
    );
    let h = HistogramVec::new(opts, &["path", "method"]).unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});

// ── gRPC Metrics ─────────────────────────────────────────────────────────────

pub static GRPC_MESSAGES: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new(
        "grpc_messages_received_total",
        "Total number of gRPC messages received from agents",
    );
    let c = CounterVec::new(opts, &["node_id", "message_type"]).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

// ── Authentication Failures ──────────────────────────────────────────────────

pub static AUTH_FAILURES: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new(
        "auth_failures_total",
        "Total number of authentication failures",
    );
    let c = CounterVec::new(opts, &["client_type", "reason"]).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

// ── Ingestion Queue Metrics ──────────────────────────────────────────────────

pub static INGESTION_DEPTH: LazyLock<Gauge> = LazyLock::new(|| {
    let g = Gauge::new(
        "ingestion_queue_depth",
        "Current number of items in the bounded database ingestion queue",
    )
    .unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

pub static INGESTION_DROPS: LazyLock<Counter> = LazyLock::new(|| {
    let c = Counter::new(
        "ingestion_drops_total",
        "Total number of ingestion items dropped due to queue saturation",
    )
    .unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

// ── Database Metrics ─────────────────────────────────────────────────────────

pub static DB_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new(
        "db_query_duration_seconds",
        "Database query duration in seconds",
    )
    .buckets(vec![
        0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
    ]);
    let h = HistogramVec::new(opts, &["operation"]).unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});

pub static DB_ERRORS: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new(
        "db_errors_total",
        "Total number of database errors encountered",
    );
    let c = CounterVec::new(opts, &["operation"]).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

// ── WebSocket Metrics ────────────────────────────────────────────────────────

pub static WS_CONNECTIONS: LazyLock<Gauge> = LazyLock::new(|| {
    let g = Gauge::new(
        "websocket_connections_active",
        "Active real-time WebSocket dashboard connections",
    )
    .unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

// ── Node Status Metrics ──────────────────────────────────────────────────────

pub static NODES_ONLINE: LazyLock<Gauge> = LazyLock::new(|| {
    let g = Gauge::new(
        "nodes_online_count",
        "Current number of active online monitored agents",
    )
    .unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

pub static NODES_OFFLINE: LazyLock<Gauge> = LazyLock::new(|| {
    let g = Gauge::new(
        "nodes_offline_count",
        "Current number of configured offline monitored agents",
    )
    .unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});
