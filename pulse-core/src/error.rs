//! Shared error types for the 伯楽 (Hakuraku) ecosystem.

use thiserror::Error;

/// Authentication errors.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing required header: {0}")]
    MissingHeader(String),

    #[error("invalid HMAC signature")]
    InvalidSignature,

    #[error("timestamp drift {drift}s exceeds maximum {max}s")]
    ExpiredTimestamp { drift: i64, max: i64 },

    #[error("invalid HMAC key")]
    InvalidKey,

    #[error("system clock error")]
    ClockError,
}

/// Metric collection errors (agent-side).
#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("failed to read {path}: {source}")]
    ProcRead {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to parse {field} from {path}: {detail}")]
    Parse {
        path: String,
        field: String,
        detail: String,
    },
}

/// Network probe errors.
#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("probe timed out after {timeout_ms}ms to {target}")]
    Timeout { target: String, timeout_ms: u32 },

    #[error("connection refused to {target}")]
    ConnectionRefused { target: String },

    #[error("DNS resolution failed for {host}: {detail}")]
    DnsFailure { host: String, detail: String },

    #[error("probe I/O error: {0}")]
    Io(#[from] std::io::Error),
}
