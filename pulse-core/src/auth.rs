//! HMAC-SHA256 authentication for agent ↔ server communication.
//!
//! # Protocol
//!
//! Each gRPC request carries four metadata headers:
//! - `x-pulse-node-id`: The agent's node identifier
//! - `x-pulse-timestamp`: Unix timestamp in seconds (as decimal string)
//! - `x-pulse-nonce`: A random 16-byte hex string (unique per request)
//! - `x-pulse-signature`: HMAC-SHA256(secret, "{node_id}:{timestamp}:{nonce}") as hex
//!
//! The server validates that:
//! 1. All four headers are present
//! 2. The timestamp is within ±`MAX_CLOCK_SKEW` of the server's clock
//! 3. The nonce has not been used before within the skew window
//! 4. The HMAC signature is valid

use std::collections::HashSet;
use std::sync::Mutex;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tonic::service::Interceptor;

use crate::error::AuthError;

type HmacSha256 = Hmac<Sha256>;

/// Maximum allowed clock skew between agent and server (seconds).
const MAX_CLOCK_SKEW: i64 = 60;

/// Generate a random nonce as a 16-byte hex string.
pub fn generate_nonce() -> String {
    let mut buf = [0u8; 16];
    // Use SHA-256 of system entropy sources as nonce material.
    // getrandom is a transitive dep — use it if available, else fall back.
    if getrandom::fill(&mut buf).is_err() {
        // Fallback: hash system time + thread ID for uniqueness
        let seed = format!(
            "{}:{:?}:{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            std::thread::current().id(),
            std::process::id(),
        );
        let hash = Sha256::digest(seed.as_bytes());
        buf.copy_from_slice(&hash[..16]);
    }
    hex::encode(buf)
}

/// Compute the HMAC-SHA256 signature for a request.
///
/// The signed message is `"{node_id}:{timestamp}:{nonce}"`.
pub fn sign_request(secret: &[u8], node_id: &str, timestamp: i64, nonce: &str) -> String {
    let message = format!("{node_id}:{timestamp}:{nonce}");
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Verify an HMAC-SHA256 signature with timestamp + nonce replay protection.
pub fn verify_request(
    secret: &[u8],
    node_id: &str,
    timestamp: i64,
    nonce: &str,
    signature: &str,
) -> Result<(), AuthError> {
    // Check timestamp freshness
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| AuthError::ClockError)?
        .as_secs() as i64;

    let drift = (now - timestamp).abs();
    if drift > MAX_CLOCK_SKEW {
        return Err(AuthError::ExpiredTimestamp {
            drift,
            max: MAX_CLOCK_SKEW,
        });
    }

    // Verify HMAC (nonce is part of the signed message)
    let message = format!("{node_id}:{timestamp}:{nonce}");
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| AuthError::InvalidKey)?;
    mac.update(message.as_bytes());

    let expected = hex::decode(signature).map_err(|_| AuthError::InvalidSignature)?;
    mac.verify_slice(&expected)
        .map_err(|_| AuthError::InvalidSignature)
}

/// tonic interceptor that validates HMAC auth headers on incoming requests.
///
/// Tracks recently seen nonces to reject replay attacks within the clock skew window.
#[derive(Clone)]
pub struct AuthInterceptor {
    secret: Vec<u8>,
    /// Set of recently seen nonces (protected by mutex for shared access).
    seen_nonces: std::sync::Arc<Mutex<NonceTracker>>,
}

/// Tracks recently used nonces to prevent replay attacks.
struct NonceTracker {
    /// Set of nonce strings seen within the current window.
    nonces: HashSet<String>,
    /// Timestamp (seconds) when the set was last purged.
    last_purge: i64,
}

impl NonceTracker {
    fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            nonces: HashSet::new(),
            last_purge: now,
        }
    }

    /// Check if a nonce has been seen before. Purges old nonces periodically.
    fn check_and_insert(&mut self, nonce: &str) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        // Purge the nonce set every 2× the skew window to bound memory usage
        if now - self.last_purge > MAX_CLOCK_SKEW * 2 {
            self.nonces.clear();
            self.last_purge = now;
        }

        self.nonces.insert(nonce.to_string())
    }
}

impl AuthInterceptor {
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
            seen_nonces: std::sync::Arc::new(Mutex::new(NonceTracker::new())),
        }
    }

    fn extract_header(request: &tonic::Request<()>, key: &str) -> Result<String, AuthError> {
        request
            .metadata()
            .get(key)
            .ok_or(AuthError::MissingHeader(key.to_string()))?
            .to_str()
            .map(|s| s.to_string())
            .map_err(|_| AuthError::MissingHeader(key.to_string()))
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let node_id = Self::extract_header(&request, "x-pulse-node-id")
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;
        let timestamp_str = Self::extract_header(&request, "x-pulse-timestamp")
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;
        let nonce = Self::extract_header(&request, "x-pulse-nonce")
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;
        let signature = Self::extract_header(&request, "x-pulse-signature")
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;

        let timestamp: i64 = timestamp_str
            .parse()
            .map_err(|_| tonic::Status::unauthenticated("invalid timestamp format"))?;

        verify_request(&self.secret, &node_id, timestamp, &nonce, &signature)
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;

        // Reject replayed nonces
        let is_new = self
            .seen_nonces
            .lock()
            .map_err(|_| tonic::Status::internal("nonce tracker lock poisoned"))?
            .check_and_insert(&nonce);
        if !is_new {
            return Err(tonic::Status::unauthenticated("replayed nonce"));
        }

        Ok(request)
    }
}

/// Inject HMAC auth headers into an outgoing gRPC request.
///
/// Used by the agent to sign every request.
pub fn inject_auth_headers(
    secret: &[u8],
    node_id: &str,
    mut request: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| tonic::Status::internal("system clock error"))?
        .as_secs() as i64;

    let nonce = generate_nonce();
    let signature = sign_request(secret, node_id, timestamp, &nonce);

    let metadata = request.metadata_mut();
    metadata.insert(
        "x-pulse-node-id",
        node_id
            .parse()
            .map_err(|_| tonic::Status::internal("invalid node id"))?,
    );
    metadata.insert(
        "x-pulse-timestamp",
        timestamp
            .to_string()
            .parse()
            .map_err(|_| tonic::Status::internal("timestamp format error"))?,
    );
    metadata.insert(
        "x-pulse-nonce",
        nonce
            .parse()
            .map_err(|_| tonic::Status::internal("nonce format error"))?,
    );
    metadata.insert(
        "x-pulse-signature",
        signature
            .parse()
            .map_err(|_| tonic::Status::internal("signature format error"))?,
    );

    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_roundtrip() {
        let secret = b"test-secret-key";
        let node_id = "node-01";
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let nonce = generate_nonce();

        let sig = sign_request(secret, node_id, timestamp, &nonce);
        assert!(verify_request(secret, node_id, timestamp, &nonce, &sig).is_ok());
    }

    #[test]
    fn reject_expired_timestamp() {
        let secret = b"test-secret-key";
        let node_id = "node-01";
        let old_timestamp = 1_000_000; // way in the past
        let nonce = generate_nonce();

        let sig = sign_request(secret, node_id, old_timestamp, &nonce);
        let result = verify_request(secret, node_id, old_timestamp, &nonce, &sig);
        assert!(matches!(result, Err(AuthError::ExpiredTimestamp { .. })));
    }

    #[test]
    fn reject_wrong_signature() {
        let secret = b"test-secret-key";
        let node_id = "node-01";
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let nonce = generate_nonce();

        let result = verify_request(secret, node_id, timestamp, &nonce, "deadbeef");
        assert!(matches!(result, Err(AuthError::InvalidSignature)));
    }

    #[test]
    fn nonce_is_unique() {
        let n1 = generate_nonce();
        let n2 = generate_nonce();
        assert_ne!(n1, n2, "consecutive nonces must be unique");
        assert_eq!(n1.len(), 32, "nonce should be 16 bytes = 32 hex chars");
    }
}
