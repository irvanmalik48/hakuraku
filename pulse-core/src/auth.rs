//! HMAC-SHA256 authentication for agent ↔ server communication.
//!
//! # Protocol
//!
//! Each gRPC request carries three metadata headers:
//! - `x-pulse-node-id`: The agent's node identifier
//! - `x-pulse-timestamp`: Unix timestamp in seconds (as decimal string)
//! - `x-pulse-signature`: HMAC-SHA256(secret, "{node_id}:{timestamp}") as hex
//!
//! The server validates that:
//! 1. All three headers are present
//! 2. The timestamp is within ±`MAX_CLOCK_SKEW` of the server's clock
//! 3. The HMAC signature is valid

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tonic::service::Interceptor;

use crate::error::AuthError;

type HmacSha256 = Hmac<Sha256>;

/// Maximum allowed clock skew between agent and server (seconds).
const MAX_CLOCK_SKEW: i64 = 60;

/// Compute the HMAC-SHA256 signature for a request.
///
/// The signed message is `"{node_id}:{timestamp}"`.
pub fn sign_request(secret: &[u8], node_id: &str, timestamp: i64) -> String {
    let message = format!("{node_id}:{timestamp}");
    let mut mac =
        HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Verify an HMAC-SHA256 signature with timestamp replay protection.
pub fn verify_request(
    secret: &[u8],
    node_id: &str,
    timestamp: i64,
    signature: &str,
) -> Result<(), AuthError> {
    // Check timestamp freshness
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| AuthError::ClockError)?
        .as_secs() as i64;

    let drift = (now - timestamp).abs();
    if drift > MAX_CLOCK_SKEW {
        return Err(AuthError::ExpiredTimestamp { drift, max: MAX_CLOCK_SKEW });
    }

    // Verify HMAC
    let message = format!("{node_id}:{timestamp}");
    let mut mac =
        HmacSha256::new_from_slice(secret).map_err(|_| AuthError::InvalidKey)?;
    mac.update(message.as_bytes());

    let expected = hex::decode(signature).map_err(|_| AuthError::InvalidSignature)?;
    mac.verify_slice(&expected)
        .map_err(|_| AuthError::InvalidSignature)
}

/// tonic interceptor that validates HMAC auth headers on incoming requests.
#[derive(Clone)]
pub struct AuthInterceptor {
    secret: Vec<u8>,
}

impl AuthInterceptor {
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self { secret: secret.into() }
    }

    fn extract_header(
        request: &tonic::Request<()>,
        key: &str,
    ) -> Result<String, AuthError> {
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
    fn call(
        &mut self,
        request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let node_id = Self::extract_header(&request, "x-pulse-node-id")
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;
        let timestamp_str = Self::extract_header(&request, "x-pulse-timestamp")
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;
        let signature = Self::extract_header(&request, "x-pulse-signature")
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;

        let timestamp: i64 = timestamp_str
            .parse()
            .map_err(|_| tonic::Status::unauthenticated("invalid timestamp format"))?;

        verify_request(&self.secret, &node_id, timestamp, &signature)
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;

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

    let signature = sign_request(secret, node_id, timestamp);

    let metadata = request.metadata_mut();
    metadata.insert(
        "x-pulse-node-id",
        node_id.parse().map_err(|_| tonic::Status::internal("invalid node id"))?,
    );
    metadata.insert(
        "x-pulse-timestamp",
        timestamp
            .to_string()
            .parse()
            .map_err(|_| tonic::Status::internal("timestamp format error"))?,
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

        let sig = sign_request(secret, node_id, timestamp);
        assert!(verify_request(secret, node_id, timestamp, &sig).is_ok());
    }

    #[test]
    fn reject_expired_timestamp() {
        let secret = b"test-secret-key";
        let node_id = "node-01";
        let old_timestamp = 1_000_000; // way in the past

        let sig = sign_request(secret, node_id, old_timestamp);
        let result = verify_request(secret, node_id, old_timestamp, &sig);
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

        let result = verify_request(secret, node_id, timestamp, "deadbeef");
        assert!(matches!(result, Err(AuthError::InvalidSignature)));
    }
}
