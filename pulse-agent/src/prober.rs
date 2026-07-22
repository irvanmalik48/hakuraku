//! Network availability prober.
//!
//! Performs TCP handshake time measurements using async sockets.
//! ICMP ping requires `CAP_NET_RAW` and is implemented as a best-effort
//! fallback — if unavailable, only TCP probing is used.

use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};

use pulse_core::error::ProbeError;
use pulse_core::proto::ProbeResult;

/// Measure TCP handshake latency to a host:port.
///
/// Returns a `ProbeResult` with the round-trip time or an error.
pub async fn tcp_probe(host: &str, port: u16, timeout_ms: u32) -> ProbeResult {
    let target = format!("{host}:{port}");
    let timeout = Duration::from_millis(timeout_ms as u64);

    match tcp_probe_inner(&target, timeout).await {
        Ok(latency_us) => ProbeResult {
            target,
            success: true,
            latency_us,
            error_message: String::new(),
        },
        Err(e) => ProbeResult {
            target,
            success: false,
            latency_us: 0,
            error_message: e.to_string(),
        },
    }
}

async fn tcp_probe_inner(target: &str, timeout: Duration) -> Result<u64, ProbeError> {
    // Resolve DNS first
    let addr = target
        .to_socket_addrs()
        .map_err(|e| ProbeError::DnsFailure {
            host: target.to_string(),
            detail: e.to_string(),
        })?
        .next()
        .ok_or_else(|| ProbeError::DnsFailure {
            host: target.to_string(),
            detail: "no addresses resolved".into(),
        })?;

    let start = Instant::now();

    tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| ProbeError::Timeout {
            target: target.to_string(),
            timeout_ms: timeout.as_millis() as u32,
        })?
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::ConnectionRefused {
                ProbeError::ConnectionRefused {
                    target: target.to_string(),
                }
            } else {
                ProbeError::Io(e)
            }
        })?;

    let elapsed = start.elapsed();
    Ok(elapsed.as_micros() as u64)
}

/// Probe multiple TCP targets concurrently.
pub async fn probe_tcp_targets(targets: &[(String, u16, u32)]) -> Vec<ProbeResult> {
    let mut handles = Vec::with_capacity(targets.len());

    for (host, port, timeout_ms) in targets {
        let host = host.clone();
        let port = *port;
        let timeout_ms = *timeout_ms;
        handles.push(tokio::spawn(async move {
            tcp_probe(&host, port, timeout_ms).await
        }));
    }

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(result) => results.push(result),
            Err(e) => results.push(ProbeResult {
                target: "unknown".into(),
                success: false,
                latency_us: 0,
                error_message: format!("task panicked: {e}"),
            }),
        }
    }

    results
}
