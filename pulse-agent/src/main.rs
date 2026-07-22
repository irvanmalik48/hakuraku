//! `pulse-agent` — Ultra-lightweight system monitoring daemon.
//!
//! Collects system telemetry from Linux `/proc` and `/sys` filesystems and
//! streams it to a `pulse-server` instance via gRPC bidirectional streaming.

mod buffer;
mod collector;
mod prober;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, Notify};
use tonic::transport::Channel;
use tracing::{info, warn};

use pulse_core::auth::inject_auth_headers;
use pulse_core::proto::monitoring_service_client::MonitoringServiceClient;
use pulse_core::proto::telemetry_message::Payload;
use pulse_core::proto::{Heartbeat, TelemetryMessage};

use crate::buffer::TelemetryBuffer;
use crate::collector::SystemCollector;

/// Agent configuration loaded from environment variables.
struct Config {
    server_addr: String,
    auth_secret: Vec<u8>,
    node_id: String,
    interval: Duration,
}

impl Config {
    fn from_env() -> Result<Self> {
        let server_addr = std::env::var("PULSE_SERVER_ADDR")
            .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
        let auth_secret = std::env::var("PULSE_AUTH_SECRET")
            .context("PULSE_AUTH_SECRET must be set")?
            .into_bytes();
        let node_id =
            std::env::var("PULSE_NODE_ID").unwrap_or_else(|_| "unknown".to_string());
        let interval_ms: u64 = std::env::var("PULSE_INTERVAL_MS")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .context("PULSE_INTERVAL_MS must be a valid u64")?;

        Ok(Self {
            server_addr,
            auth_secret,
            node_id,
            interval: Duration::from_millis(interval_ms),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pulse_agent=info".parse().unwrap()),
        )
        .compact()
        .init();

    let config = Config::from_env()?;
    info!(
        node_id = %config.node_id,
        server = %config.server_addr,
        interval_ms = config.interval.as_millis() as u64,
        "pulse-agent starting"
    );

    // Run the agent loop with reconnection backoff
    run_agent_loop(config).await
}

/// Main agent loop with exponential backoff reconnection.
async fn run_agent_loop(config: Config) -> Result<()> {
    let mut backoff = ExponentialBackoff::new();
    let holdback = Arc::new(Mutex::new(TelemetryBuffer::new(100)));
    let notify = Arc::new(Notify::new());

    // Spawn the metric collection task
    let collector_handle = {
        let node_id = config.node_id.clone();
        let interval = config.interval;
        let holdback = holdback.clone();
        let notify = notify.clone();
        tokio::spawn(async move {
            collection_loop(node_id, interval, holdback, notify).await;
        })
    };

    let result = async {
        loop {
            match run_session(&config, &holdback, &notify).await {
                Ok(()) => {
                    info!("session ended cleanly, reconnecting...");
                    backoff.reset();
                }
                Err(e) => {
                    let delay = backoff.next_delay();
                    warn!(
                        error = %e,
                        retry_in_ms = delay.as_millis() as u64,
                        "session failed, reconnecting after backoff"
                    );
                    tokio::time::sleep(delay).await;
                }
            }

            // Check for shutdown signal between reconnection attempts
            if tokio::signal::ctrl_c().now_or_never().is_some() {
                info!("shutdown signal received, exiting");
                break;
            }
        }
        Ok(())
    }
    .await;

    collector_handle.abort();
    result
}

/// Run a single streaming session to the server.
async fn run_session(
    config: &Config,
    holdback: &Arc<Mutex<TelemetryBuffer>>,
    notify: &Arc<Notify>,
) -> Result<()> {
    // Connect to server
    let channel = Channel::from_shared(config.server_addr.clone())?
        .connect()
        .await
        .context("failed to connect to pulse-server")?;

    let secret = config.auth_secret.clone();
    let node_id_for_auth = config.node_id.clone();

    let mut client = MonitoringServiceClient::with_interceptor(channel, move |req| {
        inject_auth_headers(&secret, &node_id_for_auth, req)
    });

    info!("connected to server");

    // Channel for collector/heartbeat/probes → gRPC sender
    let (tx, rx) = mpsc::channel::<TelemetryMessage>(64);

    // Spawn task to forward buffered and new stats to the server
    let forwarder_handle = {
        let holdback = holdback.clone();
        let notify = notify.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                // Drain any existing messages in the buffer first
                let messages: Vec<TelemetryMessage> = {
                    let mut buf = holdback.lock().unwrap();
                    if !buf.is_empty() {
                        let len = buf.len();
                        tracing::info!("draining {} buffered telemetry messages", len);
                    }
                    buf.drain().collect()
                };

                for msg in messages {
                    if tx.send(msg).await.is_err() {
                        return;
                    }
                }

                // Wait for new messages
                notify.notified().await;
            }
        })
    };

    // Spawn heartbeat task
    let heartbeat_handle = {
        let node_id = config.node_id.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            heartbeat_loop(node_id, tx).await;
        })
    };

    // Convert rx into a stream for gRPC
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);

    // Start the bidirectional stream
    let response = client
        .stream_telemetry(outbound)
        .await
        .context("failed to establish telemetry stream")?;

    let mut inbound = response.into_inner();

    // Process incoming server commands
    while let Some(cmd) = inbound
        .message()
        .await
        .context("error reading server command")?
    {
        if let Some(payload) = cmd.payload {
            match payload {
                pulse_core::proto::server_command::Payload::HeartbeatAck(ack) => {
                    tracing::debug!(
                        server_time = ack.server_timestamp_ms,
                        "heartbeat acknowledged"
                    );
                }
                pulse_core::proto::server_command::Payload::ConfigUpdate(update) => {
                    info!(
                        interval_ms = update.interval_ms,
                        "received config update (dynamic interval change not yet implemented)"
                    );
                }
                pulse_core::proto::server_command::Payload::AddTcpProbe(probe) => {
                    info!(
                        host = %probe.host,
                        port = probe.port,
                        "received TCP probe command"
                    );
                    let tx_clone = tx.clone();
                    tokio::spawn(async move {
                        let results = prober::probe_tcp_targets(&[(probe.host.clone(), probe.port as u16, probe.timeout_ms)]).await;
                        if let Some(result) = results.into_iter().next() {
                            let msg = TelemetryMessage {
                                payload: Some(Payload::ProbeResult(result)),
                            };
                            if let Err(e) = tx_clone.send(msg).await {
                                tracing::error!(error = %e, "failed to send probe result to telemetry channel");
                            }
                        }
                    });
                }
                _ => {
                    tracing::debug!("received unhandled server command");
                }
            }
        }
    }

    // Clean up spawned tasks
    forwarder_handle.abort();
    heartbeat_handle.abort();

    Ok(())
}

/// Continuously collect system metrics and push them to the holdback buffer.
async fn collection_loop(
    node_id: String,
    interval: Duration,
    holdback: Arc<Mutex<TelemetryBuffer>>,
    notify: Arc<Notify>,
) {
    let mut collector = SystemCollector::new();
    let mut ticker = tokio::time::interval(interval);
    // Don't burst-catch-up if we fall behind
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        match collector.collect(&node_id) {
            Ok(stats) => {
                let msg = TelemetryMessage {
                    payload: Some(Payload::Stats(stats)),
                };
                {
                    let mut buf = holdback.lock().unwrap();
                    buf.push(msg);
                }
                notify.notify_one();
            }
            Err(e) => {
                warn!(error = %e, "metric collection failed");
            }
        }
    }
}

/// Send periodic heartbeats to keep the stream alive.
async fn heartbeat_loop(node_id: String, tx: mpsc::Sender<TelemetryMessage>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(15));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let msg = TelemetryMessage {
            payload: Some(Payload::Heartbeat(Heartbeat {
                node_id: node_id.clone(),
                timestamp_ms: now,
            })),
        };

        if tx.send(msg).await.is_err() {
            return;
        }
    }
}

/// Exponential backoff with jitter for reconnection.
struct ExponentialBackoff {
    current: Duration,
    base: Duration,
    max: Duration,
}

impl ExponentialBackoff {
    fn new() -> Self {
        Self {
            current: Duration::from_secs(1),
            base: Duration::from_secs(1),
            max: Duration::from_secs(60),
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        // Double the backoff, capped at max
        self.current = (self.current * 2).min(self.max);
        // Add ±30% jitter
        let jitter_range = delay.as_millis() as f64 * 0.3;
        let jitter = (rand_simple() * 2.0 - 1.0) * jitter_range;
        Duration::from_millis((delay.as_millis() as f64 + jitter).max(100.0) as u64)
    }

    fn reset(&mut self) {
        self.current = self.base;
    }
}

/// Simple pseudo-random float [0.0, 1.0) using system time nanos.
/// Avoids pulling in a full `rand` crate dependency for the agent binary.
fn rand_simple() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos as f64) / (u32::MAX as f64)
}

/// Extension trait to check if a future resolves immediately.
trait NowOrNever {
    type Output;
    fn now_or_never(self) -> Option<Self::Output>;
}

impl<F: std::future::Future> NowOrNever for F {
    type Output = F::Output;
    fn now_or_never(self) -> Option<Self::Output> {
        let waker = futures_noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut pinned = std::pin::pin!(self);
        match pinned.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => Some(v),
            std::task::Poll::Pending => None,
        }
    }
}

fn futures_noop_waker() -> std::task::Waker {
    use std::task::{RawWaker, RawWakerVTable};
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |p| RawWaker::new(p, &VTABLE),
        |_| {},
        |_| {},
        |_| {},
    );
    // SAFETY: The no-op waker does nothing and is always valid.
    unsafe { std::task::Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}
