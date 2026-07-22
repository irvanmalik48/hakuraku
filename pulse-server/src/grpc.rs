//! gRPC service implementation for agent telemetry ingestion.

use std::pin::Pin;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

use pulse_core::proto::monitoring_service_server::MonitoringService;
use pulse_core::proto::server_command::Payload as ServerPayload;
use pulse_core::proto::telemetry_message::Payload as ClientPayload;
use pulse_core::proto::{HeartbeatAck, ServerCommand, TelemetryMessage};

use crate::db::{NodeRepository, SqliteNodeRepository};
use crate::state::{AppState, NodeInfo, NodeStatus, NodeUpdate};

/// gRPC implementation of the `MonitoringService`.
pub struct MonitoringServiceImpl {
    state: AppState,
    repo: SqliteNodeRepository,
}

impl MonitoringServiceImpl {
    pub fn new(state: AppState, repo: SqliteNodeRepository) -> Self {
        Self { state, repo }
    }
}

#[tonic::async_trait]
impl MonitoringService for MonitoringServiceImpl {
    type StreamTelemetryStream =
        Pin<Box<dyn Stream<Item = Result<ServerCommand, Status>> + Send>>;

    async fn stream_telemetry(
        &self,
        request: Request<Streaming<TelemetryMessage>>,
    ) -> Result<Response<Self::StreamTelemetryStream>, Status> {
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        info!(remote = %remote_addr, "new agent stream connected");

        let mut inbound = request.into_inner();
        let state = self.state.clone();
        let repo = self.repo.clone();

        // Channel for sending commands back to the agent
        let (cmd_tx, cmd_rx) = mpsc::channel::<Result<ServerCommand, Status>>(32);

        // Spawn a task to process incoming telemetry
        tokio::spawn(async move {
            while let Some(msg_result) = inbound.next().await {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(error = %e, "error receiving telemetry message");
                        break;
                    }
                };

                let Some(payload) = msg.payload else {
                    continue;
                };

                match payload {
                    ClientPayload::Stats(stats) => {
                        let node_id = stats.node_id.clone();
                        let timestamp_ms = stats.timestamp_ms;

                        // Serialize stats to JSON for storage and broadcast
                        let stats_json = match serde_json::to_value(StatsSerializable::from(&stats)) {
                            Ok(v) => v,
                            Err(e) => {
                                error!(error = %e, "failed to serialize stats");
                                continue;
                            }
                        };
                        let stats_str = stats_json.to_string();

                        // Update in-memory node registry
                        state.nodes.insert(
                            node_id.clone(),
                            NodeInfo {
                                node_id: node_id.clone(),
                                hostname: node_id.clone(), // Agent could send hostname in a registration message
                                last_seen_ms: timestamp_ms,
                                status: NodeStatus::Online,
                                latest_stats: Some(stats_json.clone()),
                            },
                        );

                        // Persist to database (fire-and-forget, don't block the stream)
                        let repo_clone = repo.clone();
                        let node_id_clone = node_id.clone();
                        tokio::spawn(async move {
                            if let Err(e) = repo_clone
                                .upsert_node(&node_id_clone, &node_id_clone, "online")
                                .await
                            {
                                error!(error = %e, "failed to upsert node");
                            }
                            if let Err(e) = repo_clone
                                .insert_snapshot(&node_id_clone, timestamp_ms, &stats_str)
                                .await
                            {
                                error!(error = %e, "failed to insert snapshot");
                            }
                        });

                        // Broadcast to WebSocket subscribers
                        let _ = state.broadcast_tx.send(NodeUpdate {
                            node_id,
                            timestamp_ms,
                            stats: stats_json,
                        });
                    }

                    ClientPayload::Heartbeat(hb) => {
                        tracing::debug!(
                            node_id = %hb.node_id,
                            "heartbeat received"
                        );

                        // Update last_seen in registry
                        if let Some(mut entry) = state.nodes.get_mut(&hb.node_id) {
                            entry.last_seen_ms = hb.timestamp_ms;
                            entry.status = NodeStatus::Online;
                        }

                        // Send heartbeat ack back to agent
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;
                        let ack = ServerCommand {
                            payload: Some(ServerPayload::HeartbeatAck(HeartbeatAck {
                                server_timestamp_ms: now_ms,
                            })),
                        };
                        if cmd_tx.send(Ok(ack)).await.is_err() {
                            break; // Client disconnected
                        }
                    }

                    ClientPayload::ProbeResult(probe) => {
                        tracing::debug!(
                            target = %probe.target,
                            success = probe.success,
                            latency_us = probe.latency_us,
                            "probe result received"
                        );
                        // Probe results can be stored or broadcast as needed
                    }
                }
            }

            info!(remote = %remote_addr, "agent stream disconnected");
        });

        // Return the command stream to the agent
        let output_stream = ReceiverStream::new(cmd_rx);
        Ok(Response::new(Box::pin(output_stream)))
    }
}

// ── Helper: Serializable stats wrapper ─────────────────────────────────────

/// Converts proto `NodeStats` to a serde-serializable struct for JSON storage.
#[derive(serde::Serialize)]
struct StatsSerializable {
    cpu_percent: f64,
    cpu_per_core: Vec<f64>,
    load_avg_1: f64,
    load_avg_5: f64,
    load_avg_15: f64,
    mem_total: u64,
    mem_used: u64,
    mem_free: u64,
    mem_available: u64,
    mem_buffers: u64,
    mem_cached: u64,
    swap_total: u64,
    swap_used: u64,
    disk_read_bytes_sec: u64,
    disk_write_bytes_sec: u64,
    net_interfaces: Vec<NetIfaceSerializable>,
    tcp_connections: u32,
    udp_connections: u32,
    uptime_seconds: u64,
    temperatures: Vec<TempSerializable>,
}

#[derive(serde::Serialize)]
struct NetIfaceSerializable {
    name: String,
    rx_bytes_sec: u64,
    tx_bytes_sec: u64,
    rx_bytes_total: u64,
    tx_bytes_total: u64,
}

#[derive(serde::Serialize)]
struct TempSerializable {
    label: String,
    temp_millicelsius: i32,
}

impl From<&pulse_core::proto::NodeStats> for StatsSerializable {
    fn from(s: &pulse_core::proto::NodeStats) -> Self {
        Self {
            cpu_percent: s.cpu_percent,
            cpu_per_core: s.cpu_per_core.clone(),
            load_avg_1: s.load_avg_1,
            load_avg_5: s.load_avg_5,
            load_avg_15: s.load_avg_15,
            mem_total: s.mem_total,
            mem_used: s.mem_used,
            mem_free: s.mem_free,
            mem_available: s.mem_available,
            mem_buffers: s.mem_buffers,
            mem_cached: s.mem_cached,
            swap_total: s.swap_total,
            swap_used: s.swap_used,
            disk_read_bytes_sec: s.disk_read_bytes_sec,
            disk_write_bytes_sec: s.disk_write_bytes_sec,
            net_interfaces: s
                .net_interfaces
                .iter()
                .map(|n| NetIfaceSerializable {
                    name: n.name.clone(),
                    rx_bytes_sec: n.rx_bytes_sec,
                    tx_bytes_sec: n.tx_bytes_sec,
                    rx_bytes_total: n.rx_bytes_total,
                    tx_bytes_total: n.tx_bytes_total,
                })
                .collect(),
            tcp_connections: s.tcp_connections,
            udp_connections: s.udp_connections,
            uptime_seconds: s.uptime_seconds,
            temperatures: s
                .temperatures
                .iter()
                .map(|t| TempSerializable {
                    label: t.label.clone(),
                    temp_millicelsius: t.temp_millicelsius,
                })
                .collect(),
        }
    }
}
