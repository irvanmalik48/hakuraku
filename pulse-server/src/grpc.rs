//! gRPC service implementation for agent telemetry ingestion.

use std::pin::Pin;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

use pulse_core::proto::monitoring_service_server::MonitoringService;
use pulse_core::proto::server_command::Payload as ServerPayload;
use pulse_core::proto::telemetry_message::Payload as ClientPayload;
use pulse_core::proto::{HeartbeatAck, ServerCommand, TelemetryMessage};

use crate::state::{AppState, NodeInfo, NodeStatus, NodeUpdate};

/// gRPC implementation of the `MonitoringService`.
pub struct MonitoringServiceImpl {
    state: AppState,
}

impl MonitoringServiceImpl {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl MonitoringService for MonitoringServiceImpl {
    type StreamTelemetryStream = Pin<Box<dyn Stream<Item = Result<ServerCommand, Status>> + Send>>;

    async fn stream_telemetry(
        &self,
        request: Request<Streaming<TelemetryMessage>>,
    ) -> Result<Response<Self::StreamTelemetryStream>, Status> {
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        info!(remote = %remote_addr, "new agent stream connected");

        let authenticated_node_id = match request.metadata().get("x-pulse-node-id") {
            Some(val) => match val.to_str() {
                Ok(s) => s.to_string(),
                Err(_) => return Err(Status::unauthenticated("invalid x-pulse-node-id header")),
            },
            None => return Err(Status::unauthenticated("missing x-pulse-node-id header")),
        };

        let mut inbound = request.into_inner();
        let state = self.state.clone();

        // Channel for sending commands back to the agent
        let (cmd_tx, cmd_rx) = mpsc::channel::<Result<ServerCommand, Status>>(32);

        // Spawn a task to process incoming telemetry
        let auth_node_id = authenticated_node_id.clone();
        tokio::spawn(async move {
            while let Some(msg_result) = inbound.next().await {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(error = %e, node_id = %auth_node_id, "error receiving telemetry message");
                        break;
                    }
                };

                let Some(payload) = msg.payload else {
                    continue;
                };

                match payload {
                    ClientPayload::Stats(stats) => {
                        if stats.node_id != auth_node_id {
                            error!(
                                auth_node = %auth_node_id,
                                payload_node = %stats.node_id,
                                "Node ID spoofing detected in stats payload! Closing stream."
                            );
                            crate::metrics::AUTH_FAILURES
                                .with_label_values(&["grpc", "node_id_spoofing"])
                                .inc();
                            break;
                        }

                        crate::metrics::GRPC_MESSAGES
                            .with_label_values(&[&auth_node_id, "stats"])
                            .inc();
                        let timestamp_ms = stats.timestamp_ms;

                        // Serialize stats to JSON for storage and broadcast
                        let stats_json = match serde_json::to_value(StatsSerializable::from(&stats))
                        {
                            Ok(v) => v,
                            Err(e) => {
                                error!(error = %e, "failed to serialize stats");
                                continue;
                            }
                        };
                        let stats_str = stats_json.to_string();

                        // Update in-memory node registry
                        state.nodes.insert(
                            auth_node_id.clone(),
                            NodeInfo {
                                node_id: auth_node_id.clone(),
                                hostname: auth_node_id.clone(),
                                last_seen_ms: timestamp_ms,
                                status: NodeStatus::Online,
                                latest_stats: Some(stats_json.clone()),
                            },
                        );

                        // Queue to database bounded channel (backpressure drop if full)
                        let item = crate::state::IngestionItem::Stats {
                            node_id: auth_node_id.clone(),
                            timestamp_ms,
                            stats_str,
                            stats_json: stats_json.clone(),
                        };
                        state.send_to_worker(&auth_node_id, item);

                        // Broadcast to WebSocket subscribers
                        let _ = state.broadcast_tx.send(NodeUpdate {
                            node_id: auth_node_id.clone(),
                            timestamp_ms,
                            stats: stats_json,
                        });
                    }

                    ClientPayload::Heartbeat(hb) => {
                        if hb.node_id != auth_node_id {
                            error!(
                                auth_node = %auth_node_id,
                                payload_node = %hb.node_id,
                                "Node ID spoofing detected in heartbeat payload! Closing stream."
                            );
                            crate::metrics::AUTH_FAILURES
                                .with_label_values(&["grpc", "node_id_spoofing"])
                                .inc();
                            break;
                        }

                        crate::metrics::GRPC_MESSAGES
                            .with_label_values(&[&auth_node_id, "heartbeat"])
                            .inc();

                        // Update last_seen in registry
                        if let Some(mut entry) = state.nodes.get_mut(&auth_node_id) {
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
                        crate::metrics::GRPC_MESSAGES
                            .with_label_values(&[&auth_node_id, "probe_result"])
                            .inc();

                        // Persist to database bounded channel
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;

                        let item = crate::state::IngestionItem::ProbeResult {
                            node_id: auth_node_id.clone(),
                            target: probe.target.clone(),
                            success: probe.success,
                            latency_us: probe.latency_us as i64,
                            error_message: probe.error_message.clone(),
                            timestamp: now_ms,
                        };
                        state.send_to_worker(&auth_node_id, item);
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
