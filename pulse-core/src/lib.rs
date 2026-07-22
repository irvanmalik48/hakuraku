//! `pulse-core` — Shared types, protobuf definitions, and auth utilities.
//!
//! This crate provides:
//! - Generated protobuf/gRPC types from `telemetry.proto`
//! - HMAC-SHA256 authentication utilities for agent ↔ server communication
//! - Shared error types

pub mod auth;
pub mod error;

/// Generated protobuf types and gRPC service traits.
pub mod proto {
    tonic::include_proto!("pulse");
}

// Re-export commonly used types at crate root for ergonomics.
pub use proto::{
    monitoring_service_client::MonitoringServiceClient,
    monitoring_service_server::{MonitoringService, MonitoringServiceServer},
    ConfigUpdate, Heartbeat, HeartbeatAck, NetworkInterface, NodeStats, PingTarget, ProbeResult,
    ServerCommand, TcpProbe, TelemetryMessage, TemperatureSensor,
};
