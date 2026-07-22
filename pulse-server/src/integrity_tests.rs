#[cfg(test)]
mod tests {
    use crate::api;
    use crate::db::{NodeRepository, PostgresNodeRepository};
    use crate::grpc::MonitoringServiceImpl;
    use crate::state::AppState;
    use axum::{Router, routing::get};
    use pulse_core::proto::telemetry_message::Payload as ClientPayload;
    use pulse_core::proto::{NodeStats, TelemetryMessage};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tokio_util::sync::CancellationToken;
    use tonic::Request;
    use tonic::transport::Server as TonicServer;

    #[tokio::test]
    async fn test_end_to_end_integrity() {
        // 1. Setup secrets and test DB url
        let secret = "testsecret-64-character-hex-string-for-integrity-test-321";
        unsafe {
            std::env::set_var("PULSE_AUTH_SECRET", secret);
        }
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://pulse:password@localhost:5432/pulse".to_string());

        // 2. Connect to database and clear test node data (skip if offline/unreachable)
        let pool = match tokio::time::timeout(
            Duration::from_millis(500),
            sqlx::PgPool::connect(&database_url)
        ).await {
            Ok(Ok(p)) => p,
            _ => {
                eprintln!("skipping E2E integrity test: database unreachable");
                return;
            }
        };
        let repo = PostgresNodeRepository::new(pool.clone());

        // Clean database records for "node-integrity-test"
        let _ = sqlx::query("DELETE FROM snapshots WHERE node_id = $1")
            .bind("node-integrity-test")
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM probe_results WHERE node_id = $1")
            .bind("node-integrity-test")
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM nodes WHERE id = $1")
            .bind("node-integrity-test")
            .execute(&pool)
            .await;

        // 3. Initialize Server state and Bounded Ingestion Queue (single worker for test)
        let (ingestion_tx, mut ingestion_rx) = tokio::sync::mpsc::channel(100);
        let worker_txs = std::sync::Arc::new(vec![ingestion_tx]);
        let app_state = AppState::new(pool.clone(), worker_txs);

        // Spawn ingestion workers (1 worker is enough for testing)
        let repo_worker = repo.clone();
        let worker_handle = tokio::spawn(async move {
            while let Some(item) = ingestion_rx.recv().await {
                if let crate::state::IngestionItem::Stats {
                    node_id,
                    timestamp_ms,
                    stats_str,
                    stats_json: _,
                } = item
                {
                    repo_worker
                        .upsert_node(&node_id, &node_id, "online")
                        .await
                        .unwrap();
                    repo_worker
                        .insert_snapshot(&node_id, timestamp_ms, &stats_str)
                        .await
                        .unwrap();
                }
            }
        });

        // 4. Start HTTP Server on a random free port
        let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let http_app = Router::new()
            .route("/api/v1/nodes", get(api::list_nodes))
            .with_state(app_state.clone());
        let cancel_token = CancellationToken::new();
        let cancel_token_http = cancel_token.clone();
        let http_handle = tokio::spawn(async move {
            axum::serve(http_listener, http_app)
                .with_graceful_shutdown(async move {
                    cancel_token_http.cancelled().await;
                })
                .await
                .unwrap();
        });

        // 5. Start gRPC Server on a random free port
        let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grpc_addr = grpc_listener.local_addr().unwrap();
        let grpc_incoming = TcpListenerStream::new(grpc_listener);

        let monitoring_service = MonitoringServiceImpl::new(app_state.clone());
        let secret_bytes = secret.as_bytes().to_vec();
        let auth_interceptor = move |req: tonic::Request<()>| {
            let mut interceptor = pulse_core::auth::AuthInterceptor::new(secret_bytes.clone());
            use tonic::service::Interceptor;
            interceptor.call(req)
        };

        let grpc_cancel = cancel_token.clone();
        let grpc_handle = tokio::spawn(async move {
            TonicServer::builder()
                .add_service(
                    pulse_core::proto::monitoring_service_server::MonitoringServiceServer::
                        with_interceptor(monitoring_service, auth_interceptor),
                )
                .serve_with_incoming_shutdown(grpc_incoming, grpc_cancel.cancelled())
                .await
                .unwrap();
        });

        // 6. Connect Agent (gRPC Client) — use inject_auth_headers for proper nonce generation
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", grpc_addr))
            .unwrap()
            .connect()
            .await
            .unwrap();

        let node_id_str = "node-integrity-test".to_string();
        let secret_for_client = secret.as_bytes().to_vec();

        let mut client =
            pulse_core::proto::monitoring_service_client::MonitoringServiceClient::with_interceptor(
                channel,
                move |req: Request<()>| {
                    pulse_core::auth::inject_auth_headers(&secret_for_client, &node_id_str, req)
                },
            );

        // 7. Send Telemetry stats
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let test_node_id = "node-integrity-test";
        let stats_payload = TelemetryMessage {
            payload: Some(ClientPayload::Stats(NodeStats {
                node_id: test_node_id.to_string(),
                timestamp_ms: now_ms,
                cpu_percent: 42.42,
                mem_total: 1000,
                mem_used: 500,
                mem_free: 500,
                mem_available: 500,
                swap_total: 0,
                swap_used: 0,
                disk_read_bytes_sec: 0,
                disk_write_bytes_sec: 0,
                tcp_connections: 5,
                udp_connections: 2,
                uptime_seconds: 1234,
                cpu_per_core: Vec::new(),
                load_avg_1: 0.0,
                load_avg_5: 0.0,
                load_avg_15: 0.0,
                mem_buffers: 0,
                mem_cached: 0,
                net_interfaces: Vec::new(),
                temperatures: Vec::new(),
            })),
        };

        let (tx, rx) = mpsc::channel(1);
        tx.send(stats_payload).await.unwrap();
        drop(tx);

        let rx_stream = ReceiverStream::new(rx);
        let response = client.stream_telemetry(rx_stream).await.unwrap();
        let mut inbound_stream = response.into_inner();

        // Wait a little bit for gRPC stream connection and background ingestion worker
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Verify that the client is receiving something or connection was processed
        let _heartbeat_or_ack =
            tokio::time::timeout(Duration::from_secs(2), inbound_stream.message()).await;

        // 8. Query REST API with auth to verify integrity
        let client_http = reqwest::Client::new();
        let res_http = client_http
            .get(format!("http://{}/api/v1/nodes", http_addr))
            .header("Authorization", format!("Bearer {}", secret))
            .send()
            .await
            .unwrap();

        assert_eq!(res_http.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = res_http.json().await.unwrap();
        assert_eq!(body["count"], 1);
        let first_node = &body["nodes"][0];
        assert_eq!(first_node["node_id"], test_node_id);
        assert_eq!(first_node["status"], "online");
        assert_eq!(first_node["latest_stats"]["cpu_percent"], 42.42);

        // 9. Shutdown gracefully
        drop(inbound_stream);
        drop(client);
        cancel_token.cancel();
        let _ = http_handle.await;
        let _ = grpc_handle.await;
        let _ = worker_handle.await;
    }
}
