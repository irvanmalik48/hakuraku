#![allow(dead_code)]

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use std::time::Instant;

#[path = "../db.rs"]
mod db;
#[path = "../state.rs"]
mod state;

use db::{NodeRepository, PostgresNodeRepository};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hakuraku Database Benchmarking Utility");
    println!("======================================");

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://pulse:password@localhost:5432/pulse".to_string());

    println!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .connect(&database_url)
        .await
        .context("failed to connect to PostgreSQL database")?;

    let repo = PostgresNodeRepository::new(pool);
    println!("Running database migrations...");
    repo.migrate().await.context("failed to run migrations")?;

    // Seed dummy node
    repo.upsert_node("bench-node-01", "bench-host", "online")
        .await
        .context("failed to seed node")?;

    let total_inserts = 20_000;
    let concurrency = 20;
    let inserts_per_task = total_inserts / concurrency;

    println!("\nStarting Insertion Benchmark:");
    println!("- Total snapshots: {}", total_inserts);
    println!("- Concurrency (tasks): {}", concurrency);
    println!("- Inserts per task: {}", inserts_per_task);

    let start = Instant::now();
    let mut handles = Vec::new();

    for i in 0..concurrency {
        let repo_clone = repo.clone();
        let task_id = i;
        handles.push(tokio::spawn(async move {
            let stats_json =
                r#"{"cpu_percent": 15.4, "mem_used": 4294967296, "mem_total": 16777216000}"#;
            for j in 0..inserts_per_task {
                let timestamp = 1721630000000 + (task_id * inserts_per_task + j) as i64 * 1000;
                repo_clone
                    .insert_snapshot("bench-node-01", timestamp, stats_json)
                    .await
                    .expect("failed to insert snapshot");
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }

    let elapsed = start.elapsed();
    let throughput = total_inserts as f64 / elapsed.as_secs_f64();
    let avg_latency_ms = (elapsed.as_millis() as f64) / total_inserts as f64;

    println!("\nInsertion Results:");
    println!("- Elapsed time: {:.3}s", elapsed.as_secs_f64());
    println!("- Throughput:   {:.2} inserts/sec", throughput);
    println!("- Avg Latency:  {:.3}ms per insert", avg_latency_ms);

    // ── Query Latency Benchmark ──────────────────────────────────────────────
    println!("\nStarting Query Latency Benchmark:");
    let total_queries = 2000;
    println!("- Total queries: {}", total_queries);

    let query_start = Instant::now();
    let mut latencies = Vec::with_capacity(total_queries);

    for _ in 0..total_queries {
        let single_query_start = Instant::now();
        let snapshots = repo
            .get_snapshots("bench-node-01", 1721630000000, 1721630000000 + 1000000, 100)
            .await
            .expect("failed to retrieve snapshots");
        assert!(!snapshots.is_empty());
        latencies.push(single_query_start.elapsed().as_secs_f64() * 1000.0); // ms
    }

    let query_elapsed = query_start.elapsed();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50 = latencies[total_queries / 2];
    let p90 = latencies[(total_queries as f64 * 0.90) as usize];
    let p99 = latencies[(total_queries as f64 * 0.99) as usize];
    let query_throughput = total_queries as f64 / query_elapsed.as_secs_f64();

    println!("\nQuery Results (100 rows per query):");
    println!("- Elapsed time: {:.3}s", query_elapsed.as_secs_f64());
    println!("- Throughput:   {:.2} queries/sec", query_throughput);
    println!("- Latencies (percentiles):");
    println!("  - p50 (Median): {:.3}ms", p50);
    println!("  - p90:          {:.3}ms", p90);
    println!("  - p99:          {:.3}ms", p99);

    Ok(())
}
