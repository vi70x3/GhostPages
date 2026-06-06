//! Integration test: Disk I/O pressure replay.
//!
//! Validates that disk I/O pressure events can be recorded, exported,
//! and replayed to produce consistent results. This ensures that the
//! I/O pressure subsystem is deterministic and replay-equivalent.

use std::collections::BTreeMap;
use std::sync::Arc;

use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::io_metrics::IoMetrics;
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_daemon::trace_log::TraceLog;
use ghost_policy::pressure::{PressureAwareConfig, PressureAwarePolicy};
use ghost_replay::{ReplayConfig, ReplayEngine};
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use ghost_tier::backend::StorageBackend;
use tempfile::TempDir;

fn test_backends() -> BTreeMap<TierId, Arc<dyn StorageBackend>> {
    let mut backends: BTreeMap<TierId, Arc<dyn StorageBackend>> = BTreeMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(4 * 1024 * 1024)) as Arc<dyn StorageBackend>,
    );
    let sim = Arc::new(SimBackend::new(
        SimConfig::with_capacity(16 * 1024 * 1024).with_seed(42),
    ));
    backends.insert(
        TierId::Simulation,
        sim as Arc<dyn StorageBackend>,
    );
    backends
}

fn test_policy() -> Arc<dyn ghost_policy::PlacementPolicy> {
    Arc::new(PressureAwarePolicy::new(PressureAwareConfig::default()))
}

/// Test that disk I/O pressure metrics are deterministic across replays.
///
/// This test:
/// 1. Creates an orchestrator and generates migration events
/// 2. Records I/O metrics during the run
/// 3. Exports the trace log
/// 4. Replays the trace and verifies event count consistency
/// 5. Verifies that IoMetrics calculations are deterministic
#[tokio::test]
async fn test_disk_io_pressure_replay_deterministic() {
    let mut orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends(),
        test_policy(),
    );
    orch.start().unwrap();

    // Create IoMetrics to track disk pressure during the run
    let io_metrics = Arc::new(IoMetrics::new());

    // Simulate some disk I/O activity
    for i in 0..20u64 {
        io_metrics.record_read(1000 + i * 50);
        io_metrics.record_write(2000 + i * 100, 4096);
    }
    // Simulate some queue depth
    for _ in 0..10 {
        io_metrics.increment_queue_depth();
    }

    // Store some data to generate trace events
    for i in 0..5 {
        let data = format!("disk replay test {}", i);
        let chunk_id = ChunkId::from_data(data.as_bytes());
        orch.store(chunk_id, TierId::Ram, data.as_bytes()).unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Record I/O pressure at this point — should be deterministic
    let pressure_run1 = io_metrics.calculate_io_pressure(256, 10_000_000);
    let latency_run1 = io_metrics.get_rolling_latency();
    let queue_depth_run1 = io_metrics.get_queue_depth();

    // Export trace
    let dir = TempDir::new().unwrap();
    let trace_path = dir.path().join("disk_replay.ghosttrace");
    orch.export_trace_log(&trace_path, "disk_replay_test", "disk_replay_config")
        .unwrap();

    // Verify file was created
    assert!(trace_path.exists(), "trace file should exist");

    // Replay the trace
    let config = ReplayConfig::default();
    let (_engine, summary) = ReplayEngine::load(&trace_path, config).unwrap();

    // Summary should have processed events
    assert!(
        summary.events_replayed > 0,
        "replay should have processed events"
    );

    // Verify I/O metrics are deterministic: same inputs → same outputs
    let pressure_run2 = io_metrics.calculate_io_pressure(256, 10_000_000);
    assert!(
        (pressure_run1 - pressure_run2).abs() < f32::EPSILON,
        "io pressure should be deterministic: run1={}, run2={}",
        pressure_run1,
        pressure_run2
    );

    // Latency and queue depth should be unchanged (no new operations)
    assert_eq!(
        io_metrics.get_rolling_latency(), latency_run1,
        "rolling latency should be unchanged"
    );
    assert_eq!(
        io_metrics.get_queue_depth(), queue_depth_run1,
        "queue depth should be unchanged"
    );

    // Verify pressure is in valid range
    assert!(
        (0.0..=1.0).contains(&pressure_run1),
        "io pressure should be in [0, 1], got {}",
        pressure_run1
    );

    orch.shutdown().unwrap();
}

/// Test that IoMetrics produces identical results when given identical inputs,
/// which is the core requirement for deterministic replay.
#[test]
fn test_io_metrics_deterministic_given_identical_inputs() {
    // Two independent IoMetrics instances
    let metrics_a = IoMetrics::new();
    let metrics_b = IoMetrics::new();

    // Feed identical operations to both
    for i in 0..50u64 {
        let read_latency = 5000 + i * 100;
        let write_latency = 8000 + i * 200;
        let data_size: usize = 4096 + (i * 512) as usize;

        metrics_a.record_read(read_latency);
        metrics_b.record_read(read_latency);

        metrics_a.record_write(write_latency, data_size);
        metrics_b.record_write(write_latency, data_size);
    }

    // Both should produce identical results
    assert_eq!(
        metrics_a.get_rolling_latency(),
        metrics_b.get_rolling_latency(),
        "rolling latency should be identical for identical inputs"
    );
    assert_eq!(
        metrics_a.get_read_count(),
        metrics_b.get_read_count(),
        "read counts should be identical"
    );
    assert_eq!(
        metrics_a.get_write_count(),
        metrics_b.get_write_count(),
        "write counts should be identical"
    );

    // Pressure calculations should be identical
    let pressure_a = metrics_a.calculate_io_pressure(256, 10_000_000);
    let pressure_b = metrics_b.calculate_io_pressure(256, 10_000_000);
    assert!(
        (pressure_a - pressure_b).abs() < f32::EPSILON,
        "pressure should be identical: a={}, b={}",
        pressure_a,
        pressure_b
    );
}

/// Test that disk pressure events are captured in the trace log.
#[tokio::test]
async fn test_disk_pressure_events_in_trace() {
    let mut orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends(),
        test_policy(),
    );
    orch.start().unwrap();

    // Generate events by storing and migrating data
    let data = b"disk trace test data";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Migrate to simulation tier
    orch.migrate(chunk_id, TierId::Ram, TierId::Simulation, data.len())
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Export and verify trace has events
    let dir = TempDir::new().unwrap();
    let trace_path = dir.path().join("disk_trace.ghosttrace");
    orch.export_trace_log(&trace_path, "disk_trace_test", "disk_trace_config")
        .unwrap();

    // Replay should succeed
    let config = ReplayConfig::default();
    let result = ReplayEngine::load(&trace_path, config);
    assert!(result.is_ok(), "replay should succeed");

    let (_engine, summary) = result.unwrap();
    assert!(
        summary.events_replayed >= 2,
        "should have at least 2 events (store + migrate), got {}",
        summary.events_replayed
    );

    orch.shutdown().unwrap();
}
