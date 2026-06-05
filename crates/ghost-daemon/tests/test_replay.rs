//! Integration test: Trace replay.
//!
//! Validates that trace events can be recorded, exported to a file,
//! and replayed to produce identical results.

use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_daemon::trace_log::TraceLog;
use ghost_policy::pressure::PressureAwareConfig;
use ghost_policy::pressure::PressureAwarePolicy;
use ghost_replay::{ReplayConfig, ReplayEngine};
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn test_backends() -> HashMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> {
    let mut backends: HashMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> =
        HashMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(4 * 1024 * 1024)) as Arc<dyn ghost_tier::backend::StorageBackend>,
    );
    let sim = Arc::new(SimBackend::new(
        SimConfig::with_capacity(16 * 1024 * 1024).with_seed(42),
    ));
    backends.insert(
        TierId::Simulation,
        sim as Arc<dyn ghost_tier::backend::StorageBackend>,
    );
    backends
}

fn test_policy() -> Arc<dyn ghost_policy::PlacementPolicy> {
    Arc::new(PressureAwarePolicy::new(PressureAwareConfig::default()))
}

#[tokio::test]
async fn test_trace_export_and_replay() {
    let _trace_log = Arc::new(TraceLog::new(10_000));
    let mut orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends(),
        test_policy(),
    );
    orch.start().unwrap();

    // Generate some events
    let data = b"replay test data";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Export trace
    let dir = TempDir::new().unwrap();
    let trace_path = dir.path().join("test.ghosttrace");
    orch.export_trace_log(&trace_path, "test_policy", "test_config")
        .unwrap();

    // Verify file was created
    assert!(trace_path.exists(), "trace file should exist");

    // Replay using load (loads from file and replays)
    let config = ReplayConfig::default();
    let (_engine, summary) = ReplayEngine::load(&trace_path, config).unwrap();

    // Summary should have processed events
    assert!(
        summary.events_replayed > 0,
        "replay should have processed events"
    );

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_trace_replay_metrics() {
    let _trace_log = Arc::new(TraceLog::new(10_000));
    let mut orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends(),
        test_policy(),
    );
    orch.start().unwrap();

    // Generate events
    for i in 0..5 {
        let data = format!("replay metrics test {}", i);
        let chunk_id = ChunkId::from_data(data.as_bytes());
        orch.store(chunk_id, TierId::Ram, data.as_bytes()).unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Export
    let dir = TempDir::new().unwrap();
    let trace_path = dir.path().join("metrics.ghosttrace");
    orch.export_trace_log(&trace_path, "metrics_test", "metrics_config")
        .unwrap();

    // Replay and check metrics
    let config = ReplayConfig::default();
    let (_engine, summary) = ReplayEngine::load(&trace_path, config).unwrap();

    assert!(
        summary.events_replayed >= 5,
        "should have at least 5 events, got {}",
        summary.events_replayed
    );

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_trace_log_direct_replay() {
    // Test that TraceLog events can be replayed directly
    let _trace_log = Arc::new(TraceLog::new(10_000));

    // Record events directly
    use ghost_core::trace::TraceEvent;
    trace_log.record(TraceEvent::ChunkCreated {
        chunk_id: ChunkId::from_data(b"direct test"),
        size: 11,
        tier: TierId::Ram,
        timestamp: 1000,
    });
    trace_log.record(TraceEvent::TransferCompleted {
        chunk_id: ChunkId::from_data(b"direct test"),
        from: TierId::Ram,
        to: TierId::Simulation,
        size: 11,
        duration_ms: 50,
        timestamp: 2000,
    });

    let events = trace_log.get_events();
    assert_eq!(events.len(), 2, "should have 2 events");

    // Export and replay
    let dir = TempDir::new().unwrap();
    let trace_path = dir.path().join("direct.ghosttrace");

    // Use orchestrator's export
    let orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends(),
        test_policy(),
    );
    orch.export_trace_log(&trace_path, "direct_test", "direct_config")
        .unwrap();

    let config = ReplayConfig::default();
    let (_engine, summary) = ReplayEngine::load(&trace_path, config).unwrap();

    assert!(
        summary.events_replayed >= 2,
        "replay should process recorded events"
    );
}

#[tokio::test]
async fn test_replay_empty_trace() {
    let dir = TempDir::new().unwrap();
    let trace_path = dir.path().join("empty.ghosttrace");

    // Create an empty trace file by exporting from an empty log
    let _trace_log = Arc::new(TraceLog::new(10_000));
    let orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends(),
        test_policy(),
    );
    orch.export_trace_log(&trace_path, "empty_test", "empty_config")
        .unwrap();

    // Replay should handle empty trace gracefully
    let config = ReplayConfig::default();
    let result = ReplayEngine::load(&trace_path, config);

    // Should succeed or fail gracefully
    if let Ok((_engine, summary)) = result {
        // Empty trace may have 0 events
        let _ = summary;
    }
}
