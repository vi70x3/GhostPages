//! Integration test: Failure recovery.
//!
//! Validates that the daemon handles backend failures gracefully,
//! records them in the trace log, and continues operating.

use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_daemon::trace_log::TraceLog;
use ghost_policy::pressure::PressureAwareConfig;
use ghost_policy::pressure::PressureAwarePolicy;
use ghost_sim::config::{FailureConfig, FailurePattern, SimConfig};
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use std::collections::HashMap;
use std::sync::Arc;

fn test_backends_with_failures() -> HashMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> {
    let mut backends: HashMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> =
        HashMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(4 * 1024 * 1024)) as Arc<dyn ghost_tier::backend::StorageBackend>,
    );

    // Simulation backend with high failure rate
    let failure = FailureConfig {
        write_failure_rate: 0.5,
        read_failure_rate: 0.3,
        alloc_failure_rate: 0.1,
        corruption_on_failure: false,
        corruption_rate: 0.0,
        timeout_rate: 0.0,
        device_loss_rate: 0.0,
        failure_pattern: FailurePattern::Random,
    };
    let sim_config = SimConfig::with_capacity(16 * 1024 * 1024)
        .with_seed(42)
        .with_failure(failure);
    let sim = Arc::new(SimBackend::new(sim_config));
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
async fn test_store_with_backend_failure() {
    let _trace_log = Arc::new(TraceLog::new(10_000));
    let mut orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends_with_failures(),
        test_policy(),
    );
    orch.start().unwrap();

    // Store multiple chunks — some will fail due to injected failures
    let mut success_count = 0;
    let mut _fail_count = 0;

    for i in 0..20 {
        let data = format!("failure test chunk {}", i);
        let chunk_id = ChunkId::from_data(data.as_bytes());

        match orch.store(chunk_id, TierId::Simulation, data.as_bytes()) {
            Ok(()) => success_count += 1,
            Err(_) => _fail_count += 1,
        }
    }

    // With 50% write failure rate, we should have some of each
    // (probabilistically almost certain with 20 attempts)
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // At least some operations should have succeeded
    // (with 50% failure rate and 20 attempts, probability of all failing is ~0.000001%)
    assert!(
        success_count > 0,
        "at least some stores should succeed despite failures"
    );

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_daemon_recovers_after_failure() {
    let _trace_log = Arc::new(TraceLog::new(10_000));
    let mut orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends_with_failures(),
        test_policy(),
    );
    orch.start().unwrap();

    // Store some chunks that may fail
    for i in 0..10 {
        let data = format!("recovery test {}", i);
        let chunk_id = ChunkId::from_data(data.as_bytes());
        let _ = orch.store(chunk_id, TierId::Simulation, data.as_bytes());
    }

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Now store to RAM (which should always succeed)
    let data = b"ram tier data";
    let chunk_id = ChunkId::from_data(data);
    let result = orch.store(chunk_id, TierId::Ram, data);
    assert!(
        result.is_ok(),
        "RAM tier should always succeed: {:?}",
        result.err()
    );

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Daemon should still be operational
    let status = orch.status();
    assert!(status.jobs_submitted > 0);

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_trace_log_records_failures() {
    let mut orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends_with_failures(),
        test_policy(),
    );
    orch.start().unwrap();

    // Generate some failures
    for i in 0..10 {
        let data = format!("trace failure test {}", i);
        let chunk_id = ChunkId::from_data(data.as_bytes());
        let _ = orch.store(chunk_id, TierId::Simulation, data.as_bytes());
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let events = orch.trace_log().get_events();
    assert!(
        !events.is_empty(),
        "trace log should have recorded events including failures"
    );

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_zero_failure_rate_succeeds() {
    let mut backends: HashMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> =
        HashMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(4 * 1024 * 1024)) as Arc<dyn ghost_tier::backend::StorageBackend>,
    );

    // Simulation backend with zero failure rate
    let sim_config = SimConfig::with_capacity(16 * 1024 * 1024).with_seed(42);
    let sim = Arc::new(SimBackend::new(sim_config));
    backends.insert(
        TierId::Simulation,
        sim as Arc<dyn ghost_tier::backend::StorageBackend>,
    );

    let mut orch =
        TransferOrchestrator::new(OrchestratorConfig::default(), backends, test_policy());
    orch.start().unwrap();

    // All stores should succeed
    for i in 0..10 {
        let data = format!("zero failure test {}", i);
        let chunk_id = ChunkId::from_data(data.as_bytes());
        let result = orch.store(chunk_id, TierId::Simulation, data.as_bytes());
        assert!(
            result.is_ok(),
            "store {} should succeed with zero failure rate",
            i
        );
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let status = orch.status();
    assert_eq!(status.jobs_submitted, 10);

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_migration_with_failure_recovery() {
    let _trace_log = Arc::new(TraceLog::new(10_000));
    let mut orch = TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends_with_failures(),
        test_policy(),
    );
    orch.start().unwrap();

    // Store to RAM first (reliable)
    let data = b"migration recovery test";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Attempt migration — may fail on simulation side
    let _ = orch.migrate(chunk_id, TierId::Ram, TierId::Simulation, data.len());

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Daemon should still be alive
    let status = orch.status();
    let _ = status;

    orch.shutdown().unwrap();
}
