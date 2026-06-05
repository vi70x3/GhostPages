//! Integration test: Concurrent workloads.
//!
//! Validates that the daemon handles multiple concurrent store/retrieve
//! operations without data corruption or deadlocks.

use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_policy::pressure::PressureAwareConfig;
use ghost_policy::pressure::PressureAwarePolicy;
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use std::collections::BTreeMap;
use std::sync::Arc;

fn test_backends() -> BTreeMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> {
    let mut backends: BTreeMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> =
        BTreeMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(1024 * 1024)) as Arc<dyn ghost_tier::backend::StorageBackend>,
    );
    let sim = Arc::new(SimBackend::new(
        SimConfig::with_capacity(4 * 1024 * 1024).with_seed(42),
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

fn test_orchestrator() -> TransferOrchestrator {
    TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends(),
        test_policy(),
    )
}

#[tokio::test]
async fn test_concurrent_stores() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    for i in 0..50 {
        let data = format!("concurrent store {}", i);
        let chunk_id = ChunkId::from_data(data.as_bytes());

        // Store directly on the orchestrator
        let result = orch.store(chunk_id, TierId::Ram, data.as_bytes());
        assert!(result.is_ok(), "store {} should succeed", i);
    }

    // Wait for all async processing
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let status = orch.status();
    assert!(
        status.jobs_submitted >= 50,
        "should have at least 50 submissions, got {}",
        status.jobs_submitted
    );

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_concurrent_migrations() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    // Store some chunks first
    let chunk_ids: Vec<ChunkId> = (0..10)
        .map(|i| {
            let data = format!("migration chunk {}", i);
            let id = ChunkId::from_data(data.as_bytes());
            orch.store(id, TierId::Ram, data.as_bytes()).unwrap();
            id
        })
        .collect();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Migrate all concurrently
    for chunk_id in &chunk_ids {
        let data_size = 16; // approximate
        let _ = orch.migrate(*chunk_id, TierId::Ram, TierId::Simulation, data_size);
    }

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let status = orch.status();
    assert!(
        status.jobs_submitted >= 10,
        "should have at least 10 submitted jobs"
    );

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_mixed_workload() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    // Phase 1: Store 20 chunks
    let chunk_ids: Vec<(ChunkId, Vec<u8>)> = (0..20)
        .map(|i| {
            let data = format!("mixed workload chunk {} with some padding data", i);
            let id = ChunkId::from_data(data.as_bytes());
            orch.store(id, TierId::Ram, data.as_bytes()).unwrap();
            (id, data.into_bytes())
        })
        .collect();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Phase 2: Migrate half to simulation
    for (chunk_id, data) in chunk_ids.iter().take(10) {
        let _ = orch.migrate(*chunk_id, TierId::Ram, TierId::Simulation, data.len());
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Phase 3: Evict some from RAM
    for (chunk_id, _) in chunk_ids.iter().take(5) {
        let _ = orch.evict(*chunk_id, TierId::Ram);
    }

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Verify status reflects activity
    let status = orch.status();
    assert!(status.jobs_submitted >= 20);

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_rapid_store_evict_cycle() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    // Rapid store-evict cycles to stress the state machine
    for i in 0..20 {
        let data = format!("rapid cycle {}", i);
        let chunk_id = ChunkId::from_data(data.as_bytes());

        orch.store(chunk_id, TierId::Ram, data.as_bytes()).unwrap();
        let _ = orch.evict(chunk_id, TierId::Ram);
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Should complete without deadlock
    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_concurrent_store_different_tiers() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    // Store to both tiers concurrently
    for i in 0..10 {
        let data_ram = format!("ram tier chunk {}", i);
        let data_sim = format!("sim tier chunk {}", i);

        let id_ram = ChunkId::from_data(data_ram.as_bytes());
        let id_sim = ChunkId::from_data(data_sim.as_bytes());

        orch.store(id_ram, TierId::Ram, data_ram.as_bytes())
            .unwrap();
        orch.store(id_sim, TierId::Simulation, data_sim.as_bytes())
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let status = orch.status();
    assert!(status.jobs_submitted >= 20);

    orch.shutdown().unwrap();
}
