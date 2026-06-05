//! Integration test: Tier-to-tier migration.
//!
//! Validates that chunks can be migrated between RAM and Simulation
//! tiers, with proper state machine transitions.

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

fn test_orchestrator() -> TransferOrchestrator {
    TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends(),
        test_policy(),
    )
}

#[tokio::test]
async fn test_migrate_ram_to_simulation() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let data = b"migration test data";
    let chunk_id = ChunkId::from_data(data);

    // Store in RAM
    orch.store(chunk_id, TierId::Ram, data).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Migrate RAM -> Simulation
    orch.migrate(chunk_id, TierId::Ram, TierId::Simulation, data.len())
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Verify status reflects migration
    let status = orch.status();
    assert!(
        status.jobs_submitted > 0,
        "should have recorded submitted jobs"
    );

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_migrate_simulation_to_ram() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let data = b"reverse migration test";
    let chunk_id = ChunkId::from_data(data);

    // Store in Simulation
    orch.store(chunk_id, TierId::Simulation, data).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Migrate Simulation -> RAM
    orch.migrate(chunk_id, TierId::Simulation, TierId::Ram, data.len())
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let status = orch.status();
    assert!(status.jobs_submitted > 0);

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_migrate_invalid_state_fails() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let fake_id = ChunkId::from_data(b"never stored");

    // Migrating a non-existent chunk should fail
    let result = orch.migrate(fake_id, TierId::Ram, TierId::Simulation, 100);
    assert!(result.is_err(), "migrating unregistered chunk should fail");

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_multiple_migrations() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let data = b"multi-migration test";
    let chunk_id = ChunkId::from_data(data);

    // Store in RAM
    orch.store(chunk_id, TierId::Ram, data).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Migrate RAM -> Simulation
    orch.migrate(chunk_id, TierId::Ram, TierId::Simulation, data.len())
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Migrate Simulation -> RAM
    orch.migrate(chunk_id, TierId::Simulation, TierId::Ram, data.len())
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let status = orch.status();
    assert!(
        status.jobs_submitted >= 2,
        "should have at least 2 submitted jobs"
    );

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_migration_emits_trace_events() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let data = b"trace migration test";
    let chunk_id = ChunkId::from_data(data);

    orch.store(chunk_id, TierId::Ram, data).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    orch.migrate(chunk_id, TierId::Ram, TierId::Simulation, data.len())
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let status = orch.status();
    assert!(
        status.trace_event_count >= 2,
        "should have events for store + migration, got {}",
        status.trace_event_count
    );

    orch.shutdown().unwrap();
}
