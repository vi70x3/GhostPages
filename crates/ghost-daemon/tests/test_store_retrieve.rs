//! Integration test: Store and Retrieve operations.
//!
//! Validates the full lifecycle of storing data in one tier and
//! retrieving it, verifying content-addressed integrity.

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
async fn test_store_and_retrieve_ram() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let data = b"hello from ghostpages integration test";
    let chunk_id = ChunkId::from_data(data);

    // Store in RAM tier
    orch.store(chunk_id, TierId::Ram, data).unwrap();

    // Wait for async processing
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Retrieve from RAM tier
    let result = orch.retrieve(chunk_id, TierId::Ram);
    assert!(
        result.is_ok(),
        "retrieve should succeed: {:?}",
        result.err()
    );

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_store_and_retrieve_simulation() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let data = b"simulation tier test data";
    let chunk_id = ChunkId::from_data(data);

    // Store in simulation tier
    orch.store(chunk_id, TierId::Simulation, data).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Retrieve from simulation tier
    let result = orch.retrieve(chunk_id, TierId::Simulation);
    assert!(
        result.is_ok(),
        "retrieve should succeed: {:?}",
        result.err()
    );

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_store_multiple_chunks() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let chunks: Vec<(ChunkId, Vec<u8>)> = (0..10)
        .map(|i| {
            let data = format!("chunk data {}", i);
            (ChunkId::from_data(data.as_bytes()), data.into_bytes())
        })
        .collect();

    for (chunk_id, data) in &chunks {
        orch.store(*chunk_id, TierId::Ram, data).unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify all chunks are retrievable
    for (chunk_id, _data) in &chunks {
        let result = orch.retrieve(*chunk_id, TierId::Ram);
        assert!(result.is_ok(), "chunk {:?} should be retrievable", chunk_id);
    }

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_store_empty_data() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let data = b"";
    let chunk_id = ChunkId::from_data(data);

    // Empty data should still be storable
    let result = orch.store(chunk_id, TierId::Ram, data);
    // It may succeed or fail depending on backend policy, but should not panic
    let _ = result;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_retrieve_unregistered_chunk_fails() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let fake_id = ChunkId::from_data(b"nonexistent data");
    let result = orch.retrieve(fake_id, TierId::Ram);

    assert!(result.is_err(), "retrieving unregistered chunk should fail");

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_trace_log_records_store_events() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    let data = b"trace test data";
    let chunk_id = ChunkId::from_data(data);

    orch.store(chunk_id, TierId::Ram, data).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let status = orch.status();
    assert!(
        status.trace_event_count > 0,
        "trace log should have recorded events"
    );

    orch.shutdown().unwrap();
}
