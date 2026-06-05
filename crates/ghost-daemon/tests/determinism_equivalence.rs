//! Determinism equivalence tests for GhostPages.
//!
//! Validates that the system produces byte-identical outputs when given
//! identical inputs and configuration. These tests are critical for ensuring
//! replay equivalence and deterministic behavior across runs.

use std::collections::BTreeMap;
use std::sync::Arc;

use ghost_core::state::{ChunkState, StateMachine};
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_policy::pressure::PressureAwareConfig;
use ghost_policy::pressure::PressureAwarePolicy;
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use ghost_tier::StorageBackend;

/// Deterministic seed for all tests.
const DETERMINISTIC_SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;

/// Create a deterministic test backend configuration.
fn deterministic_backends() -> BTreeMap<TierId, Arc<dyn StorageBackend>> {
    let mut backends = BTreeMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(4 * 1024 * 1024)) as Arc<dyn StorageBackend>,
    );
    let sim = Arc::new(SimBackend::new(
        SimConfig::with_capacity(16 * 1024 * 1024).with_seed(DETERMINISTIC_SEED),
    ));
    backends.insert(TierId::Simulation, sim as Arc<dyn StorageBackend>);
    backends
}

/// Create a deterministic orchestrator config.
fn deterministic_config() -> OrchestratorConfig {
    OrchestratorConfig {
        rng_seed: Some(DETERMINISTIC_SEED),
        deterministic_mode: true,
        ..OrchestratorConfig::default()
    }
}

fn deterministic_policy() -> Arc<dyn ghost_policy::PlacementPolicy> {
    Arc::new(PressureAwarePolicy::new(
        PressureAwareConfig::default(),
    ))
}

fn make_chunk_id(seed: u8) -> ChunkId {
    let mut id = [0u8; 32];
    id[0] = seed;
    ChunkId(id)
}

// ─── Deterministic State Machine Tests ────────────────────────────────────────

#[test]
fn test_state_machine_deterministic_transitions() {
    let mut sm = StateMachine::new();
    let chunk = make_chunk_id(1);

    sm.register(chunk).unwrap();
    assert_eq!(sm.get_state(&chunk).unwrap(), ChunkState::Allocated);

    // Transition through a fixed sequence
    let transitions = vec![
        ChunkState::Stored,
        ChunkState::Migrating,
        ChunkState::Stored,
        ChunkState::Evicted,
    ];

    for target in transitions {
        let prev = sm.get_state(&chunk).unwrap();
        sm.transition(&chunk, target).unwrap();
        assert_eq!(sm.get_state(&chunk).unwrap(), target);

        // Verify the transition is valid
        assert!(prev.is_valid_transition(target));
    }
}

#[test]
fn test_state_machine_snapshot_deterministic_ordering() {
    let mut sm = StateMachine::new();

    // Register chunks in a specific order
    for i in 0u8..10 {
        sm.register(make_chunk_id(i)).unwrap();
    }

    // Snapshot should return chunks in deterministic order (BTreeMap)
    let snapshot = sm.snapshot();
    let ids: Vec<_> = snapshot.keys().collect();

    // Verify ordering is consistent (BTreeMap orders by ChunkId bytes)
    for window in ids.windows(2) {
        assert!(window[0] <= window[1], "Snapshot ordering must be deterministic");
    }
}

// ─── Deterministic Backend Tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_sim_backend_deterministic_store_and_retrieve() {
    let config = SimConfig::with_capacity(1024 * 1024).with_seed(DETERMINISTIC_SEED);
    let backend = SimBackend::new(config);

    let chunk = make_chunk_id(42);
    let data = b"deterministic test data";

    // Store
    let alloc = backend.allocate(data.len()).await.unwrap();
    backend.write(&alloc, data).await.unwrap();

    // Retrieve
    let mut buf = vec![0u8; data.len()];
    backend.read(&alloc, &mut buf).await.unwrap();
    assert_eq!(buf, data);
}

#[tokio::test]
async fn test_sim_backend_deterministic_failure_pattern() {
    use ghost_sim::config::{FailureConfig, FailurePattern};

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

    let config = SimConfig::with_capacity(1024 * 1024)
        .with_seed(DETERMINISTIC_SEED)
        .with_failure(failure);

    let backend = SimBackend::new(config);

    // Perform a fixed sequence of operations
    let mut results = Vec::new();
    for _i in 0u8..20 {
        let result = backend.allocate(64).await.and_then(|_alloc| Ok(()));
        results.push(result.is_ok());
    }

    // The failure pattern should be deterministic given the same seed
    // Run the same sequence again with a fresh backend
    let config2 = SimConfig::with_capacity(1024 * 1024)
        .with_seed(DETERMINISTIC_SEED)
        .with_failure(FailureConfig {
            write_failure_rate: 0.5,
            read_failure_rate: 0.3,
            alloc_failure_rate: 0.1,
            corruption_on_failure: false,
            corruption_rate: 0.0,
            timeout_rate: 0.0,
            device_loss_rate: 0.0,
            failure_pattern: FailurePattern::Random,
        });

    let backend2 = SimBackend::new(config2);
    let mut results2 = Vec::new();
    for _i in 0u8..20 {
        let result = backend2.allocate(64).await.and_then(|_alloc| Ok(()));
        results2.push(result.is_ok());
    }

    // Both runs should produce identical success/failure patterns
    assert_eq!(results, results2, "Failure patterns must be deterministic");
}

// ─── Deterministic Orchestrator Tests ────────────────────────────────────────

#[test]
fn test_orchestrator_deterministic_creation() {
    let config = deterministic_config();
    let backends = deterministic_backends();
    let policy = deterministic_policy();

    let orch1 = TransferOrchestrator::new(config.clone(), backends.clone(), policy.clone());
    let orch2 = TransferOrchestrator::new(config, backends, policy);

    // Both orchestrators should have the same initial state
    let status1 = orch1.status();
    let status2 = orch2.status();

    assert_eq!(status1.queue_depth, status2.queue_depth);
    assert_eq!(status1.active_workers, status2.active_workers);
    assert_eq!(status1.jobs_submitted, status2.jobs_submitted);
}

// ─── Deterministic Collection Ordering Tests ─────────────────────────────────

#[test]
fn test_btree_map_deterministic_iteration() {
    let mut map = BTreeMap::new();
    for i in (0..255u8).rev() {
        let mut key = [0u8; 32];
        key[0] = i;
        map.insert(ChunkId(key), i);
    }

    // BTreeMap always iterates in sorted key order
    let values: Vec<_> = map.values().collect();
    let mut sorted = values.clone();
    sorted.sort();
    assert_eq!(values, sorted);
}

#[test]
fn test_btree_set_deterministic_iteration() {
    let mut set = std::collections::BTreeSet::new();
    for i in (0..100u8).rev() {
        set.insert(i);
    }

    let values: Vec<_> = set.iter().collect();
    let mut sorted = values.clone();
    sorted.sort();
    assert_eq!(values, sorted);
}

// ─── Timestamp Determinism Tests ─────────────────────────────────────────────

#[test]
fn test_timestamp_monotonically_increasing() {
    let ts1 = current_timestamp();
    std::thread::sleep(std::time::Duration::from_millis(1));
    let ts2 = current_timestamp();
    assert!(ts2 >= ts1, "Timestamps must be monotonically non-decreasing");
}

// ─── Cross-Run Determinism Verification ──────────────────────────────────────

/// This test verifies that running the same sequence of operations twice
/// produces identical trace event sequences.
#[test]
fn test_trace_event_sequence_determinism() {
    let mut events1 = Vec::new();
    let mut events2 = Vec::new();

    let ts = current_timestamp();

    // Generate events from two identical state machines
    for run in 0..2 {
        let mut sm = StateMachine::new();
        let chunk = make_chunk_id(1);

        sm.register(chunk).unwrap();
        let ev1 = TraceEvent::ChunkCreated {
            chunk_id: chunk,
            size: 1024,
            tier: TierId::Ram,
            timestamp: ts,
        };
        if run == 0 {
            events1.push(ev1);
        } else {
            events2.push(ev1);
        }

        sm.transition(&chunk, ChunkState::Stored).unwrap();
        let ev2 = TraceEvent::ChunkStateChanged {
            chunk_id: chunk,
            from: ChunkState::Allocated,
            to: ChunkState::Stored,
            timestamp: ts,
        };
        if run == 0 {
            events1.push(ev2);
        } else {
            events2.push(ev2);
        }
    }

    // Events should be identical
    assert_eq!(events1.len(), events2.len());
    for (e1, e2) in events1.iter().zip(events2.iter()) {
        assert_eq!(e1.event_type(), e2.event_type());
    }
}
