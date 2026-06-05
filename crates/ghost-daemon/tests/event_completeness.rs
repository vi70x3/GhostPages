//! Event completeness and ordering integration tests.
//!
//! Validates that:
//! 1. Every state mutation in ghost-daemon emits a corresponding Event.
//! 2. Paired events maintain correct ordering (issue before complete).
//! 3. All 30 Event variants are reachable and constructible.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_policy::pressure::{PressureAwareConfig, PressureAwarePolicy};
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;

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

fn test_config() -> OrchestratorConfig {
    OrchestratorConfig {
        queue_capacity: 100,
        worker_count: 2,
        max_retries: 3,
        retry_base_delay_ms: 10,
        max_retry_delay_ms: 100,
        enable_compression: false,
        trace_max_events: 1000,
        shutdown_timeout_secs: 5,
        pressure_sample_interval_ms: 100,
        pressure_smoothing_factor: 0.5,
        auto_migration_interval_ms: 1000,
        pressure_history_size: 10,
        enable_auto_migration: false,
        deterministic_mode: false,
        rng_seed: None,
    }
}

fn drain_channel(rx: &mut tokio::sync::mpsc::Receiver<Event>) -> Vec<Event> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

// ── Event Completeness: Every state mutation emits an Event ─────────────────

#[tokio::test]
async fn test_store_emits_event() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(256);
    let emitter = EventEmitter::new(tx);

    let mut orch = TransferOrchestrator::new(test_config(), test_backends(), test_policy());
    orch.set_event_emitter(emitter);
    orch.start().unwrap();

    let data = b"hello event system";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).expect("store should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_channel(&mut rx);
    let found_store = events.iter().any(|e| matches!(e, Event::Store { .. }));
    assert!(found_store, "store() must emit a Store event");

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_retrieve_emits_event() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(256);
    let emitter = EventEmitter::new(tx);

    let mut orch = TransferOrchestrator::new(test_config(), test_backends(), test_policy());
    orch.set_event_emitter(emitter);
    orch.start().unwrap();

    let data = b"retrieve me";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).expect("store should succeed");

    // Drain store events
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain_channel(&mut rx);

    orch.retrieve(chunk_id, TierId::Ram).expect("retrieve should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_channel(&mut rx);
    let found_retrieve = events.iter().any(|e| matches!(e, Event::Retrieve { .. }));
    assert!(found_retrieve, "retrieve() must emit a Retrieve event");

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_evict_emits_event() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(256);
    let emitter = EventEmitter::new(tx);

    let mut orch = TransferOrchestrator::new(test_config(), test_backends(), test_policy());
    orch.set_event_emitter(emitter);
    orch.start().unwrap();

    let data = b"evict me";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).expect("store should succeed");

    // Drain store events
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain_channel(&mut rx);

    orch.evict(chunk_id, TierId::Ram).expect("evict should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_channel(&mut rx);
    let found_eviction = events.iter().any(|e| matches!(e, Event::Eviction { .. }));
    assert!(found_eviction, "evict() must emit an Eviction event");

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_migrate_emits_started_and_completed() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(256);
    let emitter = EventEmitter::new(tx);

    let mut orch = TransferOrchestrator::new(test_config(), test_backends(), test_policy());
    orch.set_event_emitter(emitter);
    orch.start().unwrap();

    let data = b"migrate me";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).expect("store should succeed");

    // Drain store events
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain_channel(&mut rx);

    orch.migrate(chunk_id, TierId::Ram, TierId::Simulation, data.len()).expect("migrate should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_channel(&mut rx);

    let found_started = events.iter().any(|e| matches!(e, Event::MigrationStarted { .. }));
    let found_completed = events.iter().any(|e| matches!(e, Event::MigrationCompleted { .. }));
    let found_decision = events.iter().any(|e| matches!(e, Event::MigrationDecision { .. }));

    assert!(found_started, "migrate() must emit MigrationStarted");
    assert!(found_completed, "migrate() must emit MigrationCompleted");
    assert!(found_decision, "migrate() must emit MigrationDecision");

    orch.shutdown().unwrap();
}

// ── Event Ordering: Issue before Complete ───────────────────────────────────

#[tokio::test]
async fn test_migration_started_before_completed() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(256);
    let emitter = EventEmitter::new(tx);

    let mut orch = TransferOrchestrator::new(test_config(), test_backends(), test_policy());
    orch.set_event_emitter(emitter);
    orch.start().unwrap();

    let data = b"ordering test";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).expect("store should succeed");

    // Drain store events
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain_channel(&mut rx);

    orch.migrate(chunk_id, TierId::Ram, TierId::Simulation, data.len()).expect("migrate should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_channel(&mut rx);

    let started_id = events.iter().find_map(|e| {
        if let Event::MigrationStarted { sequence_id, .. } = e {
            Some(*sequence_id)
        } else {
            None
        }
    });
    let completed_id = events.iter().find_map(|e| {
        if let Event::MigrationCompleted { sequence_id, .. } = e {
            Some(*sequence_id)
        } else {
            None
        }
    });

    match (started_id, completed_id) {
        (Some(start), Some(complete)) => {
            assert!(
                start < complete,
                "MigrationStarted (seq={}) must come before MigrationCompleted (seq={})",
                start, complete
            );
        }
        _ => panic!(
            "Expected both MigrationStarted and MigrationCompleted events. Got: {:?}",
            events.iter().map(|e| e.event_name()).collect::<Vec<_>>()
        ),
    }

    orch.shutdown().unwrap();
}

#[tokio::test]
async fn test_sequence_ids_are_monotonic() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(256);
    let emitter = EventEmitter::new(tx);

    let mut orch = TransferOrchestrator::new(test_config(), test_backends(), test_policy());
    orch.set_event_emitter(emitter);
    orch.start().unwrap();

    let data = b"monotonic test";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).expect("store should succeed");
    orch.retrieve(chunk_id, TierId::Ram).expect("retrieve should succeed");
    orch.evict(chunk_id, TierId::Ram).expect("evict should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = drain_channel(&mut rx);

    assert!(
        events.len() >= 3,
        "Expected at least 3 events, got {}",
        events.len()
    );

    // Verify sequence IDs are strictly increasing.
    for window in events.windows(2) {
        let seq_a = window[0].sequence_id();
        let seq_b = window[1].sequence_id();
        assert!(
            seq_a < seq_b,
            "Sequence IDs must be strictly increasing: {} >= {}",
            seq_a, seq_b
        );
    }

    orch.shutdown().unwrap();
}

// ── Event Reachability: All 30 variants are constructible ───────────────────

#[test]
fn test_all_event_variants_constructible() {
    use ghost_core::io_events::IoOperation;
    use ghost_core::events::InvariantSeverity;
    use ghost_core::state::PressureState;

    let id = ChunkId::from_data(b"reachability");
    let _events: Vec<Event> = vec![
        Event::AllocationCreated {
            chunk_id: id,
            tier: TierId::Ram,
            size: 1024,
            sequence_id: 0,
        },
        Event::AllocationFreed {
            chunk_id: id,
            tier: TierId::Ram,
            sequence_id: 0,
        },
        Event::AllocationFailed {
            chunk_id: id,
            reason: "oom".to_string(),
            sequence_id: 0,
        },
        Event::Eviction {
            chunk_id: id,
            tier: TierId::Ram,
            reason: "pressure".to_string(),
            sequence_id: 0,
        },
        Event::Retrieve {
            key: "foo".to_string(),
            hit: true,
            sequence_id: 0,
        },
        Event::TransferCompleted {
            chunk_id: id,
            from: TierId::Ram,
            to: TierId::Simulation,
            duration_ms: 100,
            sequence_id: 0,
        },
        Event::TransferFailed {
            chunk_id: id,
            from: TierId::Ram,
            to: TierId::Simulation,
            reason: "io error".to_string(),
            sequence_id: 0,
        },
        Event::Store {
            key: "bar".to_string(),
            value_size: 512,
            sequence_id: 0,
        },
        Event::Evict {
            key: "baz".to_string(),
            sequence_id: 0,
        },
        Event::QueueEnqueue {
            task_id: 1,
            sequence_id: 0,
        },
        Event::QueueDequeue {
            task_id: 1,
            sequence_id: 0,
        },
        Event::MigrationDecision {
            chunk_id: id,
            from: TierId::Ram,
            to: TierId::Simulation,
            decision: "promote".to_string(),
            sequence_id: 0,
        },
        Event::MigrationStarted {
            chunk_id: id,
            from: TierId::Ram,
            to: TierId::Simulation,
            sequence_id: 0,
        },
        Event::MigrationCompleted {
            chunk_id: id,
            from: TierId::Ram,
            to: TierId::Simulation,
            duration_ms: 200,
            sequence_id: 0,
        },
        Event::MigrationFailed {
            chunk_id: id,
            from: TierId::Ram,
            to: TierId::Simulation,
            reason: "timeout".to_string(),
            sequence_id: 0,
        },
        Event::MigrationRolledBack {
            chunk_id: id,
            from: TierId::Simulation,
            to: TierId::Ram,
            sequence_id: 0,
        },
        Event::ReplayStarted {
            trace_path: "trace.bin".to_string(),
            sequence_id: 0,
        },
        Event::ReplayCompleted {
            trace_path: "trace.bin".to_string(),
            events: 100,
            duration_ms: 500,
            sequence_id: 0,
        },
        Event::ReplayDivergence {
            trace_path: "trace.bin".to_string(),
            expected: "foo".to_string(),
            actual: "bar".to_string(),
            sequence_id: 0,
        },
        Event::ReplayInvariantViolation {
            rule: "no_stale_reads".to_string(),
            details: "stale read detected".to_string(),
            sequence_id: 0,
        },
        Event::PressureChanged {
            tier: TierId::Ram,
            old: PressureState::new(),
            new: PressureState::new(),
            sequence_id: 0,
        },
        Event::BackpressureActivated {
            tier: TierId::Ram,
            level: "high".to_string(),
            sequence_id: 0,
        },
        Event::BackpressureDeactivated {
            tier: TierId::Ram,
            sequence_id: 0,
        },
        Event::BackendHealthChanged {
            tier: TierId::Ram,
            old: ghost_core::events::BackendHealth::Healthy,
            new: ghost_core::events::BackendHealth::Degraded,
            sequence_id: 0,
        },
        Event::RetryAttempted {
            chunk_id: id,
            attempt: 1,
            max_attempts: 3,
            sequence_id: 0,
        },
        Event::OperationFailed {
            operation: "store".to_string(),
            reason: "full".to_string(),
            sequence_id: 0,
        },
        Event::InvariantViolation {
            rule: "no_double_free".to_string(),
            details: "double free detected".to_string(),
            severity: InvariantSeverity::Error,
            sequence_id: 0,
        },
        Event::IoRequestIssued {
            operation: IoOperation::Read,
            chunk_id: id,
            tier: TierId::Ram,
            sequence_id: 0,
        },
        Event::IoRequestCompleted {
            operation: IoOperation::Read,
            chunk_id: id,
            tier: TierId::Ram,
            duration_ticks: 10,
            sequence_id: 0,
        },
        Event::IoRequestFailed {
            operation: IoOperation::Write,
            chunk_id: id,
            tier: TierId::Ram,
            error: "device failure".to_string(),
            sequence_id: 0,
        },
        Event::IoFlushIssued {
            tier: TierId::Ram,
            sequence_id: 0,
        },
        Event::IoFlushCompleted {
            tier: TierId::Ram,
            duration_ticks: 5,
            sequence_id: 0,
        },
        Event::IoBufferStateChange {
            tier: TierId::Ram,
            buffered: 4096,
            capacity: 8192,
            sequence_id: 0,
        },
    ];

    assert_eq!(_events.len(), 33, "Expected exactly 33 Event variants");
}

// ── Event Category Coverage ─────────────────────────────────────────────────

#[test]
fn test_all_categories_represented() {
    use ghost_core::io_events::IoOperation;
    use ghost_core::events::InvariantSeverity;
    use ghost_core::state::PressureState;

    let id = ChunkId::from_data(b"categories");

    let categories = vec![
        ("allocation", Event::AllocationCreated { chunk_id: id, tier: TierId::Ram, size: 1, sequence_id: 0 }),
        ("orchestration", Event::Eviction { chunk_id: id, tier: TierId::Ram, reason: "r".to_string(), sequence_id: 0 }),
        ("scheduler", Event::QueueEnqueue { task_id: 1, sequence_id: 0 }),
        ("migration", Event::MigrationStarted { chunk_id: id, from: TierId::Ram, to: TierId::Simulation, sequence_id: 0 }),
        ("replay", Event::ReplayStarted { trace_path: "t".to_string(), sequence_id: 0 }),
        ("pressure", Event::PressureChanged { tier: TierId::Ram, old: PressureState::new(), new: PressureState::new(), sequence_id: 0 }),
        ("failure", Event::OperationFailed { operation: "op".to_string(), reason: "err".to_string(), sequence_id: 0 }),
        ("invariant_violation", Event::InvariantViolation { rule: "r".to_string(), details: "d".to_string(), severity: InvariantSeverity::Error, sequence_id: 0 }),
        ("io", Event::IoRequestIssued { operation: IoOperation::Read, chunk_id: id, tier: TierId::Ram, sequence_id: 0 }),
    ];

    for (expected, event) in &categories {
        assert_eq!(
            event.category(),
            *expected,
            "Event {:?} should be in category '{}'",
            event.event_name(),
            expected
        );
    }
}
