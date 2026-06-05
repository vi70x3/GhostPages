//! Integration tests for the unified event taxonomy (Phase 2.5 §5).
//!
//! Validates the end-to-end event pipeline: EventEmitter -> EventMultiplexer
//! -> MetricsBridge/TracingBridge, event emission from subsystems, and event
//! serialization roundtrip.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ghost_core::emitter::EventEmitter;
use ghost_core::event_multiplexer::{EventMultiplexer, EventHandler, NoopHandler};
use ghost_core::events::*;
use ghost_core::state::PressureState;
use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::health::{HealthConfig, HealthTracker};
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;

// ─── Helpers ──────────────────────────────────────────────────────────────────

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
    Arc::new(ghost_policy::pressure::PressureAwarePolicy::new(
        ghost_policy::pressure::PressureAwareConfig::default(),
    ))
}

// ─── Event Taxonomy Tests ────────────────────────────────────────────────────

#[test]
fn test_event_category_allocation() {
    let event = Event::AllocationCreated {
        chunk_id: ChunkId::from_data(b"test"),
        tier: TierId::Ram,
        size: 4096,
        sequence_id: 0,
    };
    assert_eq!(event.category(), "allocation");
    assert_eq!(event.event_name(), "allocation_created");
    assert!(event.chunk_id().is_some());
    assert_eq!(event.tier(), Some(TierId::Ram));
}

#[test]
fn test_event_category_migration() {
    let event = Event::MigrationStarted {
        chunk_id: ChunkId::from_data(b"mig"),
        from: TierId::Ram,
        to: TierId::Disk,
        sequence_id: 0,
    };
    assert_eq!(event.category(), "migration");
    assert_eq!(event.event_name(), "migration_started");
}

#[test]
fn test_event_category_pressure() {
    let event = Event::PressureChanged {
        tier: TierId::Ram,
        old: PressureState::new(),
        new: PressureState::new(),
        sequence_id: 0,
    };
    assert_eq!(event.category(), "pressure");
    assert_eq!(event.event_name(), "pressure_changed");
}

#[test]
fn test_event_category_failure() {
    let event = Event::BackendHealthChanged {
        tier: TierId::Ram,
        old: BackendHealth::Healthy,
        new: BackendHealth::Degraded,
        sequence_id: 0,
    };
    assert_eq!(event.category(), "failure");
    assert_eq!(event.event_name(), "backend_health_changed");
}

#[test]
fn test_event_category_invariant() {
    let event = Event::InvariantViolation {
        rule: "no_orphaned_transfers".to_string(),
        details: "orphan detected".to_string(),
        severity: InvariantSeverity::Critical,
        sequence_id: 0,
    };
    assert_eq!(event.category(), "invariant_violation");
    assert_eq!(event.event_name(), "invariant_violation");
}

#[test]
fn test_event_serialization_roundtrip() {
    let original = Event::AllocationCreated {
        chunk_id: ChunkId::from_data(b"roundtrip"),
        tier: TierId::Ram,
        size: 8192,
        sequence_id: 0,
    };
    let json = serde_json::to_string(&original).expect("serialize");
    let deserialized: Event = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(original.category(), deserialized.category());
    assert_eq!(original.event_name(), deserialized.event_name());
    assert_eq!(original.chunk_id(), deserialized.chunk_id());
    assert_eq!(original.tier(), deserialized.tier());
}

#[test]
fn test_all_event_variants_serializable() {
    let chunk_id = ChunkId::from_data(b"variant");
    let events: Vec<Event> = vec![
        Event::AllocationCreated {
            chunk_id,
            tier: TierId::Ram,
            size: 1024,
            sequence_id: 0,
        },
        Event::AllocationFreed {
            chunk_id,
            tier: TierId::Ram,
            sequence_id: 0,
        },
        Event::AllocationFailed {
            chunk_id,
            reason: "out of memory".to_string(),
            sequence_id: 0,
        },
        Event::MigrationStarted {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Disk,
            sequence_id: 0,
        },
        Event::MigrationCompleted {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Disk,
            duration_ms: 100,
            sequence_id: 0,
        },
        Event::MigrationFailed {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Disk,
            reason: "io error".to_string(),
            sequence_id: 0,
        },
        Event::MigrationRolledBack {
            chunk_id,
            from: TierId::Disk,
            to: TierId::Ram,
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
            level: "critical".to_string(),
            sequence_id: 0,
        },
        Event::BackpressureDeactivated { tier: TierId::Ram, sequence_id: 0 },
        Event::BackendHealthChanged {
            tier: TierId::Ram,
            old: BackendHealth::Healthy,
            new: BackendHealth::Degraded,
            sequence_id: 0,
        },
        Event::RetryAttempted {
            chunk_id,
            attempt: 1,
            max_attempts: 3,
            sequence_id: 0,
        },
        Event::OperationFailed {
            operation: "store".to_string(),
            reason: "backend unavailable".to_string(),
            sequence_id: 0,
        },
        Event::InvariantViolation {
            rule: "test_rule".to_string(),
            details: "violation details".to_string(),
            severity: InvariantSeverity::Warning,
            sequence_id: 0,
        },
    ];

    for event in &events {
        let json = serde_json::to_string(event).expect("serialize");
        let deserialized: Event = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event.category(), deserialized.category());
    }
}

// ─── EventEmitter Tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_emitter_sends_events() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    emitter
        .allocation_created(ChunkId::from_data(b"emit"), TierId::Ram, 2048)
        .await
        .expect("emit should succeed");

    let event = rx.recv().await.expect("should receive event");
    match event {
        Event::AllocationCreated { chunk_id, tier, size, .. } => {
            assert_eq!(chunk_id, ChunkId::from_data(b"emit"));
            assert_eq!(tier, TierId::Ram);
            assert_eq!(size, 2048);
        }
        _ => panic!("wrong event variant"),
    }
}

#[tokio::test]
async fn test_emitter_clone_shares_channel() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter1 = EventEmitter::new(tx);
    let emitter2 = emitter1.clone();

    emitter1
        .allocation_freed(ChunkId::from_data(b"clone"), TierId::Disk)
        .await
        .expect("emit from clone 1");

    emitter2
        .migration_started(ChunkId::from_data(b"clone"), TierId::Ram, TierId::Disk)
        .await
        .expect("emit from clone 2");

    let e1 = rx.recv().await.expect("first event");
    let e2 = rx.recv().await.expect("second event");
    assert!(matches!(e1, Event::AllocationFreed { .. }));
    assert!(matches!(e2, Event::MigrationStarted { .. }));
}

#[tokio::test]
async fn test_emitter_all_event_types() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let chunk_id = ChunkId::from_data(b"all");

    // Emit one of each type
    emitter.allocation_created(chunk_id, TierId::Ram, 1024).await.unwrap();
    emitter.allocation_freed(chunk_id, TierId::Ram).await.unwrap();
    emitter.allocation_failed(chunk_id, "oom").await.unwrap();
    emitter.migration_started(chunk_id, TierId::Ram, TierId::Disk).await.unwrap();
    emitter.migration_completed(chunk_id, TierId::Ram, TierId::Disk, 50).await.unwrap();
    emitter.migration_failed(chunk_id, TierId::Ram, TierId::Disk, "io").await.unwrap();
    emitter.migration_rolled_back(chunk_id, TierId::Disk, TierId::Ram).await.unwrap();
    emitter.pressure_changed(TierId::Ram, PressureState::new(), PressureState::new()).await.unwrap();
    emitter.backpressure_activated(TierId::Ram, "high").await.unwrap();
    emitter.backpressure_deactivated(TierId::Ram).await.unwrap();
    emitter.backend_health_changed(TierId::Ram, BackendHealth::Healthy, BackendHealth::Degraded).await.unwrap();
    emitter.retry_attempted(chunk_id, 1, 3).await.unwrap();
    emitter.operation_failed("store", "full").await.unwrap();
    emitter.replay_started("/trace.log").await.unwrap();
    emitter.replay_completed("/trace.log", 100, 500).await.unwrap();
    emitter.replay_divergence("/trace.log", "a", "b").await.unwrap();
    emitter.replay_invariant_violation("rule", "details").await.unwrap();
    emitter.invariant_violation("rule", "details", InvariantSeverity::Critical).await.unwrap();

    // Verify all 18 events were received
    for _ in 0..18 {
        let event = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timeout waiting for event")
            .expect("channel closed prematurely");
        assert!(event.event_name().len() > 0);
    }
}

// ─── EventMultiplexer Tests ──────────────────────────────────────────────────

/// A test handler that counts events it receives.
struct CountingHandler {
    counter: Arc<std::sync::atomic::AtomicU64>,
}

impl EventHandler for CountingHandler {
    fn handle(
        &self,
        _event: &Event,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                > + Send
                + '_,
        >,
    > {
        self.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn test_multiplexer_fanout() {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let counter1 = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counter2 = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let handler1 = Box::new(CountingHandler {
        counter: counter1.clone(),
    });
    let handler2 = Box::new(CountingHandler {
        counter: counter2.clone(),
    });

    let multiplexer = EventMultiplexer::new(rx).with_handler(handler1).with_handler(handler2);

    // Send events
    let emitter = EventEmitter::new(tx);
    for i in 0..5 {
        emitter
            .allocation_created(ChunkId::from_data(&[i]), TierId::Ram, 1024)
            .await
            .unwrap();
    }

    // Run multiplexer (it will process until channel closes)
    let handle = tokio::spawn(async move {
        multiplexer.run().await;
    });

    // Wait for events to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drop emitter's sender to close the channel and stop the multiplexer
    drop(emitter);
    let _ = handle.await;

    assert_eq!(counter1.load(std::sync::atomic::Ordering::SeqCst), 5);
    assert_eq!(counter2.load(std::sync::atomic::Ordering::SeqCst), 5);
}

#[tokio::test]
async fn test_multiplexer_with_noop_handler() {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let multiplexer = EventMultiplexer::new(rx).with_handler(Box::new(NoopHandler));

    let emitter = EventEmitter::new(tx);
    emitter
        .allocation_created(ChunkId::from_data(b"noop"), TierId::Ram, 512)
        .await
        .unwrap();

    let handle = tokio::spawn(async move {
        multiplexer.run().await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(emitter);
    let _ = handle.await;
    // No panic = success
}

// ─── MetricsBridge Integration Tests ─────────────────────────────────────────

#[tokio::test]
async fn test_metrics_bridge_increments_on_events() {
    let registry = Arc::new(prometheus::Registry::new());
    let bridge = ghost_metrics::event_bridge::MetricsBridge::register_with(&registry)
        .expect("register bridge");

    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let multiplexer = EventMultiplexer::new(rx).with_handler(Box::new(bridge));

    let emitter = EventEmitter::new(tx);

    // Emit events
    emitter
        .allocation_created(ChunkId::from_data(b"m1"), TierId::Ram, 1024)
        .await
        .unwrap();
    emitter
        .allocation_created(ChunkId::from_data(b"m2"), TierId::Disk, 2048)
        .await
        .unwrap();
    emitter
        .migration_started(ChunkId::from_data(b"m1"), TierId::Ram, TierId::Disk)
        .await
        .unwrap();

    let handle = tokio::spawn(async move {
        multiplexer.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(emitter);
    let _ = handle.await;

    // Verify metrics were updated
    let metric_families = registry.gather();
    let text = prometheus::TextEncoder::new()
        .encode_to_string(&metric_families)
        .expect("encode metrics");
    assert!(text.contains("ghost_events_total"), "metrics should contain ghost_events_total, got:\n{}", text);
}

// ─── Subsystem Event Emission Tests ──────────────────────────────────────────

#[test]
fn test_health_tracker_emits_backend_health_changed() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    let mut tracker = HealthTracker::new(HealthConfig {
        degraded_threshold: 1,
        unavailable_threshold: 3,
        ..HealthConfig::default()
    });
    tracker.set_event_emitter(emitter);

    // Record a failure — should transition Healthy → Degraded
    tracker.record_failure(TierId::Ram);

    // The event should be sent
    let event = rx.try_recv().expect("should receive health event");
    match event {
        Event::BackendHealthChanged { tier, old, new, .. } => {
            assert_eq!(tier, TierId::Ram);
            assert_eq!(old, BackendHealth::Healthy);
            assert_eq!(new, BackendHealth::Degraded);
        }
        _ => panic!("expected BackendHealthChanged, got {:?}", event.event_name()),
    }
}

#[test]
fn test_health_tracker_emits_recovery_event() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    let mut tracker = HealthTracker::new(HealthConfig {
        degraded_threshold: 1,
        unavailable_threshold: 1,
        ..HealthConfig::default()
    });
    tracker.set_event_emitter(emitter);

    // Make it unavailable
    tracker.record_failure(TierId::Disk);
    let _ = rx.try_recv(); // consume the degraded/unavailable event

    // Begin recovery
    tracker.begin_recovery(TierId::Disk);

    let event = rx.try_recv().expect("should receive recovery event");
    match event {
        Event::BackendHealthChanged { tier, old, new, .. } => {
            assert_eq!(tier, TierId::Disk);
            assert_eq!(old, BackendHealth::Unavailable);
            assert_eq!(new, BackendHealth::Recovering);
        }
        _ => panic!("expected BackendHealthChanged, got {:?}", event.event_name()),
    }
}

#[test]
fn test_health_tracker_no_event_when_unchanged() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    let mut tracker = HealthTracker::new(HealthConfig {
        degraded_threshold: 5,
        unavailable_threshold: 10,
        ..HealthConfig::default()
    });
    tracker.set_event_emitter(emitter);

    // Record a failure but stay healthy (below threshold)
    tracker.record_failure(TierId::Ram);

    // No event should be emitted since state didn't change
    assert!(rx.try_recv().is_err(), "no event should be emitted when state unchanged");
}

// ─── InvariantSeverity Tests ─────────────────────────────────────────────────

#[test]
fn test_invariant_severity_ordering() {
    assert!(InvariantSeverity::Critical > InvariantSeverity::Warning);
    assert!(InvariantSeverity::Warning > InvariantSeverity::Info);
}

#[test]
fn test_invariant_severity_display() {
    assert_eq!(format!("{}", InvariantSeverity::Critical), "critical");
    assert_eq!(format!("{}", InvariantSeverity::Warning), "warning");
    assert_eq!(format!("{}", InvariantSeverity::Info), "info");
}

// ─── BackendHealth Tests ─────────────────────────────────────────────────────

#[test]
fn test_backend_health_display() {
    assert_eq!(format!("{}", BackendHealth::Healthy), "healthy");
    assert_eq!(format!("{}", BackendHealth::Degraded), "degraded");
    assert_eq!(format!("{}", BackendHealth::Unavailable), "unavailable");
    assert_eq!(format!("{}", BackendHealth::Recovering), "recovering");
}

// ─── Event Helper Method Tests ───────────────────────────────────────────────

#[test]
fn test_event_chunk_id_extraction() {
    let event = Event::AllocationCreated {
        chunk_id: ChunkId::from_data(b"extract"),
        tier: TierId::Ram,
        size: 1024,
        sequence_id: 0,
    };
    assert_eq!(event.chunk_id(), Some(ChunkId::from_data(b"extract")));

    let event = Event::PressureChanged {
        tier: TierId::Ram,
        old: PressureState::new(),
        new: PressureState::new(),
        sequence_id: 0,
    };
    assert!(event.chunk_id().is_none());
}

#[test]
fn test_event_tier_extraction() {
    let event = Event::AllocationFreed {
        chunk_id: ChunkId::from_data(b"tier"),
        tier: TierId::Disk,
        sequence_id: 0,
    };
    assert_eq!(event.tier(), Some(TierId::Disk));

    let event = Event::InvariantViolation {
        rule: "r".to_string(),
        details: "d".to_string(),
        severity: InvariantSeverity::Info,
        sequence_id: 0,
    };
    assert!(event.tier().is_none());
}

// ─── Orchestrator + EventEmitter Integration ─────────────────────────────────

#[test]
fn test_orchestrator_emits_allocation_event() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    let config = OrchestratorConfig::default();
    let mut orchestrator = TransferOrchestrator::new(config, test_backends(), test_policy());
    orchestrator.set_event_emitter(emitter);

    let chunk_id = ChunkId::from_data(b"orch");
    orchestrator
        .store(chunk_id, TierId::Ram, b"hello")
        .expect("store should succeed");

    let event = rx.try_recv().expect("should receive store event");
    match event {
        Event::Store { key, value_size, .. } => {
            assert_eq!(key, format!("{:?}", chunk_id));
            assert_eq!(value_size, 5); // "hello".len()
        }
        _ => panic!("expected Store, got {:?}", event.event_name()),
    }
}

#[test]
fn test_orchestrator_emits_migration_event() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    let config = OrchestratorConfig::default();
    let mut orchestrator = TransferOrchestrator::new(config, test_backends(), test_policy());
    orchestrator.set_event_emitter(emitter);

    let chunk_id = ChunkId::from_data(b"mig");
    // Store first
    orchestrator
        .store(chunk_id, TierId::Ram, b"data")
        .expect("store should succeed");
    let _ = rx.try_recv(); // consume store event

    // Migrate (size = 4 bytes for "data")
    orchestrator
        .migrate(chunk_id, TierId::Ram, TierId::Simulation, 4)
        .expect("migrate should succeed");

    let event = rx.try_recv().expect("should receive migration decision event");
    match event {
        Event::MigrationDecision { chunk_id: cid, from, to, .. } => {
            assert_eq!(cid, chunk_id);
            assert_eq!(from, TierId::Ram);
            assert_eq!(to, TierId::Simulation);
        }
        _ => panic!("expected MigrationDecision, got {:?}", event.event_name()),
    }
}

#[test]
fn test_orchestrator_emits_eviction_event() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    let config = OrchestratorConfig::default();
    let mut orchestrator = TransferOrchestrator::new(config, test_backends(), test_policy());
    orchestrator.set_event_emitter(emitter);

    let chunk_id = ChunkId::from_data(b"evict");
    orchestrator
        .store(chunk_id, TierId::Ram, b"data")
        .expect("store should succeed");
    let _ = rx.try_recv(); // consume store event

    orchestrator
        .evict(chunk_id, TierId::Ram)
        .expect("evict should succeed");

    let event = rx.try_recv().expect("should receive eviction event");
    match event {
        Event::Eviction { chunk_id: cid, tier, .. } => {
            assert_eq!(cid, chunk_id);
            assert_eq!(tier, TierId::Ram);
        }
        _ => panic!("expected Eviction, got {:?}", event.event_name()),
    }
}
