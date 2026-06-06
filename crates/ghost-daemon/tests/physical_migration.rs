//! Integration tests: Physical-aware migration engine.
//!
//! Validates that the migration engine considers I/O pressure, physical cost,
//! backpressure state, and emits the correct events for physical-aware decisions.

use std::collections::BTreeMap;
use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::state::{ChunkState, PhysicalCost, PressureState, StateMachine};
use ghost_core::transfer::TransferPriority;
use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::backpressure::{BackpressureAction, BackpressureController};
use ghost_daemon::config::{BackpressureConfig, MigrationConfig};
use ghost_daemon::hotness_tracker::HotnessTracker;
use ghost_daemon::migration::{MigrationEngine, PendingMigration};
use ghost_daemon::trace_log::TraceLog;
use ghost_policy::lru::{LruConfig, LruPolicy};
use ghost_policy::PlacementPolicy;
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use ghost_tier::StorageBackend;

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn test_backends() -> BTreeMap<TierId, Arc<dyn StorageBackend>> {
    let mut backends = BTreeMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(1024 * 1024)) as Arc<dyn StorageBackend>,
    );
    backends.insert(
        TierId::Simulation,
        Arc::new(SimBackend::new(
            SimConfig::with_capacity(4 * 1024 * 1024).with_seed(42),
        )) as Arc<dyn StorageBackend>,
    );
    backends
}

fn test_engine_with_emitter() -> (MigrationEngine, tokio::sync::mpsc::Receiver<Event>) {
    let config = MigrationConfig::default();
    let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
    let trace_log = Arc::new(TraceLog::new(1000));
    let hotness_tracker = Arc::new(HotnessTracker::new(1000, trace_log.clone()));
    let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
    let backends = test_backends();

    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let emitter = EventEmitter::new(tx);

    let mut engine = MigrationEngine::new(
        config,
        policy,
        hotness_tracker,
        state_machine,
        trace_log,
        backends,
    );
    engine.set_event_emitter(emitter);

    (engine, rx)
}

fn test_engine() -> MigrationEngine {
    let config = MigrationConfig::default();
    let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
    let trace_log = Arc::new(TraceLog::new(1000));
    let hotness_tracker = Arc::new(HotnessTracker::new(1000, trace_log.clone()));
    let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
    let backends = test_backends();

    MigrationEngine::new(
        config,
        policy,
        hotness_tracker,
        state_machine,
        trace_log,
        backends,
    )
}

fn test_chunk_id(seed: u8) -> ChunkId {
    let mut id = [0u8; 32];
    id[0] = seed;
    ChunkId(id)
}

fn test_pending_migration(seed: u8) -> PendingMigration {
    PendingMigration {
        chunk_id: test_chunk_id(seed),
        from_tier: TierId::Ram,
        to_tier: TierId::Simulation,
        priority: TransferPriority::High,
        size: 4096,
        hotness_score: 0.8,
        identified_at: 0,
    }
}

fn drain_events(rx: &mut tokio::sync::mpsc::Receiver<Event>) -> Vec<Event> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

// ─── Test 1: I/O pressure defers migration ───────────────────────────────────

#[test]
fn test_io_pressure_defers_migration() {
    let (engine, mut rx) = test_engine_with_emitter();
    let migration = test_pending_migration(1);

    // High I/O pressure: io_pressure > 0.85 or queue_depth > 64
    let io_cost = PhysicalCost {
        latency_ms: 1.0,
        bandwidth_bps: 1_000_000.0,
        reliability: 0.99,
        io_pressure: 0.95,
        queue_depth: 10,
    };

    let pressure = PressureState::new();
    let backpressure = BackpressureAction::Allow;

    let result = engine.decide_migration(&migration, &pressure, &io_cost, &backpressure);

    // Should be deferred due to high I/O pressure
    assert!(result.is_none(), "migration should be deferred under high I/O pressure");

    // Verify MigrationDeferred event was emitted
    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(e, Event::MigrationDeferred { chunk_id, .. } if *chunk_id == migration.chunk_id)),
        "should emit MigrationDeferred event, got: {:?}",
        events
    );
}

// ─── Test 2: Backpressure rejects migration ──────────────────────────────────

#[test]
fn test_backpressure_rejects_migration() {
    let (engine, mut rx) = test_engine_with_emitter();
    let migration = test_pending_migration(2);

    let io_cost = PhysicalCost {
        latency_ms: 1.0,
        bandwidth_bps: 1_000_000.0,
        reliability: 0.99,
        io_pressure: 0.1,
        queue_depth: 0,
    };

    let pressure = PressureState::new();
    // Reject action only allows Critical priority
    let backpressure = BackpressureAction::Reject;

    let result = engine.decide_migration(&migration, &pressure, &io_cost, &backpressure);

    // Should be rejected: High priority is not allowed under Reject
    assert!(result.is_none(), "migration should be rejected under Reject backpressure");

    // Verify MigrationRejected event was emitted
    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(e, Event::MigrationRejected { chunk_id, .. } if *chunk_id == migration.chunk_id)),
        "should emit MigrationRejected event, got: {:?}",
        events
    );
}

// ─── Test 3: Hot chunks get decided under normal conditions ──────────────────

#[test]
fn test_hot_chunks_migrate_under_normal_conditions() {
    let (engine, mut rx) = test_engine_with_emitter();
    let migration = test_pending_migration(3);

    let io_cost = PhysicalCost {
        latency_ms: 0.5,
        bandwidth_bps: 10_000_000.0,
        reliability: 0.99,
        io_pressure: 0.1,
        queue_depth: 0,
    };

    let pressure = PressureState::new();
    let backpressure = BackpressureAction::Allow;

    let result = engine.decide_migration(&migration, &pressure, &io_cost, &backpressure);

    // Should proceed: low pressure, low cost, Allow backpressure
    assert!(result.is_some(), "migration should proceed under normal conditions");

    // Verify MigrationDecided event was emitted
    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(e, Event::MigrationDecided { chunk_id, .. } if *chunk_id == migration.chunk_id)),
        "should emit MigrationDecided event, got: {:?}",
        events
    );
}

// ─── Test 4: Deterministic I/O cost from backends ────────────────────────────

#[test]
fn test_deterministic_io_cost_from_backends() {
    let engine = test_engine();

    // Estimate cost for RAM -> Simulation migration
    let cost = engine.estimate_io_cost(TierId::Ram, TierId::Simulation, 4096);

    // RAM backend has very low latency (0.01ms) and high bandwidth (10GB/s)
    // Sim backend has config-based latency and bandwidth
    // Combined cost should be deterministic
    assert!(cost.latency_ms > 0.0, "latency should be positive");
    assert!(cost.bandwidth_bps > 0.0, "bandwidth should be positive");

    // Same call should produce same result (deterministic)
    let cost2 = engine.estimate_io_cost(TierId::Ram, TierId::Simulation, 4096);
    assert!(
        (cost.latency_ms - cost2.latency_ms).abs() < f64::EPSILON,
        "cost should be deterministic"
    );
    assert!(
        (cost.bandwidth_bps - cost2.bandwidth_bps).abs() < f64::EPSILON,
        "bandwidth should be deterministic"
    );
}

// ─── Test 5: Event stream captures all migration decisions ───────────────────

#[test]
fn test_event_stream_captures_decisions() {
    let (engine, mut rx) = test_engine_with_emitter();

    // Case 1: Normal migration -> MigrationDecided
    let m1 = test_pending_migration(10);
    let io_cost_ok = PhysicalCost {
        latency_ms: 0.5,
        bandwidth_bps: 10_000_000.0,
        reliability: 0.99,
        io_pressure: 0.1,
        queue_depth: 0,
    };
    let _ = engine.decide_migration(&m1, &PressureState::new(), &io_cost_ok, &BackpressureAction::Allow);

    // Case 2: High I/O pressure -> MigrationDeferred
    let m2 = test_pending_migration(11);
    let io_cost_pressure = PhysicalCost {
        latency_ms: 1.0,
        bandwidth_bps: 1_000_000.0,
        reliability: 0.99,
        io_pressure: 0.95,
        queue_depth: 10,
    };
    let _ = engine.decide_migration(&m2, &PressureState::new(), &io_cost_pressure, &BackpressureAction::Allow);

    // Case 3: Backpressure reject -> MigrationRejected
    let m3 = test_pending_migration(12);
    let _ = engine.decide_migration(&m3, &PressureState::new(), &io_cost_ok, &BackpressureAction::Reject);

    let events = drain_events(&mut rx);

    // Should have at least 3 events
    assert!(events.len() >= 3, "should have at least 3 events, got {}", events.len());

    // Verify all three event types are present
    assert!(events.iter().any(|e| matches!(e, Event::MigrationDecided { .. })), "should have MigrationDecided");
    assert!(events.iter().any(|e| matches!(e, Event::MigrationDeferred { .. })), "should have MigrationDeferred");
    assert!(events.iter().any(|e| matches!(e, Event::MigrationRejected { .. })), "should have MigrationRejected");
}

// ─── Test 6: Replay equivalence — same inputs produce same decisions ─────────

#[test]
fn test_replay_equivalence() {
    // Create two identical engines with same seed backends
    let (engine1, mut rx1) = test_engine_with_emitter();
    let (engine2, mut rx2) = test_engine_with_emitter();

    let migration = test_pending_migration(20);
    let io_cost = PhysicalCost {
        latency_ms: 1.0,
        bandwidth_bps: 5_000_000.0,
        reliability: 0.95,
        io_pressure: 0.3,
        queue_depth: 5,
    };
    let pressure = PressureState {
        memory_pressure: 0.2,
        vram_pressure: 0.1,
        io_pressure: 0.3,
        queue_depth: 5,
        throughput_bps: 0,
    };
    let backpressure = BackpressureAction::Allow;

    let result1 = engine1.decide_migration(&migration, &pressure, &io_cost, &backpressure);
    let result2 = engine2.decide_migration(&migration, &pressure, &io_cost, &backpressure);

    // Both engines should produce the same decision
    assert_eq!(
        result1.is_some(),
        result2.is_some(),
        "replay should produce same decision"
    );

    // Both should emit the same event type
    let events1 = drain_events(&mut rx1);
    let events2 = drain_events(&mut rx2);

    let names1: Vec<&str> = events1.iter().map(|e| e.event_name()).collect();
    let names2: Vec<&str> = events2.iter().map(|e| e.event_name()).collect();
    assert_eq!(names1, names2, "replay should emit same event types");
}

// ─── Test: BackpressureController I/O pressure integration ────────────────────

#[test]
fn test_backpressure_io_pressure_escalation() {
    let trace_log = Arc::new(TraceLog::new(1000));
    let config = BackpressureConfig::default();
    let controller = BackpressureController::new(config, trace_log);

    // Low overall pressure but high I/O pressure should still throttle
    let pressure = PressureState {
        memory_pressure: 0.1,
        vram_pressure: 0.0,
        io_pressure: 0.7, // Above soft limit (0.6)
        queue_depth: 5,
        throughput_bps: 0,
    };

    let action = controller.evaluate(&pressure);
    assert_eq!(
        action,
        BackpressureAction::Throttle,
        "high I/O pressure should throttle even when overall pressure is low"
    );
}

#[test]
fn test_backpressure_io_hard_limit_escalates_to_reject() {
    let trace_log = Arc::new(TraceLog::new(1000));
    let config = BackpressureConfig::default();
    let controller = BackpressureController::new(config, trace_log);

    // I/O pressure above hard limit should escalate to Reject
    let pressure = PressureState {
        memory_pressure: 0.1,
        vram_pressure: 0.0,
        io_pressure: 0.9, // Above hard limit (0.85)
        queue_depth: 5,
        throughput_bps: 0,
    };

    let action = controller.evaluate(&pressure);
    assert_eq!(
        action,
        BackpressureAction::Reject,
        "I/O pressure above hard limit should reject"
    );
}

#[test]
fn test_backpressure_queue_depth_escalation() {
    let trace_log = Arc::new(TraceLog::new(1000));
    let config = BackpressureConfig::default();
    let controller = BackpressureController::new(config, trace_log);

    // Queue depth above threshold should throttle
    let pressure = PressureState {
        memory_pressure: 0.1,
        vram_pressure: 0.0,
        io_pressure: 0.1,
        queue_depth: 40, // Above threshold (32)
        throughput_bps: 0,
    };

    let action = controller.evaluate(&pressure);
    assert_eq!(
        action,
        BackpressureAction::Throttle,
        "high queue depth should throttle"
    );
}

#[test]
fn test_backpressure_queue_depth_extreme_escalates_to_reject() {
    let trace_log = Arc::new(TraceLog::new(1000));
    let config = BackpressureConfig::default();
    let controller = BackpressureController::new(config, trace_log);

    // Queue depth above 2x threshold should reject
    let pressure = PressureState {
        memory_pressure: 0.1,
        vram_pressure: 0.0,
        io_pressure: 0.1,
        queue_depth: 70, // Above 2x threshold (64)
        throughput_bps: 0,
    };

    let action = controller.evaluate(&pressure);
    assert_eq!(
        action,
        BackpressureAction::Reject,
        "extreme queue depth should reject"
    );
}

// ─── Test: SimBackend cost_model returns config-based costs ──────────────────

#[test]
fn test_sim_backend_cost_model() {
    use ghost_sim::config::{BandwidthConfig, LatencyConfig};

    // Backend with high latency and low bandwidth
    let config = SimConfig::with_capacity(1024 * 1024)
        .with_seed(42)
        .with_latency(LatencyConfig {
            base: std::time::Duration::from_millis(5),
            per_byte: std::time::Duration::from_nanos(100),
            jitter_fraction: 0.0,
        })
        .with_bandwidth(BandwidthConfig {
            bytes_per_second: 1_000_000, // 1 MB/s
        });

    let backend = SimBackend::new(config);
    let cost = backend.cost_model();

    // Latency should reflect config: base + per_byte * 1024
    assert!(cost.latency_ms >= 5.0, "latency should be at least 5ms");
    // Bandwidth should reflect config
    assert!(
        (cost.bandwidth_bps - 1_000_000.0).abs() < f64::EPSILON,
        "bandwidth should match config"
    );
}

// ─── Test: Cost score ordering ───────────────────────────────────────────────

#[test]
fn test_cost_score_ordering() {
    let cheap = PhysicalCost {
        latency_ms: 0.01,
        bandwidth_bps: 10_000_000_000.0,
        reliability: 1.0,
        io_pressure: 0.0,
        queue_depth: 0,
    };

    let expensive = PhysicalCost {
        latency_ms: 100.0,
        bandwidth_bps: 1_000_000.0,
        reliability: 0.5,
        io_pressure: 0.9,
        queue_depth: 100,
    };

    assert!(
        cheap.cost_score() < expensive.cost_score(),
        "cheap cost score should be lower than expensive"
    );
}

// ─── Test: is_too_pressured boundary ─────────────────────────────────────────

#[test]
fn test_is_too_pressured_boundary() {
    // Just below threshold
    let ok = PhysicalCost {
        latency_ms: 1.0,
        bandwidth_bps: 1_000_000.0,
        reliability: 0.99,
        io_pressure: 0.84,
        queue_depth: 64,
    };
    assert!(!ok.is_too_pressured(), "0.84 pressure should not be too high");

    // Just above threshold
    let pressured = PhysicalCost {
        latency_ms: 1.0,
        bandwidth_bps: 1_000_000.0,
        reliability: 0.99,
        io_pressure: 0.86,
        queue_depth: 10,
    };
    assert!(pressured.is_too_pressured(), "0.86 pressure should be too high");

    // Queue depth above 64
    let deep_queue = PhysicalCost {
        latency_ms: 1.0,
        bandwidth_bps: 1_000_000.0,
        reliability: 0.99,
        io_pressure: 0.1,
        queue_depth: 65,
    };
    assert!(deep_queue.is_too_pressured(), "queue depth 65 should be too high");
}
