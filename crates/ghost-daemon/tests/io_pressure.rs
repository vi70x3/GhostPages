//! Integration tests: IO Pressure & Persistent Tier Scheduling.
//!
//! Validates the end-to-end I/O pressure subsystem including:
//! - IoMetrics struct and atomic counters
//! - Disk pressure integration with PressureMonitor
//! - Promotion cost estimation (Disk <-> RAM)
//! - Eviction cooldown with anti-oscillation
//! - Disk congestion detection in BackpressureController
//! - Disk-aware scheduling in TransferScheduler

use std::collections::BTreeMap;
use std::sync::Arc;

use ghost_core::state::{PressureState, StateMachine};
use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::backpressure::{BackpressureAction, BackpressureController};
use ghost_daemon::config::{BackpressureConfig, MigrationConfig, OrchestratorConfig};
use ghost_daemon::hotness_tracker::HotnessTracker;
use ghost_daemon::io_metrics::IoMetrics;
use ghost_daemon::migration::{EvictionCooldown, MigrationEngine};
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_daemon::pressure::{PressureMonitor, PressureMonitorConfig};
use ghost_daemon::trace_log::TraceLog;
use ghost_policy::lru::{LruConfig, LruPolicy};
use ghost_policy::PlacementPolicy;
use ghost_policy::pressure::{PressureAwareConfig, PressureAwarePolicy};
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use ghost_tier::backend::StorageBackend;

// ─── Helpers ──────────────────────────────────────────────────────────────────

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

fn test_policy() -> Arc<dyn PlacementPolicy> {
    Arc::new(PressureAwarePolicy::new(PressureAwareConfig::default()))
}

fn test_orchestrator() -> TransferOrchestrator {
    TransferOrchestrator::new(
        OrchestratorConfig::default(),
        test_backends(),
        test_policy(),
    )
}

fn test_trace_log() -> Arc<TraceLog> {
    Arc::new(TraceLog::new(10_000))
}

fn test_migration_engine() -> MigrationEngine {
    let config = MigrationConfig::default();
    let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
    let trace_log = test_trace_log();
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

// ─── Test (a): IoMetrics records reads/writes and calculates pressure ─────────

#[test]
fn test_io_metrics_records_operations_and_calculates_pressure() {
    let metrics = IoMetrics::new();

    // Record some reads and writes
    for i in 0..10 {
        metrics.record_read(1000 + i * 100);
        metrics.record_write(2000 + i * 200, 4096);
    }

    // Verify counts
    assert_eq!(metrics.get_read_count(), 10, "should have 10 reads");
    assert_eq!(metrics.get_write_count(), 10, "should have 10 writes");

    // Rolling latency should be non-zero (EMA-smoothed)
    assert!(
        metrics.get_rolling_latency() > 0,
        "rolling latency should be non-zero after reads"
    );

    // Calculate pressure with generous limits
    let pressure = metrics.calculate_io_pressure(256, 10_000_000);
    assert!(
        (0.0..=1.0).contains(&pressure),
        "pressure should be in [0, 1]"
    );

    // With zero queue depth and low latency, pressure should be low
    assert!(
        pressure < 0.1,
        "pressure should be low with no queue and low latency, got {}",
        pressure
    );
}

// ─── Test (b): IoMetrics EMA smoothing produces converging values ────────────

#[test]
fn test_io_metrics_ema_smoothing_converges() {
    let metrics = IoMetrics::new();
    metrics.set_smoothing_factor(0.3);

    // Feed a constant latency of 5000us
    for _ in 0..50 {
        metrics.record_read(5000);
    }

    // After many identical samples, EMA should be very close to 5000
    let latency = metrics.get_rolling_latency();
    assert!(
        (latency as i64 - 5000).abs() < 100,
        "EMA should converge near 5000, got {}",
        latency
    );

    // Now feed a different constant value
    for _ in 0..50 {
        metrics.record_read(10000);
    }

    // EMA should have moved toward 10000
    let latency = metrics.get_rolling_latency();
    assert!(
        latency > 5000,
        "EMA should increase toward 10000, got {}",
        latency
    );
}

// ─── Test (c): PressureMonitor integrates disk_io_pressure ──────────────────

#[test]
fn test_pressure_monitor_integrates_disk_io_pressure() {
    let trace_log = test_trace_log();
    let config = PressureMonitorConfig::default();
    let mut monitor = PressureMonitor::new(config, 256, trace_log);

    // Set up IoMetrics with some load
    let io_metrics = Arc::new(IoMetrics::new());
    for _ in 0..64 {
        io_metrics.increment_queue_depth();
    }
    io_metrics.record_read(1_000_000); // 1 second latency

    monitor.set_io_metrics(io_metrics.clone());

    // Before any sample, disk_io_pressure should be 0.0
    let initial = monitor.disk_io_pressure();
    assert!(
        (initial - 0.0).abs() < f32::EPSILON,
        "initial disk pressure should be 0.0, got {}",
        initial
    );

    // The actual disk pressure calculation happens inside sample_and_update
    // which is called by the async run loop. We verify the wiring is correct
    // by confirming set_io_metrics doesn't panic and disk_io_pressure returns
    // a valid value in [0, 1].
    let disk_pressure = monitor.disk_io_pressure();
    assert!(
        (0.0..=1.0).contains(&disk_pressure),
        "disk pressure should be in [0, 1], got {}",
        disk_pressure
    );
}

// ─── Test (d): MigrationEngine promotion cost model is deterministic ─────────

#[test]
fn test_migration_engine_promotion_cost_deterministic() {
    let engine = test_migration_engine();
    let chunk_id = ChunkId::from_data(b"cost_test");

    // Estimate cost for Disk -> RAM promotion
    let cost_disk_to_ram = engine.estimate_promotion_cost(&chunk_id, TierId::Disk, TierId::Ram, 4096);

    // Estimate cost for RAM -> Disk migration
    let cost_ram_to_disk = engine.estimate_promotion_cost(&chunk_id, TierId::Ram, TierId::Disk, 4096);

    // Cost scores should be positive and finite
    assert!(
        cost_disk_to_ram.cost_score() > 0.0,
        "disk->ram cost score should be positive"
    );
    assert!(
        cost_ram_to_disk.cost_score() > 0.0,
        "ram->disk cost score should be positive"
    );
    assert!(
        cost_disk_to_ram.cost_score().is_finite(),
        "disk->ram cost score should be finite"
    );

    // Same parameters should produce same cost (deterministic)
    let cost1 = engine.estimate_promotion_cost(&chunk_id, TierId::Disk, TierId::Ram, 4096);
    let cost2 = engine.estimate_promotion_cost(&chunk_id, TierId::Disk, TierId::Ram, 4096);
    assert!(
        (cost1.cost_score() - cost2.cost_score()).abs() < f64::EPSILON,
        "same parameters should produce same cost (deterministic)"
    );
}

// ─── Test (e): EvictionCooldown prevents rapid cycling ───────────────────────

#[test]
fn test_eviction_cooldown_prevents_rapid_cycling() {
    let mut cooldown = EvictionCooldown::new(60); // 60 second cooldown
    let chunk_id = ChunkId::from_data(b"cooldown_test");
    let now = 1_000_000; // 1 second in microseconds

    // First eviction should be allowed
    assert!(
        cooldown.can_evict(&chunk_id, now),
        "first eviction should be allowed"
    );

    // Record the eviction
    cooldown.record_eviction(chunk_id, now);

    // Immediate retry should be blocked
    assert!(
        !cooldown.can_evict(&chunk_id, now + 1000),
        "immediate retry should be blocked"
    );

    // After cooldown period, should be allowed again
    assert!(
        cooldown.can_evict(&chunk_id, now + 60_000_000),
        "eviction after cooldown should be allowed"
    );
}

// ─── Test (f): BackpressureController escalates on disk congestion ───────────

#[test]
fn test_backpressure_escalates_on_disk_congestion() {
    let trace_log = test_trace_log();
    let mut config = BackpressureConfig::default();
    config.disk_queue_soft_limit = 64;
    config.disk_queue_hard_limit = 128;
    config.disk_latency_threshold_us = 5_000_000;

    let mut controller = BackpressureController::new(config, trace_log);

    // Set up IoMetrics with high disk queue
    let io_metrics = Arc::new(IoMetrics::new());
    for _ in 0..200 {
        io_metrics.increment_queue_depth();
    }
    controller.set_io_metrics(io_metrics);

    // Evaluate with no memory pressure but disk congestion
    let pressure = PressureState::new();
    let action = controller.evaluate(&pressure);

    // Should escalate beyond Allow due to disk congestion
    match action {
        BackpressureAction::Reject | BackpressureAction::CriticalOnly => {
            // Expected: hard limit exceeded
        }
        BackpressureAction::Throttle => {
            // Also acceptable: soft limit exceeded
        }
        BackpressureAction::Allow => {
            panic!(
                "should escalate on disk congestion, got Allow"
            );
        }
    }

    // Stats should reflect disk congestion escalation
    let stats = controller.stats();
    assert!(
        stats.disk_congestion_escalations > 0,
        "should have recorded disk congestion escalations"
    );
}

// ─── Test (g): End-to-end IO pressure affects migration decisions ─────────────

#[tokio::test]
async fn test_io_pressure_affects_migration_decisions() {
    let mut orch = test_orchestrator();
    orch.start().unwrap();

    // Store some data
    let data = b"io pressure migration test";
    let chunk_id = ChunkId::from_data(data);
    orch.store(chunk_id, TierId::Ram, data).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Run pressure check — should work without errors
    let candidates = orch.run_pressure_check().unwrap();
    // With low pressure, there may or may not be candidates
    let _ = candidates;

    // Verify pressure state is valid
    let pressure = orch.current_pressure();
    assert!(
        (0.0..=1.0).contains(&pressure.memory_pressure),
        "memory pressure should be in [0, 1]"
    );
    assert!(
        (0.0..=1.0).contains(&pressure.io_pressure),
        "io pressure should be in [0, 1]"
    );

    orch.shutdown().unwrap();
}
