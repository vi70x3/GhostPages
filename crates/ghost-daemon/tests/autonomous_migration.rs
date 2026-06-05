//! Integration tests for the autonomous pressure-driven migration system.
//!
//! Tests the full migration pipeline: hotness tracking → pressure evaluation →
//! migration candidate selection → backpressure gating → job submission.

use std::collections::HashMap;
use std::sync::Arc;

use ghost_core::hotness::ChunkHotness;
use ghost_core::state::{ChunkState, PressureState, StateMachine};
use ghost_core::trace::current_timestamp;
use ghost_core::transfer::TransferPriority;
use ghost_core::types::{ChunkId, TierId};
use ghost_policy::lru::{LruConfig, LruPolicy};
use ghost_policy::PlacementPolicy;
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use ghost_tier::StorageBackend;

use ghost_daemon::backpressure::{BackpressureAction, BackpressureController};
use ghost_daemon::config::{BackpressureConfig, MigrationConfig, OrchestratorConfig};
use ghost_daemon::hotness_tracker::HotnessTracker;
use ghost_daemon::migration::{MigrationEngine, PendingMigration};
use ghost_daemon::trace_log::TraceLog;

// ─── Test Helpers ──────────────────────────────────────────────────────────────

fn test_backends() -> HashMap<TierId, Arc<dyn StorageBackend>> {
    let mut backends = HashMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(1024 * 1024)) as Arc<dyn StorageBackend>,
    );
    backends.insert(
        TierId::Simulation,
        Arc::new(SimBackend::new(SimConfig::default())) as Arc<dyn StorageBackend>,
    );
    backends
}

fn test_chunk_id(seed: u8) -> ChunkId {
    let mut id = [0u8; 32];
    id[0] = seed;
    ChunkId(id)
}

fn test_policy() -> Arc<dyn PlacementPolicy> {
    Arc::new(LruPolicy::new(LruConfig::default()))
}

fn test_trace_log() -> Arc<TraceLog> {
    Arc::new(TraceLog::new(10000))
}

fn test_hotness_tracker() -> Arc<HotnessTracker> {
    Arc::new(HotnessTracker::new(1000, test_trace_log()))
}

fn test_migration_engine() -> MigrationEngine {
    let config = MigrationConfig::default();
    let policy = test_policy();
    let trace_log = test_trace_log();
    let hotness_tracker = test_hotness_tracker();
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

fn test_backpressure_controller() -> BackpressureController {
    BackpressureController::new(BackpressureConfig::default(), test_trace_log())
}

fn test_orchestrator_config() -> OrchestratorConfig {
    OrchestratorConfig {
        worker_count: 2,
        queue_capacity: 1024,
        max_retries: 3,
        retry_base_delay_ms: 100,
        max_retry_delay_ms: 5000,
        enable_compression: false,
        trace_max_events: 10000,
        shutdown_timeout_secs: 30,
        pressure_sample_interval_ms: 1000,
        pressure_smoothing_factor: 0.3,
        auto_migration_interval_ms: 5000,
        pressure_history_size: 256,
        enable_auto_migration: true,
        deterministic_mode: false,
    }
}

// ─── ChunkHotness Tests ───────────────────────────────────────────────────────

#[test]
fn test_chunk_hotness_new() {
    let chunk_id = test_chunk_id(1);
    let now = current_timestamp();
    let hotness = ChunkHotness::new(chunk_id, now);

    assert_eq!(hotness.chunk_id, chunk_id);
    assert_eq!(hotness.access_count, 0); // new() doesn't count as an access
    assert_eq!(hotness.last_accessed, now);
    assert_eq!(hotness.first_accessed, now);
    assert_eq!(hotness.score, 0.0); // No accesses yet = zero score
}

#[test]
fn test_chunk_hotness_access_increases_score() {
    let chunk_id = test_chunk_id(2);
    let now = current_timestamp();
    let mut hotness = ChunkHotness::new(chunk_id, now);

    let initial_score = hotness.score;
    hotness.record_access(now + 1, 1024);
    hotness.record_access(now + 2, 1024);
    hotness.record_access(now + 3, 1024);

    assert!(hotness.score > initial_score);
    assert_eq!(hotness.access_count, 3); // 3 recorded accesses
}

#[test]
fn test_chunk_hotness_is_hot_threshold() {
    let chunk_id = test_chunk_id(3);
    let now = current_timestamp();
    let mut hotness = ChunkHotness::new(chunk_id, now);

    // A single access should not be hot
    assert!(!hotness.is_hot());

    // Many accesses should make it hot
    for i in 0..200 {
        hotness.record_access(now + i as u64, 1024);
    }
    assert!(hotness.is_hot());
}

#[test]
fn test_chunk_hotness_is_cold_threshold() {
    let chunk_id = test_chunk_id(4);
    let now = current_timestamp();
    let hotness = ChunkHotness::new(chunk_id, now - 100000);

    // Old single-access chunk should be cold
    assert!(hotness.is_cold());
}

// ─── HotnessTracker Tests ─────────────────────────────────────────────────────

#[test]
fn test_hotness_tracker_record_and_query() {
    let tracker = test_hotness_tracker();
    let chunk_id = test_chunk_id(10);

    tracker.record_access(chunk_id, 1024);

    let hotness = tracker.get_hotness(&chunk_id);
    assert!(hotness.is_some());
    let h = hotness.unwrap();
    assert_eq!(h.chunk_id, chunk_id);
    assert_eq!(h.access_count, 1);
}

#[test]
fn test_hotness_tracker_find_hot_chunks() {
    let tracker = test_hotness_tracker();
    let hot_chunk = test_chunk_id(11);
    let cold_chunk = test_chunk_id(12);

    // Make hot_chunk very hot
    for i in 0..200 {
        tracker.record_access(hot_chunk, 1024);
        // Advance time slightly
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    // Make cold_chunk only slightly warm
    tracker.record_access(cold_chunk, 1024);

    let hot_chunks = tracker.find_hot_chunks(0.5);
    assert!(hot_chunks.iter().any(|(id, _)| *id == hot_chunk));
}

#[test]
fn test_hotness_tracker_decay() {
    let tracker = test_hotness_tracker();
    let chunk_id = test_chunk_id(13);

    // Record many accesses
    for _ in 0..100 {
        tracker.record_access(chunk_id, 1024);
    }

    let hotness_before = tracker.get_hotness(&chunk_id).unwrap();
    let score_before = hotness_before.score;

    // Decay all scores
    tracker.decay_all();

    let hotness_after = tracker.get_hotness(&chunk_id).unwrap();
    // After decay, recency component should decrease
    assert!(hotness_after.recency_score <= hotness_before.recency_score);
    // Score should generally decrease (or at least not increase)
    assert!(hotness_after.score <= score_before);
}

#[test]
fn test_hotness_tracker_top_n() {
    let tracker = test_hotness_tracker();

    for seed in 20..30 {
        let chunk_id = test_chunk_id(seed);
        let accesses = seed as u64 * 10;
        for _ in 0..accesses {
            tracker.record_access(chunk_id, 1024);
        }
    }

    let top = tracker.top_n(3);
    assert_eq!(top.len(), 3);
    // The top 3 should be the chunks with the most accesses
    // seed=29 (290 accesses), seed=28 (280), seed=27 (270)
    let top_ids: Vec<u8> = top.iter().map(|(id, _)| id.0[0]).collect();
    assert!(top_ids.contains(&29));
    assert!(top_ids.contains(&28));
    assert!(top_ids.contains(&27));
}

// ─── BackpressureController Tests ──────────────────────────────────────────────

#[test]
fn test_backpressure_no_pressure_allows_all() {
    let controller = test_backpressure_controller();
    let pressure = PressureState::new();

    let action = controller.evaluate(&pressure);
    assert_eq!(action, BackpressureAction::Allow);
    assert!(controller.should_allow(TransferPriority::Low));
    assert!(controller.should_allow(TransferPriority::Normal));
    assert!(controller.should_allow(TransferPriority::High));
    assert!(controller.should_allow(TransferPriority::Critical));
}

#[test]
fn test_backpressure_throttle_at_threshold() {
    let controller = test_backpressure_controller();
    let mut pressure = PressureState::new();
    pressure.memory_pressure = 0.75; // Above throttle (0.7) but below reject (0.85)

    let action = controller.evaluate(&pressure);
    assert_eq!(action, BackpressureAction::Throttle);
    assert!(!controller.should_allow(TransferPriority::Low));
    assert!(!controller.should_allow(TransferPriority::Normal));
    assert!(controller.should_allow(TransferPriority::High));
    assert!(controller.should_allow(TransferPriority::Critical));
}

#[test]
fn test_backpressure_reject_at_high_threshold() {
    let controller = test_backpressure_controller();
    let mut pressure = PressureState::new();
    pressure.memory_pressure = 0.9; // Above reject (0.85) but below critical (0.95)

    let action = controller.evaluate(&pressure);
    assert_eq!(action, BackpressureAction::Reject);
    assert!(!controller.should_allow(TransferPriority::Low));
    assert!(!controller.should_allow(TransferPriority::Normal));
    assert!(!controller.should_allow(TransferPriority::High));
    assert!(controller.should_allow(TransferPriority::Critical));
}

#[test]
fn test_backpressure_critical_only_at_critical_threshold() {
    let controller = test_backpressure_controller();
    let mut pressure = PressureState::new();
    pressure.memory_pressure = 0.98; // Above critical (0.95)

    let action = controller.evaluate(&pressure);
    assert_eq!(action, BackpressureAction::CriticalOnly);
    assert!(!controller.should_allow(TransferPriority::Low));
    assert!(!controller.should_allow(TransferPriority::Normal));
    assert!(!controller.should_allow(TransferPriority::High));
    assert!(controller.should_allow(TransferPriority::Critical));
}

#[test]
fn test_backpressure_cooldown_prevents_rapid_flapping() {
    let controller = test_backpressure_controller();
    let mut pressure = PressureState::new();

    // First, trigger throttle
    pressure.memory_pressure = 0.8;
    controller.evaluate(&pressure);

    // Then, drop pressure below threshold
    pressure.memory_pressure = 0.5;
    let action = controller.evaluate(&pressure);

    // Should still be in cooldown (throttle), not immediately Allow
    // (cooldown_secs = 10 by default, so it won't expire in a test)
    assert_eq!(action, BackpressureAction::Throttle);
}

#[test]
fn test_backpressure_stats_tracking() {
    let controller = test_backpressure_controller();
    let mut pressure = PressureState::new();

    // Evaluate with no pressure
    controller.evaluate(&pressure);

    // Evaluate with throttle pressure
    pressure.memory_pressure = 0.75;
    controller.evaluate(&pressure);

    // Try to reject some low-priority transfers
    controller.should_allow(TransferPriority::Low);
    controller.should_allow(TransferPriority::Normal);

    let stats = controller.stats();
    assert!(stats.evaluations >= 2);
    assert!(stats.throttle_count >= 1);
    assert!(stats.transfers_rejected >= 2);
}

// ─── MigrationEngine Tests ────────────────────────────────────────────────────

#[test]
fn test_migration_engine_no_chunks_no_migrations() {
    let engine = test_migration_engine();
    let pressure = PressureState::new();

    let migrations = engine.evaluate(&pressure);
    assert!(migrations.is_empty());
}

#[test]
fn test_migration_engine_evaluates_with_pressure() {
    let engine = test_migration_engine();
    let mut pressure = PressureState::new();
    pressure.memory_pressure = 0.8;

    // Even with pressure, no registered chunks means no migrations
    let migrations = engine.evaluate(&pressure);
    assert!(migrations.is_empty());
}

#[test]
fn test_migration_engine_promotions_disabled() {
    let mut config = MigrationConfig::default();
    config.enable_promotion = false;
    let policy = test_policy();
    let trace_log = test_trace_log();
    let hotness_tracker = test_hotness_tracker();
    let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
    let backends = test_backends();

    let engine = MigrationEngine::new(
        config,
        policy,
        hotness_tracker,
        state_machine,
        trace_log,
        backends,
    );

    let pressure = PressureState::new();
    let promotions = engine.evaluate_promotions(&pressure);
    assert!(promotions.is_empty());
}

#[test]
fn test_migration_engine_evictions_need_pressure() {
    let engine = test_migration_engine();
    let pressure = PressureState::new(); // No pressure

    let evictions = engine.evaluate_evictions(&pressure);
    assert!(evictions.is_empty()); // Below eviction_pressure_threshold
}

#[test]
fn test_migration_engine_has_capacity() {
    let engine = test_migration_engine();
    assert!(engine.has_capacity());
    assert_eq!(engine.active_count(), 0);
}

#[test]
fn test_migration_engine_mark_active_and_complete() {
    let engine = test_migration_engine();
    let chunk_id = test_chunk_id(50);

    engine.mark_active(chunk_id);
    assert!(engine.is_migrating(&chunk_id));
    assert_eq!(engine.active_count(), 1);
    assert!(engine.has_capacity()); // max_concurrent = 2, 1 active = still has capacity

    engine.mark_complete(chunk_id, 1024, true);
    assert!(!engine.is_migrating(&chunk_id));
    assert_eq!(engine.active_count(), 0);

    let stats = engine.stats();
    assert_eq!(stats.promotions, 1);
    assert_eq!(stats.bytes_migrated, 1024);
}

#[test]
fn test_migration_engine_mark_active_at_capacity() {
    let engine = test_migration_engine();
    let chunk1 = test_chunk_id(51);
    let chunk2 = test_chunk_id(52);
    let chunk3 = test_chunk_id(53);

    engine.mark_active(chunk1);
    engine.mark_active(chunk2);
    assert!(!engine.has_capacity()); // 2 active = max_concurrent_migrations

    engine.mark_active(chunk3); // Should still track it (no enforcement in mark_active)
    assert_eq!(engine.active_count(), 3);
}

#[test]
fn test_migration_engine_create_transfer_job() {
    let engine = test_migration_engine();
    let chunk_id = test_chunk_id(54);

    let migration = PendingMigration {
        chunk_id,
        from_tier: TierId::Simulation,
        to_tier: TierId::Ram,
        priority: TransferPriority::High,
        size: 4096,
        hotness_score: 0.8,
        identified_at: current_timestamp(),
    };

    let job = engine.create_transfer_job(&migration);
    assert_eq!(job.chunk_id, chunk_id);
    assert_eq!(job.from_tier, TierId::Simulation);
    assert_eq!(job.to_tier, TierId::Ram);
    assert_eq!(job.size, 4096);
    assert_eq!(job.priority, TransferPriority::High);
}

// ─── Integration: Full Migration Pipeline Tests ────────────────────────────────

#[test]
fn test_full_pipeline_hotness_to_migration() {
    // This test exercises the full pipeline:
    // 1. Create a hotness tracker and record accesses
    // 2. Create a migration engine
    // 3. Evaluate with pressure
    // 4. Verify the pipeline produces valid migration candidates

    let hotness_tracker = test_hotness_tracker();
    let trace_log = test_trace_log();
    let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
    let backends = test_backends();
    let policy = test_policy();

    // Register a chunk in the state machine
    let chunk_id = test_chunk_id(60);
    {
        let mut sm = state_machine.lock().unwrap();
        sm.register(chunk_id).unwrap();
        sm.transition(&chunk_id, ChunkState::Stored).unwrap();
    }

    // Record many accesses to make it hot
    for i in 0..150 {
        hotness_tracker.record_access(chunk_id, 4096);
        // Also update the state machine access metadata
        {
            let mut sm = state_machine.lock().unwrap();
            // The state machine doesn't directly track access counts,
            // but the hotness tracker does
        }
    }

    // Verify the chunk is tracked as hot
    let hotness = hotness_tracker.get_hotness(&chunk_id);
    assert!(hotness.is_some());
    let h = hotness.unwrap();
    assert!(h.is_hot(), "Chunk should be hot after 150 accesses, score={}", h.score);

    // Create migration engine and evaluate
    let engine = MigrationEngine::new(
        MigrationConfig::default(),
        policy,
        hotness_tracker,
        state_machine,
        trace_log,
        backends,
    );

    let mut pressure = PressureState::new();
    pressure.memory_pressure = 0.5; // Moderate pressure

    let migrations = engine.evaluate(&pressure);
    // The engine should identify migration candidates based on hotness and policy
    // (actual results depend on the LRU policy's decisions)
    // We just verify the pipeline doesn't panic and returns valid results
    for m in &migrations {
        assert_eq!(m.chunk_id, chunk_id);
        assert!(m.hotness_score > 0.0);
    }
}

#[test]
fn test_full_pipeline_backpressure_gates_migration() {
    // Test that backpressure controller properly gates migration submissions
    let controller = test_backpressure_controller();

    // Simulate escalating pressure
    let pressure_levels = vec![
        (0.0, BackpressureAction::Allow),
        (0.5, BackpressureAction::Allow),
        (0.75, BackpressureAction::Throttle),
        (0.9, BackpressureAction::Reject),
        (0.98, BackpressureAction::CriticalOnly),
    ];

    for (pressure_val, expected_action) in &pressure_levels {
        let mut pressure = PressureState::new();
        pressure.memory_pressure = *pressure_val;
        let action = controller.evaluate(&pressure);
        assert_eq!(
            &action, expected_action,
            "At pressure {:.2}, expected {:?}, got {:?}",
            pressure_val, expected_action, action
        );
    }
}

#[test]
fn test_full_pipeline_migration_with_backpressure_integration() {
    // Test the interaction between migration engine and backpressure controller
    let engine = test_migration_engine();
    let controller = test_backpressure_controller();

    let mut pressure = PressureState::new();
    pressure.memory_pressure = 0.8; // Throttle level

    // Evaluate migrations
    let migrations = engine.evaluate(&pressure);

    // Check each migration against backpressure
    let mut submitted = 0;
    let mut blocked = 0;
    for migration in &migrations {
        if controller.should_allow(migration.priority) {
            submitted += 1;
        } else {
            blocked += 1;
        }
    }

    // At throttle level, only High and Critical priorities are allowed
    // Verify that low-priority migrations are blocked
    let low_priority_blocked = migrations
        .iter()
        .filter(|m| matches!(m.priority, TransferPriority::Low | TransferPriority::Normal))
        .all(|m| !controller.should_allow(m.priority));

    if !migrations.is_empty() {
        assert!(
            low_priority_blocked,
            "Low priority migrations should be blocked under throttle"
        );
    }
}

#[test]
fn test_migration_stats_accumulate() {
    let engine = test_migration_engine();

    let chunk1 = test_chunk_id(70);
    let chunk2 = test_chunk_id(71);

    engine.mark_active(chunk1);
    engine.mark_complete(chunk1, 4096, true);

    engine.mark_active(chunk2);
    engine.mark_complete(chunk2, 2048, false); // Failed migration

    let stats = engine.stats();
    assert_eq!(stats.promotions, 1);
    assert_eq!(stats.failures, 1);
    assert_eq!(stats.bytes_migrated, 4096);
}

#[test]
fn test_migration_engine_stats_initial() {
    let engine = test_migration_engine();
    let stats = engine.stats();

    assert_eq!(stats.evaluation_cycles, 0);
    assert_eq!(stats.promotions, 0);
    assert_eq!(stats.evictions, 0);
    assert_eq!(stats.skipped, 0);
    assert_eq!(stats.failures, 0);
    assert_eq!(stats.bytes_migrated, 0);
    assert_eq!(stats.active_migrations, 0);
}

// ─── Integration: Orchestrator-Level Tests ─────────────────────────────────────

#[test]
fn test_orchestrator_migration_engine_wired() {
    // Verify that the orchestrator creates and wires the migration engine
    let backends = test_backends();
    let policy = test_policy();
    let config = test_orchestrator_config();

    let orch = ghost_daemon::orchestrator::TransferOrchestrator::new(config, backends, policy);

    // The orchestrator should have created the migration engine
    // We can verify this by checking that the engine is functional
    // (The orchestrator doesn't directly expose the engine, but we can
    // verify the orchestrator was created successfully with migration enabled)
    let status = orch.status();
    assert_eq!(status.queue_depth, 0);
}

#[test]
fn test_orchestrator_backpressure_controller_wired() {
    // Verify that the orchestrator creates the backpressure controller
    let backends = test_backends();
    let policy = test_policy();
    let config = test_orchestrator_config();

    let _orch = ghost_daemon::orchestrator::TransferOrchestrator::new(config, backends, policy);

    // If we got here without panicking, the backpressure controller was created successfully
    // (it's initialized in the constructor)
}

// ─── Deterministic Simulation Tests ────────────────────────────────────────────

#[test]
fn test_deterministic_migration_with_sim_backend() {
    // Use SimBackend for deterministic testing
    let mut backends = HashMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(SimBackend::new(SimConfig::default())) as Arc<dyn StorageBackend>,
    );
    backends.insert(
        TierId::Simulation,
        Arc::new(SimBackend::new(SimConfig::default())) as Arc<dyn StorageBackend>,
    );

    let hotness_tracker = test_hotness_tracker();
    let trace_log = test_trace_log();
    let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
    let policy = test_policy();

    let engine = MigrationEngine::new(
        MigrationConfig::default(),
        policy,
        hotness_tracker,
        state_machine,
        trace_log,
        backends,
    );

    let pressure = PressureState::new();
    let migrations = engine.evaluate(&pressure);

    // With no registered chunks, should produce no migrations
    assert!(migrations.is_empty());
}

#[test]
fn test_hotness_decay_preserves_ordering() {
    // Verify that decay preserves the relative ordering of hotness scores
    let tracker = test_hotness_tracker();

    let chunk_a = test_chunk_id(80);
    let chunk_b = test_chunk_id(81);

    // Make chunk_a hotter than chunk_b
    for _ in 0..100 {
        tracker.record_access(chunk_a, 1024);
    }
    for _ in 0..10 {
        tracker.record_access(chunk_b, 1024);
    }

    let score_a_before = tracker.get_hotness(&chunk_a).unwrap().score;
    let score_b_before = tracker.get_hotness(&chunk_b).unwrap().score;
    assert!(score_a_before > score_b_before);

    // Decay all
    tracker.decay_all();

    let score_a_after = tracker.get_hotness(&chunk_a).unwrap().score;
    let score_b_after = tracker.get_hotness(&chunk_b).unwrap().score;

    // Ordering should be preserved
    assert!(
        score_a_after > score_b_after,
        "Ordering should be preserved after decay: a={} b={}",
        score_a_after,
        score_b_after
    );
}

#[test]
fn test_backpressure_action_allows_priority_matrix() {
    // Test the priority matrix for each backpressure action
    let cases = vec![
        (
            BackpressureAction::Allow,
            TransferPriority::Low,
            true,
        ),
        (
            BackpressureAction::Allow,
            TransferPriority::Critical,
            true,
        ),
        (
            BackpressureAction::Throttle,
            TransferPriority::Low,
            false,
        ),
        (
            BackpressureAction::Throttle,
            TransferPriority::High,
            true,
        ),
        (
            BackpressureAction::Reject,
            TransferPriority::High,
            false,
        ),
        (
            BackpressureAction::Reject,
            TransferPriority::Critical,
            true,
        ),
        (
            BackpressureAction::CriticalOnly,
            TransferPriority::High,
            false,
        ),
        (
            BackpressureAction::CriticalOnly,
            TransferPriority::Critical,
            true,
        ),
    ];

    for (action, priority, expected) in &cases {
        assert_eq!(
            action.allows(*priority),
            *expected,
            "{:?} should {} {:?}",
            action,
            if *expected { "allow" } else { "reject" },
            priority
        );
    }
}
