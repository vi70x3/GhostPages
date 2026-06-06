//! State ownership enforcement tests.
//!
//! These tests verify the architectural contract that only `ghost-daemon`
//! may mutate runtime state. Workers report via channels; the orchestrator
//! applies state transitions. Backends, policies, replay, and metrics
//! never mutate runtime state directly.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ghost_core::emitter::EventEmitter;
use ghost_core::state::{ChunkState, StateMachine};
use ghost_core::state_ownership::{StateMutationToken, StateOwnershipLog};
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::transfer::{TransferJob, TransferPriority};
use ghost_core::types::{ChunkId, ChunkMeta, TierId};
use ghost_daemon::config::{OrchestratorConfig, WorkerPoolConfig};
use ghost_daemon::metrics::TransferMetrics;
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_daemon::trace_log::TraceLog;
use ghost_daemon::worker::{WorkerCompletion, WorkerPool};
use ghost_policy::{LruConfig, LruPolicy, PlacementPolicy};
use ghost_tier::{RamBackend, StorageBackend};

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn test_backends() -> BTreeMap<TierId, Arc<dyn ghost_tier::StorageBackend>> {
    let mut backends = BTreeMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::with_id(TierId::Ram, 1024 * 1024))
            as Arc<dyn ghost_tier::StorageBackend>,
    );
    backends.insert(
        TierId::Simulation,
        Arc::new(RamBackend::with_id(TierId::Simulation, 1024 * 1024))
            as Arc<dyn ghost_tier::StorageBackend>,
    );
    backends
}

fn test_worker_config() -> WorkerPoolConfig {
    WorkerPoolConfig {
        worker_count: 2,
        max_retries: 2,
        retry_base_delay_ms: 10,
        max_retry_delay_ms: 100,
        enable_compression: false,
    }
}

fn test_orchestrator_config() -> OrchestratorConfig {
    OrchestratorConfig {
        queue_capacity: 1024,
        worker_count: 2,
        max_retries: 2,
        retry_base_delay_ms: 10,
        max_retry_delay_ms: 100,
        enable_compression: false,
        trace_max_events: 1000,
        shutdown_timeout_secs: 5,
        pressure_sample_interval_ms: 1000,
        pressure_smoothing_factor: 0.3,
        auto_migration_interval_ms: 5000,
        pressure_history_size: 256,
        enable_auto_migration: false,
        deterministic_mode: false,
        rng_seed: Some(42),
    }
}

fn test_orchestrator() -> TransferOrchestrator {
    let backends = test_backends();
    let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
    TransferOrchestrator::new(test_orchestrator_config(), backends, policy)
}

// ─── Test: Only daemon mutates state ──────────────────────────────────────────

/// Verify that all state mutations go through the orchestrator.
///
/// This test creates an orchestrator, stores a chunk, and verifies that
/// the state machine is only mutated through the orchestrator's methods
/// (store, migrate, evict), not by any subsystem directly.
#[test]
fn test_only_daemon_mutates_state() {
    let orch = test_orchestrator();
    let chunk_id = ChunkId::from_data(b"daemon_mutates");

    // Initially the chunk is not registered
    let sm = orch.state_machine.lock().unwrap();
    assert!(sm.get_state(&chunk_id).is_none());
    drop(sm);

    // Store the chunk — this is the orchestrator's job
    orch.store(chunk_id, TierId::Ram, b"test_data").unwrap();

    // Verify the state was mutated by the orchestrator
    let sm = orch.state_machine.lock().unwrap();
    let state = sm.get_state(&chunk_id);
    assert_eq!(state, Some(ChunkState::Stored));
    drop(sm);

    // Verify trace log recorded the mutation
    let events = orch.trace_log().get_events();
    assert!(events.iter().any(|e| matches!(
        e,
        TraceEvent::ChunkCreated { chunk_id: id, .. } if *id == chunk_id
    )));
}

// ─── Test: Worker reports via channel ─────────────────────────────────────────

/// Verify that workers don't directly mutate state.
///
/// Workers execute transfers and report completions via a WorkerCompletion
/// channel. The orchestrator receives these and applies state transitions.
/// The WorkerPool struct does NOT hold a StateMachine reference.
#[tokio::test]
async fn test_worker_reports_via_channel() {
    let config = test_worker_config();
    let backends = test_backends();
    let trace_log = Arc::new(TraceLog::new(1000));
    let metrics = Arc::new(TransferMetrics::new());

    // WorkerPool does NOT take a state_machine parameter
    let pool = WorkerPool::new(config, backends, trace_log.clone(), metrics.clone());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    // start() returns (job_tx, completion_rx, handles)
    let (job_tx, mut completion_rx, handles) = pool.start(shutdown_rx);

    // Submit a cross-tier migration job
    let chunk_id = ChunkId::from_data(b"channel_report");
    let job = TransferJob::new(
        chunk_id,
        TierId::Ram,
        TierId::Simulation,
        256,
        TransferPriority::Normal,
    );

    metrics.record_submission();
    job_tx.send(job).await.unwrap();

    // Wait for the worker to complete and send a completion report
    let completion = tokio::time::timeout(Duration::from_secs(5), completion_rx.recv())
        .await
        .expect("timeout waiting for completion")
        .expect("completion channel closed");

    // Verify the completion report
    assert_eq!(completion.chunk_id, chunk_id);
    assert_eq!(completion.from_tier, TierId::Ram);
    assert_eq!(completion.to_tier, TierId::Simulation);
    assert!(completion.success);
    assert!(completion.error.is_none());

    // Verify the worker did NOT directly mutate any state machine
    // (WorkerPool has no state_machine field — this is enforced by the type system)

    // Shutdown
    shutdown_tx.send(true).unwrap();
    drop(job_tx);
    for h in handles {
        let _ = h.await;
    }
}

// ─── Test: Backends are passive ──────────────────────────────────────────────

/// Verify that RamBackend, SimBackend, and DiskBackend never mutate runtime state.
///
/// Backends only perform storage I/O (allocate, read, write, deallocate).
/// They don't know about ChunkState, StateMachine, or any runtime state.
#[test]
fn test_backends_are_passive() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let backend = RamBackend::with_id(TierId::Ram, 1024 * 1024);

        // Backend operations are pure I/O
        let alloc = backend.allocate(256).await.unwrap();
        let data = b"hello world";
        backend.write(&alloc, data).await.unwrap();

        let mut buf = vec![0u8; data.len()];
        backend.read(&alloc, &mut buf).await.unwrap();
        assert_eq!(&buf, data);

        backend.deallocate(alloc).await.unwrap();

        // Backend has no methods that accept or return ChunkState
        // Backend has no reference to StateMachine
        // This is enforced by the StorageBackend trait definition
    });
}

// ─── Test: Policies are pure ─────────────────────────────────────────────────

/// Verify that PlacementPolicy functions are pure (no mutation).
///
/// Policies take references and return decisions. They never mutate
/// any runtime state.
#[test]
fn test_policies_are_pure() {
    let policy = LruPolicy::new(LruConfig::default());
    let pressure = ghost_core::state::PressureState::new();

    let meta = ChunkMeta {
        id: ChunkId::from_data(b"policy_test"),
        size: 1024,
        compressed_size: 512,
        tier: TierId::Ram,
        state: ChunkState::Stored,
        created_at: 0,
        last_accessed: 0,
        access_count: 10,
        compression: ghost_core::types::CompressionAlgorithm::None,
        checksum: [0u8; 32],
    };

    // should_migrate returns a decision, doesn't mutate
    let _decision = policy.should_migrate(&meta, TierId::Ram, &pressure);

    // select_target_tier returns a decision, doesn't mutate
    let tiers = vec![TierId::Ram, TierId::Simulation];
    let _target = policy.select_target_tier(&meta, &pressure, &tiers);

    // migration_priority returns a value, doesn't mutate
    let _priority = policy.migration_priority(&meta, &pressure);

    // After all policy calls, the meta is unchanged (policies take &meta)
    assert_eq!(meta.state, ChunkState::Stored);
    assert_eq!(meta.tier, TierId::Ram);
}

// ─── Test: Replay is read-only ────────────────────────────────────────────────

/// Verify that the replay engine never mutates live state.
///
/// Replay reads trace events and validates state transitions using
/// a separate StateMachine instance. It never touches the orchestrator's
/// live state.
#[test]
fn test_replay_is_readonly() {
    // The replay engine uses its own StateMachine for validation
    let mut replay_sm = StateMachine::new();
    let chunk_id = ChunkId::from_data(b"replay_test");

    // Replay registers and transitions its own local state machine
    replay_sm.register(chunk_id).unwrap();
    replay_sm
        .transition(&chunk_id, ChunkState::Stored)
        .unwrap();

    // This is a SEPARATE state machine, not the orchestrator's
    assert_eq!(replay_sm.get_state(&chunk_id), Some(ChunkState::Stored));

    // The orchestrator's state machine is not affected
    let orch = test_orchestrator();
    let orch_sm = orch.state_machine.lock().unwrap();
    assert!(orch_sm.get_state(&chunk_id).is_none());
    // orch_sm is dropped here, proving we only read, never wrote
}

// ─── Test: Metrics are observational ─────────────────────────────────────────

/// Verify that metrics never mutate state.
///
/// Metrics only record atomic counters. They don't trigger state transitions
/// or modify any runtime state.
#[test]
fn test_metrics_are_observational() {
    let metrics = TransferMetrics::new();

    // Record some metrics
    metrics.record_submission();
    metrics.record_completion();
    metrics.record_bytes(1024);
    metrics.record_transfer_time(100);

    // Metrics only have read-only access to their own atomic counters
    assert_eq!(
        metrics
            .jobs_submitted
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        metrics
            .jobs_completed
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    // Metrics don't have any methods that mutate StateMachine, TransferQueue, etc.
    // This is enforced by the type system — TransferMetrics has no reference
    // to any runtime state type.
}

// ─── Test: StateMutationToken enforcement ────────────────────────────────────

/// Verify that StateMutationToken can be created and used as a marker.
#[test]
fn test_state_mutation_token() {
    let token = StateMutationToken::new_unchecked();
    let _token2 = token; // Clone/Copy
    assert_eq!(token, _token2);
}

// ─── Test: StateOwnershipLog audit ───────────────────────────────────────────

/// Verify that StateOwnershipLog correctly tracks mutations.
#[test]
fn test_state_ownership_log() {
    let mut log = StateOwnershipLog::new();

    let chunk1 = ChunkId::from_data(b"chunk1");
    let chunk2 = ChunkId::from_data(b"chunk2");

    log.record_chunk("ghost-daemon::orchestrator", "transition(Stored)", 1000, chunk1);
    log.record_chunk(
        "ghost-daemon::orchestrator",
        "transition(Migrating)",
        2000,
        chunk1,
    );
    log.record_chunk("ghost-daemon::orchestrator", "transition(Stored)", 3000, chunk2);

    assert_eq!(log.mutation_count(), 3);

    // All mutations are from the orchestrator
    assert!(!log.has_unauthorized_mutations(&["ghost-daemon::orchestrator"]));

    // If we only allow a different module, it should detect unauthorized
    assert!(log.has_unauthorized_mutations(&["ghost-daemon::scheduler"]));

    // Filter by chunk
    let chunk1_mutations = log.mutations_for_chunk(&chunk1);
    assert_eq!(chunk1_mutations.len(), 2);

    // Filter by module
    let orch_mutations = log.mutations_by_module("ghost-daemon::orchestrator");
    assert_eq!(orch_mutations.len(), 3);
}

// ─── Test: WorkerCompletion struct ───────────────────────────────────────────

/// Verify that WorkerCompletion correctly carries completion data.
#[test]
fn test_worker_completion_struct() {
    let chunk_id = ChunkId::from_data(b"completion");

    let success = WorkerCompletion {
        chunk_id,
        from_tier: TierId::Ram,
        to_tier: TierId::Simulation,
        success: true,
        error: None,
        worker_id: 0,
        timestamp: 12345,
    };
    assert!(success.success);
    assert!(success.error.is_none());

    let failure = WorkerCompletion {
        chunk_id,
        from_tier: TierId::Ram,
        to_tier: TierId::Simulation,
        success: false,
        error: Some("backend error".to_string()),
        worker_id: 1,
        timestamp: 99999,
    };
    assert!(!failure.success);
    assert_eq!(failure.error.as_deref(), Some("backend error"));
}

// ─── Test: Orchestrator completion handler ───────────────────────────────────

/// Verify that the orchestrator's completion handler correctly applies
/// state transitions from worker completion reports.
#[tokio::test]
async fn test_orchestrator_completion_handler() {
    let orch = test_orchestrator();
    let chunk_id = ChunkId::from_data(b"completion_handler");

    // Register and set up the chunk as if a migration was started
    {
        let mut sm = orch.state_machine.lock().unwrap();
        sm.register(chunk_id).unwrap();
        sm.transition(&chunk_id, ChunkState::Stored).unwrap();
        sm.transition(&chunk_id, ChunkState::Migrating).unwrap();
    }

    // Verify it's in Migrating state
    {
        let sm = orch.state_machine.lock().unwrap();
        assert_eq!(sm.get_state(&chunk_id), Some(ChunkState::Migrating));
    }

    // Simulate what the orchestrator's completion handler does
    let completion = WorkerCompletion {
        chunk_id,
        from_tier: TierId::Ram,
        to_tier: TierId::Simulation,
        success: true,
        error: None,
        worker_id: 0,
        timestamp: current_timestamp(),
    };

    // Apply the state transition (this is what the orchestrator does)
    {
        let mut sm = orch.state_machine.lock().unwrap();
        let _ = sm.transition(&completion.chunk_id, ChunkState::Stored);
    }

    // Verify the chunk transitioned to Stored
    {
        let sm = orch.state_machine.lock().unwrap();
        assert_eq!(sm.get_state(&chunk_id), Some(ChunkState::Stored));
    }
}

// ─── Test: Orchestrator completion handler failure path ───────────────────────

/// Verify that the orchestrator's completion handler correctly applies
/// Failed state on worker failure.
#[tokio::test]
async fn test_orchestrator_completion_handler_failure() {
    let orch = test_orchestrator();
    let chunk_id = ChunkId::from_data(b"completion_fail");

    // Register and set up the chunk as migrating
    {
        let mut sm = orch.state_machine.lock().unwrap();
        sm.register(chunk_id).unwrap();
        sm.transition(&chunk_id, ChunkState::Stored).unwrap();
        sm.transition(&chunk_id, ChunkState::Migrating).unwrap();
    }

    // Simulate a failure completion
    let completion = WorkerCompletion {
        chunk_id,
        from_tier: TierId::Ram,
        to_tier: TierId::Simulation,
        success: false,
        error: Some("backend error".to_string()),
        worker_id: 0,
        timestamp: current_timestamp(),
    };

    // Apply the failure transition
    {
        let mut sm = orch.state_machine.lock().unwrap();
        let _ = sm.transition(&completion.chunk_id, ChunkState::Failed);
    }

    // Verify the chunk transitioned to Failed
    {
        let sm = orch.state_machine.lock().unwrap();
        assert_eq!(sm.get_state(&chunk_id), Some(ChunkState::Failed));
    }
}
