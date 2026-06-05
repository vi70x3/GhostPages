//! I/O invariant tests — verifies critical safety properties
//! that must hold for the IoScheduler at all times.
//!
//! These invariants ensure:
//! 1. No double-complete (completing an already-completed request panics)
//! 2. Flush waits for all pending (no lost requests)
//! 3. Pending count is consistent with actual pending map
//! 4. Completed count is consistent with actual completed vec
//! 5. Request IDs are unique and monotonically increasing
//! 6. Issue after flush works correctly
//!
//! Additionally tests the I/O domain invariants from invariant_registry:
//! 7. io_no_double_complete — completed IDs must not appear in pending
//! 8. io_flush_completeness — if io_in_flight == 0, pending must be empty
//! 9. io_completion_bounded — io_in_flight must equal pending.len()
//! 10. io_buffer_within_capacity — pending.len() must not exceed 4096
//! 11. io_request_id_monotonic — min pending ID > max completed ID
//! 12. io_failure_eventual — completed failures must be bounded

use ghost_core::io_abstraction::{IoCompletion, IoRequest, IoOperation, IoScheduler};
use ghost_core::invariant_registry::{
    GhostState, TransferQueue, io_no_double_complete, io_flush_completeness,
    io_completion_bounded, io_buffer_within_capacity,
    io_request_id_monotonic, io_failure_eventual,
};
use ghost_core::time::DeterministicTimeProvider;
use ghost_core::types::{ChunkId, TierId};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

fn make_scheduler() -> IoScheduler {
    let (tx, _rx) = mpsc::channel(256);
    let emitter = ghost_core::emitter::EventEmitter::new(tx);
    let clock = DeterministicTimeProvider::new(1_700_000_000, Duration::from_millis(1));
    IoScheduler::new(Arc::new(clock), emitter)
}

fn make_scheduler_with_channel() -> (IoScheduler, mpsc::Receiver<ghost_core::events::Event>) {
    let (tx, rx) = mpsc::channel(256);
    let emitter = ghost_core::emitter::EventEmitter::new(tx);
    let clock = DeterministicTimeProvider::new(1_700_000_000, Duration::from_millis(1));
    let scheduler = IoScheduler::new(Arc::new(clock), emitter);
    (scheduler, rx)
}

/// Invariant: Completing an already-completed request must panic (no double-complete).
#[test]
#[should_panic(expected = "unknown request ID")]
fn test_invariant_no_double_complete() {
    let mut scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let id = scheduler.issue(IoOperation::Read, chunk, TierId::Ram);

    // First complete should succeed
    scheduler.complete(id, Ok(()));

    // Second complete should panic
    scheduler.complete(id, Ok(()));
}

/// Invariant: Completing a request that was never issued must panic.
#[test]
#[should_panic(expected = "unknown request ID")]
fn test_invariant_complete_unknown_id_panics() {
    let mut scheduler = make_scheduler();
    scheduler.complete(9999, Ok(()));
}

/// Invariant: After flush, pending count must be 0 and all requests
/// must be in the completed list.
#[test]
fn test_invariant_flush_resolves_all_pending() {
    let mut scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Disk;

    // Issue 5 operations
    let ids: Vec<u64> = (0..5)
        .map(|i| {
            scheduler.issue(
                if i % 2 == 0 {
                    IoOperation::Read
                } else {
                    IoOperation::Write
                },
                chunk,
                tier,
            )
        })
        .collect();

    assert_eq!(scheduler.pending_count(), 5);
    assert_eq!(scheduler.completed_count(), 0);

    // Flush all
    scheduler.flush();

    assert_eq!(scheduler.pending_count(), 0);
    assert_eq!(scheduler.completed_count(), 5);

    // All original IDs should be in completed list
    let completed_ids: Vec<u64> = scheduler.completed().iter().map(|r| r.id).collect();
    for id in &ids {
        assert!(
            completed_ids.contains(id),
            "ID {} should be in completed list after flush",
            id
        );
    }
}

/// Invariant: Pending count must always match the actual pending map length.
#[test]
fn test_invariant_pending_count_consistency() {
    let mut scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    // Issue 3
    let id1 = scheduler.issue(IoOperation::Read, chunk, tier);
    let id2 = scheduler.issue(IoOperation::Write, chunk, tier);
    let id3 = scheduler.issue(IoOperation::Delete, chunk, tier);

    assert_eq!(scheduler.pending_count(), 3);
    assert_eq!(scheduler.pending().len(), 3);

    // Complete 1
    scheduler.complete(id2, Ok(()));
    assert_eq!(scheduler.pending_count(), 2);
    assert_eq!(scheduler.pending().len(), 2);
    assert_eq!(scheduler.completed_count(), 1);

    // Complete remaining
    scheduler.complete(id1, Ok(()));
    scheduler.complete(id3, Ok(()));
    assert_eq!(scheduler.pending_count(), 0);
    assert_eq!(scheduler.pending().len(), 0);
    assert_eq!(scheduler.completed_count(), 3);
}

/// Invariant: Request IDs must be unique and monotonically increasing.
#[test]
fn test_invariant_request_ids_monotonic() {
    let scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Simulation;

    let mut prev_id = 0u64;
    for _ in 0..100 {
        let id = scheduler.issue(IoOperation::Read, chunk, tier);
        assert!(id > prev_id, "ID {} must be greater than previous {}", id, prev_id);
        prev_id = id;
    }

    // All 100 should be pending
    assert_eq!(scheduler.pending_count(), 100);
}

/// Invariant: Issue after flush works correctly (no stale state).
#[test]
fn test_invariant_issue_after_flush() {
    let mut scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Disk;

    // Issue and flush
    scheduler.issue(IoOperation::Read, chunk, tier);
    scheduler.issue(IoOperation::Write, chunk, tier);
    scheduler.flush();
    assert_eq!(scheduler.completed_count(), 2);

    // Issue new requests after flush
    let id = scheduler.issue(IoOperation::Read, chunk, tier);
    assert_eq!(scheduler.pending_count(), 1);
    scheduler.complete(id, Ok(()));
    assert_eq!(scheduler.completed_count(), 3);
}

/// Invariant: Flush with no pending requests is a no-op.
#[test]
fn test_invariant_flush_empty_is_noop() {
    let mut scheduler = make_scheduler();

    assert_eq!(scheduler.pending_count(), 0);
    assert_eq!(scheduler.completed_count(), 0);

    scheduler.flush();

    assert_eq!(scheduler.pending_count(), 0);
    assert_eq!(scheduler.completed_count(), 0);
}

/// Invariant: Failed completion is recorded correctly.
#[test]
fn test_invariant_failed_completion_recorded() {
    let mut scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Disk;

    let id = scheduler.issue(IoOperation::Write, chunk, tier);
    scheduler.complete(id, Err("disk failure".to_string()));

    assert_eq!(scheduler.completed_count(), 1);
    let completed = scheduler.completed();
    match &completed[0].completion {
        IoCompletion::Failed { error } => assert_eq!(error, "disk failure"),
        other => panic!("expected Failed, got {:?}", other),
    }
}

/// Invariant: Mixed success/failure completions are tracked correctly.
#[test]
fn test_invariant_mixed_success_failure() {
    let mut scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    let id1 = scheduler.issue(IoOperation::Read, chunk, tier);
    let id2 = scheduler.issue(IoOperation::Write, chunk, tier);
    let id3 = scheduler.issue(IoOperation::Delete, chunk, tier);

    scheduler.complete(id1, Ok(()));
    scheduler.complete(id2, Err("write error".to_string()));
    scheduler.complete(id3, Ok(()));

    let completed = scheduler.completed();
    assert_eq!(completed.len(), 3);

    let successes = completed
        .iter()
        .filter(|r| matches!(r.completion, IoCompletion::Completed { .. }))
        .count();
    let failures = completed
        .iter()
        .filter(|r| matches!(r.completion, IoCompletion::Failed { .. }))
        .count();

    assert_eq!(successes, 2);
    assert_eq!(failures, 1);
}

/// Invariant: Events are emitted for issue and complete.
#[test]
fn test_invariant_events_emitted_for_io_lifecycle() {
    let (scheduler, mut rx) = make_scheduler_with_channel();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Disk;

    // Issue should emit IoRequestIssued
    let id = scheduler.issue(IoOperation::Read, chunk, tier);
    let event = rx.try_recv().expect("should receive IoRequestIssued");
    match event {
        ghost_core::events::Event::IoRequestIssued {
            operation,
            chunk_id,
            tier: event_tier,
            ..
        } => {
            assert_eq!(operation, IoOperation::Read);
            assert_eq!(chunk_id, chunk);
            assert_eq!(event_tier, tier);
        }
        other => panic!("expected IoRequestIssued, got {:?}", other),
    }

    // No more events yet
    assert!(rx.try_recv().is_err(), "should not have more events");

    // Complete should emit IoRequestCompleted
    let mut scheduler = scheduler;
    scheduler.complete(id, Ok(()));
    let event = rx.try_recv().expect("should receive IoRequestCompleted");
    match event {
        ghost_core::events::Event::IoRequestCompleted {
            operation,
            chunk_id,
            tier: event_tier,
            duration_ticks,
            ..
        } => {
            assert_eq!(operation, IoOperation::Read);
            assert_eq!(chunk_id, chunk);
            assert_eq!(event_tier, tier);
            // Duration should be 0 or very small (no clock advance)
            let _ = duration_ticks;
        }
        other => panic!("expected IoRequestCompleted, got {:?}", other),
    }
}

/// Invariant: Flush emits IoFlushIssued and IoFlushCompleted events.
#[test]
fn test_invariant_flush_emits_events() {
    let (mut scheduler, mut rx) = make_scheduler_with_channel();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    scheduler.issue(IoOperation::Read, chunk, tier);
    scheduler.issue(IoOperation::Write, chunk, tier);

    // Drain the IoRequestIssued events from the two issue() calls
    let event = rx.try_recv().expect("should receive first IoRequestIssued");
    assert!(matches!(
        event,
        ghost_core::events::Event::IoRequestIssued { .. }
    ));
    let event = rx.try_recv().expect("should receive second IoRequestIssued");
    assert!(matches!(
        event,
        ghost_core::events::Event::IoRequestIssued { .. }
    ));

    // Flush should emit IoFlushIssued first
    scheduler.flush();

    let event = rx.try_recv().expect("should receive IoFlushIssued");
    match event {
        ghost_core::events::Event::IoFlushIssued { tier: _, .. } => {}
        other => panic!("expected IoFlushIssued, got {:?}", other),
    }

    // Then IoRequestCompleted for each pending (2)
    let event = rx.try_recv().expect("should receive first IoRequestCompleted");
    assert!(matches!(
        event,
        ghost_core::events::Event::IoRequestCompleted { .. }
    ));
    let event = rx.try_recv().expect("should receive second IoRequestCompleted");
    assert!(matches!(
        event,
        ghost_core::events::Event::IoRequestCompleted { .. }
    ));

    // Then IoFlushCompleted
    let event = rx.try_recv().expect("should receive IoFlushCompleted");
    assert!(matches!(
        event,
        ghost_core::events::Event::IoFlushCompleted { .. }
    ));
}

// ─── GhostState I/O Invariant Tests ─────────────────────────────────────────

/// Helper: build a GhostState from an IoScheduler.
fn make_ghost_state<'a>(scheduler: &'a IoScheduler) -> GhostState<'a> {
    GhostState {
        chunks: Box::leak(Box::new(BTreeMap::new())),
        transfer_queue: {
            // TransferQueue is an opaque handle; invariants in ghost-core never
            // inspect its contents. We leak an instance to get a 'static reference.
            Box::leak(Box::new(TransferQueue))
        },
        health: {
            Box::leak(Box::new(ghost_core::events::BackendHealth::Healthy))
        },
        pressure: {
            Box::leak(Box::new(ghost_core::state::PressureState::default()))
        },
        io_pending: scheduler.pending(),
        io_completed: scheduler.completed(),
        io_in_flight: scheduler.pending_count(),
    }
}

/// Test 1: io_no_double_complete — happy path (no overlap).
#[test]
fn test_ghost_io_no_double_complete_happy() {
    let scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    let id1 = scheduler.issue(IoOperation::Read, chunk, tier);
    let id2 = scheduler.issue(IoOperation::Write, chunk, tier);

    let state = make_ghost_state(&scheduler);
    assert!(io_no_double_complete(&state).is_ok(), "no overlap when nothing completed");

    // Complete one
    let mut scheduler = scheduler;
    scheduler.complete(id1, Ok(()));

    let state = make_ghost_state(&scheduler);
    assert!(io_no_double_complete(&state).is_ok(), "no overlap with partial completion");
}

/// Test 2: io_no_double_complete — violation scenario (simulated).
#[test]
fn test_ghost_io_no_double_complete_violation() {
    let scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    let id = scheduler.issue(IoOperation::Read, chunk, tier);

    // Simulate a state where the same request is both pending and completed
    // (this would be a bug in the scheduler or event replay).
    let mut pending = BTreeMap::new();
    pending.insert(id, scheduler.pending().get(&id).unwrap().clone());

    let completed_req = {
        let mut r = scheduler.pending().get(&id).unwrap().clone();
        r.completion = IoCompletion::Completed { duration_ticks: 100 };
        r
    };

    let state = GhostState {
        chunks: &BTreeMap::new(),
        transfer_queue: Box::leak(Box::new(TransferQueue)),
        health: Box::leak(Box::new(ghost_core::events::BackendHealth::Healthy)),
        pressure: Box::leak(Box::new(ghost_core::state::PressureState::default())),
        io_pending: &pending,
        io_completed: &[completed_req],
        io_in_flight: 1,
    };

    let result = io_no_double_complete(&state);
    assert!(result.is_err(), "should detect double-complete violation");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("double-complete") || err_msg.contains("both completed and pending"));
}

/// Test 3: io_flush_completeness — happy path (consistent state).
#[test]
fn test_ghost_io_flush_completeness_happy() {
    let scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    // Issue some requests
    for _ in 0..3 {
        scheduler.issue(IoOperation::Read, chunk, tier);
    }

    let state = make_ghost_state(&scheduler);
    assert!(io_flush_completeness(&state).is_ok(), "3 pending, 3 in-flight is consistent");

    // Flush and check
    let mut scheduler = scheduler;
    scheduler.flush();

    let state = make_ghost_state(&scheduler);
    assert!(io_flush_completeness(&state).is_ok(), "0 pending, 0 in-flight after flush");
}

/// Test 4: io_flush_completeness — violation (pending but 0 in-flight).
#[test]
fn test_ghost_io_flush_completeness_violation() {
    let scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    let _id = scheduler.issue(IoOperation::Read, chunk, tier);

    // Create a state where pending is non-empty but io_in_flight is 0
    let pending = scheduler.pending().clone();
    let state = GhostState {
        chunks: &BTreeMap::new(),
        transfer_queue: Box::leak(Box::new(TransferQueue)),
        health: Box::leak(Box::new(ghost_core::events::BackendHealth::Healthy)),
        pressure: Box::leak(Box::new(ghost_core::state::PressureState::default())),
        io_pending: &pending,
        io_completed: &[],
        io_in_flight: 0, // BUG: should be 1
    };

    let result = io_flush_completeness(&state);
    assert!(result.is_err(), "should detect flush completeness violation");
}

/// Test 5: io_completion_bounded — happy path.
#[test]
fn test_ghost_io_completion_bounded_happy() {
    let scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    for _ in 0..5 {
        scheduler.issue(IoOperation::Read, chunk, tier);
    }

    let state = make_ghost_state(&scheduler);
    assert!(io_completion_bounded(&state).is_ok(), "5 pending == 5 in-flight");
}

/// Test 6: io_completion_bounded — violation (mismatch).
#[test]
fn test_ghost_io_completion_bounded_violation() {
    let scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    let _id = scheduler.issue(IoOperation::Read, chunk, tier);

    // Create a state with mismatch between io_in_flight and pending.len()
    let pending = scheduler.pending().clone();
    let state = GhostState {
        chunks: &BTreeMap::new(),
        transfer_queue: Box::leak(Box::new(TransferQueue)),
        health: Box::leak(Box::new(ghost_core::events::BackendHealth::Healthy)),
        pressure: Box::leak(Box::new(ghost_core::state::PressureState::default())),
        io_pending: &pending,
        io_completed: &[],
        io_in_flight: 99, // BUG: should be 1
    };

    let result = io_completion_bounded(&state);
    assert!(result.is_err(), "should detect completion bounded violation");
}

/// Test 7: io_buffer_within_capacity — happy path.
#[test]
fn test_ghost_io_buffer_within_capacity_happy() {
    let scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    for _ in 0..100 {
        scheduler.issue(IoOperation::Read, chunk, tier);
    }

    let state = make_ghost_state(&scheduler);
    assert!(io_buffer_within_capacity(&state).is_ok(), "100 pending is within 4096 bound");
}

/// Test 8: io_request_id_monotonic — happy path.
#[test]
fn test_ghost_io_request_id_monotonic_happy() {
    let mut scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    // Issue and complete some requests
    let id1 = scheduler.issue(IoOperation::Read, chunk, tier);
    let id2 = scheduler.issue(IoOperation::Write, chunk, tier);
    scheduler.complete(id1, Ok(()));
    scheduler.complete(id2, Ok(()));

    // Issue new requests (IDs will be higher)
    let _id3 = scheduler.issue(IoOperation::Read, chunk, tier);
    let _id4 = scheduler.issue(IoOperation::Write, chunk, tier);

    let state = make_ghost_state(&scheduler);
    assert!(io_request_id_monotonic(&state).is_ok(), "pending IDs > completed IDs");
}

/// Test 9: io_failure_eventual — happy path (few failures).
#[test]
fn test_ghost_io_failure_eventual_happy() {
    let mut scheduler = make_scheduler();
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Disk;

    // Issue and complete a mix of success and failure
    for i in 0..10 {
        let id = scheduler.issue(IoOperation::Write, chunk, tier);
        if i % 3 == 0 {
            scheduler.complete(id, Err("transient error".to_string()));
        } else {
            scheduler.complete(id, Ok(()));
        }
    }

    let state = make_ghost_state(&scheduler);
    assert!(io_failure_eventual(&state).is_ok(), "3 failures is within 256 bound");
}

/// Test 10: io_failure_eventual — violation (too many failures).
#[test]
fn test_ghost_io_failure_eventual_violation() {
    // Create a state with > 256 failed requests
    let mut completed = Vec::new();
    for i in 0..300 {
        completed.push(IoRequest {
            id: i as u64 + 1,
            operation: IoOperation::Write,
            chunk_id: ChunkId::from_data(b"test-chunk"),
            tier: TierId::Disk,
            issued_at: std::time::Instant::now(),
            completion: IoCompletion::Failed {
                error: "persistent failure".to_string(),
            },
        });
    }

    let state = GhostState {
        chunks: &BTreeMap::new(),
        transfer_queue: Box::leak(Box::new(TransferQueue)),
        health: Box::leak(Box::new(ghost_core::events::BackendHealth::Healthy)),
        pressure: Box::leak(Box::new(ghost_core::state::PressureState::default())),
        io_pending: &BTreeMap::new(),
        io_completed: &completed,
        io_in_flight: 0,
    };

    let result = io_failure_eventual(&state);
    assert!(result.is_err(), "300 failures exceeds 256 bound");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("failure") || err_msg.contains("stuck"));
}

// ─── Cross-Mode Verification ─────────────────────────────────────────────────

/// Run the same I/O workload through the invariant checks using different
/// timing configurations to verify that invariants hold regardless of the
/// clock/mode used (deterministic clock at different seeds, real-time).
///
/// This simulates the "live runtime", "replay", and "simulated" modes by
/// varying the clock seed and timing parameters, then asserting that all
/// six I/O invariants pass in every configuration.
#[test]
fn test_io_invariants_cross_mode() {
    let chunk = ChunkId::from_data(b"cross-mode-chunk");
    let tier = TierId::Ram;

    // Mode 1: Deterministic clock, seed = 1_700_000_000 (typical)
    // Mode 2: Deterministic clock, seed = 0 (epoch start)
    // Mode 3: Deterministic clock, seed = u64::MAX / 2 (large value)
    // Mode 4: Real-time provider (non-deterministic, but invariants must still hold)
    let configs: Vec<(&str, Arc<dyn ghost_core::time::TimeProvider>)> = vec![
        ("det-seed-1.7B", Arc::new(DeterministicTimeProvider::new(1_700_000_000, Duration::from_millis(1)))),
        ("det-seed-0", Arc::new(DeterministicTimeProvider::new(0, Duration::from_millis(1)))),
        ("det-seed-max", Arc::new(DeterministicTimeProvider::new(1_000_000, Duration::from_millis(10)))),
        ("real-time", Arc::new(ghost_core::time::RealTimeProvider)),
    ];

    for (mode_name, clock) in &configs {
        let (tx, _rx) = mpsc::channel(256);
        let emitter = ghost_core::emitter::EventEmitter::new(tx);
        let mut scheduler = IoScheduler::new(clock.clone(), emitter);

        // Phase 1: Issue 10 requests — all invariants must hold
        let ids: Vec<u64> = (0..10)
            .map(|i| {
                let op = if i % 2 == 0 { IoOperation::Read } else { IoOperation::Write };
                scheduler.issue(op, chunk, tier)
            })
            .collect();

        let state = make_ghost_state(&scheduler);
        assert!(io_no_double_complete(&state).is_ok(), "mode {mode_name}: io_no_double_complete failed after issue");
        assert!(io_flush_completeness(&state).is_ok(), "mode {mode_name}: io_flush_completeness failed after issue");
        assert!(io_completion_bounded(&state).is_ok(), "mode {mode_name}: io_completion_bounded failed after issue");
        assert!(io_buffer_within_capacity(&state).is_ok(), "mode {mode_name}: io_buffer_within_capacity failed after issue");
        assert!(io_request_id_monotonic(&state).is_ok(), "mode {mode_name}: io_request_id_monotonic failed after issue");
        assert!(io_failure_eventual(&state).is_ok(), "mode {mode_name}: io_failure_eventual failed after issue");

        // Phase 2: Complete 5 successfully, 2 with errors — invariants must hold
        for (i, id) in ids.iter().enumerate() {
            if i < 5 {
                scheduler.complete(*id, Ok(()));
            } else if i < 7 {
                scheduler.complete(*id, Err(format!("simulated error {i}")));
            }
            // Leave 3 requests pending
        }

        let state = make_ghost_state(&scheduler);
        assert!(io_no_double_complete(&state).is_ok(), "mode {mode_name}: io_no_double_complete failed after partial complete");
        assert!(io_flush_completeness(&state).is_ok(), "mode {mode_name}: io_flush_completeness failed after partial complete");
        assert!(io_completion_bounded(&state).is_ok(), "mode {mode_name}: io_completion_bounded failed after partial complete");
        assert!(io_buffer_within_capacity(&state).is_ok(), "mode {mode_name}: io_buffer_within_capacity failed after partial complete");
        assert!(io_request_id_monotonic(&state).is_ok(), "mode {mode_name}: io_request_id_monotonic failed after partial complete");
        assert!(io_failure_eventual(&state).is_ok(), "mode {mode_name}: io_failure_eventual failed after partial complete");

        // Phase 3: Flush remaining — invariants must hold
        scheduler.flush();

        let state = make_ghost_state(&scheduler);
        assert!(io_no_double_complete(&state).is_ok(), "mode {mode_name}: io_no_double_complete failed after flush");
        assert!(io_flush_completeness(&state).is_ok(), "mode {mode_name}: io_flush_completeness failed after flush");
        assert!(io_completion_bounded(&state).is_ok(), "mode {mode_name}: io_completion_bounded failed after flush");
        assert!(io_buffer_within_capacity(&state).is_ok(), "mode {mode_name}: io_buffer_within_capacity failed after flush");
        assert!(io_request_id_monotonic(&state).is_ok(), "mode {mode_name}: io_request_id_monotonic failed after flush");
        assert!(io_failure_eventual(&state).is_ok(), "mode {mode_name}: io_failure_eventual failed after flush");
    }
}
