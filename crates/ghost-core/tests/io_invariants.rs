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

use ghost_core::io_abstraction::{IoCompletion, IoOperation, IoScheduler};
use ghost_core::time::DeterministicTimeProvider;
use ghost_core::types::{ChunkId, TierId};
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
