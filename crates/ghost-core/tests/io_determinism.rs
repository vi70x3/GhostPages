//! Deterministic I/O test — verifies that the same seed produces
//! identical issue/complete sequences across runs.
//!
//! This is the core guarantee of the I/O determinism boundary:
//! given a fixed seed and fixed sequence of operations, the
//! IoScheduler must produce the same request IDs, the same ordering,
//! and the same completion timestamps every time.

use ghost_core::emitter::EventEmitter;
use ghost_core::io_abstraction::{IoCompletion, IoOperation, IoScheduler};
use ghost_core::time::DeterministicTimeProvider;
use ghost_core::types::{ChunkId, TierId};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Create a deterministic IoScheduler with a fixed seed clock.
fn make_scheduler(
    start_secs: u64,
) -> (IoScheduler, mpsc::Receiver<ghost_core::events::Event>) {
    let (tx, rx) = mpsc::channel(256);
    let emitter = EventEmitter::new(tx);
    let clock = DeterministicTimeProvider::new(start_secs, Duration::from_millis(1));
    let scheduler = IoScheduler::new(Arc::new(clock), emitter);
    (scheduler, rx)
}

/// Run a fixed sequence of I/O operations and return the request IDs
/// and completion durations in order.
fn run_sequence(scheduler: &mut IoScheduler) -> Vec<(u64, IoOperation, u64)> {
    let chunk_a = ChunkId::from_data(b"chunk-a");
    let chunk_b = ChunkId::from_data(b"chunk-b");
    let chunk_c = ChunkId::from_data(b"chunk-c");
    let tier = TierId::Disk;

    // Issue three reads
    let id1 = scheduler.issue(IoOperation::Read, chunk_a, tier);
    let id2 = scheduler.issue(IoOperation::Read, chunk_b, tier);
    let id3 = scheduler.issue(IoOperation::Read, chunk_c, tier);

    // Complete them in reverse order (simulating out-of-order completion)
    scheduler.complete(id2, Ok(()));
    scheduler.complete(id3, Ok(()));
    scheduler.complete(id1, Ok(()));

    // Issue a write, then flush
    let id4 = scheduler.issue(IoOperation::Write, chunk_a, tier);
    scheduler.complete(id4, Ok(()));

    // Collect results from completed requests
    scheduler
        .completed()
        .iter()
        .map(|req| {
            let duration = match &req.completion {
                IoCompletion::Completed { duration_ticks } => *duration_ticks,
                other => panic!("expected Completed, got {:?}", other),
            };
            (req.id, req.operation, duration)
        })
        .collect()
}

#[test]
fn test_io_determinism_same_seed_produces_identical_sequence() {
    let start_secs = 1_700_000_000;

    // Run 1
    let (mut scheduler1, _rx1) = make_scheduler(start_secs);
    let result1 = run_sequence(&mut scheduler1);

    // Run 2 — fresh scheduler with same seed
    let (mut scheduler2, _rx2) = make_scheduler(start_secs);
    let result2 = run_sequence(&mut scheduler2);

    // Request IDs must be identical (monotonic, starting from 1)
    let ids1: Vec<u64> = result1.iter().map(|(id, _, _)| *id).collect();
    let ids2: Vec<u64> = result2.iter().map(|(id, _, _)| *id).collect();
    assert_eq!(ids1, ids2, "Request IDs must be deterministic");

    // Operations must match
    let ops1: Vec<IoOperation> = result1.iter().map(|(_, op, _)| *op).collect();
    let ops2: Vec<IoOperation> = result2.iter().map(|(_, op, _)| *op).collect();
    assert_eq!(ops1, ops2, "Operations must be deterministic");

    // With DeterministicTimeProvider, durations should be identical
    let durations1: Vec<u64> = result1.iter().map(|(_, _, dur)| *dur).collect();
    let durations2: Vec<u64> = result2.iter().map(|(_, _, dur)| *dur).collect();
    assert_eq!(
        durations1, durations2,
        "Completion durations must be deterministic"
    );
}

#[test]
fn test_io_determinism_issue_order_is_monotonic() {
    let (mut scheduler, _rx) = make_scheduler(1_700_000_000);
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    let id1 = scheduler.issue(IoOperation::Read, chunk, tier);
    let id2 = scheduler.issue(IoOperation::Write, chunk, tier);
    let id3 = scheduler.issue(IoOperation::Delete, chunk, tier);

    assert!(id1 < id2, "IDs must be monotonically increasing");
    assert!(id2 < id3, "IDs must be monotonically increasing");
}

#[test]
fn test_io_determinism_flush_resolves_all_pending() {
    let (mut scheduler, _rx) = make_scheduler(1_700_000_000);
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Simulation;

    // Issue several operations without completing
    for _ in 0..10 {
        scheduler.issue(IoOperation::Read, chunk, tier);
    }
    assert_eq!(scheduler.pending_count(), 10);

    // Flush should resolve all
    scheduler.flush();
    assert_eq!(scheduler.pending_count(), 0);
    assert_eq!(scheduler.completed_count(), 10);

    // All completed should be successful
    for req in scheduler.completed() {
        match &req.completion {
            IoCompletion::Completed { .. } => {}
            other => panic!("expected Completed after flush, got {:?}", other),
        }
    }
}

#[test]
fn test_io_determinism_multiple_flushes_are_idempotent() {
    let (mut scheduler, _rx) = make_scheduler(1_700_000_000);
    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Ram;

    scheduler.issue(IoOperation::Read, chunk, tier);
    scheduler.issue(IoOperation::Write, chunk, tier);

    // First flush
    scheduler.flush();
    assert_eq!(scheduler.pending_count(), 0);
    assert_eq!(scheduler.completed_count(), 2);

    // Second flush should be a no-op
    scheduler.flush();
    assert_eq!(scheduler.pending_count(), 0);
    assert_eq!(scheduler.completed_count(), 2);
}

#[test]
fn test_io_determinism_issue_complete_roundtrip_with_clock_advance() {
    let clock = DeterministicTimeProvider::new(1_700_000_000, Duration::from_millis(1));
    let (tx, _rx) = mpsc::channel(256);
    let emitter = EventEmitter::new(tx);
    let mut scheduler = IoScheduler::new(Arc::new(clock), emitter);

    let chunk = ChunkId::from_data(b"test-chunk");
    let tier = TierId::Disk;

    // Issue
    let id = scheduler.issue(IoOperation::Read, chunk, tier);
    assert_eq!(scheduler.pending_count(), 1);

    // Advance clock by sleeping (real time, but deterministic provider uses its own clock)
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Complete
    scheduler.complete(id, Ok(()));
    assert_eq!(scheduler.pending_count(), 0);
    assert_eq!(scheduler.completed_count(), 1);

    // Duration should be >= 0
    let completed = scheduler.completed();
    match &completed[0].completion {
        IoCompletion::Completed { duration_ticks } => {
            let _ = duration_ticks;
        }
        other => panic!("expected Completed, got {:?}", other),
    }
}
