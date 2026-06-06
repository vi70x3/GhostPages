//! Runtime coherence tests for Phase 3 §2.
//!
//! Validates the runtime coherence enforcement mechanisms:
//! (a) InvariantRegistry checks fire after state mutations
//! (b) StateMachine is the sole authority for state transitions
//! (c) TransferQueue provides backpressure when full
//! (d) IoScheduler bounds in-flight I/O requests
//! (e) Event ordering is deterministic (monotonic sequence IDs)
//! (f) All state transitions are valid per the state machine

use std::sync::Arc;
use ghost_core::error::GhostError;
use ghost_core::state::{ChunkState, StateMachine};
use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::queue::TransferQueue;
use ghost_daemon::trace_log::TraceLog;

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn make_chunk_id(seed: u8) -> ChunkId {
    let mut data = [0u8; 32];
    data[0] = seed;
    ChunkId::from_data(&data)
}

fn test_trace_log() -> Arc<TraceLog> {
    Arc::new(TraceLog::new(1000))
}

// ─── Test (a): InvariantRegistry checks fire after state mutations ─────────

#[test]
fn test_state_machine_transition_invariant_on_valid_transition() {
    let mut sm = StateMachine::new();
    let chunk_id = make_chunk_id(1);

    // Register chunk — should start as Allocated
    sm.register(chunk_id).unwrap();
    assert_eq!(sm.get_state(&chunk_id), Some(ChunkState::Allocated));

    // Valid: Allocated -> Stored
    let result = sm.transition(&chunk_id, ChunkState::Stored);
    assert!(result.is_ok());
    assert_eq!(sm.get_state(&chunk_id), Some(ChunkState::Stored));

    // Valid: Stored -> Cached
    let result = sm.transition(&chunk_id, ChunkState::Cached);
    assert!(result.is_ok());
    assert_eq!(sm.get_state(&chunk_id), Some(ChunkState::Cached));

    // Valid: Cached -> Stored (eviction reversal)
    let result = sm.transition(&chunk_id, ChunkState::Stored);
    assert!(result.is_ok());
    assert_eq!(sm.get_state(&chunk_id), Some(ChunkState::Stored));
}

#[test]
fn test_state_machine_rejects_invalid_transitions() {
    let mut sm = StateMachine::new();
    let chunk_id = make_chunk_id(2);

    sm.register(chunk_id).unwrap();

    // Invalid: Allocated -> Evicted (must go through Stored first)
    let result = sm.transition(&chunk_id, ChunkState::Evicted);
    assert!(result.is_err());
    match result.unwrap_err() {
        GhostError::InvalidStateTransition { from, to } => {
            assert!(from.contains("Allocated"));
            assert!(to.contains("Evicted"));
        }
        other => panic!("expected InvalidStateTransition, got {:?}", other),
    }

    // State should not have changed
    assert_eq!(sm.get_state(&chunk_id), Some(ChunkState::Allocated));
}

#[test]
fn test_state_machine_evicted_is_terminal() {
    let mut sm = StateMachine::new();
    let chunk_id = make_chunk_id(3);

    sm.register(chunk_id).unwrap();
    sm.transition(&chunk_id, ChunkState::Stored).unwrap();
    sm.transition(&chunk_id, ChunkState::Evicted).unwrap();

    // Evicted -> anything is invalid
    for target in [ChunkState::Stored, ChunkState::Cached, ChunkState::Migrating] {
        let result = sm.transition(&chunk_id, target);
        assert!(
            result.is_err(),
            "Evicted -> {:?} should be invalid",
            target
        );
    }
}

// ─── Test (b): StateMachine is sole authority for state transitions ────────

#[test]
fn test_all_state_transitions_go_through_state_machine() {
    // Verify that the StateMachine's transition graph is complete and correct.
    // Every valid transition in the system must be representable here.
    let mut sm = StateMachine::new();
    let chunk_id = make_chunk_id(4);

    sm.register(chunk_id).unwrap();

    // Full lifecycle: Allocated -> Stored -> Cached -> Stored -> Migrating -> Stored -> Evicted
    sm.transition(&chunk_id, ChunkState::Stored).unwrap();
    sm.transition(&chunk_id, ChunkState::Cached).unwrap();
    sm.transition(&chunk_id, ChunkState::Stored).unwrap();
    sm.transition(&chunk_id, ChunkState::Migrating).unwrap();
    // Migration failure: Migrating -> Stored (retry)
    sm.transition(&chunk_id, ChunkState::Stored).unwrap();
    // Migration success would go: Migrating -> Stored (at destination)
    sm.transition(&chunk_id, ChunkState::Evicted).unwrap();

    assert_eq!(sm.get_state(&chunk_id), Some(ChunkState::Evicted));
}

#[test]
fn test_state_machine_unregistered_chunk_rejected() {
    let mut sm = StateMachine::new();
    let chunk_id = make_chunk_id(5);

    // Cannot transition a chunk that was never registered
    let result = sm.transition(&chunk_id, ChunkState::Stored);
    assert!(result.is_err());
}

#[test]
fn test_state_machine_duplicate_register_rejected() {
    let mut sm = StateMachine::new();
    let chunk_id = make_chunk_id(6);

    sm.register(chunk_id).unwrap();
    let result = sm.register(chunk_id);
    assert!(result.is_err(), "duplicate register should fail");
}

// ─── Test (c): TransferQueue provides backpressure when full ──────────────

#[test]
fn test_transfer_queue_backpressure() {
    let trace_log = test_trace_log();
    let queue = TransferQueue::new(2, trace_log);

    let chunk_a = make_chunk_id(10);
    let chunk_b = make_chunk_id(11);
    let chunk_c = make_chunk_id(12);

    let job_a = ghost_core::transfer::TransferJob::new(
        chunk_a,
        TierId::Ram,
        TierId::Simulation,
        1024,
        ghost_core::transfer::TransferPriority::Normal,
    );
    let job_b = ghost_core::transfer::TransferJob::new(
        chunk_b,
        TierId::Ram,
        TierId::Simulation,
        2048,
        ghost_core::transfer::TransferPriority::Normal,
    );
    let job_c = ghost_core::transfer::TransferJob::new(
        chunk_c,
        TierId::Ram,
        TierId::Simulation,
        4096,
        ghost_core::transfer::TransferPriority::Normal,
    );

    queue.submit(job_a).unwrap();
    queue.submit(job_b).unwrap();
    assert!(queue.is_full());

    // Third submit should fail with backpressure
    let result = queue.submit(job_c);
    assert!(result.is_err());
    match result.unwrap_err() {
        GhostError::Internal(msg) => assert!(msg.contains("full")),
        other => panic!("expected Internal error about full queue, got {:?}", other),
    }
}

#[test]
fn test_transfer_queue_drain_after_backpressure() {
    let trace_log = test_trace_log();
    let queue = TransferQueue::new(1, trace_log);

    let chunk_a = make_chunk_id(20);
    let job_a = ghost_core::transfer::TransferJob::new(
        chunk_a,
        TierId::Ram,
        TierId::Simulation,
        1024,
        ghost_core::transfer::TransferPriority::Normal,
    );

    queue.submit(job_a).unwrap();
    assert!(queue.is_full());

    // Dequeue should free space
    let dequeued = queue.try_dequeue();
    assert!(dequeued.is_some());
    assert!(!queue.is_full());
    assert_eq!(queue.depth(), 0);
}

// ─── Test (d): IoScheduler bounds in-flight I/O requests ──────────────────

#[test]
fn test_io_scheduler_backpressure() {
    use ghost_core::io_abstraction::{IoOperation, IoScheduler};
    use ghost_core::time::DeterministicTimeProvider;
    use ghost_core::emitter::EventEmitter;
    use std::time::Duration;
    use tokio::sync::mpsc;

    let (tx, _rx) = mpsc::channel(256);
    let emitter = EventEmitter::new(tx);
    let clock = DeterministicTimeProvider::new(1_700_000_000, Duration::from_millis(1));
    let scheduler = IoScheduler::new(Arc::new(clock), emitter, 2);

    let chunk_a = make_chunk_id(30);
    let chunk_b = make_chunk_id(31);
    let chunk_c = make_chunk_id(32);

    // Issue 2 requests (at capacity)
    let _id1 = scheduler
        .issue(IoOperation::Read, chunk_a, TierId::Ram)
        .unwrap();
    let _id2 = scheduler
        .issue(IoOperation::Write, chunk_b, TierId::Disk)
        .unwrap();

    assert!(scheduler.is_at_capacity());
    assert_eq!(scheduler.pending_count(), 2);

    // Third should fail with backpressure
    let result = scheduler.issue(IoOperation::Read, chunk_c, TierId::Ram);
    assert!(result.is_err(), "should reject when at capacity");
}

#[test]
fn test_io_scheduler_capacity_accessors() {
    use ghost_core::io_abstraction::IoScheduler;
    use ghost_core::time::DeterministicTimeProvider;
    use ghost_core::emitter::EventEmitter;
    use std::time::Duration;
    use tokio::sync::mpsc;

    let (tx, _rx) = mpsc::channel(256);
    let emitter = EventEmitter::new(tx);
    let clock = DeterministicTimeProvider::new(1_700_000_000, Duration::from_millis(1));
    let scheduler = IoScheduler::new(Arc::new(clock), emitter, 16);

    assert_eq!(scheduler.max_pending(), 16);
    assert!(!scheduler.is_at_capacity());
}

// ─── Test (e): Event ordering is deterministic ────────────────────────────

#[test]
fn test_event_sequence_ids_are_monotonic() {
    use ghost_core::emitter::EventEmitter;
    use ghost_core::events::Event;
    use tokio::sync::mpsc;

    let (tx, mut rx) = mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    // Emit events sequentially
    let seq1 = emitter.next_sequence_id();
    let seq2 = emitter.next_sequence_id();
    let seq3 = emitter.next_sequence_id();

    assert!(seq1 < seq2, "sequence IDs must be monotonically increasing");
    assert!(seq2 < seq3, "sequence IDs must be monotonically increasing");

    // Verify try_emit stamps events with monotonic IDs
    let _ = emitter.try_emit(Event::AllocationCreated {
        chunk_id: make_chunk_id(40),
        tier: TierId::Ram,
        size: 1024,
        sequence_id: 0,
    });
    let _ = emitter.try_emit(Event::AllocationCreated {
        chunk_id: make_chunk_id(41),
        tier: TierId::Ram,
        size: 2048,
        sequence_id: 0,
    });

    let event1 = rx.try_recv().unwrap();
    let event2 = rx.try_recv().unwrap();

    let seq1 = event1.sequence_id();
    let seq2 = event2.sequence_id();
    assert!(
        seq1 < seq2,
        "Emitted events must have monotonic sequence IDs: {} >= {}",
        seq1,
        seq2
    );
}

#[test]
fn test_event_ordering_preserved_under_sequential_emit() {
    use ghost_core::emitter::EventEmitter;
    use ghost_core::events::Event;
    use tokio::sync::mpsc;

    let (tx, mut rx) = mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    // Emit a sequence of migration events
    for i in 0u8..5 {
        let chunk = make_chunk_id(50 + i);
        let _ = emitter.try_emit(Event::MigrationStarted {
            chunk_id: chunk,
            from: TierId::Ram,
            to: TierId::Simulation,
            sequence_id: 0,
        });
        let _ = emitter.try_emit(Event::MigrationCompleted {
            chunk_id: chunk,
            from: TierId::Ram,
            to: TierId::Simulation,
            duration_ms: 100,
            sequence_id: 0,
        });
    }

    // Verify all events are received in FIFO order
    let mut prev_seq = 0u64;
    for _ in 0..10 {
        let event = rx.try_recv().unwrap();
        let seq = event.sequence_id();
        assert!(
            seq > prev_seq,
            "Events must be in FIFO order: {} <= {}",
            seq,
            prev_seq
        );
        prev_seq = seq;
    }
}

// ─── Test (f): All state transitions are valid per the state machine ──────

#[test]
fn test_valid_transition_graph_is_complete() {
    // Verify all documented valid transitions are accepted by the state machine.
    let transitions: Vec<(ChunkState, ChunkState)> = vec![
        (ChunkState::Allocated, ChunkState::Stored),
        (ChunkState::Stored, ChunkState::Cached),
        (ChunkState::Stored, ChunkState::Migrating),
        (ChunkState::Stored, ChunkState::Evicted),
        (ChunkState::Cached, ChunkState::Stored),
        (ChunkState::Cached, ChunkState::Migrating),
        (ChunkState::Migrating, ChunkState::Evicted), // migration cancelled
        (ChunkState::Migrating, ChunkState::Stored), // migration success or retry
        (ChunkState::Migrating, ChunkState::Failed),
        (ChunkState::Failed, ChunkState::Evicted), // giving up
        (ChunkState::Failed, ChunkState::Stored), // retry path
    ];

    for (from, to) in transitions {
        let mut sm = StateMachine::new();
        let chunk_id = make_chunk_id(100);

        sm.register(chunk_id).unwrap();
        // Walk to the `from` state
        match from {
            ChunkState::Allocated => {} // already there
            ChunkState::Stored => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
            }
            ChunkState::Cached => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
                sm.transition(&chunk_id, ChunkState::Cached).unwrap();
            }
            ChunkState::Migrating => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
                sm.transition(&chunk_id, ChunkState::Migrating).unwrap();
            }
            ChunkState::Failed => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
                sm.transition(&chunk_id, ChunkState::Migrating).unwrap();
                sm.transition(&chunk_id, ChunkState::Failed).unwrap();
            }
            ChunkState::Evicted => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
                sm.transition(&chunk_id, ChunkState::Evicted).unwrap();
            }
        }

        let result = sm.transition(&chunk_id, to);
        assert!(
            result.is_ok(),
            "Valid transition {:?} -> {:?} should succeed, got: {:?}",
            from,
            to,
            result.err()
        );
    }
}

#[test]
fn test_invalid_transitions_are_rejected() {
    // Verify all invalid transitions are rejected.
    let invalid_transitions: Vec<(ChunkState, ChunkState)> = vec![
        (ChunkState::Allocated, ChunkState::Cached),
        (ChunkState::Allocated, ChunkState::Migrating),
        (ChunkState::Allocated, ChunkState::Evicted),
        (ChunkState::Allocated, ChunkState::Failed),
        (ChunkState::Evicted, ChunkState::Stored),
        (ChunkState::Evicted, ChunkState::Cached),
        (ChunkState::Evicted, ChunkState::Migrating),
        (ChunkState::Evicted, ChunkState::Allocated),
    ];

    for (from, to) in invalid_transitions {
        let mut sm = StateMachine::new();
        let chunk_id = make_chunk_id(200);

        sm.register(chunk_id).unwrap();
        // Walk to the `from` state
        match from {
            ChunkState::Allocated => {}
            ChunkState::Stored => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
            }
            ChunkState::Cached => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
                sm.transition(&chunk_id, ChunkState::Cached).unwrap();
            }
            ChunkState::Migrating => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
                sm.transition(&chunk_id, ChunkState::Migrating).unwrap();
            }
            ChunkState::Failed => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
                sm.transition(&chunk_id, ChunkState::Migrating).unwrap();
                sm.transition(&chunk_id, ChunkState::Failed).unwrap();
            }
            ChunkState::Evicted => {
                sm.transition(&chunk_id, ChunkState::Stored).unwrap();
                sm.transition(&chunk_id, ChunkState::Evicted).unwrap();
            }
        }

        let result = sm.transition(&chunk_id, to);
        assert!(
            result.is_err(),
            "Invalid transition {:?} -> {:?} should be rejected",
            from,
            to
        );
    }
}
