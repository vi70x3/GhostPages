//! Dedicated invariant validation tests for Phase 2.
//!
//! Each test validates one of the 6 built-in invariant rules:
//! - NoOrphanedTransfers
//! - NoIllegalTransitions
//! - NoDanglingAllocations
//! - NoTimestampRegression
//! - NoMissingCompletions
//! - StateMachineConsistency

use ghost_core::state::ChunkState;
use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, TierId};
use ghost_replay::invariants::{
    InvariantValidator, ReplayInvariant, ViolationSeverity,
};
use ghost_replay::invariants::{
    NoDanglingAllocations, NoIllegalTransitions, NoMissingCompletions,
    NoOrphanedTransfers, NoTimestampRegression, StateMachineConsistency,
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn make_chunk_id(seed: u8) -> ChunkId {
    let mut data = [0u8; 32];
    data[0] = seed;
    ChunkId::from_data(&data)
}

/// Create a well-formed event stream with no violations.
fn valid_event_stream() -> Vec<TraceEvent> {
    let mut events = Vec::new();
    let chunk_id = make_chunk_id(1);
    let base_ts = 1_000_000;

    // Create chunk
    events.push(TraceEvent::ChunkCreated {
        chunk_id,
        size: 4096,
        tier: TierId::Ram,
        timestamp: base_ts,
    });

    // Allocated -> Stored
    events.push(TraceEvent::ChunkStateChanged {
        chunk_id,
        from: ChunkState::Allocated,
        to: ChunkState::Stored,
        timestamp: base_ts + 10,
    });

    // Transfer queued
    events.push(TraceEvent::TransferQueued {
        chunk_id,
        from: TierId::Ram,
        to: TierId::Simulation,
        priority: ghost_core::transfer::TransferPriority::Normal,
        timestamp: base_ts + 20,
    });

    // Transfer started
    events.push(TraceEvent::TransferStarted {
        job: ghost_core::transfer::TransferJob::new(
            chunk_id,
            TierId::Ram,
            TierId::Simulation,
            4096,
            ghost_core::transfer::TransferPriority::Normal,
        ),
        timestamp: base_ts + 30,
    });

    // Transfer completed
    events.push(TraceEvent::TransferCompleted {
        chunk_id,
        from: TierId::Ram,
        to: TierId::Simulation,
        size: 4096,
        duration_ms: 50,
        timestamp: base_ts + 40,
    });

    // Stored -> Cached
    events.push(TraceEvent::ChunkStateChanged {
        chunk_id,
        from: ChunkState::Stored,
        to: ChunkState::Cached,
        timestamp: base_ts + 50,
    });

    // Eviction event (no state change — just a record)
    events.push(TraceEvent::Eviction {
        chunk_id,
        tier: TierId::Simulation,
        reason: ghost_core::trace::EvictionReason::Capacity,
        timestamp: base_ts + 60,
    });

    // Cached -> Stored (valid transition, instead of invalid Cached -> Evicted)
    events.push(TraceEvent::ChunkStateChanged {
        chunk_id,
        from: ChunkState::Cached,
        to: ChunkState::Stored,
        timestamp: base_ts + 70,
    });

    // Stored -> Evicted (valid transition)
    events.push(TraceEvent::ChunkStateChanged {
        chunk_id,
        from: ChunkState::Stored,
        to: ChunkState::Evicted,
        timestamp: base_ts + 80,
    });

    // Delete chunk
    events.push(TraceEvent::ChunkDeleted {
        chunk_id,
        tier: TierId::Simulation,
        timestamp: base_ts + 90,
    });

    events
}

// ─── Test (a): No orphaned transfers ─────────────────────────────────────────

#[test]
fn test_no_orphaned_transfers() {
    let invariant = NoOrphanedTransfers;

    // Valid stream: all transfers have matching start/complete or start/fail
    let events = valid_event_stream();
    let violations = invariant.validate(&events);
    assert!(
        violations.is_empty(),
        "Valid stream should have no orphaned transfer violations, got: {:?}",
        violations
    );

    // Invalid stream: transfer started but never completed or failed
    let chunk_id = make_chunk_id(10);
    let mut bad_events = Vec::new();
    bad_events.push(TraceEvent::ChunkCreated {
        chunk_id,
        size: 1024,
        tier: TierId::Ram,
        timestamp: 1000,
    });
    bad_events.push(TraceEvent::TransferStarted {
        job: ghost_core::transfer::TransferJob::new(
            chunk_id,
            TierId::Ram,
            TierId::Simulation,
            1024,
            ghost_core::transfer::TransferPriority::Normal,
        ),
        timestamp: 1010,
    });
    // No TransferCompleted or TransferFailed — orphaned!

    let violations = invariant.validate(&bad_events);
    assert!(
        !violations.is_empty(),
        "Orphaned transfer should produce violations"
    );
    assert!(
        violations.iter().any(|v| v.severity == ViolationSeverity::Error),
        "Orphaned transfers should be Error severity"
    );

    // Verify violation message mentions the issue
    let desc = format!("{}", violations[0]);
    assert!(
        desc.to_lowercase().contains("orphan") || desc.to_lowercase().contains("transfer"),
        "Violation should mention orphaned transfer: {}",
        desc
    );
}

// ─── Test (b): No illegal transitions ────────────────────────────────────────

#[test]
fn test_no_illegal_transitions() {
    let invariant = NoIllegalTransitions;

    // Valid stream: all transitions are legal
    let events = valid_event_stream();
    let violations = invariant.validate(&events);
    assert!(
        violations.is_empty(),
        "Valid stream should have no illegal transition violations, got: {:?}",
        violations
    );

    // Invalid stream: Evicted -> Stored is not a valid transition
    let chunk_id = make_chunk_id(20);
    let mut bad_events = Vec::new();
    bad_events.push(TraceEvent::ChunkStateChanged {
        chunk_id,
        from: ChunkState::Allocated,
        to: ChunkState::Stored,
        timestamp: 1000,
    });
    bad_events.push(TraceEvent::ChunkStateChanged {
        chunk_id,
        from: ChunkState::Stored,
        to: ChunkState::Evicted,
        timestamp: 1010,
    });
    // Illegal: Evicted -> Stored
    bad_events.push(TraceEvent::ChunkStateChanged {
        chunk_id,
        from: ChunkState::Evicted,
        to: ChunkState::Stored,
        timestamp: 1020,
    });

    let violations = invariant.validate(&bad_events);
    assert!(
        !violations.is_empty(),
        "Illegal transition should produce violations"
    );

    // Verify the violation mentions the states
    let desc = format!("{}", violations[0]);
    assert!(
        desc.contains("Evicted") || desc.contains("Stored"),
        "Violation should mention the states involved: {}",
        desc
    );
}

// ─── Test (c): No dangling allocations ───────────────────────────────────────

#[test]
fn test_no_dangling_allocations() {
    let invariant = NoDanglingAllocations;

    // Valid stream: chunk created then deleted — no dangling
    let events = valid_event_stream();
    let violations = invariant.validate(&events);
    assert!(
        violations.is_empty(),
        "Valid stream should have no dangling allocation violations, got: {:?}",
        violations
    );

    // Invalid stream: chunk deleted without being created first
    // (The NoDanglingAllocations invariant flags chunks that are deleted
    // without ever having been created.)
    let chunk_id = make_chunk_id(30);
    let mut bad_events = Vec::new();
    bad_events.push(TraceEvent::ChunkDeleted {
        chunk_id,
        tier: TierId::Ram,
        timestamp: 1000,
    });

    let violations = invariant.validate(&bad_events);
    assert!(
        !violations.is_empty(),
        "Deleting a chunk that was never created should produce violations"
    );
    assert!(
        violations.iter().any(|v| v.severity == ViolationSeverity::Warning),
        "Dangling allocations should be Warning severity"
    );
}

// ─── Test (d): No timestamp regression ───────────────────────────────────────

#[test]
fn test_no_timestamp_regression() {
    let invariant = NoTimestampRegression;

    // Valid stream: monotonically increasing timestamps
    let events = valid_event_stream();
    let violations = invariant.validate(&events);
    assert!(
        violations.is_empty(),
        "Valid stream should have no timestamp regression violations, got: {:?}",
        violations
    );

    // Invalid stream: timestamp goes backwards
    let chunk_id = make_chunk_id(40);
    let mut bad_events = Vec::new();
    bad_events.push(TraceEvent::ChunkCreated {
        chunk_id,
        size: 1024,
        tier: TierId::Ram,
        timestamp: 2000,
    });
    bad_events.push(TraceEvent::ChunkStateChanged {
        chunk_id,
        from: ChunkState::Allocated,
        to: ChunkState::Stored,
        timestamp: 1500, // Regression! Earlier than previous event
    });

    let violations = invariant.validate(&bad_events);
    assert!(
        !violations.is_empty(),
        "Timestamp regression should produce violations"
    );
    assert!(
        violations.iter().any(|v| v.severity == ViolationSeverity::Error),
        "Timestamp regression should be Error severity"
    );

    // Verify the violation message
    let desc = format!("{}", violations[0]);
    assert!(
        desc.to_lowercase().contains("timestamp") || desc.to_lowercase().contains("regression"),
        "Violation should mention timestamp regression: {}",
        desc
    );
}

// ─── Test (e): No missing completions ────────────────────────────────────────

#[test]
fn test_no_missing_completions() {
    let invariant = NoMissingCompletions;

    // Valid stream: transfer started and completed
    let events = valid_event_stream();
    let violations = invariant.validate(&events);
    assert!(
        violations.is_empty(),
        "Valid stream should have no missing completion violations, got: {:?}",
        violations
    );

    // Invalid stream: transfer started but never completed or failed
    let chunk_id = make_chunk_id(50);
    let mut bad_events = Vec::new();
    bad_events.push(TraceEvent::TransferStarted {
        job: ghost_core::transfer::TransferJob::new(
            chunk_id,
            TierId::Ram,
            TierId::Simulation,
            1024,
            ghost_core::transfer::TransferPriority::Normal,
        ),
        timestamp: 1000,
    });
    // No TransferCompleted or TransferFailed — missing completion!

    let violations = invariant.validate(&bad_events);
    assert!(
        !violations.is_empty(),
        "Unfinished transfer should produce violations"
    );
}

// ─── Test (f): State machine consistency ─────────────────────────────────────

#[test]
fn test_state_machine_consistency() {
    let invariant = StateMachineConsistency;

    // Valid stream: state transitions follow the state machine
    let events = valid_event_stream();
    let violations = invariant.validate(&events);
    assert!(
        violations.is_empty(),
        "Valid stream should have no state machine consistency violations, got: {:?}",
        violations
    );

    // Invalid stream: duplicate ChunkCreated for same chunk
    let chunk_id = make_chunk_id(60);
    let mut bad_events = Vec::new();
    bad_events.push(TraceEvent::ChunkCreated {
        chunk_id,
        size: 1024,
        tier: TierId::Ram,
        timestamp: 1000,
    });
    // Duplicate create
    bad_events.push(TraceEvent::ChunkCreated {
        chunk_id,
        size: 1024,
        tier: TierId::Ram,
        timestamp: 1010,
    });

    let violations = invariant.validate(&bad_events);
    assert!(
        !violations.is_empty(),
        "Duplicate chunk creation should produce violations"
    );

    // Also test: state mismatch — ChunkStateChanged says from=Stored but
    // the invariant tracks the chunk as Allocated (the initial state from ChunkCreated)
    let chunk_id2 = make_chunk_id(61);
    let mut bad_events2 = Vec::new();
    bad_events2.push(TraceEvent::ChunkCreated {
        chunk_id: chunk_id2,
        size: 1024,
        tier: TierId::Ram,
        timestamp: 1000,
    });
    // The chunk is tracked as Allocated after ChunkCreated.
    // Now say it transitions from Stored (mismatch!)
    bad_events2.push(TraceEvent::ChunkStateChanged {
        chunk_id: chunk_id2,
        from: ChunkState::Stored,
        to: ChunkState::Cached,
        timestamp: 1010,
    });

    let violations = invariant.validate(&bad_events2);
    assert!(
        !violations.is_empty(),
        "State mismatch should produce violations"
    );
}

// ─── Additional: Combined validator with all invariants ───────────────────────

#[test]
fn test_combined_validator_all_invariants() {
    let validator = InvariantValidator::with_defaults();

    // Valid stream should pass all invariants
    let events = valid_event_stream();
    let violations = validator.validate(&events);
    assert!(
        violations.is_empty(),
        "Valid stream should pass all 6 invariants, got: {:?}",
        violations
    );

    // Severity ordering: Critical > Error > Warning > Info
    assert!(
        ViolationSeverity::Critical > ViolationSeverity::Error,
        "Critical should be more severe than Error"
    );
    assert!(
        ViolationSeverity::Error > ViolationSeverity::Warning,
        "Error should be more severe than Warning"
    );
    assert!(
        ViolationSeverity::Warning > ViolationSeverity::Info,
        "Warning should be more severe than Info"
    );
}
