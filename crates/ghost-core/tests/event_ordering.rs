//! Event ordering verification tests.
//!
//! These tests verify the [Canonical Event Ordering Contract](EVENT_ORDERING_CONTRACT.md):
//! - Sequence IDs are strictly monotonically increasing
//! - Causal ordering is preserved (e.g., MigrationStarted before MigrationCompleted)
//! - Per-chunk ordering is maintained
//! - Cross-subsystem event ordering is deterministic
//! - EventMultiplexer does not reorder events
//! - Gap detection works correctly

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::event_multiplexer::{EventHandler, EventMultiplexer, NoopHandler};
use ghost_core::events::{Event, EventRecord, InvariantSeverity};
use ghost_core::io_events::IoOperation;
use ghost_core::state::PressureState;
use ghost_core::types::{ChunkId, TierId};

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn test_channel() -> (EventEmitter, tokio::sync::mpsc::Receiver<EventRecord>) {
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    (EventEmitter::new(tx), rx)
}

fn make_event_record(sequence_id: u64, event: Event) -> EventRecord {
    EventRecord {
        sequence_id,
        timestamp: 0,
        event,
    }
}

fn chunk_id(name: &[u8]) -> ChunkId {
    ChunkId::from_data(name)
}

/// A handler that records all events it receives.
struct RecordingHandler {
    events: Arc<std::sync::Mutex<Vec<EventRecord>>>,
}

impl RecordingHandler {
    fn new() -> (Self, Arc<std::sync::Mutex<Vec<EventRecord>>>) {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        (
            Self {
                events: Arc::clone(&events),
            },
            events,
        )
    }
}

impl EventHandler for RecordingHandler {
    fn handle(
        &self,
        event: &EventRecord,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                > + Send
                + '_,
        >,
    > {
        self.events.lock().unwrap().push(event.clone());
        Box::pin(async { Ok(()) })
    }
}

// ─── Test: Sequence ID Monotonicity ───────────────────────────────────────────

/// Emit 100 events and verify sequence IDs are strictly increasing.
#[tokio::test]
async fn test_sequence_id_monotonic() {
    let (emitter, mut rx) = test_channel();
    let id = chunk_id(b"test");

    // Emit 100 events
    for _ in 0..100 {
        emitter
            .allocation_created(id, TierId::Ram, 1024)
            .await
            .unwrap();
    }

    // Drop the emitter so the channel closes
    drop(emitter);

    let mut prev_id = 0u64;
    let mut count = 0;
    while let Some(record) = rx.recv().await {
        assert!(
            record.sequence_id > prev_id,
            "sequence_id {} should be strictly greater than previous {}",
            record.sequence_id,
            prev_id
        );
        prev_id = record.sequence_id;
        count += 1;
    }
    assert_eq!(count, 100, "Should have received exactly 100 events");
}

// ─── Test: Causal Ordering (Migration) ────────────────────────────────────────

/// Run a migration, verify MigrationStarted < MigrationCompleted in sequence IDs.
#[tokio::test]
async fn test_causal_ordering_migration() {
    let (emitter, mut rx) = test_channel();
    let id = chunk_id(b"migration_test");

    // Emit migration lifecycle events
    emitter
        .migration_started(id, TierId::Ram, TierId::Disk)
        .await
        .unwrap();
    emitter
        .migration_completed(id, TierId::Ram, TierId::Disk, 150)
        .await
        .unwrap();

    // Drop the emitter so the channel closes
    drop(emitter);

    let mut started_seq = None;
    let mut completed_seq = None;

    while let Some(record) = rx.recv().await {
        match &record.event {
            Event::MigrationStarted { chunk_id, .. } if *chunk_id == id => {
                started_seq = Some(record.sequence_id);
            }
            Event::MigrationCompleted { chunk_id, .. } if *chunk_id == id => {
                completed_seq = Some(record.sequence_id);
            }
            _ => {}
        }
    }

    let started = started_seq.expect("MigrationStarted event should have been received");
    let completed = completed_seq.expect("MigrationCompleted event should have been received");
    assert!(
        started < completed,
        "MigrationStarted (seq={}) should have lower sequence_id than MigrationCompleted (seq={})",
        started,
        completed
    );
}

// ─── Test: Causal Ordering (I/O) ──────────────────────────────────────────────

/// Run I/O operations, verify IoRequestIssued < IoRequestCompleted.
#[tokio::test]
async fn test_causal_ordering_io() {
    let (emitter, mut rx) = test_channel();
    let id = chunk_id(b"io_test");

    // Emit I/O lifecycle events
    emitter
        .io_request_issued(IoOperation::Write, id, TierId::Disk)
        .await
        .unwrap();
    emitter
        .io_request_completed(IoOperation::Write, id, TierId::Disk, 42)
        .await
        .unwrap();

    // Drop the emitter so the channel closes
    drop(emitter);

    let mut issued_seq = None;
    let mut completed_seq = None;

    while let Some(record) = rx.recv().await {
        match &record.event {
            Event::IoRequestIssued {
                chunk_id, operation, ..
            } if *chunk_id == id && *operation == IoOperation::Write => {
                issued_seq = Some(record.sequence_id);
            }
            Event::IoRequestCompleted {
                chunk_id, operation, ..
            } if *chunk_id == id && *operation == IoOperation::Write => {
                completed_seq = Some(record.sequence_id);
            }
            _ => {}
        }
    }

    let issued = issued_seq.expect("IoRequestIssued event should have been received");
    let completed = completed_seq.expect("IoRequestCompleted event should have been received");
    assert!(
        issued < completed,
        "IoRequestIssued (seq={}) should have lower sequence_id than IoRequestCompleted (seq={})",
        issued,
        completed
    );
}

// ─── Test: Per-Chunk Ordering ─────────────────────────────────────────────────

/// Run operations on the same chunk, verify causal order is maintained.
#[tokio::test]
async fn test_per_chunk_ordering() {
    let (emitter, mut rx) = test_channel();
    let id = chunk_id(b"chunk_ordering_test");

    // Emit a sequence of operations on the same chunk
    emitter
        .allocation_created(id, TierId::Ram, 4096)
        .await
        .unwrap();
    emitter
        .io_request_issued(IoOperation::Write, id, TierId::Ram)
        .await
        .unwrap();
    emitter
        .io_request_completed(IoOperation::Write, id, TierId::Ram, 100)
        .await
        .unwrap();
    emitter
        .migration_started(id, TierId::Ram, TierId::Disk)
        .await
        .unwrap();
    emitter
        .migration_completed(id, TierId::Ram, TierId::Disk, 200)
        .await
        .unwrap();

    // Drop the emitter so the channel closes
    drop(emitter);

    let mut chunk_events = Vec::new();
    while let Some(record) = rx.recv().await {
        if record.chunk_id() == Some(id) {
            chunk_events.push(record.sequence_id);
        }
    }

    // Verify sequence IDs for this chunk are strictly increasing
    for window in chunk_events.windows(2) {
        assert!(
            window[0] < window[1],
            "Events for chunk should be in strictly increasing order: {} >= {}",
            window[0],
            window[1]
        );
    }

    assert_eq!(chunk_events.len(), 5, "Should have 5 events for the chunk");
}

// ─── Test: Cross-Subsystem Determinism ────────────────────────────────────────

/// Run identical workloads 3 times, verify identical event sequences.
#[tokio::test]
async fn test_cross_subsystem_determinism() {
    let mut all_sequences = Vec::new();

    for _run in 0..3 {
        let (emitter, mut rx) = test_channel();
        let id = chunk_id(b"determinism_test");

        // Emit a mixed workload across subsystems
        emitter
            .allocation_created(id, TierId::Ram, 4096)
            .await
            .unwrap();
        emitter
            .pressure_changed(TierId::Ram, PressureState::new(), PressureState::new())
            .await
            .unwrap();
        emitter
            .io_request_issued(IoOperation::Read, id, TierId::Ram)
            .await
            .unwrap();
        emitter
            .migration_started(id, TierId::Ram, TierId::Disk)
            .await
            .unwrap();
        emitter
            .io_request_completed(IoOperation::Read, id, TierId::Ram, 50)
            .await
            .unwrap();
        emitter
            .migration_completed(id, TierId::Ram, TierId::Disk, 100)
            .await
            .unwrap();

        // Drop the emitter so the channel closes
        drop(emitter);

        let mut sequence = Vec::new();
        while let Some(record) = rx.recv().await {
            sequence.push(record.event_name().to_string());
        }
        all_sequences.push(sequence);
    }

    // All three runs should produce identical event name sequences
    assert_eq!(all_sequences.len(), 3);
    assert_eq!(
        all_sequences[0], all_sequences[1],
        "Run 1 and Run 2 should produce identical event sequences"
    );
    assert_eq!(
        all_sequences[1], all_sequences[2],
        "Run 2 and Run 3 should produce identical event sequences"
    );
}

// ─── Test: No Multiplexer Reordering ──────────────────────────────────────────

/// Verify EventMultiplexer delivers events in channel order.
#[tokio::test]
async fn test_no_multiplexer_reordering() {
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (handler, recorded) = RecordingHandler::new();

    let multiplexer = EventMultiplexer::new(rx).with_handler(Box::new(handler));

    // Send events with explicit sequence IDs in a specific order
    let events = vec![
        make_event_record(1, Event::AllocationCreated {
            chunk_id: chunk_id(b"a"),
            tier: TierId::Ram,
            size: 1024,
            sequence_id: 0,
        }),
        make_event_record(2, Event::MigrationStarted {
            chunk_id: chunk_id(b"b"),
            from: TierId::Ram,
            to: TierId::Disk,
            sequence_id: 0,
        }),
        make_event_record(3, Event::IoRequestIssued {
            operation: IoOperation::Read,
            chunk_id: chunk_id(b"c"),
            tier: TierId::Ram,
            sequence_id: 0,
        }),
        make_event_record(4, Event::PressureChanged {
            tier: TierId::Ram,
            old: PressureState::new(),
            new: PressureState::new(),
            sequence_id: 0,
        }),
        make_event_record(5, Event::MigrationCompleted {
            chunk_id: chunk_id(b"b"),
            from: TierId::Ram,
            to: TierId::Disk,
            duration_ms: 100,
            sequence_id: 0,
        }),
    ];

    for event in events {
        tx.send(event).await.unwrap();
    }
    drop(tx);

    multiplexer.run().await;

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.len(), 5);

    // Verify the order matches what was sent
    let expected_order = vec![
        "allocation_created",
        "migration_started",
        "io_request_issued",
        "pressure_changed",
        "migration_completed",
    ];
    let actual_order: Vec<&str> = recorded.iter().map(|r| r.event_name()).collect();
    assert_eq!(
        actual_order, expected_order,
        "Multiplexer should deliver events in exact channel order"
    );

    // Verify sequence IDs are in order
    let seq_ids: Vec<u64> = recorded.iter().map(|r| r.sequence_id).collect();
    assert_eq!(seq_ids, vec![1, 2, 3, 4, 5]);
}

// ─── Test: Gap Detection ──────────────────────────────────────────────────────

/// Simulate a gap in sequence IDs, verify InvariantViolation is emitted.
#[tokio::test]
async fn test_gap_detection() {
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (inv_tx, mut inv_rx) = tokio::sync::mpsc::channel(64);

    let multiplexer = EventMultiplexer::new(rx)
        .with_handler(Box::new(NoopHandler))
        .with_invariant_channel(inv_tx);

    // Send events with a gap: 1, 2, 5 (missing 3 and 4)
    let event = Event::AllocationCreated {
        chunk_id: chunk_id(b"gap_test"),
        tier: TierId::Ram,
        size: 1024,
        sequence_id: 0,
    };

    tx.send(make_event_record(1, event.clone())).await.unwrap();
    tx.send(make_event_record(2, event.clone())).await.unwrap();
    tx.send(make_event_record(5, event)).await.unwrap(); // Gap: missing 3, 4
    drop(tx);

    multiplexer.run().await;

    // Should have received an invariant violation for the gap
    let violation = inv_rx.recv().await.expect("Should receive invariant violation");
    match &violation.event {
        Event::InvariantViolation { rule, details, severity, .. } => {
            assert_eq!(rule, "event_ordering_gap");
            assert!(details.contains("gap"), "Details should mention gap: {}", details);
            assert_eq!(*severity, InvariantSeverity::Error);
        }
        other => panic!("Expected InvariantViolation, got {:?}", other),
    }
}

// ─── Test: Reordering Detection ───────────────────────────────────────────────

/// Simulate event reordering, verify InvariantViolation is emitted.
#[tokio::test]
async fn test_reordering_detection() {
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (inv_tx, mut inv_rx) = tokio::sync::mpsc::channel(64);

    let multiplexer = EventMultiplexer::new(rx)
        .with_handler(Box::new(NoopHandler))
        .with_invariant_channel(inv_tx);

    // Send events with reordering: 2, 1 (pure reordering, no gap)
    let event = Event::AllocationCreated {
        chunk_id: chunk_id(b"reorder_test"),
        tier: TierId::Ram,
        size: 1024,
        sequence_id: 0,
    };

    tx.send(make_event_record(2, event.clone())).await.unwrap();
    tx.send(make_event_record(1, event)).await.unwrap(); // Reordering: 1 < 2
    drop(tx);

    multiplexer.run().await;

    // Should have received an invariant violation for the reordering
    let violation = inv_rx.recv().await.expect("Should receive invariant violation");
    match &violation.event {
        Event::InvariantViolation { rule, details, severity, .. } => {
            assert_eq!(rule, "event_ordering_monotonic");
            assert!(details.contains("reordering"), "Details should mention reordering: {}", details);
            assert_eq!(*severity, InvariantSeverity::Critical);
        }
        other => panic!("Expected InvariantViolation, got {:?}", other),
    }
}

// ─── Test: Timestamps Non-Decreasing ──────────────────────────────────────────

/// Verify that timestamps are non-decreasing for sequential events.
#[tokio::test]
async fn test_timestamps_non_decreasing() {
    let (emitter, mut rx) = test_channel();
    let id = chunk_id(b"timestamp_test");

    // Emit events
    for _ in 0..10 {
        emitter
            .allocation_created(id, TierId::Ram, 1024)
            .await
            .unwrap();
    }

    // Drop the emitter so the channel closes
    drop(emitter);

    let mut prev_timestamp = 0u64;
    while let Some(record) = rx.recv().await {
        assert!(
            record.timestamp >= prev_timestamp,
            "Timestamp {} should be >= previous {}",
            record.timestamp,
            prev_timestamp
        );
        prev_timestamp = record.timestamp;
    }
}

// ─── Test: EventRecord Delegation ─────────────────────────────────────────────

/// Verify EventRecord properly delegates to inner Event methods.
#[test]
fn test_event_record_delegation() {
    let event = Event::MigrationStarted {
        chunk_id: chunk_id(b"delegation_test"),
        from: TierId::Ram,
        to: TierId::Disk,
        sequence_id: 42,
    };

    let record = EventRecord {
        sequence_id: 100,
        timestamp: 1234567890,
        event,
    };

    assert_eq!(record.chunk_id(), Some(chunk_id(b"delegation_test")));
    assert_eq!(record.tier(), Some(TierId::Ram));
    assert_eq!(record.category(), "migration");
    assert_eq!(record.event_name(), "migration_started");
    assert_eq!(record.sequence_id, 100);
    assert_eq!(record.timestamp, 1234567890);
}
