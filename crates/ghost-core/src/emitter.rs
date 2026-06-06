//! Typed event emitter for GhostPages.
//!
//! [`EventEmitter`] wraps an `mpsc::Sender<EventRecord>` and provides typed
//! convenience methods for emitting events from each category. Subsystems
//! hold an `EventEmitter` and call the appropriate method instead of
//! constructing raw `Event` values manually.
//!
//! Events are automatically stamped with a monotonically increasing
//! [`sequence_id`](EventRecord::sequence_id) and a timestamp at emission
//! time, providing total ordering across all events emitted through this
//! emitter.
//!
//! # Example
//!
//! ```
//! use ghost_core::emitter::EventEmitter;
//! use ghost_core::types::{ChunkId, TierId};
//!
//! let (tx, rx) = tokio::sync::mpsc::channel(256);
//! let emitter = EventEmitter::new(tx);
//!
//! emitter.allocation_created(ChunkId::from_data(b"chunk1"), TierId::Ram, 4096);
//! // rx.recv() would now return EventRecord { event: Event::AllocationCreated { ... }, ... }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

use crate::events::{BackendHealth, Event, EventRecord, InvariantSeverity};
use crate::state::PressureState;
use crate::types::{ChunkId, TierId};

/// Typed event emitter that wraps an `mpsc::Sender<EventRecord>`.
///
/// Use [`EventEmitter::new`] to create an emitter, then call the typed
/// methods to emit events. The emitter is `Clone` so it can be shared
/// across tasks and subsystems.
///
/// Each emitter owns an internal monotonic counter. Every event is stamped
/// with the next sequence id at emission time, so events from a single
/// emitter are totally ordered.
#[derive(Clone)]
pub struct EventEmitter {
    tx: mpsc::Sender<EventRecord>,
    sequence_counter: std::sync::Arc<AtomicU64>,
}

impl std::fmt::Debug for EventEmitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventEmitter")
            .field("sequence_counter", &self.sequence_counter.load(Ordering::SeqCst))
            .finish()
    }
}

impl EventEmitter {
    /// Create a new emitter that sends events to the given channel.
    ///
    /// Sequence ids start at 1 and increase monotonically with each
    /// emission.
    pub fn new(tx: mpsc::Sender<EventRecord>) -> Self {
        Self {
            tx,
            sequence_counter: std::sync::Arc::new(AtomicU64::new(1)),
        }
    }

    /// Return the next sequence id (for testing / diagnostics).
    pub fn next_sequence_id(&self) -> u64 {
        self.sequence_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Create an EventRecord with the next sequence id and current timestamp.
    fn make_record(&self, event: Event) -> EventRecord {
        EventRecord {
            sequence_id: self.next_sequence_id(),
            timestamp: current_timestamp(),
            event,
        }
    }

    /// Emit an event synchronously using `try_send`.
    ///
    /// Returns `Err` if the channel is full or closed. This is intended for
    /// use from non-async contexts (e.g. synchronous subsystem methods).
    ///
    /// The event is automatically stamped with a monotonic sequence id and timestamp.
    pub fn try_emit(&self, event: Event) -> Result<(), mpsc::error::TrySendError<EventRecord>> {
        self.tx.try_send(self.make_record(event))
    }

    /// Emit an event, returning `Err` if the channel is closed.
    ///
    /// The event is automatically stamped with a monotonic sequence id and timestamp.
    pub async fn emit(&self, event: Event) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.tx.send(self.make_record(event)).await
    }

    // ── Allocation events ────────────────────────────────────────────────────

    /// Emit [`Event::AllocationCreated`].
    pub async fn allocation_created(
        &self,
        chunk_id: ChunkId,
        tier: TierId,
        size: usize,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::AllocationCreated {
            chunk_id,
            tier,
            size,
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::AllocationFreed`].
    pub async fn allocation_freed(
        &self,
        chunk_id: ChunkId,
        tier: TierId,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::AllocationFreed { chunk_id, tier, sequence_id: 0 }).await
    }

    /// Emit [`Event::AllocationFailed`].
    pub async fn allocation_failed(
        &self,
        chunk_id: ChunkId,
        reason: impl Into<String>,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::AllocationFailed {
            chunk_id,
            reason: reason.into(),
            sequence_id: 0,
        })
        .await
    }

    // ── Migration events ─────────────────────────────────────────────────────

    /// Emit [`Event::MigrationStarted`].
    pub async fn migration_started(
        &self,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::MigrationStarted { chunk_id, from, to, sequence_id: 0 })
            .await
    }

    /// Emit [`Event::MigrationCompleted`].
    pub async fn migration_completed(
        &self,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        duration_ms: u64,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::MigrationCompleted {
            chunk_id,
            from,
            to,
            duration_ms,
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::MigrationFailed`].
    pub async fn migration_failed(
        &self,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        reason: impl Into<String>,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::MigrationFailed {
            chunk_id,
            from,
            to,
            reason: reason.into(),
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::MigrationRolledBack`].
    pub async fn migration_rolled_back(
        &self,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::MigrationRolledBack { chunk_id, from, to, sequence_id: 0 })
            .await
    }

    // ── Replay events ────────────────────────────────────────────────────────

    /// Emit [`Event::ReplayStarted`].
    pub async fn replay_started(
        &self,
        trace_path: impl Into<String>,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::ReplayStarted {
            trace_path: trace_path.into(),
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::ReplayCompleted`].
    pub async fn replay_completed(
        &self,
        trace_path: impl Into<String>,
        events: usize,
        duration_ms: u64,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::ReplayCompleted {
            trace_path: trace_path.into(),
            events,
            duration_ms,
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::ReplayDivergence`].
    pub async fn replay_divergence(
        &self,
        trace_path: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::ReplayDivergence {
            trace_path: trace_path.into(),
            expected: expected.into(),
            actual: actual.into(),
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::ReplayInvariantViolation`].
    pub async fn replay_invariant_violation(
        &self,
        rule: impl Into<String>,
        details: impl Into<String>,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::ReplayInvariantViolation {
            rule: rule.into(),
            details: details.into(),
            sequence_id: 0,
        })
        .await
    }

    // ── Pressure events ──────────────────────────────────────────────────────

    /// Emit [`Event::PressureChanged`].
    pub async fn pressure_changed(
        &self,
        tier: TierId,
        old: PressureState,
        new: PressureState,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::PressureChanged { tier, old, new, sequence_id: 0 }).await
    }

    /// Emit [`Event::BackpressureActivated`].
    pub async fn backpressure_activated(
        &self,
        tier: TierId,
        level: impl Into<String>,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::BackpressureActivated {
            tier,
            level: level.into(),
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::BackpressureDeactivated`].
    pub async fn backpressure_deactivated(
        &self,
        tier: TierId,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::BackpressureDeactivated { tier, sequence_id: 0 }).await
    }

    // ── Failure events ───────────────────────────────────────────────────────

    /// Emit [`Event::BackendHealthChanged`].
    pub async fn backend_health_changed(
        &self,
        tier: TierId,
        old: BackendHealth,
        new: BackendHealth,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::BackendHealthChanged { tier, old, new, sequence_id: 0 })
            .await
    }

    /// Emit [`Event::RetryAttempted`].
    pub async fn retry_attempted(
        &self,
        chunk_id: ChunkId,
        attempt: u32,
        max_attempts: u32,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::RetryAttempted {
            chunk_id,
            attempt,
            max_attempts,
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::OperationFailed`].
    pub async fn operation_failed(
        &self,
        operation: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::OperationFailed {
            operation: operation.into(),
            reason: reason.into(),
            sequence_id: 0,
        })
        .await
    }

    // ── Invariant violation events ───────────────────────────────────────────

    /// Emit [`Event::InvariantViolation`].
    pub async fn invariant_violation(
        &self,
        rule: impl Into<String>,
        details: impl Into<String>,
        severity: InvariantSeverity,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::InvariantViolation {
            rule: rule.into(),
            details: details.into(),
            severity,
            sequence_id: 0,
        })
        .await
    }

    // ── I/O events ───────────────────────────────────────────────────────────

    /// Emit [`Event::IoRequestIssued`].
    pub async fn io_request_issued(
        &self,
        operation: crate::io_events::IoOperation,
        chunk_id: ChunkId,
        tier: TierId,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::IoRequestIssued {
            operation,
            chunk_id,
            tier,
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::IoRequestCompleted`].
    pub async fn io_request_completed(
        &self,
        operation: crate::io_events::IoOperation,
        chunk_id: ChunkId,
        tier: TierId,
        duration_ticks: u64,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::IoRequestCompleted {
            operation,
            chunk_id,
            tier,
            duration_ticks,
            sequence_id: 0,
        })
        .await
    }

    /// Emit [`Event::IoRequestFailed`].
    pub async fn io_request_failed(
        &self,
        operation: crate::io_events::IoOperation,
        chunk_id: ChunkId,
        tier: TierId,
        error: impl Into<String>,
    ) -> Result<(), mpsc::error::SendError<EventRecord>> {
        self.emit(Event::IoRequestFailed {
            operation,
            chunk_id,
            tier,
            error: error.into(),
            sequence_id: 0,
        })
        .await
    }
}

/// Get the current timestamp in seconds since Unix epoch.
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_channel() -> (EventEmitter, mpsc::Receiver<EventRecord>) {
        let (tx, rx) = mpsc::channel(64);
        (EventEmitter::new(tx), rx)
    }

    #[tokio::test]
    async fn test_allocation_created() {
        let (emitter, mut rx) = test_channel();
        let id = ChunkId::from_data(b"test");
        emitter
            .allocation_created(id, TierId::Ram, 4096)
            .await
            .unwrap();
        match rx.recv().await.unwrap() {
            EventRecord {
                event: Event::AllocationCreated {
                    chunk_id,
                    tier,
                    size,
                    ..
                },
                sequence_id,
                ..
            } => {
                assert_eq!(chunk_id, id);
                assert_eq!(tier, TierId::Ram);
                assert_eq!(size, 4096);
                assert!(sequence_id > 0, "sequence_id should be auto-stamped");
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_migration_completed() {
        let (emitter, mut rx) = test_channel();
        let id = ChunkId::from_data(b"mig");
        emitter
            .migration_completed(id, TierId::Ram, TierId::Disk, 150)
            .await
            .unwrap();
        match rx.recv().await.unwrap() {
            EventRecord {
                event: Event::MigrationCompleted {
                    chunk_id,
                    from,
                    to,
                    duration_ms,
                    ..
                },
                sequence_id,
                ..
            } => {
                assert_eq!(chunk_id, id);
                assert_eq!(from, TierId::Ram);
                assert_eq!(to, TierId::Disk);
                assert_eq!(duration_ms, 150);
                assert!(sequence_id > 0, "sequence_id should be auto-stamped");
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_backend_health_changed() {
        let (emitter, mut rx) = test_channel();
        emitter
            .backend_health_changed(TierId::Disk, BackendHealth::Healthy, BackendHealth::Degraded)
            .await
            .unwrap();
        match rx.recv().await.unwrap() {
            EventRecord {
                event: Event::BackendHealthChanged { tier, old, new, .. },
                sequence_id,
                ..
            } => {
                assert_eq!(tier, TierId::Disk);
                assert_eq!(old, BackendHealth::Healthy);
                assert_eq!(new, BackendHealth::Degraded);
                assert!(sequence_id > 0, "sequence_id should be auto-stamped");
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_invariant_violation() {
        let (emitter, mut rx) = test_channel();
        emitter
            .invariant_violation("no_orphans", "orphaned transfer detected", InvariantSeverity::Error)
            .await
            .unwrap();
        match rx.recv().await.unwrap() {
            EventRecord {
                event: Event::InvariantViolation {
                    rule,
                    details,
                    severity,
                    ..
                },
                sequence_id,
                ..
            } => {
                assert_eq!(rule, "no_orphans");
                assert_eq!(details, "orphaned transfer detected");
                assert_eq!(severity, InvariantSeverity::Error);
                assert!(sequence_id > 0, "sequence_id should be auto-stamped");
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_emitter_clone() {
        let (emitter, mut rx) = test_channel();
        let emitter2 = emitter.clone();

        emitter
            .allocation_created(ChunkId::from_data(b"a"), TierId::Ram, 100)
            .await
            .unwrap();
        emitter2
            .allocation_freed(ChunkId::from_data(b"b"), TierId::Disk)
            .await
            .unwrap();

        let rec1 = rx.recv().await.unwrap();
        let rec2 = rx.recv().await.unwrap();
        assert!(matches!(rec1.event, Event::AllocationCreated { .. }));
        assert!(matches!(rec2.event, Event::AllocationFreed { .. }));
    }

    #[tokio::test]
    async fn test_sequence_ids_monotonic() {
        let (emitter, mut rx) = test_channel();
        let id = ChunkId::from_data(b"test");

        // Emit 10 events
        for _ in 0..10 {
            emitter
                .allocation_created(id, TierId::Ram, 1024)
                .await
                .unwrap();
        }

        let mut prev_id = 0;
        for _ in 0..10 {
            let rec = rx.recv().await.unwrap();
            assert!(
                rec.sequence_id > prev_id,
                "sequence_id {} should be > {}",
                rec.sequence_id,
                prev_id
            );
            prev_id = rec.sequence_id;
        }
    }

    #[tokio::test]
    async fn test_timestamps_non_decreasing() {
        let (emitter, mut rx) = test_channel();
        let id = ChunkId::from_data(b"test");

        for _ in 0..5 {
            emitter
                .allocation_created(id, TierId::Ram, 1024)
                .await
                .unwrap();
        }

        let mut prev_ts = 0;
        for _ in 0..5 {
            let rec = rx.recv().await.unwrap();
            assert!(
                rec.timestamp >= prev_ts,
                "timestamp {} should be >= {}",
                rec.timestamp,
                prev_ts
            );
            prev_ts = rec.timestamp;
        }
    }
}
