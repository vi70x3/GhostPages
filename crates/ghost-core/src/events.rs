//! Unified event taxonomy for GhostPages observability.
//!
//! This module defines the [`Event`] enum — a single, structured event type that
//! all subsystems emit. Events are categorised into six groups:
//!
//! 1. **Allocation** — chunk creation, freeing, and allocation failures
//! 2. **Migration** — transfer start, completion, failure, and rollback
//! 3. **Replay** — replay lifecycle, divergence, and invariant violations
//! 4. **Pressure** — pressure changes and backpressure activation
//! 5. **Failure** — backend health changes, retries, and operation failures
//! 6. **InvariantViolation** — post-replay invariant check results
//!
//! Events flow through the [`EventMultiplexer`] to multiple [`EventHandler`]s
//! (trace log, metrics, tracing spans, custom consumers).

use serde::{Deserialize, Serialize};

use crate::io_events::{IoEvent, IoOperation};
use crate::state::PressureState;
use crate::types::{ChunkId, TierId};

// ─── Invariant Severity ────────────────────────────────────────────────────────

/// Severity of an invariant violation detected during replay validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum InvariantSeverity {
    /// Informational — not a correctness issue.
    Info,

    /// Warning — potential issue worth investigating.
    Warning,

    /// Error — correctness issue that should be addressed.
    Error,

    /// Critical — data corruption or safety issue.
    Critical,
}

impl std::fmt::Display for InvariantSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvariantSeverity::Info => write!(f, "info"),
            InvariantSeverity::Warning => write!(f, "warning"),
            InvariantSeverity::Error => write!(f, "error"),
            InvariantSeverity::Critical => write!(f, "critical"),
        }
    }
}

// ─── Backend Health ────────────────────────────────────────────────────────────

/// Health status of a storage backend.
///
/// This is the event-system representation of backend health, independent of
/// the daemon-internal `BackendHealth` type. Keeping it in `ghost-core` avoids
/// a circular dependency (ghost-core must not depend on ghost-daemon).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendHealth {
    /// Backend is operating normally.
    Healthy,

    /// Backend is experiencing degraded performance or reliability.
    Degraded,

    /// Backend is unavailable.
    Unavailable,

    /// Backend is recovering from a degraded/unavailable state.
    Recovering,
}

impl std::fmt::Display for BackendHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendHealth::Healthy => write!(f, "healthy"),
            BackendHealth::Degraded => write!(f, "degraded"),
            BackendHealth::Unavailable => write!(f, "unavailable"),
            BackendHealth::Recovering => write!(f, "recovering"),
        }
    }
}

// ─── Unified Event Enum ────────────────────────────────────────────────────────

/// A structured event emitted by any GhostPages subsystem.
///
/// All events are serialisable (`Serialize` + `Deserialize`) and carry a
/// timestamp for ordering. The [`EventMultiplexer`] fans events out to
/// registered [`EventHandler`]s.
///
/// # Example
///
/// ```
/// use ghost_core::events::{Event, InvariantSeverity};
/// use ghost_core::types::{ChunkId, TierId};
///
/// let event = Event::AllocationCreated {
///     chunk_id: ChunkId::from_data(b"hello"),
///     tier: TierId::Ram,
///     size: 4096,
///     sequence_id: 0,
/// };
///
/// match event {
///     Event::AllocationCreated { chunk_id, tier, size, .. } => {
///         println!("Created {} on {:?} ({} bytes)", chunk_id, tier, size);
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    // ── Allocation ───────────────────────────────────────────────────────────

    /// A new chunk was allocated in a tier.
    AllocationCreated {
        sequence_id: u64,
        chunk_id: ChunkId,
        tier: TierId,
        size: usize,
    },

    /// A chunk was freed from a tier.
    AllocationFreed {
        sequence_id: u64,
        chunk_id: ChunkId,
        tier: TierId,
    },

    /// An allocation operation failed.
    AllocationFailed {
        sequence_id: u64,
        chunk_id: ChunkId,
        reason: String,
    },
    /// A chunk eviction occurred.
    Eviction {
        sequence_id: u64,
        chunk_id: ChunkId,
        tier: TierId,
        reason: String,
    },
    /// A retrieve operation was performed.
    Retrieve {
        sequence_id: u64,
        key: String,
        hit: bool,
    },
    /// A transfer completed successfully.
    TransferCompleted {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        duration_ms: u64,
    },
    /// A transfer failed.
    TransferFailed {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        reason: String,
    },
    /// A store operation was performed (state mutation).
    Store {
        sequence_id: u64,
        key: String,
        value_size: usize,
    },
    /// An evict operation was performed.
    Evict {
        sequence_id: u64,
        key: String,
    },
    /// An item was enqueued in the scheduler queue.
    QueueEnqueue {
        sequence_id: u64,
        task_id: u64,
    },
    /// An item was dequeued from the scheduler queue.
    QueueDequeue {
        sequence_id: u64,
        task_id: u64,
    },
    /// A migration decision was made.
    MigrationDecision {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        decision: String,
    },

    /// A migration was approved and will proceed.
    MigrationDecided {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        cost_score: f64,
    },

    /// A migration was deferred due to I/O pressure or queue depth.
    MigrationDeferred {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        reason: String,
    },

    /// A migration was rejected due to physical cost exceeding threshold.
    MigrationRejected {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        cost_score: f64,
        threshold: f64,
    },

    // ── Migration ────────────────────────────────────────────────────────────

    /// A chunk migration between tiers was started.
    MigrationStarted {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
    },

    /// A chunk migration completed successfully.
    MigrationCompleted {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        duration_ms: u64,
    },

    /// A chunk migration failed.
    MigrationFailed {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        reason: String,
    },

    /// A failed migration was rolled back to the source tier.
    MigrationRolledBack {
        sequence_id: u64,
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
    },

    // ── Replay ───────────────────────────────────────────────────────────────

    /// A trace replay was started.
    ReplayStarted {
        sequence_id: u64,
        trace_path: String,
    },

    /// A trace replay completed successfully.
    ReplayCompleted {
        sequence_id: u64,
        trace_path: String,
        events: usize,
        duration_ms: u64,
    },

    /// A replay diverged from the expected trace.
    ReplayDivergence {
        sequence_id: u64,
        trace_path: String,
        expected: String,
        actual: String,
    },

    /// An invariant violation was detected during replay.
    ReplayInvariantViolation {
        sequence_id: u64,
        rule: String,
        details: String,
    },

    // ── Pressure ─────────────────────────────────────────────────────────────

    /// System pressure changed for a tier.
    PressureChanged {
        sequence_id: u64,
        tier: TierId,
        old: PressureState,
        new: PressureState,
    },

    /// Backpressure was activated for a tier.
    BackpressureActivated {
        sequence_id: u64,
        tier: TierId,
        level: String,
    },

    /// Backpressure was deactivated for a tier.
    BackpressureDeactivated {
        sequence_id: u64,
        tier: TierId,
    },

    // ── Failure ──────────────────────────────────────────────────────────────

    /// A backend's health status changed.
    BackendHealthChanged {
        sequence_id: u64,
        tier: TierId,
        old: BackendHealth,
        new: BackendHealth,
    },

    /// A transfer is being retried.
    RetryAttempted {
        sequence_id: u64,
        chunk_id: ChunkId,
        attempt: u32,
        max_attempts: u32,
    },

    /// An operation failed irrecoverably.
    OperationFailed {
        sequence_id: u64,
        operation: String,
        reason: String,
    },

    // ── Invariant Violation ──────────────────────────────────────────────────

    /// An invariant was violated during replay validation.
    InvariantViolation {
        sequence_id: u64,
        rule: String,
        details: String,
        severity: InvariantSeverity,
    },

    // ── I/O ────────────────────────────────────────────────────────────────────

    /// An I/O request was issued to a tier.
    IoRequestIssued {
        sequence_id: u64,
        operation: IoOperation,
        chunk_id: ChunkId,
        tier: TierId,
    },

    /// An I/O request completed successfully.
    IoRequestCompleted {
        sequence_id: u64,
        operation: IoOperation,
        chunk_id: ChunkId,
        tier: TierId,
        duration_ticks: u64,
    },

    /// An I/O request failed.
    IoRequestFailed {
        sequence_id: u64,
        operation: IoOperation,
        chunk_id: ChunkId,
        tier: TierId,
        error: String,
    },

    /// A flush (fsync) was issued for a tier.
    IoFlushIssued {
        sequence_id: u64,
        tier: TierId,
    },

    /// A flush (fsync) completed for a tier.
    IoFlushCompleted {
        sequence_id: u64,
        tier: TierId,
        duration_ticks: u64,
    },

    /// The buffer fill level changed for a tier.
    IoBufferStateChange {
        sequence_id: u64,
        tier: TierId,
        buffered: usize,
        capacity: usize,
    },
}

// ─── Event Record ──────────────────────────────────────────────────────────────

/// A wrapper around [`Event`] that adds ordering metadata.
///
/// `EventRecord` is the canonical type emitted by [`EventEmitter`] and
/// consumed by [`EventMultiplexer`]. It provides:
///
/// - `sequence_id`: A monotonically increasing counter for total ordering.
/// - `timestamp`: The emission time from [`TimeProvider`].
/// - `event`: The inner [`Event`] payload.
///
/// # Ordering Contract
///
/// Per the Canonical Event Ordering Contract:
/// - `sequence_id` MUST be strictly monotonically increasing.
/// - `timestamp` MUST be non-decreasing.
/// - The `EventMultiplexer` MUST deliver `EventRecord`s in `sequence_id` order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    /// Monotonically increasing sequence ID.
    ///
    /// Assigned by [`EventEmitter`] at emission time. Provides a total order
    /// across all events in a single process run.
    pub sequence_id: u64,

    /// Emission timestamp (seconds since Unix epoch).
    ///
    /// Sourced from the [`TimeProvider`] active at emission time.
    pub timestamp: u64,

    /// The inner event payload.
    pub event: Event,
}

impl EventRecord {
    /// Get the [`Event`] reference.
    pub fn event(&self) -> &Event {
        &self.event
    }

    /// Get the [`ChunkId`] associated with this event, if any.
    pub fn chunk_id(&self) -> Option<ChunkId> {
        self.event.chunk_id()
    }

    /// Get the [`TierId`] associated with this event, if any.
    pub fn tier(&self) -> Option<TierId> {
        self.event.tier()
    }

    /// Get the human-readable category name for this event.
    pub fn category(&self) -> &'static str {
        self.event.category()
    }

    /// Get the human-readable event name.
    pub fn event_name(&self) -> &'static str {
        self.event.event_name()
    }
}

impl Event {
    /// Get the sequence ID for this event.
    ///
    /// The sequence ID is a monotonically increasing counter assigned by the
    /// [`EventEmitter`] at emission time. It provides a total order across all
    /// events in a single process run, enabling replay equivalence checks.
    pub fn sequence_id(&self) -> u64 {
        match self {
            Event::AllocationCreated { sequence_id, .. } => *sequence_id,
            Event::AllocationFreed { sequence_id, .. } => *sequence_id,
            Event::AllocationFailed { sequence_id, .. } => *sequence_id,
            Event::Eviction { sequence_id, .. } => *sequence_id,
            Event::Retrieve { sequence_id, .. } => *sequence_id,
            Event::TransferCompleted { sequence_id, .. } => *sequence_id,
            Event::TransferFailed { sequence_id, .. } => *sequence_id,
            Event::Store { sequence_id, .. } => *sequence_id,
            Event::Evict { sequence_id, .. } => *sequence_id,
            Event::QueueEnqueue { sequence_id, .. } => *sequence_id,
            Event::QueueDequeue { sequence_id, .. } => *sequence_id,
            Event::MigrationDecision { sequence_id, .. } => *sequence_id,
            Event::MigrationDecided { sequence_id, .. } => *sequence_id,
            Event::MigrationDeferred { sequence_id, .. } => *sequence_id,
            Event::MigrationRejected { sequence_id, .. } => *sequence_id,
            Event::MigrationStarted { sequence_id, .. } => *sequence_id,
            Event::MigrationCompleted { sequence_id, .. } => *sequence_id,
            Event::MigrationFailed { sequence_id, .. } => *sequence_id,
            Event::MigrationRolledBack { sequence_id, .. } => *sequence_id,
            Event::ReplayStarted { sequence_id, .. } => *sequence_id,
            Event::ReplayCompleted { sequence_id, .. } => *sequence_id,
            Event::ReplayDivergence { sequence_id, .. } => *sequence_id,
            Event::ReplayInvariantViolation { sequence_id, .. } => *sequence_id,
            Event::PressureChanged { sequence_id, .. } => *sequence_id,
            Event::BackpressureActivated { sequence_id, .. } => *sequence_id,
            Event::BackpressureDeactivated { sequence_id, .. } => *sequence_id,
            Event::BackendHealthChanged { sequence_id, .. } => *sequence_id,
            Event::RetryAttempted { sequence_id, .. } => *sequence_id,
            Event::OperationFailed { sequence_id, .. } => *sequence_id,
            Event::InvariantViolation { sequence_id, .. } => *sequence_id,
            Event::IoRequestIssued { sequence_id, .. } => *sequence_id,
            Event::IoRequestCompleted { sequence_id, .. } => *sequence_id,
            Event::IoRequestFailed { sequence_id, .. } => *sequence_id,
            Event::IoFlushIssued { sequence_id, .. } => *sequence_id,
            Event::IoFlushCompleted { sequence_id, .. } => *sequence_id,
            Event::IoBufferStateChange { sequence_id, .. } => *sequence_id,
        }
    }

    /// Set the sequence ID for this event, returning a new event with the
    /// given sequence ID. This is used by the [`EventEmitter`] to stamp
    /// events with a monotonically increasing counter at emission time.
    pub fn with_sequence_id(mut self, sequence_id: u64) -> Self {
        match &mut self {
            Event::AllocationCreated { sequence_id: s, .. } => *s = sequence_id,
            Event::AllocationFreed { sequence_id: s, .. } => *s = sequence_id,
            Event::AllocationFailed { sequence_id: s, .. } => *s = sequence_id,
            Event::Eviction { sequence_id: s, .. } => *s = sequence_id,
            Event::Retrieve { sequence_id: s, .. } => *s = sequence_id,
            Event::TransferCompleted { sequence_id: s, .. } => *s = sequence_id,
            Event::TransferFailed { sequence_id: s, .. } => *s = sequence_id,
            Event::Store { sequence_id: s, .. } => *s = sequence_id,
            Event::Evict { sequence_id: s, .. } => *s = sequence_id,
            Event::QueueEnqueue { sequence_id: s, .. } => *s = sequence_id,
            Event::QueueDequeue { sequence_id: s, .. } => *s = sequence_id,
            Event::MigrationDecision { sequence_id: s, .. } => *s = sequence_id,
            Event::MigrationDecided { sequence_id: s, .. } => *s = sequence_id,
            Event::MigrationDeferred { sequence_id: s, .. } => *s = sequence_id,
            Event::MigrationRejected { sequence_id: s, .. } => *s = sequence_id,
            Event::MigrationStarted { sequence_id: s, .. } => *s = sequence_id,
            Event::MigrationCompleted { sequence_id: s, .. } => *s = sequence_id,
            Event::MigrationFailed { sequence_id: s, .. } => *s = sequence_id,
            Event::MigrationRolledBack { sequence_id: s, .. } => *s = sequence_id,
            Event::ReplayStarted { sequence_id: s, .. } => *s = sequence_id,
            Event::ReplayCompleted { sequence_id: s, .. } => *s = sequence_id,
            Event::ReplayDivergence { sequence_id: s, .. } => *s = sequence_id,
            Event::ReplayInvariantViolation { sequence_id: s, .. } => *s = sequence_id,
            Event::PressureChanged { sequence_id: s, .. } => *s = sequence_id,
            Event::BackpressureActivated { sequence_id: s, .. } => *s = sequence_id,
            Event::BackpressureDeactivated { sequence_id: s, .. } => *s = sequence_id,
            Event::BackendHealthChanged { sequence_id: s, .. } => *s = sequence_id,
            Event::RetryAttempted { sequence_id: s, .. } => *s = sequence_id,
            Event::OperationFailed { sequence_id: s, .. } => *s = sequence_id,
            Event::InvariantViolation { sequence_id: s, .. } => *s = sequence_id,
            Event::IoRequestIssued { sequence_id: s, .. } => *s = sequence_id,
            Event::IoRequestCompleted { sequence_id: s, .. } => *s = sequence_id,
            Event::IoRequestFailed { sequence_id: s, .. } => *s = sequence_id,
            Event::IoFlushIssued { sequence_id: s, .. } => *s = sequence_id,
            Event::IoFlushCompleted { sequence_id: s, .. } => *s = sequence_id,
            Event::IoBufferStateChange { sequence_id: s, .. } => *s = sequence_id,
        }
        self
    }

    /// Get the [`ChunkId`] associated with this event, if any.
    pub fn chunk_id(&self) -> Option<ChunkId> {
        match self {
            Event::AllocationCreated { chunk_id, .. } => Some(*chunk_id),
            Event::AllocationFreed { chunk_id, .. } => Some(*chunk_id),
            Event::AllocationFailed { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationDecided { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationDeferred { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationRejected { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationStarted { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationCompleted { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationFailed { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationRolledBack { chunk_id, .. } => Some(*chunk_id),
            Event::RetryAttempted { chunk_id, .. } => Some(*chunk_id),
            Event::IoRequestIssued { chunk_id, .. } => Some(*chunk_id),
            Event::IoRequestCompleted { chunk_id, .. } => Some(*chunk_id),
            Event::IoRequestFailed { chunk_id, .. } => Some(*chunk_id),
            _ => None,
        }
    }

    /// Get the [`TierId`] associated with this event, if any.
    pub fn tier(&self) -> Option<TierId> {
        match self {
            Event::AllocationCreated { tier, .. } => Some(*tier),
            Event::AllocationFreed { tier, .. } => Some(*tier),
            Event::MigrationDecided { from, .. } => Some(*from),
            Event::MigrationDeferred { from, .. } => Some(*from),
            Event::MigrationRejected { from, .. } => Some(*from),
            Event::MigrationStarted { from, .. } => Some(*from),
            Event::MigrationCompleted { from, .. } => Some(*from),
            Event::MigrationFailed { from, .. } => Some(*from),
            Event::MigrationRolledBack { from, .. } => Some(*from),
            Event::PressureChanged { tier, .. } => Some(*tier),
            Event::BackpressureActivated { tier, .. } => Some(*tier),
            Event::BackpressureDeactivated { tier, .. } => Some(*tier),
            Event::BackendHealthChanged { tier, .. } => Some(*tier),
            Event::IoRequestIssued { tier, .. } => Some(*tier),
            Event::IoRequestCompleted { tier, .. } => Some(*tier),
            Event::IoRequestFailed { tier, .. } => Some(*tier),
            Event::IoFlushIssued { tier, .. } => Some(*tier),
            Event::IoFlushCompleted { tier, .. } => Some(*tier),
            Event::IoBufferStateChange { tier, .. } => Some(*tier),
            _ => None,
        }
    }

    /// Get a human-readable category name for this event.
    pub fn category(&self) -> &'static str {
        match self {
            Event::AllocationCreated { .. }
            | Event::AllocationFreed { .. }
            | Event::AllocationFailed { .. } => "allocation",

            Event::Eviction { .. }
            | Event::Retrieve { .. }
            | Event::TransferCompleted { .. }
            | Event::TransferFailed { .. }
            | Event::Store { .. }
            | Event::Evict { .. } => "orchestration",

            Event::QueueEnqueue { .. }
            | Event::QueueDequeue { .. } => "scheduler",

            Event::MigrationDecision { .. }
            | Event::MigrationDecided { .. }
            | Event::MigrationDeferred { .. }
            | Event::MigrationRejected { .. }
            | Event::MigrationStarted { .. }
            | Event::MigrationCompleted { .. }
            | Event::MigrationFailed { .. }
            | Event::MigrationRolledBack { .. } => "migration",

            Event::ReplayStarted { .. }
            | Event::ReplayCompleted { .. }
            | Event::ReplayDivergence { .. }
            | Event::ReplayInvariantViolation { .. } => "replay",

            Event::PressureChanged { .. }
            | Event::BackpressureActivated { .. }
            | Event::BackpressureDeactivated { .. } => "pressure",

            Event::BackendHealthChanged { .. }
            | Event::RetryAttempted { .. }
            | Event::OperationFailed { .. } => "failure",

            Event::InvariantViolation { .. } => "invariant_violation",

            Event::IoRequestIssued { .. }
            | Event::IoRequestCompleted { .. }
            | Event::IoRequestFailed { .. }
            | Event::IoFlushIssued { .. }
            | Event::IoFlushCompleted { .. }
            | Event::IoBufferStateChange { .. } => "io",
        }
    }

    /// Get a human-readable name for this event variant.
    pub fn event_name(&self) -> &'static str {
        match self {
            Event::AllocationCreated { .. } => "allocation_created",
            Event::AllocationFreed { .. } => "allocation_freed",
            Event::AllocationFailed { .. } => "allocation_failed",
            Event::Eviction { .. } => "eviction",
            Event::Retrieve { .. } => "retrieve",
            Event::TransferCompleted { .. } => "transfer_completed",
            Event::TransferFailed { .. } => "transfer_failed",
            Event::Store { .. } => "store",
            Event::Evict { .. } => "evict",
            Event::QueueEnqueue { .. } => "queue_enqueue",
            Event::QueueDequeue { .. } => "queue_dequeue",
            Event::MigrationDecision { .. } => "migration_decision",
            Event::MigrationDecided { .. } => "migration_decided",
            Event::MigrationDeferred { .. } => "migration_deferred",
            Event::MigrationRejected { .. } => "migration_rejected",
            Event::MigrationStarted { .. } => "migration_started",
            Event::MigrationCompleted { .. } => "migration_completed",
            Event::MigrationFailed { .. } => "migration_failed",
            Event::MigrationRolledBack { .. } => "migration_rolled_back",
            Event::ReplayStarted { .. } => "replay_started",
            Event::ReplayCompleted { .. } => "replay_completed",
            Event::ReplayDivergence { .. } => "replay_divergence",
            Event::ReplayInvariantViolation { .. } => "replay_invariant_violation",
            Event::PressureChanged { .. } => "pressure_changed",
            Event::BackpressureActivated { .. } => "backpressure_activated",
            Event::BackpressureDeactivated { .. } => "backpressure_deactivated",
            Event::BackendHealthChanged { .. } => "backend_health_changed",
            Event::RetryAttempted { .. } => "retry_attempted",
            Event::OperationFailed { .. } => "operation_failed",
            Event::InvariantViolation { .. } => "invariant_violation",
            Event::IoRequestIssued { .. } => "io_request_issued",
            Event::IoRequestCompleted { .. } => "io_request_completed",
            Event::IoRequestFailed { .. } => "io_request_failed",
            Event::IoFlushIssued { .. } => "io_flush_issued",
            Event::IoFlushCompleted { .. } => "io_flush_completed",
            Event::IoBufferStateChange { .. } => "io_buffer_state_change",
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_chunk_id() {
        let id = ChunkId::from_data(b"test");

        let event = Event::AllocationCreated {
            chunk_id: id,
            tier: TierId::Ram,
            size: 1024,
            sequence_id: 0,
        };
        assert_eq!(event.chunk_id(), Some(id));

        let event = Event::MigrationStarted {
            chunk_id: id,
            from: TierId::Ram,
            to: TierId::Disk,
            sequence_id: 0,
        };
        assert_eq!(event.chunk_id(), Some(id));

        let event = Event::PressureChanged {
            tier: TierId::Ram,
            old: PressureState::new(),
            new: PressureState::new(),
            sequence_id: 0,
        };
        assert_eq!(event.chunk_id(), None);
    }

    #[test]
    fn test_event_tier() {
        let event = Event::AllocationCreated {
            chunk_id: ChunkId::from_data(b"test"),
            tier: TierId::GpuVram,
            size: 1024,
            sequence_id: 0,
        };
        assert_eq!(event.tier(), Some(TierId::GpuVram));

        let event = Event::BackendHealthChanged {
            tier: TierId::Disk,
            old: BackendHealth::Healthy,
            new: BackendHealth::Degraded,
            sequence_id: 0,
        };
        assert_eq!(event.tier(), Some(TierId::Disk));

        let event = Event::OperationFailed {
            operation: "store".to_string(),
            reason: "full".to_string(),
            sequence_id: 0,
        };
        assert_eq!(event.tier(), None);
    }

    #[test]
    fn test_event_category() {
        assert_eq!(
            Event::AllocationCreated {
                chunk_id: ChunkId::from_data(b"t"),
                tier: TierId::Ram,
                size: 1,
                sequence_id: 0,
            }
            .category(),
            "allocation"
        );
        assert_eq!(
            Event::MigrationStarted {
                chunk_id: ChunkId::from_data(b"t"),
                from: TierId::Ram,
                to: TierId::Disk,
                sequence_id: 0,
            }
            .category(),
            "migration"
        );
        assert_eq!(
            Event::ReplayStarted {
                trace_path: "trace.bin".to_string(),
                sequence_id: 0,
            }
            .category(),
            "replay"
        );
        assert_eq!(
            Event::PressureChanged {
                tier: TierId::Ram,
                old: PressureState::new(),
                new: PressureState::new(),
                sequence_id: 0,
            }
            .category(),
            "pressure"
        );
        assert_eq!(
            Event::OperationFailed {
                operation: "store".to_string(),
                reason: "err".to_string(),
                sequence_id: 0,
            }
            .category(),
            "failure"
        );
        assert_eq!(
            Event::InvariantViolation {
                rule: "test".to_string(),
                details: "bad".to_string(),
                severity: InvariantSeverity::Error,
                sequence_id: 0,
            }
            .category(),
            "invariant_violation"
        );
    }

    #[test]
    fn test_event_name() {
        assert_eq!(
            Event::AllocationCreated {
                chunk_id: ChunkId::from_data(b"t"),
                tier: TierId::Ram,
                size: 1,
                sequence_id: 0,
            }
            .event_name(),
            "allocation_created"
        );
        assert_eq!(
            Event::MigrationRolledBack {
                chunk_id: ChunkId::from_data(b"t"),
                from: TierId::Ram,
                to: TierId::Disk,
                sequence_id: 0,
            }
            .event_name(),
            "migration_rolled_back"
        );
    }

    #[test]
    fn test_invariant_severity_ordering() {
        assert!(InvariantSeverity::Info < InvariantSeverity::Warning);
        assert!(InvariantSeverity::Warning < InvariantSeverity::Error);
        assert!(InvariantSeverity::Error < InvariantSeverity::Critical);
    }

    #[test]
    fn test_invariant_severity_display() {
        assert_eq!(format!("{}", InvariantSeverity::Info), "info");
        assert_eq!(format!("{}", InvariantSeverity::Warning), "warning");
        assert_eq!(format!("{}", InvariantSeverity::Error), "error");
        assert_eq!(format!("{}", InvariantSeverity::Critical), "critical");
    }

    #[test]
    fn test_backend_health_display() {
        assert_eq!(format!("{}", BackendHealth::Healthy), "healthy");
        assert_eq!(format!("{}", BackendHealth::Degraded), "degraded");
        assert_eq!(format!("{}", BackendHealth::Unavailable), "unavailable");
        assert_eq!(format!("{}", BackendHealth::Recovering), "recovering");
    }

    #[test]
    fn test_event_serialization_roundtrip() {
        let event = Event::MigrationCompleted {
            chunk_id: ChunkId::from_data(b"roundtrip"),
            from: TierId::Ram,
            to: TierId::Disk,
            duration_ms: 150,
            sequence_id: 0,
        };

        let json = serde_json::to_string(&event).expect("serialize event");
        let deserialized: Event = serde_json::from_str(&json).expect("deserialize event");

        assert_eq!(event.category(), deserialized.category());
        assert_eq!(event.event_name(), deserialized.event_name());
        assert_eq!(event.chunk_id(), deserialized.chunk_id());
    }

    #[test]
    fn test_all_event_variants_constructible() {
        let id = ChunkId::from_data(b"test");
        let _events: Vec<Event> = vec![
            Event::AllocationCreated {
                chunk_id: id,
                tier: TierId::Ram,
                size: 1024,
                sequence_id: 0,
            },
            Event::AllocationFreed {
                chunk_id: id,
                tier: TierId::Ram,
                sequence_id: 0,
            },
            Event::AllocationFailed {
                chunk_id: id,
                reason: "out of memory".to_string(),
                sequence_id: 0,
            },
            Event::MigrationDecided {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                cost_score: 1.5,
                sequence_id: 0,
            },
            Event::MigrationDeferred {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                reason: "high io pressure".to_string(),
                sequence_id: 0,
            },
            Event::MigrationRejected {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                cost_score: 9.5,
                threshold: 5.0,
                sequence_id: 0,
            },
            Event::MigrationStarted {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                sequence_id: 0,
            },
            Event::MigrationCompleted {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                duration_ms: 100,
                sequence_id: 0,
            },
            Event::MigrationFailed {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                reason: "timeout".to_string(),
                sequence_id: 0,
            },
            Event::MigrationRolledBack {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                sequence_id: 0,
            },
            Event::ReplayStarted {
                trace_path: "trace.bin".to_string(),
                sequence_id: 0,
            },
            Event::ReplayCompleted {
                trace_path: "trace.bin".to_string(),
                events: 100,
                duration_ms: 50,
                sequence_id: 0,
            },
            Event::ReplayDivergence {
                trace_path: "trace.bin".to_string(),
                expected: "stored".to_string(),
                actual: "failed".to_string(),
                sequence_id: 0,
            },
            Event::ReplayInvariantViolation {
                rule: "no_orphans".to_string(),
                details: "orphaned transfer".to_string(),
                sequence_id: 0,
            },
            Event::PressureChanged {
                tier: TierId::Ram,
                old: PressureState::new(),
                new: PressureState::new(),
                sequence_id: 0,
            },
            Event::BackpressureActivated {
                tier: TierId::Ram,
                level: "soft".to_string(),
                sequence_id: 0,
            },
            Event::BackpressureDeactivated {
                tier: TierId::Ram,
                sequence_id: 0,
            },
            Event::BackendHealthChanged {
                tier: TierId::Disk,
                old: BackendHealth::Healthy,
                new: BackendHealth::Degraded,
                sequence_id: 0,
            },
            Event::RetryAttempted {
                chunk_id: id,
                attempt: 2,
                max_attempts: 3,
                sequence_id: 0,
            },
            Event::OperationFailed {
                operation: "store".to_string(),
                reason: "backend unavailable".to_string(),
                sequence_id: 0,
            },
            Event::InvariantViolation {
                rule: "no_illegal_transitions".to_string(),
                details: "Allocated -> Cached".to_string(),
                severity: InvariantSeverity::Error,
                sequence_id: 0,
            },
        ];
    }
}
