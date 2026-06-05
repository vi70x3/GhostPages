//! I/O lifecycle events for deterministic I/O scheduling.
//!
//! This module defines the [`IoEvent`] enum — a set of structured events that
//! track the lifecycle of I/O operations across all storage backends. These
//! events are emitted by the [`IoScheduler`](crate::io_abstraction::IoScheduler)
//! to provide observability into I/O ordering, timing, and failures.
//!
//! The key design principle is **issue/completion separation**: an I/O request
//! is *issued* when the syscall starts and *completed* when it finishes. In
//! deterministic simulation, both happen at controlled ticks, ensuring event
//! ordering is independent of wall clock.

use serde::{Deserialize, Serialize};

use crate::types::{ChunkId, TierId};

/// The type of I/O operation being performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IoOperation {
    /// Read data from a tier.
    Read,

    /// Write data to a tier.
    Write,

    /// Delete data from a tier.
    Delete,

    /// Flush pending I/O for a tier (fsync simulation).
    Flush,
}

impl std::fmt::Display for IoOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoOperation::Read => write!(f, "read"),
            IoOperation::Write => write!(f, "write"),
            IoOperation::Delete => write!(f, "delete"),
            IoOperation::Flush => write!(f, "flush"),
        }
    }
}

/// An I/O lifecycle event emitted by the [`IoScheduler`].
///
/// These events track the full lifecycle of I/O operations:
/// 1. `IoRequestIssued` — an I/O request was submitted
/// 2. `IoRequestCompleted` — an I/O request finished successfully
/// 3. `IoRequestFailed` — an I/O request failed with an error
/// 4. `IoFlushIssued` — a flush (fsync) was requested
/// 5. `IoFlushCompleted` — a flush completed
/// 6. `IoBufferStateChange` — the buffer fill level changed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IoEvent {
    /// An I/O request was issued to a tier.
    IoRequestIssued {
        /// The type of operation.
        operation: IoOperation,
        /// The chunk being operated on.
        chunk_id: ChunkId,
        /// The tier the operation targets.
        tier: TierId,
    },

    /// An I/O request completed successfully.
    IoRequestCompleted {
        /// The type of operation.
        operation: IoOperation,
        /// The chunk that was operated on.
        chunk_id: ChunkId,
        /// The tier the operation targeted.
        tier: TierId,
        /// Duration in deterministic ticks.
        duration_ticks: u64,
    },

    /// An I/O request failed.
    IoRequestFailed {
        /// The type of operation that failed.
        operation: IoOperation,
        /// The chunk being operated on.
        chunk_id: ChunkId,
        /// The tier targeted.
        tier: TierId,
        /// Human-readable error description.
        error: String,
    },

    /// A flush (fsync) was issued for a tier.
    IoFlushIssued {
        /// The tier being flushed.
        tier: TierId,
    },

    /// A flush (fsync) completed for a tier.
    IoFlushCompleted {
        /// The tier that was flushed.
        tier: TierId,
        /// Duration in deterministic ticks.
        duration_ticks: u64,
    },

    /// The buffer fill level changed for a tier.
    IoBufferStateChange {
        /// The tier whose buffer changed.
        tier: TierId,
        /// Current number of buffered bytes.
        buffered: usize,
        /// Total buffer capacity in bytes.
        capacity: usize,
    },
}

impl IoEvent {
    /// Get the [`ChunkId`] associated with this event, if any.
    pub fn chunk_id(&self) -> Option<ChunkId> {
        match self {
            IoEvent::IoRequestIssued { chunk_id, .. } => Some(*chunk_id),
            IoEvent::IoRequestCompleted { chunk_id, .. } => Some(*chunk_id),
            IoEvent::IoRequestFailed { chunk_id, .. } => Some(*chunk_id),
            IoEvent::IoFlushIssued { .. }
            | IoEvent::IoFlushCompleted { .. }
            | IoEvent::IoBufferStateChange { .. } => None,
        }
    }

    /// Get the [`TierId`] associated with this event.
    pub fn tier(&self) -> Option<TierId> {
        match self {
            IoEvent::IoRequestIssued { tier, .. } => Some(*tier),
            IoEvent::IoRequestCompleted { tier, .. } => Some(*tier),
            IoEvent::IoRequestFailed { tier, .. } => Some(*tier),
            IoEvent::IoFlushIssued { tier, .. } => Some(*tier),
            IoEvent::IoFlushCompleted { tier, .. } => Some(*tier),
            IoEvent::IoBufferStateChange { tier, .. } => Some(*tier),
        }
    }

    /// Get a human-readable category name for this event.
    pub fn category(&self) -> &'static str {
        "io"
    }

    /// Get a human-readable name for this event variant.
    pub fn event_name(&self) -> &'static str {
        match self {
            IoEvent::IoRequestIssued { .. } => "io_request_issued",
            IoEvent::IoRequestCompleted { .. } => "io_request_completed",
            IoEvent::IoRequestFailed { .. } => "io_request_failed",
            IoEvent::IoFlushIssued { .. } => "io_flush_issued",
            IoEvent::IoFlushCompleted { .. } => "io_flush_completed",
            IoEvent::IoBufferStateChange { .. } => "io_buffer_state_change",
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_operation_display() {
        assert_eq!(format!("{}", IoOperation::Read), "read");
        assert_eq!(format!("{}", IoOperation::Write), "write");
        assert_eq!(format!("{}", IoOperation::Delete), "delete");
        assert_eq!(format!("{}", IoOperation::Flush), "flush");
    }

    #[test]
    fn test_io_event_chunk_id() {
        let id = ChunkId::from_data(b"test");

        let event = IoEvent::IoRequestIssued {
            operation: IoOperation::Read,
            chunk_id: id,
            tier: TierId::Disk,
        };
        assert_eq!(event.chunk_id(), Some(id));

        let event = IoEvent::IoFlushIssued { tier: TierId::Disk };
        assert_eq!(event.chunk_id(), None);
    }

    #[test]
    fn test_io_event_tier() {
        let id = ChunkId::from_data(b"test");

        let event = IoEvent::IoRequestCompleted {
            operation: IoOperation::Write,
            chunk_id: id,
            tier: TierId::Ram,
            duration_ticks: 100,
        };
        assert_eq!(event.tier(), Some(TierId::Ram));

        let event = IoEvent::IoBufferStateChange {
            tier: TierId::Disk,
            buffered: 512,
            capacity: 4096,
        };
        assert_eq!(event.tier(), Some(TierId::Disk));
    }

    #[test]
    fn test_io_event_category() {
        let event = IoEvent::IoRequestIssued {
            operation: IoOperation::Read,
            chunk_id: ChunkId::from_data(b"t"),
            tier: TierId::Ram,
        };
        assert_eq!(event.category(), "io");
    }

    #[test]
    fn test_io_event_name() {
        let event = IoEvent::IoRequestIssued {
            operation: IoOperation::Read,
            chunk_id: ChunkId::from_data(b"t"),
            tier: TierId::Ram,
        };
        assert_eq!(event.event_name(), "io_request_issued");

        let event = IoEvent::IoFlushCompleted {
            tier: TierId::Disk,
            duration_ticks: 50,
        };
        assert_eq!(event.event_name(), "io_flush_completed");
    }

    #[test]
    fn test_io_event_serialization_roundtrip() {
        let event = IoEvent::IoRequestCompleted {
            operation: IoOperation::Write,
            chunk_id: ChunkId::from_data(b"roundtrip"),
            tier: TierId::Disk,
            duration_ticks: 42,
        };

        let json = serde_json::to_string(&event).expect("serialize io event");
        let deserialized: IoEvent = serde_json::from_str(&json).expect("deserialize io event");

        assert_eq!(event.category(), deserialized.category());
        assert_eq!(event.event_name(), deserialized.event_name());
        assert_eq!(event.chunk_id(), deserialized.chunk_id());
        assert_eq!(event.tier(), deserialized.tier());
    }

    #[test]
    fn test_all_io_event_variants_constructible() {
        let id = ChunkId::from_data(b"test");
        let _events: Vec<IoEvent> = vec![
            IoEvent::IoRequestIssued {
                operation: IoOperation::Read,
                chunk_id: id,
                tier: TierId::Ram,
            },
            IoEvent::IoRequestCompleted {
                operation: IoOperation::Write,
                chunk_id: id,
                tier: TierId::Disk,
                duration_ticks: 100,
            },
            IoEvent::IoRequestFailed {
                operation: IoOperation::Delete,
                chunk_id: id,
                tier: TierId::Simulation,
                error: "device lost".to_string(),
            },
            IoEvent::IoFlushIssued {
                tier: TierId::Disk,
            },
            IoEvent::IoFlushCompleted {
                tier: TierId::Disk,
                duration_ticks: 50,
            },
            IoEvent::IoBufferStateChange {
                tier: TierId::Ram,
                buffered: 1024,
                capacity: 8192,
            },
        ];
    }
}
