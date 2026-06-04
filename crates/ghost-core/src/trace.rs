//! Trace events for the append-only trace log.
//!
//! This module defines the event types recorded in the system's trace log.
//! Events are serialized for later replay and analysis.

use serde::{Deserialize, Serialize};

use crate::state::PressureState;
use crate::transfer::TransferJob;
use crate::types::{ChunkId, TierId};
use crate::ChunkState;

/// Reason for chunk eviction.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EvictionReason {
    /// Tier is at capacity.
    Capacity,

    /// System pressure forced eviction.
    Pressure,

    /// Placement policy decided to evict.
    Policy,

    /// Manual/user-requested eviction.
    Manual,
}

impl std::fmt::Display for EvictionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionReason::Capacity => write!(f, "capacity"),
            EvictionReason::Pressure => write!(f, "pressure"),
            EvictionReason::Policy => write!(f, "policy"),
            EvictionReason::Manual => write!(f, "manual"),
        }
    }
}

/// An event in the append-only trace log.
///
/// Each event represents a discrete occurrence in the system, with a
/// timestamp for ordering and replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceEvent {
    /// A new chunk was created.
    ChunkCreated {
        chunk_id: ChunkId,
        size: usize,
        tier: TierId,
        timestamp: u64,
    },

    /// A chunk's state changed.
    ChunkStateChanged {
        chunk_id: ChunkId,
        from: ChunkState,
        to: ChunkState,
        timestamp: u64,
    },

    /// A transfer job was started.
    TransferStarted {
        job: TransferJob,
        timestamp: u64,
    },

    /// A transfer completed successfully.
    TransferCompleted {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        duration_ms: u64,
        timestamp: u64,
    },

    /// A transfer failed.
    TransferFailed {
        chunk_id: ChunkId,
        error: String,
        timestamp: u64,
    },

    /// A pressure sample was recorded.
    PressureSample {
        state: PressureState,
        timestamp: u64,
    },

    /// A chunk was evicted from a tier.
    Eviction {
        chunk_id: ChunkId,
        tier: TierId,
        reason: EvictionReason,
        timestamp: u64,
    },
}

impl TraceEvent {
    /// Get the timestamp of this event.
    pub fn timestamp(&self) -> u64 {
        match self {
            TraceEvent::ChunkCreated { timestamp, .. } => *timestamp,
            TraceEvent::ChunkStateChanged { timestamp, .. } => *timestamp,
            TraceEvent::TransferStarted { timestamp, .. } => *timestamp,
            TraceEvent::TransferCompleted { timestamp, .. } => *timestamp,
            TraceEvent::TransferFailed { timestamp, .. } => *timestamp,
            TraceEvent::PressureSample { timestamp, .. } => *timestamp,
            TraceEvent::Eviction { timestamp, .. } => *timestamp,
        }
    }

    /// Get the chunk ID associated with this event, if any.
    pub fn chunk_id(&self) -> Option<ChunkId> {
        match self {
            TraceEvent::ChunkCreated { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::ChunkStateChanged { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::TransferStarted { job, .. } => Some(job.chunk_id),
            TraceEvent::TransferCompleted { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::TransferFailed { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::PressureSample { .. } => None,
            TraceEvent::Eviction { chunk_id, .. } => Some(*chunk_id),
        }
    }

    /// Get a human-readable name for this event type.
    pub fn event_type(&self) -> &'static str {
        match self {
            TraceEvent::ChunkCreated { .. } => "chunk_created",
            TraceEvent::ChunkStateChanged { .. } => "chunk_state_changed",
            TraceEvent::TransferStarted { .. } => "transfer_started",
            TraceEvent::TransferCompleted { .. } => "transfer_completed",
            TraceEvent::TransferFailed { .. } => "transfer_failed",
            TraceEvent::PressureSample { .. } => "pressure_sample",
            TraceEvent::Eviction { .. } => "eviction",
        }
    }
}

/// Get the current timestamp in milliseconds since epoch.
pub fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransferPriority;

    #[test]
    fn test_eviction_reason_display() {
        assert_eq!(format!("{}", EvictionReason::Capacity), "capacity");
        assert_eq!(format!("{}", EvictionReason::Pressure), "pressure");
        assert_eq!(format!("{}", EvictionReason::Policy), "policy");
        assert_eq!(format!("{}", EvictionReason::Manual), "manual");
    }

    #[test]
    fn test_trace_event_timestamp() {
        let ts = 12345;
        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test"),
            size: 100,
            tier: TierId::Ram,
            timestamp: ts,
        };
        assert_eq!(event.timestamp(), ts);
    }

    #[test]
    fn test_trace_event_chunk_id() {
        let chunk_id = ChunkId::from_data(b"test");

        let created = TraceEvent::ChunkCreated {
            chunk_id,
            size: 100,
            tier: TierId::Ram,
            timestamp: 0,
        };
        assert_eq!(created.chunk_id(), Some(chunk_id));

        let state_changed = TraceEvent::ChunkStateChanged {
            chunk_id,
            from: ChunkState::Allocated,
            to: ChunkState::Stored,
            timestamp: 0,
        };
        assert_eq!(state_changed.chunk_id(), Some(chunk_id));

        let pressure = TraceEvent::PressureSample {
            state: PressureState::new(),
            timestamp: 0,
        };
        assert_eq!(pressure.chunk_id(), None);
    }

    #[test]
    fn test_trace_event_event_type() {
        let chunk_id = ChunkId::from_data(b"test");

        assert_eq!(
            TraceEvent::ChunkCreated {
                chunk_id,
                size: 100,
                tier: TierId::Ram,
                timestamp: 0,
            }
            .event_type(),
            "chunk_created"
        );

        assert_eq!(
            TraceEvent::ChunkStateChanged {
                chunk_id,
                from: ChunkState::Allocated,
                to: ChunkState::Stored,
                timestamp: 0,
            }
            .event_type(),
            "chunk_state_changed"
        );

        assert_eq!(
            TraceEvent::PressureSample {
                state: PressureState::new(),
                timestamp: 0,
            }
            .event_type(),
            "pressure_sample"
        );
    }

    #[test]
    fn test_current_timestamp() {
        let ts = current_timestamp();
        // Should be a reasonable timestamp (after year 2020)
        assert!(ts > 1_577_836_800_000); // 2020-01-01 in millis
    }

    #[test]
    fn test_trace_event_serialization_roundtrip() {
        let event = TraceEvent::ChunkStateChanged {
            chunk_id: ChunkId::from_data(b"roundtrip"),
            from: ChunkState::Stored,
            to: ChunkState::Cached,
            timestamp: 42,
        };

        let serialized = serde_json::to_string(&event).expect("serialize trace event");
        let deserialized: TraceEvent = serde_json::from_str(&serialized).expect("deserialize trace event");

        assert_eq!(event.timestamp(), deserialized.timestamp());
        assert_eq!(event.chunk_id(), deserialized.chunk_id());
        assert_eq!(event.event_type(), deserialized.event_type());
    }

    #[test]
    fn test_transfer_started_event() {
        let job = TransferJob::new(
            ChunkId::from_data(b"transfer"),
            TierId::Ram,
            TierId::Disk,
            4096,
            TransferPriority::Normal,
        );

        let event = TraceEvent::TransferStarted {
            job: job.clone(),
            timestamp: 100,
        };

        assert_eq!(event.timestamp(), 100);
        assert_eq!(event.chunk_id(), Some(job.chunk_id));
        assert_eq!(event.event_type(), "transfer_started");
    }

    #[test]
    fn test_transfer_completed_event() {
        let event = TraceEvent::TransferCompleted {
            chunk_id: ChunkId::from_data(b"done"),
            from: TierId::Ram,
            to: TierId::GpuVram,
            duration_ms: 150,
            timestamp: 200,
        };

        assert_eq!(event.timestamp(), 200);
        assert_eq!(event.chunk_id(), Some(ChunkId::from_data(b"done")));
        assert_eq!(event.event_type(), "transfer_completed");
    }

    #[test]
    fn test_transfer_failed_event() {
        let event = TraceEvent::TransferFailed {
            chunk_id: ChunkId::from_data(b"fail"),
            error: "connection lost".to_string(),
            timestamp: 300,
        };

        assert_eq!(event.timestamp(), 300);
        assert_eq!(event.chunk_id(), Some(ChunkId::from_data(b"fail")));
        assert_eq!(event.event_type(), "transfer_failed");
    }

    #[test]
    fn test_eviction_event() {
        let event = TraceEvent::Eviction {
            chunk_id: ChunkId::from_data(b"evict"),
            tier: TierId::GpuVram,
            reason: EvictionReason::Pressure,
            timestamp: 400,
        };

        assert_eq!(event.timestamp(), 400);
        assert_eq!(event.chunk_id(), Some(ChunkId::from_data(b"evict")));
        assert_eq!(event.event_type(), "eviction");
    }
}
