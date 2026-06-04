//! Trace event types for GhostPages replay system.
//!
//! Skeleton implementation for Phase 0.

use ghost_core::types::{ChunkId, TierId};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// A recorded trace event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    /// Timestamp of the event.
    pub timestamp: SystemTime,

    /// Type of event.
    pub kind: TraceEventKind,
}

/// Types of trace events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceEventKind {
    /// A chunk was migrated between tiers.
    Migration {
        /// The chunk that was migrated.
        chunk_id: ChunkId,
        /// Source tier.
        source: TierId,
        /// Destination tier.
        destination: TierId,
        /// Size of the chunk in bytes.
        size: usize,
        /// Duration of the migration.
        duration_ms: u64,
    },

    /// A chunk was evicted from a tier.
    Eviction {
        /// The evicted chunk.
        chunk_id: ChunkId,
        /// The tier it was evicted from.
        tier: TierId,
        /// Reason for eviction.
        reason: EvictionReason,
    },

    /// Memory pressure changed.
    PressureChange {
        /// Previous pressure level.
        from: u8,
        /// New pressure level.
        to: u8,
    },
}

/// Reason for chunk eviction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionReason {
    /// Evicted due to memory pressure.
    Pressure,
    /// Evicted due to policy decision.
    Policy,
    /// Evicted due to explicit deletion.
    Deletion,
}
