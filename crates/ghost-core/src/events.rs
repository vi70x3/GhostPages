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
/// };
///
/// match event {
///     Event::AllocationCreated { chunk_id, tier, size } => {
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
        chunk_id: ChunkId,
        tier: TierId,
        size: usize,
    },

    /// A chunk was freed from a tier.
    AllocationFreed {
        chunk_id: ChunkId,
        tier: TierId,
    },

    /// An allocation operation failed.
    AllocationFailed {
        chunk_id: ChunkId,
        reason: String,
    },

    // ── Migration ────────────────────────────────────────────────────────────

    /// A chunk migration between tiers was started.
    MigrationStarted {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
    },

    /// A chunk migration completed successfully.
    MigrationCompleted {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        duration_ms: u64,
    },

    /// A chunk migration failed.
    MigrationFailed {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        reason: String,
    },

    /// A failed migration was rolled back to the source tier.
    MigrationRolledBack {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
    },

    // ── Replay ───────────────────────────────────────────────────────────────

    /// A trace replay was started.
    ReplayStarted {
        trace_path: String,
    },

    /// A trace replay completed successfully.
    ReplayCompleted {
        trace_path: String,
        events: usize,
        duration_ms: u64,
    },

    /// A replay diverged from the expected trace.
    ReplayDivergence {
        trace_path: String,
        expected: String,
        actual: String,
    },

    /// An invariant violation was detected during replay.
    ReplayInvariantViolation {
        rule: String,
        details: String,
    },

    // ── Pressure ─────────────────────────────────────────────────────────────

    /// System pressure changed for a tier.
    PressureChanged {
        tier: TierId,
        old: PressureState,
        new: PressureState,
    },

    /// Backpressure was activated for a tier.
    BackpressureActivated {
        tier: TierId,
        level: String,
    },

    /// Backpressure was deactivated for a tier.
    BackpressureDeactivated {
        tier: TierId,
    },

    // ── Failure ──────────────────────────────────────────────────────────────

    /// A backend's health status changed.
    BackendHealthChanged {
        tier: TierId,
        old: BackendHealth,
        new: BackendHealth,
    },

    /// A transfer is being retried.
    RetryAttempted {
        chunk_id: ChunkId,
        attempt: u32,
        max_attempts: u32,
    },

    /// An operation failed irrecoverably.
    OperationFailed {
        operation: String,
        reason: String,
    },

    // ── Invariant Violation ──────────────────────────────────────────────────

    /// An invariant was violated during replay validation.
    InvariantViolation {
        rule: String,
        details: String,
        severity: InvariantSeverity,
    },
}

impl Event {
    /// Get the [`ChunkId`] associated with this event, if any.
    pub fn chunk_id(&self) -> Option<ChunkId> {
        match self {
            Event::AllocationCreated { chunk_id, .. } => Some(*chunk_id),
            Event::AllocationFreed { chunk_id, .. } => Some(*chunk_id),
            Event::AllocationFailed { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationStarted { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationCompleted { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationFailed { chunk_id, .. } => Some(*chunk_id),
            Event::MigrationRolledBack { chunk_id, .. } => Some(*chunk_id),
            Event::RetryAttempted { chunk_id, .. } => Some(*chunk_id),
            _ => None,
        }
    }

    /// Get the [`TierId`] associated with this event, if any.
    pub fn tier(&self) -> Option<TierId> {
        match self {
            Event::AllocationCreated { tier, .. } => Some(*tier),
            Event::AllocationFreed { tier, .. } => Some(*tier),
            Event::MigrationStarted { from, .. } => Some(*from),
            Event::MigrationCompleted { from, .. } => Some(*from),
            Event::MigrationFailed { from, .. } => Some(*from),
            Event::MigrationRolledBack { from, .. } => Some(*from),
            Event::PressureChanged { tier, .. } => Some(*tier),
            Event::BackpressureActivated { tier, .. } => Some(*tier),
            Event::BackpressureDeactivated { tier, .. } => Some(*tier),
            Event::BackendHealthChanged { tier, .. } => Some(*tier),
            _ => None,
        }
    }

    /// Get a human-readable category name for this event.
    pub fn category(&self) -> &'static str {
        match self {
            Event::AllocationCreated { .. }
            | Event::AllocationFreed { .. }
            | Event::AllocationFailed { .. } => "allocation",

            Event::MigrationStarted { .. }
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
        }
    }

    /// Get a human-readable name for this event variant.
    pub fn event_name(&self) -> &'static str {
        match self {
            Event::AllocationCreated { .. } => "allocation_created",
            Event::AllocationFreed { .. } => "allocation_freed",
            Event::AllocationFailed { .. } => "allocation_failed",
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
        };
        assert_eq!(event.chunk_id(), Some(id));

        let event = Event::MigrationStarted {
            chunk_id: id,
            from: TierId::Ram,
            to: TierId::Disk,
        };
        assert_eq!(event.chunk_id(), Some(id));

        let event = Event::PressureChanged {
            tier: TierId::Ram,
            old: PressureState::new(),
            new: PressureState::new(),
        };
        assert_eq!(event.chunk_id(), None);
    }

    #[test]
    fn test_event_tier() {
        let event = Event::AllocationCreated {
            chunk_id: ChunkId::from_data(b"test"),
            tier: TierId::GpuVram,
            size: 1024,
        };
        assert_eq!(event.tier(), Some(TierId::GpuVram));

        let event = Event::BackendHealthChanged {
            tier: TierId::Disk,
            old: BackendHealth::Healthy,
            new: BackendHealth::Degraded,
        };
        assert_eq!(event.tier(), Some(TierId::Disk));

        let event = Event::OperationFailed {
            operation: "store".to_string(),
            reason: "full".to_string(),
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
            }
            .category(),
            "allocation"
        );
        assert_eq!(
            Event::MigrationStarted {
                chunk_id: ChunkId::from_data(b"t"),
                from: TierId::Ram,
                to: TierId::Disk,
            }
            .category(),
            "migration"
        );
        assert_eq!(
            Event::ReplayStarted {
                trace_path: "trace.bin".to_string(),
            }
            .category(),
            "replay"
        );
        assert_eq!(
            Event::PressureChanged {
                tier: TierId::Ram,
                old: PressureState::new(),
                new: PressureState::new(),
            }
            .category(),
            "pressure"
        );
        assert_eq!(
            Event::OperationFailed {
                operation: "store".to_string(),
                reason: "err".to_string(),
            }
            .category(),
            "failure"
        );
        assert_eq!(
            Event::InvariantViolation {
                rule: "test".to_string(),
                details: "bad".to_string(),
                severity: InvariantSeverity::Error,
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
            }
            .event_name(),
            "allocation_created"
        );
        assert_eq!(
            Event::MigrationRolledBack {
                chunk_id: ChunkId::from_data(b"t"),
                from: TierId::Ram,
                to: TierId::Disk,
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
            },
            Event::AllocationFreed {
                chunk_id: id,
                tier: TierId::Ram,
            },
            Event::AllocationFailed {
                chunk_id: id,
                reason: "out of memory".to_string(),
            },
            Event::MigrationStarted {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
            },
            Event::MigrationCompleted {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                duration_ms: 100,
            },
            Event::MigrationFailed {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                reason: "timeout".to_string(),
            },
            Event::MigrationRolledBack {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
            },
            Event::ReplayStarted {
                trace_path: "trace.bin".to_string(),
            },
            Event::ReplayCompleted {
                trace_path: "trace.bin".to_string(),
                events: 100,
                duration_ms: 50,
            },
            Event::ReplayDivergence {
                trace_path: "trace.bin".to_string(),
                expected: "stored".to_string(),
                actual: "failed".to_string(),
            },
            Event::ReplayInvariantViolation {
                rule: "no_orphans".to_string(),
                details: "orphaned transfer".to_string(),
            },
            Event::PressureChanged {
                tier: TierId::Ram,
                old: PressureState::new(),
                new: PressureState::new(),
            },
            Event::BackpressureActivated {
                tier: TierId::Ram,
                level: "soft".to_string(),
            },
            Event::BackpressureDeactivated {
                tier: TierId::Ram,
            },
            Event::BackendHealthChanged {
                tier: TierId::Disk,
                old: BackendHealth::Healthy,
                new: BackendHealth::Degraded,
            },
            Event::RetryAttempted {
                chunk_id: id,
                attempt: 2,
                max_attempts: 3,
            },
            Event::OperationFailed {
                operation: "store".to_string(),
                reason: "backend unavailable".to_string(),
            },
            Event::InvariantViolation {
                rule: "no_illegal_transitions".to_string(),
                details: "Allocated -> Cached".to_string(),
                severity: InvariantSeverity::Error,
            },
        ];
    }
}
