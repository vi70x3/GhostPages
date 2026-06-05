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

    /// A chunk was deleted from a tier.
    ChunkDeleted {
        chunk_id: ChunkId,
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

    /// A transfer job was queued.
    TransferQueued {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        priority: crate::transfer::TransferPriority,
        timestamp: u64,
    },

    /// A transfer job was started.
    TransferStarted { job: TransferJob, timestamp: u64 },

    /// A transfer completed successfully.
    TransferCompleted {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        size: usize,
        duration_ms: u64,
        timestamp: u64,
    },

    /// A transfer failed.
    TransferFailed {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        error: String,
        attempt: u32,
        timestamp: u64,
    },

    /// A transfer is being retried.
    TransferRetry {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        attempt: u32,
        timestamp: u64,
    },

    /// A transfer was cancelled.
    TransferCancelled {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        timestamp: u64,
    },

    /// A pressure sample was recorded.
    PressureSample {
        state: PressureState,
        timestamp: u64,
    },

    /// A pressure alert was triggered.
    PressureAlert {
        memory_pressure: f32,
        vram_pressure: f32,
        io_pressure: f32,
        timestamp: u64,
    },

    /// A chunk was evicted from a tier.
    Eviction {
        chunk_id: ChunkId,
        tier: TierId,
        reason: EvictionReason,
        timestamp: u64,
    },

    /// A placement policy decision was made.
    PolicyDecision {
        chunk_id: ChunkId,
        from: TierId,
        to: TierId,
        reason: String,
        timestamp: u64,
    },

    /// The daemon was started.
    DaemonStarted { timestamp: u64 },

    /// The daemon is stopping.
    DaemonStopping { timestamp: u64 },

    /// A storage backend was registered.
    BackendRegistered { tier: TierId, timestamp: u64 },

    /// A worker was spawned.
    WorkerSpawned { worker_id: usize, timestamp: u64 },

    /// A worker was stopped.
    WorkerStopped { worker_id: usize, timestamp: u64 },

    /// An IPC request was received.
    IpcRequestReceived {
        request_type: String,
        timestamp: u64,
    },

    /// An IPC response was sent.
    IpcResponseSent {
        request_type: String,
        success: bool,
        timestamp: u64,
    },

    /// An IPC connection was accepted.
    IpcConnectionAccepted { timestamp: u64 },

    /// An IPC connection was closed.
    IpcConnectionClosed { timestamp: u64 },

    /// Compression started for a chunk.
    CompressionStarted {
        chunk_id: ChunkId,
        original_size: usize,
        timestamp: u64,
    },

    /// Compression completed for a chunk.
    CompressionCompleted {
        chunk_id: ChunkId,
        original_size: usize,
        compressed_size: usize,
        timestamp: u64,
    },

    /// Decompression started for a chunk.
    DecompressionStarted {
        chunk_id: ChunkId,
        compressed_size: usize,
        timestamp: u64,
    },

    /// Decompression completed for a chunk.
    DecompressionCompleted {
        chunk_id: ChunkId,
        compressed_size: usize,
        decompressed_size: usize,
        timestamp: u64,
    },
}

impl TraceEvent {
    /// Get the timestamp of this event.
    pub fn timestamp(&self) -> u64 {
        match self {
            TraceEvent::ChunkCreated { timestamp, .. } => *timestamp,
            TraceEvent::ChunkDeleted { timestamp, .. } => *timestamp,
            TraceEvent::ChunkStateChanged { timestamp, .. } => *timestamp,
            TraceEvent::TransferQueued { timestamp, .. } => *timestamp,
            TraceEvent::TransferStarted { timestamp, .. } => *timestamp,
            TraceEvent::TransferCompleted { timestamp, .. } => *timestamp,
            TraceEvent::TransferFailed { timestamp, .. } => *timestamp,
            TraceEvent::TransferRetry { timestamp, .. } => *timestamp,
            TraceEvent::TransferCancelled { timestamp, .. } => *timestamp,
            TraceEvent::PressureSample { timestamp, .. } => *timestamp,
            TraceEvent::PressureAlert { timestamp, .. } => *timestamp,
            TraceEvent::Eviction { timestamp, .. } => *timestamp,
            TraceEvent::PolicyDecision { timestamp, .. } => *timestamp,
            TraceEvent::DaemonStarted { timestamp, .. } => *timestamp,
            TraceEvent::DaemonStopping { timestamp, .. } => *timestamp,
            TraceEvent::BackendRegistered { timestamp, .. } => *timestamp,
            TraceEvent::WorkerSpawned { timestamp, .. } => *timestamp,
            TraceEvent::WorkerStopped { timestamp, .. } => *timestamp,
            TraceEvent::IpcRequestReceived { timestamp, .. } => *timestamp,
            TraceEvent::IpcResponseSent { timestamp, .. } => *timestamp,
            TraceEvent::IpcConnectionAccepted { timestamp, .. } => *timestamp,
            TraceEvent::IpcConnectionClosed { timestamp, .. } => *timestamp,
            TraceEvent::CompressionStarted { timestamp, .. } => *timestamp,
            TraceEvent::CompressionCompleted { timestamp, .. } => *timestamp,
            TraceEvent::DecompressionStarted { timestamp, .. } => *timestamp,
            TraceEvent::DecompressionCompleted { timestamp, .. } => *timestamp,
        }
    }

    /// Get the chunk ID associated with this event, if any.
    pub fn chunk_id(&self) -> Option<ChunkId> {
        match self {
            TraceEvent::ChunkCreated { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::ChunkDeleted { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::ChunkStateChanged { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::TransferQueued { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::TransferStarted { job, .. } => Some(job.chunk_id),
            TraceEvent::TransferCompleted { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::TransferFailed { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::TransferRetry { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::TransferCancelled { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::PressureSample { .. } => None,
            TraceEvent::PressureAlert { .. } => None,
            TraceEvent::Eviction { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::PolicyDecision { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::DaemonStarted { .. } => None,
            TraceEvent::DaemonStopping { .. } => None,
            TraceEvent::BackendRegistered { .. } => None,
            TraceEvent::WorkerSpawned { .. } => None,
            TraceEvent::WorkerStopped { .. } => None,
            TraceEvent::IpcRequestReceived { .. } => None,
            TraceEvent::IpcResponseSent { .. } => None,
            TraceEvent::IpcConnectionAccepted { .. } => None,
            TraceEvent::IpcConnectionClosed { .. } => None,
            TraceEvent::CompressionStarted { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::CompressionCompleted { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::DecompressionStarted { chunk_id, .. } => Some(*chunk_id),
            TraceEvent::DecompressionCompleted { chunk_id, .. } => Some(*chunk_id),
        }
    }

    /// Get a human-readable name for this event type.
    pub fn event_type(&self) -> &'static str {
        match self {
            TraceEvent::ChunkCreated { .. } => "chunk_created",
            TraceEvent::ChunkDeleted { .. } => "chunk_deleted",
            TraceEvent::ChunkStateChanged { .. } => "chunk_state_changed",
            TraceEvent::TransferQueued { .. } => "transfer_queued",
            TraceEvent::TransferStarted { .. } => "transfer_started",
            TraceEvent::TransferCompleted { .. } => "transfer_completed",
            TraceEvent::TransferFailed { .. } => "transfer_failed",
            TraceEvent::TransferRetry { .. } => "transfer_retry",
            TraceEvent::TransferCancelled { .. } => "transfer_cancelled",
            TraceEvent::PressureSample { .. } => "pressure_sample",
            TraceEvent::PressureAlert { .. } => "pressure_alert",
            TraceEvent::Eviction { .. } => "eviction",
            TraceEvent::PolicyDecision { .. } => "policy_decision",
            TraceEvent::DaemonStarted { .. } => "daemon_started",
            TraceEvent::DaemonStopping { .. } => "daemon_stopping",
            TraceEvent::BackendRegistered { .. } => "backend_registered",
            TraceEvent::WorkerSpawned { .. } => "worker_spawned",
            TraceEvent::WorkerStopped { .. } => "worker_stopped",
            TraceEvent::IpcRequestReceived { .. } => "ipc_request_received",
            TraceEvent::IpcResponseSent { .. } => "ipc_response_sent",
            TraceEvent::IpcConnectionAccepted { .. } => "ipc_connection_accepted",
            TraceEvent::IpcConnectionClosed { .. } => "ipc_connection_closed",
            TraceEvent::CompressionStarted { .. } => "compression_started",
            TraceEvent::CompressionCompleted { .. } => "compression_completed",
            TraceEvent::DecompressionStarted { .. } => "decompression_started",
            TraceEvent::DecompressionCompleted { .. } => "decompression_completed",
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

/// Get the current timestamp in milliseconds since epoch.
///
/// Alias for `current_timestamp()` to match the spec naming.
pub fn timestamp_now() -> u64 {
    current_timestamp()
}

/// Record a trace event with the current timestamp.
///
/// # Example
///
/// ```
/// use ghost_core::{trace_event, trace::TraceEvent};
/// use ghost_core::types::{ChunkId, TierId};
///
/// let event = trace_event!(TraceEvent::ChunkCreated {
///     chunk_id: ChunkId::from_data(b"test"),
///     size: 1024,
///     tier: TierId::Ram,
///     timestamp: 0, // Will be overwritten
/// });
/// ```
#[macro_export]
macro_rules! trace_event {
    ($event:expr) => {{
        let ts = $crate::trace::current_timestamp();
        match $event {
            $crate::trace::TraceEvent::ChunkCreated {
                chunk_id,
                size,
                tier,
                ..
            } => $crate::trace::TraceEvent::ChunkCreated {
                chunk_id,
                size,
                tier,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::ChunkDeleted { chunk_id, tier, .. } => {
                $crate::trace::TraceEvent::ChunkDeleted {
                    chunk_id,
                    tier,
                    timestamp: ts,
                }
            }
            $crate::trace::TraceEvent::ChunkStateChanged {
                chunk_id, from, to, ..
            } => $crate::trace::TraceEvent::ChunkStateChanged {
                chunk_id,
                from,
                to,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::TransferQueued {
                chunk_id,
                from,
                to,
                priority,
                ..
            } => $crate::trace::TraceEvent::TransferQueued {
                chunk_id,
                from,
                to,
                priority,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::TransferStarted { job, .. } => {
                $crate::trace::TraceEvent::TransferStarted { job, timestamp: ts }
            }
            $crate::trace::TraceEvent::TransferCompleted {
                chunk_id,
                from,
                to,
                size,
                duration_ms,
                ..
            } => $crate::trace::TraceEvent::TransferCompleted {
                chunk_id,
                from,
                to,
                size,
                duration_ms,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::TransferFailed {
                chunk_id,
                from,
                to,
                error,
                attempt,
                ..
            } => $crate::trace::TraceEvent::TransferFailed {
                chunk_id,
                from,
                to,
                error,
                attempt,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::TransferRetry {
                chunk_id,
                from,
                to,
                attempt,
                ..
            } => $crate::trace::TraceEvent::TransferRetry {
                chunk_id,
                from,
                to,
                attempt,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::TransferCancelled {
                chunk_id, from, to, ..
            } => $crate::trace::TraceEvent::TransferCancelled {
                chunk_id,
                from,
                to,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::PressureSample { state, .. } => {
                $crate::trace::TraceEvent::PressureSample {
                    state,
                    timestamp: ts,
                }
            }
            $crate::trace::TraceEvent::PressureAlert {
                memory_pressure,
                vram_pressure,
                io_pressure,
                ..
            } => $crate::trace::TraceEvent::PressureAlert {
                memory_pressure,
                vram_pressure,
                io_pressure,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::Eviction {
                chunk_id,
                tier,
                reason,
                ..
            } => $crate::trace::TraceEvent::Eviction {
                chunk_id,
                tier,
                reason,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::PolicyDecision {
                chunk_id,
                from,
                to,
                reason,
                ..
            } => $crate::trace::TraceEvent::PolicyDecision {
                chunk_id,
                from,
                to,
                reason,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::DaemonStarted { .. } => {
                $crate::trace::TraceEvent::DaemonStarted { timestamp: ts }
            }
            $crate::trace::TraceEvent::DaemonStopping { .. } => {
                $crate::trace::TraceEvent::DaemonStopping { timestamp: ts }
            }
            $crate::trace::TraceEvent::BackendRegistered { tier, .. } => {
                $crate::trace::TraceEvent::BackendRegistered {
                    tier,
                    timestamp: ts,
                }
            }
            $crate::trace::TraceEvent::WorkerSpawned { worker_id, .. } => {
                $crate::trace::TraceEvent::WorkerSpawned {
                    worker_id,
                    timestamp: ts,
                }
            }
            $crate::trace::TraceEvent::WorkerStopped { worker_id, .. } => {
                $crate::trace::TraceEvent::WorkerStopped {
                    worker_id,
                    timestamp: ts,
                }
            }
            $crate::trace::TraceEvent::IpcRequestReceived { request_type, .. } => {
                $crate::trace::TraceEvent::IpcRequestReceived {
                    request_type,
                    timestamp: ts,
                }
            }
            $crate::trace::TraceEvent::IpcResponseSent {
                request_type,
                success,
                ..
            } => $crate::trace::TraceEvent::IpcResponseSent {
                request_type,
                success,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::IpcConnectionAccepted { .. } => {
                $crate::trace::TraceEvent::IpcConnectionAccepted { timestamp: ts }
            }
            $crate::trace::TraceEvent::IpcConnectionClosed { .. } => {
                $crate::trace::TraceEvent::IpcConnectionClosed { timestamp: ts }
            }
            $crate::trace::TraceEvent::CompressionStarted {
                chunk_id,
                original_size,
                ..
            } => $crate::trace::TraceEvent::CompressionStarted {
                chunk_id,
                original_size,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::CompressionCompleted {
                chunk_id,
                original_size,
                compressed_size,
                ..
            } => $crate::trace::TraceEvent::CompressionCompleted {
                chunk_id,
                original_size,
                compressed_size,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::DecompressionStarted {
                chunk_id,
                compressed_size,
                ..
            } => $crate::trace::TraceEvent::DecompressionStarted {
                chunk_id,
                compressed_size,
                timestamp: ts,
            },
            $crate::trace::TraceEvent::DecompressionCompleted {
                chunk_id,
                compressed_size,
                decompressed_size,
                ..
            } => $crate::trace::TraceEvent::DecompressionCompleted {
                chunk_id,
                compressed_size,
                decompressed_size,
                timestamp: ts,
            },
        }
    }};
}

/// Record a lifecycle trace event with the current timestamp.
///
/// Convenience macro for lifecycle events that only need a timestamp.
///
/// # Example
///
/// ```
/// use ghost_core::trace_lifecycle;
/// use ghost_core::trace::TraceEvent;
///
/// let event = trace_lifecycle!(TraceEvent::DaemonStarted);
/// ```
#[macro_export]
macro_rules! trace_lifecycle {
    (TraceEvent::DaemonStarted) => {
        $crate::trace::TraceEvent::DaemonStarted {
            timestamp: $crate::trace::current_timestamp(),
        }
    };
    (TraceEvent::DaemonStopping) => {
        $crate::trace::TraceEvent::DaemonStopping {
            timestamp: $crate::trace::current_timestamp(),
        }
    };
    (TraceEvent::IpcConnectionAccepted) => {
        $crate::trace::TraceEvent::IpcConnectionAccepted {
            timestamp: $crate::trace::current_timestamp(),
        }
    };
    (TraceEvent::IpcConnectionClosed) => {
        $crate::trace::TraceEvent::IpcConnectionClosed {
            timestamp: $crate::trace::current_timestamp(),
        }
    };
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

        let daemon_started = TraceEvent::DaemonStarted { timestamp: 0 };
        assert_eq!(daemon_started.chunk_id(), None);

        let ipc_request = TraceEvent::IpcRequestReceived {
            request_type: "store".to_string(),
            timestamp: 0,
        };
        assert_eq!(ipc_request.chunk_id(), None);
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

        assert_eq!(
            TraceEvent::DaemonStarted { timestamp: 0 }.event_type(),
            "daemon_started"
        );

        assert_eq!(
            TraceEvent::IpcRequestReceived {
                request_type: "store".to_string(),
                timestamp: 0,
            }
            .event_type(),
            "ipc_request_received"
        );
    }

    #[test]
    fn test_current_timestamp() {
        let ts = current_timestamp();
        // Should be a reasonable timestamp (after year 2020)
        assert!(ts > 1_577_836_800_000); // 2020-01-01 in millis
    }

    #[test]
    fn test_timestamp_now() {
        let ts = timestamp_now();
        assert!(ts > 1_577_836_800_000);
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
        let deserialized: TraceEvent =
            serde_json::from_str(&serialized).expect("deserialize trace event");

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
            size: 4096,
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
            from: TierId::Ram,
            to: TierId::Disk,
            error: "connection lost".to_string(),
            attempt: 2,
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

    #[test]
    fn test_new_trace_event_variants() {
        // Verify all new variants can be constructed and have correct event_type
        let chunk_id = ChunkId::from_data(b"new_variants");

        let deleted = TraceEvent::ChunkDeleted {
            chunk_id,
            tier: TierId::Ram,
            timestamp: 1,
        };
        assert_eq!(deleted.event_type(), "chunk_deleted");
        assert_eq!(deleted.chunk_id(), Some(chunk_id));

        let queued = TraceEvent::TransferQueued {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Disk,
            priority: TransferPriority::High,
            timestamp: 2,
        };
        assert_eq!(queued.event_type(), "transfer_queued");

        let retry = TraceEvent::TransferRetry {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Disk,
            attempt: 1,
            timestamp: 3,
        };
        assert_eq!(retry.event_type(), "transfer_retry");

        let cancelled = TraceEvent::TransferCancelled {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Disk,
            timestamp: 4,
        };
        assert_eq!(cancelled.event_type(), "transfer_cancelled");

        let pressure_alert = TraceEvent::PressureAlert {
            memory_pressure: 0.9,
            vram_pressure: 0.8,
            io_pressure: 0.7,
            timestamp: 5,
        };
        assert_eq!(pressure_alert.event_type(), "pressure_alert");
        assert_eq!(pressure_alert.chunk_id(), None);

        let policy = TraceEvent::PolicyDecision {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Disk,
            reason: "LRU eviction".to_string(),
            timestamp: 6,
        };
        assert_eq!(policy.event_type(), "policy_decision");

        let daemon_started = TraceEvent::DaemonStarted { timestamp: 7 };
        assert_eq!(daemon_started.event_type(), "daemon_started");

        let daemon_stopping = TraceEvent::DaemonStopping { timestamp: 8 };
        assert_eq!(daemon_stopping.event_type(), "daemon_stopping");

        let backend = TraceEvent::BackendRegistered {
            tier: TierId::Ram,
            timestamp: 9,
        };
        assert_eq!(backend.event_type(), "backend_registered");

        let worker_spawned = TraceEvent::WorkerSpawned {
            worker_id: 0,
            timestamp: 10,
        };
        assert_eq!(worker_spawned.event_type(), "worker_spawned");

        let worker_stopped = TraceEvent::WorkerStopped {
            worker_id: 0,
            timestamp: 11,
        };
        assert_eq!(worker_stopped.event_type(), "worker_stopped");

        let ipc_req = TraceEvent::IpcRequestReceived {
            request_type: "store".to_string(),
            timestamp: 12,
        };
        assert_eq!(ipc_req.event_type(), "ipc_request_received");

        let ipc_resp = TraceEvent::IpcResponseSent {
            request_type: "store".to_string(),
            success: true,
            timestamp: 13,
        };
        assert_eq!(ipc_resp.event_type(), "ipc_response_sent");

        let ipc_accept = TraceEvent::IpcConnectionAccepted { timestamp: 14 };
        assert_eq!(ipc_accept.event_type(), "ipc_connection_accepted");

        let ipc_close = TraceEvent::IpcConnectionClosed { timestamp: 15 };
        assert_eq!(ipc_close.event_type(), "ipc_connection_closed");

        let comp_start = TraceEvent::CompressionStarted {
            chunk_id,
            original_size: 4096,
            timestamp: 16,
        };
        assert_eq!(comp_start.event_type(), "compression_started");

        let comp_done = TraceEvent::CompressionCompleted {
            chunk_id,
            original_size: 4096,
            compressed_size: 2048,
            timestamp: 17,
        };
        assert_eq!(comp_done.event_type(), "compression_completed");

        let decomp_start = TraceEvent::DecompressionStarted {
            chunk_id,
            compressed_size: 2048,
            timestamp: 18,
        };
        assert_eq!(decomp_start.event_type(), "decompression_started");

        let decomp_done = TraceEvent::DecompressionCompleted {
            chunk_id,
            compressed_size: 2048,
            decompressed_size: 4096,
            timestamp: 19,
        };
        assert_eq!(decomp_done.event_type(), "decompression_completed");
    }

    #[test]
    fn test_new_variants_serialization_roundtrip() {
        let events: Vec<TraceEvent> = vec![
            TraceEvent::ChunkDeleted {
                chunk_id: ChunkId::from_data(b"del"),
                tier: TierId::Ram,
                timestamp: 1,
            },
            TraceEvent::TransferQueued {
                chunk_id: ChunkId::from_data(b"q"),
                from: TierId::Ram,
                to: TierId::Disk,
                priority: TransferPriority::High,
                timestamp: 2,
            },
            TraceEvent::TransferRetry {
                chunk_id: ChunkId::from_data(b"retry"),
                from: TierId::Ram,
                to: TierId::Disk,
                attempt: 3,
                timestamp: 3,
            },
            TraceEvent::TransferCancelled {
                chunk_id: ChunkId::from_data(b"cancel"),
                from: TierId::Ram,
                to: TierId::Disk,
                timestamp: 4,
            },
            TraceEvent::PressureAlert {
                memory_pressure: 0.95,
                vram_pressure: 0.85,
                io_pressure: 0.75,
                timestamp: 5,
            },
            TraceEvent::PolicyDecision {
                chunk_id: ChunkId::from_data(b"policy"),
                from: TierId::Ram,
                to: TierId::Disk,
                reason: "capacity".to_string(),
                timestamp: 6,
            },
            TraceEvent::DaemonStarted { timestamp: 7 },
            TraceEvent::DaemonStopping { timestamp: 8 },
            TraceEvent::BackendRegistered {
                tier: TierId::Disk,
                timestamp: 9,
            },
            TraceEvent::WorkerSpawned {
                worker_id: 42,
                timestamp: 10,
            },
            TraceEvent::WorkerStopped {
                worker_id: 42,
                timestamp: 11,
            },
            TraceEvent::IpcRequestReceived {
                request_type: "retrieve".to_string(),
                timestamp: 12,
            },
            TraceEvent::IpcResponseSent {
                request_type: "retrieve".to_string(),
                success: false,
                timestamp: 13,
            },
            TraceEvent::IpcConnectionAccepted { timestamp: 14 },
            TraceEvent::IpcConnectionClosed { timestamp: 15 },
            TraceEvent::CompressionStarted {
                chunk_id: ChunkId::from_data(b"comp"),
                original_size: 8192,
                timestamp: 16,
            },
            TraceEvent::CompressionCompleted {
                chunk_id: ChunkId::from_data(b"comp"),
                original_size: 8192,
                compressed_size: 4096,
                timestamp: 17,
            },
            TraceEvent::DecompressionStarted {
                chunk_id: ChunkId::from_data(b"decomp"),
                compressed_size: 4096,
                timestamp: 18,
            },
            TraceEvent::DecompressionCompleted {
                chunk_id: ChunkId::from_data(b"decomp"),
                compressed_size: 4096,
                decompressed_size: 8192,
                timestamp: 19,
            },
        ];

        for event in &events {
            let serialized = serde_json::to_string(event).expect("serialize trace event");
            let deserialized: TraceEvent =
                serde_json::from_str(&serialized).expect("deserialize trace event");
            assert_eq!(event.timestamp(), deserialized.timestamp());
            assert_eq!(event.chunk_id(), deserialized.chunk_id());
            assert_eq!(event.event_type(), deserialized.event_type());
        }
    }
}
