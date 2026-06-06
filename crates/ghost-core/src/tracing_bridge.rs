//! Bridge from unified [`Event`]s to structured `tracing` spans.
//!
//! [`TracingHandler`] implements [`EventHandler`] and converts each event
//! into a `tracing::info_span!` entry with structured fields. This provides
//! a human-readable log of all system events without replacing existing
//! `tracing::info!` / `tracing::debug!` calls.
//!
//! # Example
//!
//! ```
//! use ghost_core::tracing_bridge::TracingHandler;
//! use ghost_core::event_multiplexer::EventHandler;
//! use ghost_core::events::Event;
//! use ghost_core::types::{ChunkId, TierId};
//!
//! let handler = TracingHandler;
//! let event = Event::AllocationCreated {
//!     chunk_id: ChunkId::from_data(b"test"),
//!     tier: TierId::Ram,
//!     size: 4096,
//!     sequence_id: 0,
//! };
//! // handler.handle(&EventRecord { sequence_id: 0, timestamp: 0, event: event.clone() }).await.unwrap();
//! ```

use std::future::Future;
use std::pin::Pin;

use crate::event_multiplexer::EventHandler;
use crate::events::{Event, EventRecord};

#[cfg(test)]
use crate::events::{BackendHealth, InvariantSeverity};
#[cfg(test)]
use crate::state::PressureState;

/// Converts unified events into structured `tracing` spans.
///
/// Each event is logged at `INFO` level with structured fields:
/// - `event.name` — the event variant name (e.g. `"migration_completed"`)
/// - `event.category` — the event category (e.g. `"migration"`)
/// - `chunk_id` — present when the event is associated with a chunk
/// - `tier` — present when the event is associated with a tier
#[derive(Debug, Clone, Default)]
pub struct TracingHandler;

impl EventHandler for TracingHandler {
    fn handle(
        &self,
        event: &EventRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>>
    {
        let event_name = event.event.event_name();
        let category = event.event.category();
        let chunk_id_str = event.event.chunk_id().map(|id| id.short_hex());
        let tier_str = event.event.tier().map(|t| format!("{:?}", t));
        let inner_event = event.event.clone();

        Box::pin(async move {
            let span = tracing::info_span!(
                "ghost_event",
                event.name = event_name,
                event.category = category,
                chunk_id = chunk_id_str.as_deref().unwrap_or("-"),
                tier = tier_str.as_deref().unwrap_or("-"),
            );

            let _guard = span.enter();

            // Log event-specific details
            match inner_event {
                Event::AllocationCreated {
                    chunk_id, tier, size, ..
                } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        tier = ?tier,
                        size,
                        "Chunk allocated"
                    );
                }
                Event::AllocationFreed { chunk_id, tier, .. } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        tier = ?tier,
                        "Chunk freed"
                    );
                }
                Event::AllocationFailed { chunk_id, reason, .. } => {
                    tracing::warn!(
                        chunk_id = %chunk_id,
                        reason,
                        "Allocation failed"
                    );
                }
                Event::MigrationStarted { chunk_id, from, to, .. } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        "Migration started"
                    );
                }
                Event::MigrationCompleted {
                    chunk_id,
                    from,
                    to,
                    duration_ms,
                    ..
                } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        duration_ms,
                        "Migration completed"
                    );
                }
                Event::MigrationFailed {
                    chunk_id,
                    from,
                    to,
                    reason,
                    ..
                } => {
                    tracing::error!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        reason,
                        "Migration failed"
                    );
                }
                Event::MigrationRolledBack { chunk_id, from, to, .. } => {
                    tracing::warn!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        "Migration rolled back"
                    );
                }
                Event::ReplayStarted { trace_path, .. } => {
                    tracing::info!(
                        trace_path,
                        "Replay started"
                    );
                }
                Event::ReplayCompleted {
                    trace_path,
                    events,
                    duration_ms,
                    ..
                } => {
                    tracing::info!(
                        trace_path,
                        events,
                        duration_ms,
                        "Replay completed"
                    );
                }
                Event::ReplayDivergence {
                    trace_path,
                    expected,
                    actual,
                    ..
                } => {
                    tracing::error!(
                        trace_path,
                        expected,
                        actual,
                        "Replay diverged"
                    );
                }
                Event::ReplayInvariantViolation { rule, details, .. } => {
                    tracing::warn!(
                        rule,
                        details,
                        "Replay invariant violation"
                    );
                }
                Event::PressureChanged { tier, old, new, .. } => {
                    tracing::info!(
                        tier = ?tier,
                        memory_old = old.memory_pressure,
                        memory_new = new.memory_pressure,
                        vram_old = old.vram_pressure,
                        vram_new = new.vram_pressure,
                        io_old = old.io_pressure,
                        io_new = new.io_pressure,
                        "Pressure changed"
                    );
                }
                Event::BackpressureActivated { tier, level, .. } => {
                    tracing::warn!(
                        tier = ?tier,
                        level,
                        "Backpressure activated"
                    );
                }
                Event::BackpressureDeactivated { tier, .. } => {
                    tracing::info!(
                        tier = ?tier,
                        "Backpressure deactivated"
                    );
                }
                Event::BackendHealthChanged { tier, old, new, .. } => {
                    tracing::warn!(
                        tier = ?tier,
                        old = ?old,
                        new = ?new,
                        "Backend health changed"
                    );
                }
                Event::RetryAttempted {
                    chunk_id,
                    attempt,
                    max_attempts,
                    ..
                } => {
                    tracing::warn!(
                        chunk_id = %chunk_id,
                        attempt,
                        max_attempts,
                        "Retry attempted"
                    );
                }
                Event::OperationFailed { operation, reason, .. } => {
                    tracing::error!(
                        operation,
                        reason,
                        "Operation failed"
                    );
                }
                Event::InvariantViolation {
                    rule,
                    details,
                    severity,
                    ..
                } => {
                    tracing::error!(
                        rule,
                        details,
                        severity = %severity,
                        "Invariant violation"
                    );
                }
                Event::IoRequestIssued {
                    operation,
                    chunk_id,
                    tier,
                    ..
                } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        tier = ?tier,
                        operation = ?operation,
                        "I/O request issued"
                    );
                }
                Event::IoRequestCompleted {
                    operation,
                    chunk_id,
                    tier,
                    duration_ticks,
                    ..
                } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        tier = ?tier,
                        operation = ?operation,
                        duration_ticks,
                        "I/O request completed"
                    );
                }
                Event::IoRequestFailed {
                    operation,
                    chunk_id,
                    tier,
                    error,
                    ..
                } => {
                    tracing::error!(
                        chunk_id = %chunk_id,
                        tier = ?tier,
                        operation = ?operation,
                        error,
                        "I/O request failed"
                    );
                }
                Event::IoFlushIssued { tier, .. } => {
                    tracing::info!(tier = ?tier, "I/O flush issued");
                }
                Event::IoFlushCompleted {
                    tier,
                    duration_ticks,
                    ..
                } => {
                    tracing::info!(
                        tier = ?tier,
                        duration_ticks,
                        "I/O flush completed"
                    );
                }
                Event::IoBufferStateChange {
                    tier,
                    buffered,
                    capacity,
                    ..
                } => {
                    tracing::debug!(
                        tier = ?tier,
                        buffered,
                        capacity,
                        "I/O buffer state changed"
                    );
                }
                Event::Eviction {
                    chunk_id, tier, reason, ..
                } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        tier = ?tier,
                        reason,
                        "Chunk evicted"
                    );
                }
                Event::Retrieve {
                    key, hit, ..
                } => {
                    tracing::info!(
                        key,
                        hit,
                        "Retrieve operation"
                    );
                }
                Event::TransferCompleted {
                    chunk_id,
                    from,
                    to,
                    duration_ms,
                    ..
                } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        duration_ms,
                        "Transfer completed"
                    );
                }
                Event::TransferFailed {
                    chunk_id,
                    from,
                    to,
                    reason,
                    ..
                } => {
                    tracing::error!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        reason,
                        "Transfer failed"
                    );
                }
                Event::Store {
                    key, value_size, ..
                } => {
                    tracing::info!(
                        key,
                        value_size,
                        "Store operation"
                    );
                }
                Event::Evict {
                    key, ..
                } => {
                    tracing::info!(
                        key,
                        "Evict operation"
                    );
                }
                Event::QueueEnqueue {
                    task_id, ..
                } => {
                    tracing::info!(
                        task_id,
                        "Task enqueued"
                    );
                }
                Event::QueueDequeue {
                    task_id, ..
                } => {
                    tracing::info!(
                        task_id,
                        "Task dequeued"
                    );
                }
                Event::MigrationDecision {
                    chunk_id,
                    from,
                    to,
                    decision,
                    ..
                } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        decision,
                        "Migration decision"
                    );
                }
                Event::MigrationDecided {
                    chunk_id,
                    from,
                    to,
                    cost_score,
                    ..
                } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        cost_score,
                        "Migration decided"
                    );
                }
                Event::MigrationDeferred {
                    chunk_id,
                    from,
                    to,
                    reason,
                    ..
                } => {
                    tracing::info!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        reason,
                        "Migration deferred"
                    );
                }
                Event::MigrationRejected {
                    chunk_id,
                    from,
                    to,
                    cost_score,
                    threshold,
                    ..
                } => {
                    tracing::warn!(
                        chunk_id = %chunk_id,
                        from = ?from,
                        to = ?to,
                        cost_score,
                        threshold,
                        "Migration rejected"
                    );
                }
                Event::MemoryPressureChanged { avg10, avg60, avg300, .. } => {
                    tracing::info!(
                        avg10,
                        avg60,
                        avg300,
                        "Memory pressure changed"
                    );
                }
                Event::IoPressureChanged { avg10, avg60, avg300, .. } => {
                    tracing::info!(
                        avg10,
                        avg60,
                        avg300,
                        "IO pressure changed"
                    );
                }
            }

            Ok(())
        })
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChunkId, TierId};

    #[tokio::test]
    async fn test_tracing_handler_no_error() {
        let handler = TracingHandler;
        let event = Event::AllocationCreated {
            chunk_id: ChunkId::from_data(b"test"),
            tier: TierId::Ram,
            size: 4096,
            sequence_id: 0,
        };
        handler.handle(&EventRecord { sequence_id: 0, timestamp: 0, event: event.clone() }).await.unwrap();
    }

    #[tokio::test]
    async fn test_tracing_handler_all_variants() {
        let handler = TracingHandler;
        let id = ChunkId::from_data(b"test");

        let events: Vec<Event> = vec![
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
                reason: "oom".to_string(),
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
                details: "orphan".to_string(),
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
                reason: "full".to_string(),
                sequence_id: 0,
            },
            Event::InvariantViolation {
                rule: "test".to_string(),
                details: "bad".to_string(),
                severity: InvariantSeverity::Error,
                sequence_id: 0,
            },
        ];

        for event in &events { let event = EventRecord { sequence_id: 0, timestamp: 0, event: event.clone() };
            handler.handle(&event).await.unwrap();
        }
    }
}
