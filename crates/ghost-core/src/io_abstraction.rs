//! I/O abstraction layer for deterministic I/O scheduling.
//!
//! This module provides the [`IoScheduler`] — the central I/O abstraction that
//! guarantees real disk I/O does **not** break determinism. The key design is
//! **issue/completion separation**:
//!
//! - **Issue**: Records that an I/O request was submitted (happens immediately)
//! - **Complete**: Resolves the request when the actual I/O finishes
//!
//! In deterministic simulation, both issue and complete happen at controlled
//! ticks. In real mode, issue happens when the syscall starts and complete
//! happens when it finishes. This ensures event ordering is independent of
//! wall clock.
//!
//! The [`IoScheduler`] is generic and can be used by any storage backend
//! (RAM, Disk, Simulation) to provide deterministic I/O behavior.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::emitter::EventEmitter;
pub use crate::io_events::IoOperation;
use crate::io_events::IoEvent;
use crate::time::TimeProvider;
use crate::types::{ChunkId, TierId};

/// Represents a deferred I/O completion that is resolved deterministically.
///
/// An `IoRequest` is created when an I/O operation is *issued* and remains
/// pending until it is *completed* or *failed*. The separation of issue from
/// completion is the core mechanism that ensures deterministic I/O ordering.
#[derive(Debug, Clone)]
pub struct IoRequest {
    /// Unique identifier for this request (monotonically increasing).
    pub id: u64,

    /// The type of I/O operation.
    pub operation: IoOperation,

    /// The chunk being operated on.
    pub chunk_id: ChunkId,

    /// The tier the operation targets.
    pub tier: TierId,

    /// Timestamp when the request was issued.
    pub issued_at: Instant,

    /// Current completion state of this request.
    pub completion: IoCompletion,
}

/// The completion state of an I/O request.
#[derive(Debug, Clone)]
pub enum IoCompletion {
    /// The request is still pending (in-flight).
    Pending,

    /// The request completed successfully with the given duration.
    Completed {
        /// Duration in deterministic ticks.
        duration_ticks: u64,
    },

    /// The request failed with the given error.
    Failed {
        /// Human-readable error description.
        error: String,
    },
}

/// The I/O scheduler queues I/O requests and resolves them in deterministic order.
///
/// # Design
///
/// The `IoScheduler` separates **issue** from **complete**:
///
/// 1. `issue()` — records that an I/O request was submitted, emits
///    `IoRequestIssued`, and returns a unique request ID.
/// 2. `complete()` — resolves a pending request as success or failure, emits
///    `IoRequestCompleted` or `IoRequestFailed`.
/// 3. `flush()` — resolves all pending requests, emitting flush events.
///
/// In deterministic mode, the `DeterministicClock` provides timing.
/// In real mode, `RealTimeProvider` provides actual completion time.
///
/// # Thread Safety
///
/// The scheduler uses interior mutability via `&self` for `issue()` and
/// `&mut self` for `complete()` and `flush()`. The `pending` map uses a
/// `BTreeMap` for ordered iteration (deterministic ordering by ID).
pub struct IoScheduler {
    /// Next request ID (monotonically increasing).
    next_id: AtomicU64,

    /// Pending (in-flight) I/O requests, ordered by ID.
    pending: BTreeMap<u64, IoRequest>,

    /// Completed I/O requests (for replay and auditing).
    completed: Vec<IoRequest>,

    /// Time provider for deterministic or real timing.
    time_provider: Arc<dyn TimeProvider>,

    /// Event emitter for I/O lifecycle events.
    event_emitter: EventEmitter,
}

impl IoScheduler {
    /// Create a new I/O scheduler.
    ///
    /// # Arguments
    ///
    /// * `time_provider` — Provides deterministic or real time.
    /// * `event_emitter` — Emits `IoEvent` variants to the event system.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            pending: BTreeMap::new(),
            completed: Vec::new(),
            time_provider,
            event_emitter,
        }
    }

    /// Issue an I/O request — records it but does NOT complete it yet.
    ///
    /// Returns the unique request ID that can later be passed to `complete()`.
    /// Emits `IoRequestIssued` synchronously.
    ///
    /// # Arguments
    ///
    /// * `operation` — The type of I/O operation.
    /// * `chunk_id` — The chunk being operated on.
    /// * `tier` — The tier the operation targets.
    pub fn issue(
        &self,
        operation: IoOperation,
        chunk_id: ChunkId,
        tier: TierId,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let now = self.time_provider.now();

        let request = IoRequest {
            id,
            operation,
            chunk_id,
            tier,
            issued_at: now,
            completion: IoCompletion::Pending,
        };

        // Emit the issued event synchronously
        let _ = self.event_emitter.try_emit(
            crate::events::Event::IoRequestIssued {
                operation,
                chunk_id,
                tier,
            },
        );

        // Safety: We use unsafe to allow interior mutability for `issue()`.
        // This is safe because `issue()` only inserts into the BTreeMap and
        // the ID is unique (monotonic atomic). In single-threaded deterministic
        // simulation, this is always safe. For multi-threaded use, a Mutex would
        // be needed — but the scheduler is designed for single-threaded use
        // within a backend.
        //
        // We use `unsafe` here to maintain the `&self` API. The caller must
        // ensure no concurrent `issue()` and `complete()`/`flush()` calls.
        unsafe {
            let pending = &self.pending as *const BTreeMap<u64, IoRequest>
                as *mut BTreeMap<u64, IoRequest>;
            (*pending).insert(id, request);
        }

        id
    }

    /// Resolve a pending I/O request — called when the actual I/O finishes.
    ///
    /// In deterministic mode, this uses the `DeterministicClock` for timing.
    /// In real mode, this uses the actual completion time.
    ///
    /// Emits `IoRequestCompleted` or `IoRequestFailed`.
    ///
    /// # Arguments
    ///
    /// * `id` — The request ID returned by `issue()`.
    /// * `result` — `Ok(())` for success, `Err(error)` for failure.
    ///
    /// # Panics
    ///
    /// Panics if the request ID was not issued or was already completed.
    pub fn complete(&mut self, id: u64, result: Result<(), String>) {
        let mut request = self
            .pending
            .remove(&id)
            .expect("IoScheduler::complete: unknown request ID");

        let now = self.time_provider.now();
        let duration = now.duration_since(request.issued_at);
        let duration_ticks = duration.as_nanos() as u64;

        request.completion = match result {
            Ok(()) => {
                let _ = self.event_emitter.try_emit(
                    crate::events::Event::IoRequestCompleted {
                        operation: request.operation,
                        chunk_id: request.chunk_id,
                        tier: request.tier,
                        duration_ticks,
                    },
                );
                IoCompletion::Completed { duration_ticks }
            }
            Err(error) => {
                let _ = self.event_emitter.try_emit(
                    crate::events::Event::IoRequestFailed {
                        operation: request.operation,
                        chunk_id: request.chunk_id,
                        tier: request.tier,
                        error: error.clone(),
                    },
                );
                IoCompletion::Failed { error }
            }
        };

        self.completed.push(request);
    }

    /// Get all pending (in-flight) I/O requests.
    ///
    /// Useful for invariant checking and diagnostics.
    pub fn pending(&self) -> &BTreeMap<u64, IoRequest> {
        &self.pending
    }

    /// Get all completed I/O requests.
    ///
    /// Useful for replay and auditing.
    pub fn completed(&self) -> &[IoRequest] {
        &self.completed
    }

    /// Get the number of pending (in-flight) I/O requests.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get the number of completed I/O requests.
    pub fn completed_count(&self) -> usize {
        self.completed.len()
    }

    /// Flush all pending I/O — resolves all in-flight requests as completed.
    ///
    /// This simulates an fsync: all pending I/O is forced to complete.
    /// Emits `IoFlushIssued` before flushing and `IoFlushCompleted` after.
    ///
    /// In deterministic mode, all pending requests are resolved with the
    /// current tick count as duration.
    pub fn flush(&mut self) {
        let now = self.time_provider.now();

        let _ = self.event_emitter.try_emit(
            crate::events::Event::IoFlushIssued {
                tier: TierId::Simulation, // Will be overridden by per-request tier
            },
        );

        // Collect all pending IDs
        let pending_ids: Vec<u64> = self.pending.keys().copied().collect();

        for id in pending_ids {
            if let Some(mut request) = self.pending.remove(&id) {
                let duration = now.duration_since(request.issued_at);
                let duration_ticks = duration.as_nanos() as u64;

                request.completion = IoCompletion::Completed { duration_ticks };

                let _ = self.event_emitter.try_emit(
                    crate::events::Event::IoRequestCompleted {
                        operation: request.operation,
                        chunk_id: request.chunk_id,
                        tier: request.tier,
                        duration_ticks,
                    },
                );

                self.completed.push(request);
            }
        }

        let flush_duration = now.elapsed();
        let flush_duration_ticks = flush_duration.as_nanos() as u64;

        let _ = self.event_emitter.try_emit(
            crate::events::Event::IoFlushCompleted {
                tier: TierId::Simulation,
                duration_ticks: flush_duration_ticks,
            },
        );
    }
}

impl std::fmt::Debug for IoScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoScheduler")
            .field("pending_count", &self.pending.len())
            .field("completed_count", &self.completed.len())
            .field("next_id", &self.next_id.load(Ordering::Relaxed))
            .finish()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_events::IoOperation;
    use crate::time::DeterministicTimeProvider;
    use crate::types::{ChunkId, TierId};
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn test_scheduler() -> (IoScheduler, mpsc::Receiver<crate::events::Event>) {
        let (tx, rx) = mpsc::channel(256);
        let emitter = EventEmitter::new(tx);
        let clock = DeterministicTimeProvider::new(1_700_000_000, Duration::from_millis(1));
        let scheduler = IoScheduler::new(Arc::new(clock), emitter);
        (scheduler, rx)
    }

    #[test]
    fn test_issue_returns_incrementing_ids() {
        let (scheduler, _rx) = test_scheduler();
        let id1 = scheduler.issue(IoOperation::Read, ChunkId::from_data(b"a"), TierId::Ram);
        let id2 = scheduler.issue(IoOperation::Write, ChunkId::from_data(b"b"), TierId::Disk);
        assert!(id1 < id2);
        assert_eq!(scheduler.pending_count(), 2);
    }

    #[test]
    fn test_complete_moves_to_completed() {
        let (mut scheduler, _rx) = test_scheduler();
        let id = scheduler.issue(IoOperation::Read, ChunkId::from_data(b"a"), TierId::Ram);
        assert_eq!(scheduler.pending_count(), 1);
        assert_eq!(scheduler.completed_count(), 0);

        scheduler.complete(id, Ok(()));
        assert_eq!(scheduler.pending_count(), 0);
        assert_eq!(scheduler.completed_count(), 1);
    }

    #[test]
    fn test_complete_with_error() {
        let (mut scheduler, _rx) = test_scheduler();
        let id = scheduler.issue(IoOperation::Write, ChunkId::from_data(b"a"), TierId::Disk);
        scheduler.complete(id, Err("device failure".to_string()));

        let completed = scheduler.completed();
        assert_eq!(completed.len(), 1);
        match &completed[0].completion {
            IoCompletion::Failed { error } => assert_eq!(error, "device failure"),
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[test]
    #[should_panic(expected = "unknown request ID")]
    fn test_complete_unknown_id_panics() {
        let (mut scheduler, _rx) = test_scheduler();
        scheduler.complete(999, Ok(()));
    }

    #[test]
    fn test_flush_completes_all_pending() {
        let (mut scheduler, _rx) = test_scheduler();
        for i in 0..5 {
            scheduler.issue(
                IoOperation::Read,
                ChunkId::from_data(format!("chunk-{}", i).as_bytes()),
                TierId::Ram,
            );
        }
        assert_eq!(scheduler.pending_count(), 5);

        scheduler.flush();
        assert_eq!(scheduler.pending_count(), 0);
        assert_eq!(scheduler.completed_count(), 5);
    }

    #[test]
    fn test_flush_empty_is_noop() {
        let (mut scheduler, _rx) = test_scheduler();
        scheduler.flush();
        assert_eq!(scheduler.pending_count(), 0);
        assert_eq!(scheduler.completed_count(), 0);
    }
}
