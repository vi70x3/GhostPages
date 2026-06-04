//! Transfer queue for the GhostPages daemon.
//!
//! A bounded, async, multi-producer single-consumer queue for transfer jobs.
//! Provides backpressure by rejecting submissions when the queue is full.

use std::collections::VecDeque;
use std::sync::Arc;

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::transfer::{TransferJob, TransferPriority};
use ghost_core::types::ChunkId;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::trace_log::TraceLog;

/// A bounded, async transfer queue.
///
/// Supports priority insertion for high-priority jobs and provides
/// backpressure when the queue is full.
#[derive(Debug)]
pub struct TransferQueue {
    inner: Arc<Mutex<TransferQueueInner>>,
    capacity: usize,
    notify: Arc<Notify>,
    trace_log: Arc<TraceLog>,
}

#[derive(Debug)]
struct TransferQueueInner {
    jobs: VecDeque<TransferJob>,
    current_depth: usize,
    shutdown: bool,
}

impl TransferQueue {
    /// Create a new transfer queue with the given capacity.
    pub fn new(capacity: usize, trace_log: Arc<TraceLog>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TransferQueueInner {
                jobs: VecDeque::with_capacity(capacity.min(1024)),
                current_depth: 0,
                shutdown: false,
            })),
            capacity,
            notify: Arc::new(Notify::new()),
            trace_log,
        }
    }

    /// Submit a job to the queue.
    ///
    /// Fails with `GhostError::Internal` if the queue is full (backpressure).
    /// Fails with `GhostError::Cancelled` if the queue is shut down.
    pub fn submit(&self, job: TransferJob) -> GhostResult<()> {
        self.submit_priority(job)
    }

    /// Submit a job with priority insertion.
    ///
    /// High and Critical priority jobs are inserted at the front of the queue
    /// (after other high-priority jobs). Normal and Low priority jobs are
    /// inserted at the back.
    ///
    /// Fails with `GhostError::Internal` if the queue is full.
    /// Fails with `GhostError::Cancelled` if the queue is shut down.
    pub fn submit_priority(&self, job: TransferJob) -> GhostResult<()> {
        let mut inner = self.inner.lock();
        if inner.shutdown {
            return Err(GhostError::Cancelled);
        }
        if inner.current_depth >= self.capacity {
            // Log backpressure event
            self.trace_log.record(TraceEvent::TransferQueued {
                chunk_id: job.chunk_id,
                from: job.from_tier,
                to: job.to_tier,
                priority: job.priority,
                timestamp: current_timestamp(),
            });
            return Err(GhostError::Internal(format!(
                "transfer queue is full (capacity: {})",
                self.capacity
            )));
        }
        inner.current_depth += 1;

        // Capture fields before moving job
        let chunk_id = job.chunk_id;
        let from_tier = job.from_tier;
        let to_tier = job.to_tier;
        let priority = job.priority;

        // Insert high-priority jobs at the front, low-priority at the back
        if job.priority.is_higher_than(TransferPriority::Normal) {
            // Find the right position: after all critical, before normal/low
            let pos = inner
                .jobs
                .iter()
                .position(|j| !j.priority.is_higher_than(TransferPriority::Normal))
                .unwrap_or(inner.jobs.len());
            inner.jobs.insert(pos, job);
        } else {
            inner.jobs.push_back(job);
        }

        // Log transfer queued event
        self.trace_log.record(TraceEvent::TransferQueued {
            chunk_id,
            from: from_tier,
            to: to_tier,
            priority,
            timestamp: current_timestamp(),
        });

        // Log warning if depth exceeds 75% capacity
        let threshold = self.capacity * 3 / 4;
        if inner.current_depth > threshold && inner.current_depth > 1 {
            tracing::info!(
                "Queue depth {} exceeds 75% of capacity {}",
                inner.current_depth,
                self.capacity
            );
        }

        drop(inner);
        self.notify.notify_one();
        Ok(())
    }

    /// Dequeue the next job. Returns `None` if the queue is empty.
    ///
    /// This is a non-blocking call. Use `dequeue_wait` for async waiting.
    pub fn try_dequeue(&self) -> Option<TransferJob> {
        let mut inner = self.inner.lock();
        if let Some(job) = inner.jobs.pop_front() {
            inner.current_depth -= 1;
            Some(job)
        } else {
            None
        }
    }

    /// Dequeue the next job, waiting if the queue is empty.
    ///
    /// Returns `None` if the queue is shut down and empty.
    pub async fn dequeue_wait(&self) -> Option<TransferJob> {
        loop {
            {
                let mut inner = self.inner.lock();
                if let Some(job) = inner.jobs.pop_front() {
                    inner.current_depth -= 1;
                    return Some(job);
                }
                if inner.shutdown {
                    return None;
                }
            }
            self.notify.notified().await;
        }
    }

    /// Get the current queue depth.
    pub fn depth(&self) -> usize {
        self.inner.lock().current_depth
    }

    /// Check if the queue is full.
    pub fn is_full(&self) -> bool {
        let inner = self.inner.lock();
        inner.current_depth >= self.capacity
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().current_depth == 0
    }

    /// Get the queue capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Signal shutdown — no new submissions will be accepted,
    /// and any waiting dequeue will return `None` once the queue is drained.
    pub fn shutdown(&self) {
        let mut inner = self.inner.lock();
        inner.shutdown = true;
        drop(inner);
        // Wake up any waiters so they can see the shutdown flag
        self.notify.notify_waiters();
    }

    /// Check if the queue is shut down.
    pub fn is_shutdown(&self) -> bool {
        self.inner.lock().shutdown
    }
}

impl Default for TransferQueue {
    fn default() -> Self {
        let trace_log = Arc::new(TraceLog::new(1000));
        Self::new(1024, trace_log)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::TierId;

    fn test_trace_log() -> Arc<TraceLog> {
        Arc::new(TraceLog::new(1000))
    }

    fn make_job(id: &[u8], priority: TransferPriority) -> TransferJob {
        TransferJob::new(
            ChunkId::from_data(id),
            TierId::Ram,
            TierId::Simulation,
            1024,
            priority,
        )
    }

    #[test]
    fn test_queue_new() {
        let q = TransferQueue::new(10, test_trace_log());
        assert_eq!(q.capacity(), 10);
        assert_eq!(q.depth(), 0);
        assert!(q.is_empty());
        assert!(!q.is_full());
    }

    #[test]
    fn test_queue_default() {
        let q = TransferQueue::default();
        assert_eq!(q.capacity(), 1024);
    }

    #[test]
    fn test_submit_and_dequeue() {
        let q = TransferQueue::new(10, test_trace_log());
        let job = make_job(b"test", TransferPriority::Normal);
        q.submit(job).unwrap();
        assert_eq!(q.depth(), 1);
        assert!(!q.is_empty());

        let dequeued = q.try_dequeue().unwrap();
        assert_eq!(dequeued.chunk_id, ChunkId::from_data(b"test"));
        assert_eq!(q.depth(), 0);
        assert!(q.is_empty());
    }

    #[test]
    fn test_backpressure_full_queue() {
        let q = TransferQueue::new(2, test_trace_log());
        q.submit(make_job(b"a", TransferPriority::Normal)).unwrap();
        q.submit(make_job(b"b", TransferPriority::Normal)).unwrap();
        assert!(q.is_full());

        // Third submit should fail
        let result = q.submit(make_job(b"c", TransferPriority::Normal));
        assert!(result.is_err());
        match result.unwrap_err() {
            GhostError::Internal(msg) => assert!(msg.contains("full")),
            other => panic!("expected Internal error, got {:?}", other),
        }
    }

    #[test]
    fn test_priority_insertion() {
        let q = TransferQueue::new(10, test_trace_log());

        // Submit in order: Normal, Low, Critical, High
        q.submit(make_job(b"normal1", TransferPriority::Normal))
            .unwrap();
        q.submit(make_job(b"low1", TransferPriority::Low))
            .unwrap();
        q.submit(make_job(b"critical1", TransferPriority::Critical))
            .unwrap();
        q.submit(make_job(b"high1", TransferPriority::High))
            .unwrap();

        // Expected order: Critical, High, Normal, Low
        let j1 = q.try_dequeue().unwrap();
        assert_eq!(j1.chunk_id, ChunkId::from_data(b"critical1"));

        let j2 = q.try_dequeue().unwrap();
        assert_eq!(j2.chunk_id, ChunkId::from_data(b"high1"));

        let j3 = q.try_dequeue().unwrap();
        assert_eq!(j3.chunk_id, ChunkId::from_data(b"normal1"));

        let j4 = q.try_dequeue().unwrap();
        assert_eq!(j4.chunk_id, ChunkId::from_data(b"low1"));
    }

    #[test]
    fn test_submit_priority_method() {
        let q = TransferQueue::new(10, test_trace_log());

        // Use submit_priority for all
        q.submit_priority(make_job(b"normal", TransferPriority::Normal))
            .unwrap();
        q.submit_priority(make_job(b"low", TransferPriority::Low))
            .unwrap();
        q.submit_priority(make_job(b"critical", TransferPriority::Critical))
            .unwrap();
        q.submit_priority(make_job(b"high", TransferPriority::High))
            .unwrap();

        // Critical should come first
        let j1 = q.try_dequeue().unwrap();
        assert_eq!(j1.chunk_id, ChunkId::from_data(b"critical"));

        // High should come next
        let j2 = q.try_dequeue().unwrap();
        assert_eq!(j2.chunk_id, ChunkId::from_data(b"high"));

        // Normal before low
        let j3 = q.try_dequeue().unwrap();
        assert_eq!(j3.chunk_id, ChunkId::from_data(b"normal"));

        let j4 = q.try_dequeue().unwrap();
        assert_eq!(j4.chunk_id, ChunkId::from_data(b"low"));
    }

    #[test]
    fn test_shutdown_rejects_submissions() {
        let q = TransferQueue::new(10, test_trace_log());
        q.shutdown();
        assert!(q.is_shutdown());

        let result = q.submit(make_job(b"test", TransferPriority::Normal));
        assert!(result.is_err());
        match result.unwrap_err() {
            GhostError::Cancelled => {}
            other => panic!("expected Cancelled, got {:?}", other),
        }
    }

    #[test]
    fn test_shutdown_priority_rejects() {
        let q = TransferQueue::new(10, test_trace_log());
        q.shutdown();

        let result = q.submit_priority(make_job(b"test", TransferPriority::Critical));
        assert!(result.is_err());
    }

    #[test]
    fn test_try_dequeue_empty() {
        let q = TransferQueue::new(10, test_trace_log());
        assert!(q.try_dequeue().is_none());
    }

    #[tokio::test]
    async fn test_dequeue_wait_returns_job() {
        let q = Arc::new(TransferQueue::new(10, test_trace_log()));
        let q2 = q.clone();

        // Spawn a task that waits for a job
        let handle = tokio::spawn(async move {
            let job = q2.dequeue_wait().await;
            assert!(job.is_some());
            job.unwrap()
        });

        // Give the spawned task time to start waiting
        tokio::task::yield_now().await;

        // Submit a job — this should wake the waiter
        q.submit(make_job(b"async_test", TransferPriority::Normal))
            .unwrap();

        let job = handle.await.unwrap();
        assert_eq!(job.chunk_id, ChunkId::from_data(b"async_test"));
    }

    #[tokio::test]
    async fn test_dequeue_wait_shutdown() {
        let q = Arc::new(TransferQueue::new(10, test_trace_log()));
        let q2 = q.clone();

        let handle = tokio::spawn(async move {
            let job = q2.dequeue_wait().await;
            assert!(job.is_none());
        });

        tokio::task::yield_now().await;
        q.shutdown();

        handle.await.unwrap();
    }

    #[test]
    fn test_multiple_priority_levels() {
        let q = TransferQueue::new(10, test_trace_log());

        // Submit multiple of each priority
        for i in 0..3 {
            q.submit(make_job(format!("normal{}", i).as_bytes(), TransferPriority::Normal))
                .unwrap();
        }
        for i in 0..2 {
            q.submit(make_job(format!("critical{}", i).as_bytes(), TransferPriority::Critical))
                .unwrap();
        }
        q.submit(make_job(b"low1", TransferPriority::Low))
            .unwrap();

        // All criticals first
        let j1 = q.try_dequeue().unwrap();
        assert_eq!(j1.chunk_id, ChunkId::from_data(b"critical0"));
        let j2 = q.try_dequeue().unwrap();
        assert_eq!(j2.chunk_id, ChunkId::from_data(b"critical1"));

        // Then normals
        let j3 = q.try_dequeue().unwrap();
        assert_eq!(j3.chunk_id, ChunkId::from_data(b"normal0"));
        let j4 = q.try_dequeue().unwrap();
        assert_eq!(j4.chunk_id, ChunkId::from_data(b"normal1"));
        let j5 = q.try_dequeue().unwrap();
        assert_eq!(j5.chunk_id, ChunkId::from_data(b"normal2"));

        // Then low
        let j6 = q.try_dequeue().unwrap();
        assert_eq!(j6.chunk_id, ChunkId::from_data(b"low1"));

        assert!(q.is_empty());
    }

    #[test]
    fn test_queue_emits_transfer_queued_trace() {
        let trace_log = test_trace_log();
        let q = TransferQueue::new(10, trace_log.clone());
        let job = make_job(b"trace_test", TransferPriority::Normal);
        q.submit(job).unwrap();

        let events = trace_log.get_events();
        assert!(events.iter().any(|e| matches!(e, TraceEvent::TransferQueued { .. })));
    }
}
