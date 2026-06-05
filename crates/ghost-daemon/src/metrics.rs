//! Transfer metrics for the GhostPages daemon.
//!
//! Atomic counters for tracking transfer pipeline performance.

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for the transfer pipeline.
///
/// All counters are atomic and can be read/written from multiple threads.
#[derive(Debug)]
pub struct TransferMetrics {
    /// Total number of jobs submitted.
    pub jobs_submitted: AtomicU64,

    /// Total number of jobs completed successfully.
    pub jobs_completed: AtomicU64,

    /// Total number of jobs that failed (after all retries).
    pub jobs_failed: AtomicU64,

    /// Total number of jobs that were cancelled.
    pub jobs_cancelled: AtomicU64,

    /// Total bytes transferred.
    pub bytes_transferred: AtomicU64,

    /// Total transfer time in milliseconds.
    pub total_transfer_time_ms: AtomicU64,

    /// Current queue depth.
    pub queue_depth: AtomicU64,

    /// Number of currently active workers.
    pub active_workers: AtomicU64,
}

impl TransferMetrics {
    /// Create a new metrics instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            jobs_submitted: AtomicU64::new(0),
            jobs_completed: AtomicU64::new(0),
            jobs_failed: AtomicU64::new(0),
            jobs_cancelled: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            total_transfer_time_ms: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            active_workers: AtomicU64::new(0),
        }
    }

    /// Record a job submission.
    pub fn record_submission(&self) {
        self.jobs_submitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful job completion.
    pub fn record_completion(&self) {
        self.jobs_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a job failure.
    pub fn record_failure(&self) {
        self.jobs_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a job cancellation.
    pub fn record_cancellation(&self) {
        self.jobs_cancelled.fetch_add(1, Ordering::Relaxed);
    }

    /// Record bytes transferred.
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record transfer time.
    pub fn record_transfer_time(&self, duration_ms: u64) {
        self.total_transfer_time_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
    }

    /// Update the current queue depth.
    pub fn set_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    /// Update the active worker count.
    pub fn set_active_workers(&self, count: u64) {
        self.active_workers.store(count, Ordering::Relaxed);
    }

    /// Get the average transfer time in milliseconds.
    /// Returns 0.0 if no transfers have completed.
    pub fn avg_transfer_time_ms(&self) -> f64 {
        let completed = self.jobs_completed.load(Ordering::Relaxed);
        if completed == 0 {
            0.0
        } else {
            let total = self.total_transfer_time_ms.load(Ordering::Relaxed);
            total as f64 / completed as f64
        }
    }

    /// Get the success rate as a ratio (0.0 to 1.0).
    /// Returns 1.0 if no jobs have been submitted.
    pub fn success_rate(&self) -> f64 {
        let submitted = self.jobs_submitted.load(Ordering::Relaxed);
        if submitted == 0 {
            1.0
        } else {
            let completed = self.jobs_completed.load(Ordering::Relaxed);
            completed as f64 / submitted as f64
        }
    }
}

impl Default for TransferMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let m = TransferMetrics::new();
        assert_eq!(m.jobs_submitted.load(Ordering::Relaxed), 0);
        assert_eq!(m.jobs_completed.load(Ordering::Relaxed), 0);
        assert_eq!(m.jobs_failed.load(Ordering::Relaxed), 0);
        assert_eq!(m.jobs_cancelled.load(Ordering::Relaxed), 0);
        assert_eq!(m.bytes_transferred.load(Ordering::Relaxed), 0);
        assert_eq!(m.total_transfer_time_ms.load(Ordering::Relaxed), 0);
        assert_eq!(m.queue_depth.load(Ordering::Relaxed), 0);
        assert_eq!(m.active_workers.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_submission() {
        let m = TransferMetrics::new();
        m.record_submission();
        m.record_submission();
        assert_eq!(m.jobs_submitted.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_record_completion() {
        let m = TransferMetrics::new();
        m.record_completion();
        assert_eq!(m.jobs_completed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_record_failure() {
        let m = TransferMetrics::new();
        m.record_failure();
        assert_eq!(m.jobs_failed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_record_cancellation() {
        let m = TransferMetrics::new();
        m.record_cancellation();
        assert_eq!(m.jobs_cancelled.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_record_bytes() {
        let m = TransferMetrics::new();
        m.record_bytes(1024);
        m.record_bytes(2048);
        assert_eq!(m.bytes_transferred.load(Ordering::Relaxed), 3072);
    }

    #[test]
    fn test_record_transfer_time() {
        let m = TransferMetrics::new();
        m.record_transfer_time(100);
        m.record_transfer_time(200);
        assert_eq!(m.total_transfer_time_ms.load(Ordering::Relaxed), 300);
    }

    #[test]
    fn test_set_queue_depth() {
        let m = TransferMetrics::new();
        m.set_queue_depth(42);
        assert_eq!(m.queue_depth.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn test_set_active_workers() {
        let m = TransferMetrics::new();
        m.set_active_workers(4);
        assert_eq!(m.active_workers.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn test_avg_transfer_time_ms() {
        let m = TransferMetrics::new();
        assert_eq!(m.avg_transfer_time_ms(), 0.0);

        m.record_transfer_time(100);
        m.record_completion();
        m.record_transfer_time(200);
        m.record_completion();

        assert!((m.avg_transfer_time_ms() - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_success_rate() {
        let m = TransferMetrics::new();
        assert!((m.success_rate() - 1.0).abs() < f64::EPSILON);

        m.record_submission();
        m.record_submission();
        m.record_submission();
        m.record_completion();
        m.record_completion();
        m.record_failure();

        assert!((m.success_rate() - 2.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn test_metrics_default() {
        let m = TransferMetrics::default();
        assert_eq!(m.jobs_submitted.load(Ordering::Relaxed), 0);
    }
}
