//! Metrics for the simulation backend.
//!
//! This module defines the metrics tracked by `SimBackend`. It is shared
//! between the pure simulation backend and the disk backend's simulation layer.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Metrics tracked by the simulation backend.
///
/// All counters use atomic operations for thread safety.
#[derive(Debug)]
pub struct SimMetrics {
    /// Total number of allocate operations.
    alloc_count: AtomicU64,
    /// Total number of deallocate operations.
    dealloc_count: AtomicU64,
    /// Total number of write operations.
    write_count: AtomicU64,
    /// Total number of read operations.
    read_count: AtomicU64,
    /// Total number of failed operations (any type).
    failure_count: AtomicU64,
    /// Total number of bytes allocated (cumulative).
    bytes_allocated: AtomicU64,
    /// Total number of bytes deallocated (cumulative).
    bytes_deallocated: AtomicU64,
    /// Total number of bytes written (cumulative).
    bytes_written: AtomicU64,
    /// Total number of bytes read (cumulative).
    bytes_read: AtomicU64,
    /// Total simulated latency in microseconds (cumulative).
    total_latency_us: AtomicU64,
    /// Time the backend was created.
    created_at: Instant,
}

impl SimMetrics {
    /// Create a new metrics instance.
    pub fn new() -> Self {
        Self {
            alloc_count: AtomicU64::new(0),
            dealloc_count: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
            read_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
            bytes_deallocated: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    /// Record a successful allocation.
    pub fn record_alloc(&self, size: usize) {
        self.alloc_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_allocated
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    /// Record a deallocation.
    pub fn record_dealloc(&self, size: usize) {
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_deallocated
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    /// Record a write operation.
    pub fn record_write(&self, size: usize) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(size as u64, Ordering::Relaxed);
    }

    /// Record a read operation.
    pub fn record_read(&self, size: usize) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(size as u64, Ordering::Relaxed);
    }

    /// Record a failed operation.
    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record simulated latency in microseconds.
    pub fn record_latency(&self, latency_us: u64) {
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    /// Get the total allocation count.
    pub fn alloc_count(&self) -> u64 {
        self.alloc_count.load(Ordering::Relaxed)
    }

    /// Get the total deallocation count.
    pub fn dealloc_count(&self) -> u64 {
        self.dealloc_count.load(Ordering::Relaxed)
    }

    /// Get the total write count.
    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Relaxed)
    }

    /// Get the total read count.
    pub fn read_count(&self) -> u64 {
        self.read_count.load(Ordering::Relaxed)
    }

    /// Get the total failure count.
    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::Relaxed)
    }

    /// Get cumulative bytes allocated.
    pub fn bytes_allocated(&self) -> u64 {
        self.bytes_allocated.load(Ordering::Relaxed)
    }

    /// Get cumulative bytes deallocated.
    pub fn bytes_deallocated(&self) -> u64 {
        self.bytes_deallocated.load(Ordering::Relaxed)
    }

    /// Get cumulative bytes written.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get cumulative bytes read.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Get total simulated latency in microseconds.
    pub fn total_latency_us(&self) -> u64 {
        self.total_latency_us.load(Ordering::Relaxed)
    }

    /// Get the time since the backend was created.
    pub fn uptime(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Get the average latency per operation in microseconds.
    pub fn avg_latency_us(&self) -> f64 {
        let total_ops =
            self.alloc_count() + self.dealloc_count() + self.write_count() + self.read_count();
        if total_ops == 0 {
            0.0
        } else {
            self.total_latency_us() as f64 / total_ops as f64
        }
    }

    /// Reset all metrics to zero.
    pub fn reset(&self) {
        self.alloc_count.store(0, Ordering::Relaxed);
        self.dealloc_count.store(0, Ordering::Relaxed);
        self.write_count.store(0, Ordering::Relaxed);
        self.read_count.store(0, Ordering::Relaxed);
        self.failure_count.store(0, Ordering::Relaxed);
        self.bytes_allocated.store(0, Ordering::Relaxed);
        self.bytes_deallocated.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.total_latency_us.store(0, Ordering::Relaxed);
    }
}

impl Default for SimMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_default_zero() {
        let m = SimMetrics::new();
        assert_eq!(m.alloc_count(), 0);
        assert_eq!(m.write_count(), 0);
        assert_eq!(m.read_count(), 0);
        assert_eq!(m.failure_count(), 0);
    }

    #[test]
    fn test_metrics_record_operations() {
        let m = SimMetrics::new();
        m.record_alloc(1024);
        m.record_write(1024);
        m.record_read(1024);
        m.record_dealloc(1024);

        assert_eq!(m.alloc_count(), 1);
        assert_eq!(m.write_count(), 1);
        assert_eq!(m.read_count(), 1);
        assert_eq!(m.dealloc_count(), 1);
        assert_eq!(m.bytes_allocated(), 1024);
        assert_eq!(m.bytes_written(), 1024);
        assert_eq!(m.bytes_read(), 1024);
        assert_eq!(m.bytes_deallocated(), 1024);
    }

    #[test]
    fn test_metrics_failure() {
        let m = SimMetrics::new();
        m.record_failure();
        m.record_failure();
        assert_eq!(m.failure_count(), 2);
    }

    #[test]
    fn test_metrics_latency() {
        let m = SimMetrics::new();
        m.record_alloc(64);
        m.record_latency(100);
        m.record_write(64);
        m.record_latency(200);
        assert_eq!(m.total_latency_us(), 300);
        assert!((m.avg_latency_us() - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_metrics_reset() {
        let m = SimMetrics::new();
        m.record_alloc(1024);
        m.record_write(1024);
        m.record_failure();
        m.reset();
        assert_eq!(m.alloc_count(), 0);
        assert_eq!(m.write_count(), 0);
        assert_eq!(m.failure_count(), 0);
    }

    #[test]
    fn test_metrics_uptime() {
        let m = SimMetrics::new();
        assert!(m.uptime().as_nanos() > 0);
    }
}
