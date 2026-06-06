//! I/O metrics tracking for the GhostPages daemon.
//!
//! Provides atomic, lock-free I/O metrics including rolling latency (EMA-smoothed),
//! queue depth, flush duration, read/write/error counts, and write amplification.
//! All operations are deterministic when using a deterministic clock.

use std::sync::atomic::{AtomicU64, Ordering};

/// I/O metrics for tracking disk performance and congestion.
///
/// All counters are atomic and can be read/written from multiple threads.
/// Latency is tracked as an EMA-smoothed value in microseconds.
#[derive(Debug)]
pub struct IoMetrics {
    /// EMA-smoothed latency in microseconds.
    rolling_latency: AtomicU64,

    /// Current pending I/O count.
    queue_depth: AtomicU64,

    /// Last fsync duration in microseconds.
    flush_duration: AtomicU64,

    /// Total reads completed.
    read_count: AtomicU64,

    /// Total writes completed.
    write_count: AtomicU64,

    /// Total I/O errors.
    error_count: AtomicU64,

    /// Write amplification factor * 1000 (e.g., 1500 = 1.5x amplification).
    amplification_factor: AtomicU64,

    /// Total bytes read (for amplification calculation).
    total_bytes_read: AtomicU64,

    /// Total bytes written (for amplification calculation).
    total_bytes_written: AtomicU64,

    /// Total bytes written to physical media (includes amplification).
    total_bytes_physical: AtomicU64,

    /// EMA smoothing factor * 1000000 (for integer arithmetic).
    smoothing_factor: AtomicU64,
}

impl IoMetrics {
    /// Create a new IoMetrics instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            rolling_latency: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            flush_duration: AtomicU64::new(0),
            read_count: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            amplification_factor: AtomicU64::new(1000), // 1.0x default
            total_bytes_read: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            total_bytes_physical: AtomicU64::new(0),
            smoothing_factor: AtomicU64::new(300_000), // 0.3 * 1_000_000
        }
    }

    /// Set the EMA smoothing factor (0.0 to 1.0).
    ///
    /// Lower values produce smoother (more heavily averaged) latency readings.
    pub fn set_smoothing_factor(&self, factor: f32) {
        let encoded = (factor.clamp(0.0, 1.0) * 1_000_000.0) as u64;
        self.smoothing_factor.store(encoded, Ordering::Relaxed);
    }

    /// Record a completed read operation.
    ///
    /// Updates the rolling latency via EMA smoothing and increments the read counter.
    pub fn record_read(&self, duration_us: u64) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        self.update_rolling_latency(duration_us);
    }

    /// Record a completed write operation.
    ///
    /// Updates the rolling latency via EMA smoothing, increments the write counter,
    /// and tracks bytes for amplification calculation.
    pub fn record_write(&self, duration_us: u64, data_size: usize) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_written
            .fetch_add(data_size as u64, Ordering::Relaxed);
        self.update_rolling_latency(duration_us);
    }

    /// Record a flush (fsync) operation duration.
    pub fn record_flush(&self, duration_us: u64) {
        self.flush_duration.store(duration_us, Ordering::Relaxed);
    }

    /// Record an I/O error.
    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the queue depth (called when an I/O is submitted).
    pub fn increment_queue_depth(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the queue depth (called when an I/O completes).
    pub fn decrement_queue_depth(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record physical bytes written (for amplification tracking).
    ///
    /// Call this with the actual bytes written to physical media,
    /// which may differ from logical bytes due to compression/overhead.
    pub fn record_physical_write(&self, physical_bytes: usize) {
        self.total_bytes_physical
            .fetch_add(physical_bytes as u64, Ordering::Relaxed);
        self.update_amplification_factor();
    }

    /// Record bytes read (for amplification tracking).
    pub fn record_bytes_read(&self, bytes: usize) {
        self.total_bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Get the EMA-smoothed rolling latency in microseconds.
    pub fn get_rolling_latency(&self) -> u64 {
        self.rolling_latency.load(Ordering::Relaxed)
    }

    /// Get the current queue depth (number of in-flight I/O operations).
    pub fn get_queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// Get the last flush (fsync) duration in microseconds.
    pub fn get_flush_duration(&self) -> u64 {
        self.flush_duration.load(Ordering::Relaxed)
    }

    /// Get the total number of read operations.
    pub fn get_read_count(&self) -> u64 {
        self.read_count.load(Ordering::Relaxed)
    }

    /// Get the total number of write operations.
    pub fn get_write_count(&self) -> u64 {
        self.write_count.load(Ordering::Relaxed)
    }

    /// Get the total number of I/O errors.
    pub fn get_error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    /// Get the read amplification factor (logical / physical).
    ///
    /// Returns 1.0 if no reads have been recorded.
    pub fn get_read_amplification(&self) -> f64 {
        let physical = self.total_bytes_physical.load(Ordering::Relaxed);
        let logical = self.total_bytes_read.load(Ordering::Relaxed);
        if physical == 0 {
            1.0
        } else {
            logical as f64 / physical as f64
        }
    }

    /// Get the write amplification factor (physical / logical).
    ///
    /// Returns 1.0 if no writes have been recorded.
    pub fn get_write_amplification(&self) -> f64 {
        let logical = self.total_bytes_written.load(Ordering::Relaxed);
        let physical = self.total_bytes_physical.load(Ordering::Relaxed);
        if logical == 0 {
            1.0
        } else {
            physical as f64 / logical as f64
        }
    }

    /// Get the stored amplification factor (scaled by 1000).
    pub fn get_amplification_factor(&self) -> u64 {
        self.amplification_factor.load(Ordering::Relaxed)
    }

    /// Calculate the error rate (errors / total operations).
    ///
    /// Returns 0.0 if no operations have been recorded.
    pub fn get_error_rate(&self) -> f64 {
        let reads = self.read_count.load(Ordering::Relaxed);
        let writes = self.write_count.load(Ordering::Relaxed);
        let total = reads + writes;
        let errors = self.error_count.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            errors as f64 / total as f64
        }
    }

    /// Calculate disk I/O pressure (0.0 to 1.0) from current metrics.
    ///
    /// Formula: `io_pressure = (queue_depth / max_queue) * 0.4 + (latency / max_latency) * 0.4 + error_rate * 0.2`
    pub fn calculate_io_pressure(&self, max_queue: u64, max_latency_us: u64) -> f32 {
        let queue_depth = self.queue_depth.load(Ordering::Relaxed) as f32;
        let queue_pressure = if max_queue > 0 {
            (queue_depth / max_queue as f32).min(1.0)
        } else {
            0.0
        };

        let latency = self.rolling_latency.load(Ordering::Relaxed) as f32;
        let latency_pressure = if max_latency_us > 0 {
            (latency / max_latency_us as f32).min(1.0)
        } else {
            0.0
        };

        let error_rate = self.get_error_rate();

        (queue_pressure * 0.4 + latency_pressure * 0.4 + error_rate as f32 * 0.2).min(1.0)
    }

    /// Update the rolling latency using EMA smoothing.
    fn update_rolling_latency(&self, new_latency: u64) {
        let alpha = self.smoothing_factor.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        loop {
            let current = self.rolling_latency.load(Ordering::Relaxed);
            let smoothed = if current == 0 {
                // First sample: initialize directly
                new_latency
            } else {
                let current_f = current as f64;
                let new_f = new_latency as f64;
                let result = alpha * new_f + (1.0 - alpha) * current_f;
                result as u64
            };
            if self
                .rolling_latency
                .compare_exchange(current, smoothed, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Update the amplification factor from current byte counters.
    fn update_amplification_factor(&self) {
        let logical = self.total_bytes_written.load(Ordering::Relaxed);
        let physical = self.total_bytes_physical.load(Ordering::Relaxed);
        let factor = if logical == 0 {
            1000 // 1.0x
        } else {
            ((physical as f64 / logical as f64) * 1000.0).clamp(0.0, 10_000.0) as u64
        };
        self.amplification_factor.store(factor, Ordering::Relaxed);
    }

    /// Reset all metrics to zero.
    pub fn reset(&self) {
        self.rolling_latency.store(0, Ordering::Relaxed);
        self.queue_depth.store(0, Ordering::Relaxed);
        self.flush_duration.store(0, Ordering::Relaxed);
        self.read_count.store(0, Ordering::Relaxed);
        self.write_count.store(0, Ordering::Relaxed);
        self.error_count.store(0, Ordering::Relaxed);
        self.amplification_factor.store(1000, Ordering::Relaxed);
        self.total_bytes_read.store(0, Ordering::Relaxed);
        self.total_bytes_written.store(0, Ordering::Relaxed);
        self.total_bytes_physical.store(0, Ordering::Relaxed);
    }
}

impl Default for IoMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_metrics_new() {
        let m = IoMetrics::new();
        assert_eq!(m.get_rolling_latency(), 0);
        assert_eq!(m.get_queue_depth(), 0);
        assert_eq!(m.get_flush_duration(), 0);
        assert_eq!(m.get_read_count(), 0);
        assert_eq!(m.get_write_count(), 0);
        assert_eq!(m.get_error_count(), 0);
    }

    #[test]
    fn test_record_read_updates_count_and_latency() {
        let m = IoMetrics::new();
        m.record_read(1000);
        assert_eq!(m.get_read_count(), 1);
        // First sample initializes EMA directly
        assert_eq!(m.get_rolling_latency(), 1000);
    }

    #[test]
    fn test_record_write_updates_count_and_bytes() {
        let m = IoMetrics::new();
        m.record_write(2000, 4096);
        assert_eq!(m.get_write_count(), 1);
        assert_eq!(m.get_rolling_latency(), 2000);
    }

    #[test]
    fn test_ema_smoothing() {
        let m = IoMetrics::new();
        m.set_smoothing_factor(0.5);

        // First sample initializes
        m.record_read(1000);
        assert_eq!(m.get_rolling_latency(), 1000);

        // Second sample: 0.5 * 2000 + 0.5 * 1000 = 1500
        m.record_read(2000);
        assert_eq!(m.get_rolling_latency(), 1500);

        // Third sample: 0.5 * 3000 + 0.5 * 1500 = 2250
        m.record_read(3000);
        assert_eq!(m.get_rolling_latency(), 2250);
    }

    #[test]
    fn test_queue_depth_tracking() {
        let m = IoMetrics::new();
        assert_eq!(m.get_queue_depth(), 0);
        m.increment_queue_depth();
        m.increment_queue_depth();
        assert_eq!(m.get_queue_depth(), 2);
        m.decrement_queue_depth();
        assert_eq!(m.get_queue_depth(), 1);
    }

    #[test]
    fn test_flush_duration() {
        let m = IoMetrics::new();
        m.record_flush(5000);
        assert_eq!(m.get_flush_duration(), 5000);
        m.record_flush(3000);
        assert_eq!(m.get_flush_duration(), 3000);
    }

    #[test]
    fn test_error_count() {
        let m = IoMetrics::new();
        m.record_error();
        m.record_error();
        assert_eq!(m.get_error_count(), 2);
    }

    #[test]
    fn test_error_rate() {
        let m = IoMetrics::new();
        assert!((m.get_error_rate() - 0.0).abs() < f64::EPSILON);

        m.record_read(100);
        m.record_read(100);
        m.record_write(100, 1024);
        m.record_error();

        // 1 error out of 3 operations
        assert!((m.get_error_rate() - 1.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn test_write_amplification() {
        let m = IoMetrics::new();
        // Default: 1.0x
        assert!((m.get_write_amplification() - 1.0).abs() < f64::EPSILON);

        m.record_write(100, 4096);
        m.record_physical_write(6144); // 1.5x amplification

        let amp = m.get_write_amplification();
        assert!((amp - 1.5).abs() < 0.01);
    }

    #[test]
    fn test_read_amplification() {
        let m = IoMetrics::new();
        assert!((m.get_read_amplification() - 1.0).abs() < f64::EPSILON);

        m.record_bytes_read(8192);
        m.record_physical_write(4096); // physical < logical

        let amp = m.get_read_amplification();
        assert!((amp - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_calculate_io_pressure_zero() {
        let m = IoMetrics::new();
        let pressure = m.calculate_io_pressure(256, 10_000);
        assert!((pressure - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    fn test_calculate_io_pressure_with_queue() {
        let m = IoMetrics::new();
        // Queue depth 128 out of 256 max = 0.5
        for _ in 0..128 {
            m.increment_queue_depth();
        }
        let pressure = m.calculate_io_pressure(256, 10_000);
        // 0.4 * 0.5 + 0.4 * 0 + 0.2 * 0 = 0.2
        assert!((pressure - 0.2).abs() < 0.01);
    }

    #[test]
    fn test_calculate_io_pressure_with_latency() {
        let m = IoMetrics::new();
        m.record_read(5_000_000); // 5 seconds in microseconds
        let pressure = m.calculate_io_pressure(256, 10_000_000);
        // 0.4 * 0 + 0.4 * 0.5 + 0.2 * 0 = 0.2
        assert!((pressure - 0.2).abs() < 0.01);
    }

    #[test]
    fn test_calculate_io_pressure_with_errors() {
        let m = IoMetrics::new();
        m.record_read(100);
        m.record_read(100);
        m.record_read(100);
        m.record_read(100);
        m.record_read(100);
        m.record_error();
        m.record_error();
        // error_rate = 2/5 = 0.4
        let pressure = m.calculate_io_pressure(256, 10_000);
        // 0 + 0 + 0.2 * 0.4 = 0.08
        assert!((pressure - 0.08).abs() < 0.01);
    }

    #[test]
    fn test_reset() {
        let m = IoMetrics::new();
        m.record_read(1000);
        m.record_write(2000, 4096);
        m.record_error();
        m.increment_queue_depth();
        m.record_flush(5000);

        m.reset();

        assert_eq!(m.get_rolling_latency(), 0);
        assert_eq!(m.get_queue_depth(), 0);
        assert_eq!(m.get_flush_duration(), 0);
        assert_eq!(m.get_read_count(), 0);
        assert_eq!(m.get_write_count(), 0);
        assert_eq!(m.get_error_count(), 0);
    }

    #[test]
    fn test_smoothing_factor() {
        let m = IoMetrics::new();
        m.set_smoothing_factor(0.5);
        // Verify it doesn't panic and subsequent operations work
        m.record_read(1000);
        m.record_read(2000);
        assert_eq!(m.get_rolling_latency(), 1500);
    }
}
