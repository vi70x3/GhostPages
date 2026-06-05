//! Metrics collection module.

use ghost_core::error::GhostResult;
use std::sync::atomic::{AtomicU64, Ordering};

/// Core operation metrics for GhostPages.
///
/// Tracks operation counts, byte throughput, and error rates.
#[derive(Debug)]
pub struct OperationMetrics {
    /// Total number of store operations.
    pub store_total: AtomicU64,
    /// Total bytes stored.
    pub store_bytes_total: AtomicU64,
    /// Total number of store errors.
    pub store_errors_total: AtomicU64,

    /// Total number of retrieve operations.
    pub retrieve_total: AtomicU64,
    /// Total bytes retrieved.
    pub retrieve_bytes_total: AtomicU64,
    /// Total number of retrieve errors.
    pub retrieve_errors_total: AtomicU64,

    /// Total number of delete operations.
    pub delete_total: AtomicU64,
    /// Total number of delete errors.
    pub delete_errors_total: AtomicU64,
}

impl Default for OperationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationMetrics {
    /// Create a new OperationMetrics instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            store_total: AtomicU64::new(0),
            store_bytes_total: AtomicU64::new(0),
            store_errors_total: AtomicU64::new(0),
            retrieve_total: AtomicU64::new(0),
            retrieve_bytes_total: AtomicU64::new(0),
            retrieve_errors_total: AtomicU64::new(0),
            delete_total: AtomicU64::new(0),
            delete_errors_total: AtomicU64::new(0),
        }
    }

    /// Record a successful store operation.
    pub fn record_store(&self, bytes: usize) {
        self.store_total.fetch_add(1, Ordering::Relaxed);
        self.store_bytes_total
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Record a store operation error.
    pub fn record_store_error(&self) {
        self.store_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful retrieve operation.
    pub fn record_retrieve(&self, bytes: usize) {
        self.retrieve_total.fetch_add(1, Ordering::Relaxed);
        self.retrieve_bytes_total
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Record a retrieve operation error.
    pub fn record_retrieve_error(&self) {
        self.retrieve_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful delete operation.
    pub fn record_delete(&self) {
        self.delete_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delete operation error.
    pub fn record_delete_error(&self) {
        self.delete_errors_total.fetch_add(1, Ordering::Relaxed);
    }
}

/// Tier-specific metrics.
#[derive(Debug)]
pub struct TierMetrics {
    /// Total tier capacity in bytes.
    pub tier_capacity_bytes: AtomicU64,
    /// Currently used bytes in tier.
    pub tier_used_bytes: AtomicU64,
    /// Number of migrations into this tier.
    pub tier_migration_in_total: AtomicU64,
    /// Number of migrations out of this tier.
    pub tier_migration_out_total: AtomicU64,
}

impl Default for TierMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TierMetrics {
    /// Create a new TierMetrics instance.
    pub fn new() -> Self {
        Self {
            tier_capacity_bytes: AtomicU64::new(0),
            tier_used_bytes: AtomicU64::new(0),
            tier_migration_in_total: AtomicU64::new(0),
            tier_migration_out_total: AtomicU64::new(0),
        }
    }

    /// Update the tier capacity.
    pub fn set_capacity(&self, capacity: usize) {
        self.tier_capacity_bytes
            .store(capacity as u64, Ordering::Relaxed);
    }

    /// Update the used bytes.
    pub fn set_used(&self, used: usize) {
        self.tier_used_bytes.store(used as u64, Ordering::Relaxed);
    }

    /// Record a migration into this tier.
    pub fn record_migration_in(&self) {
        self.tier_migration_in_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a migration out of this tier.
    pub fn record_migration_out(&self) {
        self.tier_migration_out_total
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Central metrics collector for all GhostPages metrics.
#[derive(Debug)]
pub struct MetricsCollector {
    /// Core operation metrics.
    pub operations: OperationMetrics,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCollector {
    /// Create a new MetricsCollector instance.
    pub fn new() -> Self {
        Self {
            operations: OperationMetrics::new(),
        }
    }

    /// Initialize metrics with default values.
    pub fn init() -> GhostResult<Self> {
        tracing::info!("Initializing metrics collector");
        Ok(Self::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_metrics_store() {
        let metrics = OperationMetrics::new();
        metrics.record_store(1024);
        metrics.record_store(2048);

        assert_eq!(metrics.store_total.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.store_bytes_total.load(Ordering::Relaxed), 3072);
    }

    #[test]
    fn test_operation_metrics_retrieve() {
        let metrics = OperationMetrics::new();
        metrics.record_retrieve(512);

        assert_eq!(metrics.retrieve_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.retrieve_bytes_total.load(Ordering::Relaxed), 512);
    }

    #[test]
    fn test_operation_metrics_errors() {
        let metrics = OperationMetrics::new();
        metrics.record_store_error();
        metrics.record_retrieve_error();
        metrics.record_delete_error();

        assert_eq!(metrics.store_errors_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.retrieve_errors_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.delete_errors_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_tier_metrics() {
        let metrics = TierMetrics::new();
        metrics.set_capacity(1024 * 1024);
        metrics.set_used(512 * 1024);
        metrics.record_migration_in();
        metrics.record_migration_out();

        assert_eq!(
            metrics.tier_capacity_bytes.load(Ordering::Relaxed),
            1024 * 1024
        );
        assert_eq!(metrics.tier_used_bytes.load(Ordering::Relaxed), 512 * 1024);
        assert_eq!(metrics.tier_migration_in_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.tier_migration_out_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_metrics_collector_init() {
        let collector = MetricsCollector::init().unwrap();
        assert_eq!(collector.operations.store_total.load(Ordering::Relaxed), 0);
    }
}
