//! Allocator metrics for memory allocation tracking.

use prometheus::{IntCounter, IntGauge, Histogram, HistogramOpts, Registry};

/// Metrics for the memory allocator.
#[derive(Debug, Clone)]
pub struct AllocatorMetrics {
    /// Total number of allocation requests.
    pub allocations_total: IntCounter,
    /// Total number of deallocation requests.
    pub deallocations_total: IntCounter,
    /// Total number of allocation failures.
    pub allocation_failures_total: IntCounter,
    /// Currently allocated bytes.
    pub allocated_bytes: IntGauge,
    /// Peak allocated bytes.
    pub peak_allocated_bytes: IntGauge,
    /// Total bytes allocated over time.
    pub bytes_allocated_total: IntCounter,
    /// Total bytes deallocated over time.
    pub bytes_deallocated_total: IntCounter,
    /// Histogram of allocation sizes.
    pub allocation_size_bytes: Histogram,
    /// Number of active allocations.
    pub active_allocations: IntGauge,
}

impl AllocatorMetrics {
    /// Create a new AllocatorMetrics instance and register with the given registry.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let allocations_total = IntCounter::new(
            "ghostpages_allocator_allocations_total",
            "Total number of allocation requests",
        )?;
        let deallocations_total = IntCounter::new(
            "ghostpages_allocator_deallocations_total",
            "Total number of deallocation requests",
        )?;
        let allocation_failures_total = IntCounter::new(
            "ghostpages_allocator_allocation_failures_total",
            "Total number of allocation failures",
        )?;
        let allocated_bytes = IntGauge::new(
            "ghostpages_allocator_allocated_bytes",
            "Currently allocated bytes",
        )?;
        let peak_allocated_bytes = IntGauge::new(
            "ghostpages_allocator_peak_allocated_bytes",
            "Peak allocated bytes",
        )?;
        let bytes_allocated_total = IntCounter::new(
            "ghostpages_allocator_bytes_allocated_total",
            "Total bytes allocated over time",
        )?;
        let bytes_deallocated_total = IntCounter::new(
            "ghostpages_allocator_bytes_deallocated_total",
            "Total bytes deallocated over time",
        )?;
        let allocation_size_bytes = Histogram::with_opts(
            HistogramOpts::new(
                "ghostpages_allocator_allocation_size_bytes",
                "Allocation size in bytes",
            )
            .buckets(vec![
                64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0,
            ]),
        )?;
        let active_allocations = IntGauge::new(
            "ghostpages_allocator_active_allocations",
            "Number of active allocations",
        )?;

        registry.register(Box::new(allocations_total.clone()))?;
        registry.register(Box::new(deallocations_total.clone()))?;
        registry.register(Box::new(allocation_failures_total.clone()))?;
        registry.register(Box::new(allocated_bytes.clone()))?;
        registry.register(Box::new(peak_allocated_bytes.clone()))?;
        registry.register(Box::new(bytes_allocated_total.clone()))?;
        registry.register(Box::new(bytes_deallocated_total.clone()))?;
        registry.register(Box::new(allocation_size_bytes.clone()))?;
        registry.register(Box::new(active_allocations.clone()))?;

        Ok(Self {
            allocations_total,
            deallocations_total,
            allocation_failures_total,
            allocated_bytes,
            peak_allocated_bytes,
            bytes_allocated_total,
            bytes_deallocated_total,
            allocation_size_bytes,
            active_allocations,
        })
    }
}
