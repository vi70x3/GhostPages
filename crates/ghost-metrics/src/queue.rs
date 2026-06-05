//! Queue metrics for the transfer queue.

use prometheus::{IntCounter, IntGauge, Registry};

/// Metrics for the transfer queue.
#[derive(Debug, Clone)]
pub struct QueueMetrics {
    /// Current queue depth.
    pub depth: IntGauge,
    /// Total number of jobs submitted.
    pub submitted_total: IntCounter,
    /// Total number of jobs dequeued.
    pub dequeued_total: IntCounter,
    /// Total number of priority insertions.
    pub priority_insertions_total: IntCounter,
    /// Total number of submissions rejected (queue full).
    pub rejected_total: IntCounter,
    /// Queue capacity.
    pub capacity: IntGauge,
}

impl QueueMetrics {
    /// Create a new QueueMetrics instance and register with the given registry.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let depth = IntGauge::new(
            "ghostpages_queue_depth",
            "Current transfer queue depth",
        )?;
        let submitted_total = IntCounter::new(
            "ghostpages_queue_submitted_total",
            "Total number of jobs submitted to the queue",
        )?;
        let dequeued_total = IntCounter::new(
            "ghostpages_queue_dequeued_total",
            "Total number of jobs dequeued from the queue",
        )?;
        let priority_insertions_total = IntCounter::new(
            "ghostpages_queue_priority_insertions_total",
            "Total number of priority insertions",
        )?;
        let rejected_total = IntCounter::new(
            "ghostpages_queue_rejected_total",
            "Total number of submissions rejected (queue full)",
        )?;
        let capacity = IntGauge::new(
            "ghostpages_queue_capacity",
            "Queue capacity",
        )?;

        registry.register(Box::new(depth.clone()))?;
        registry.register(Box::new(submitted_total.clone()))?;
        registry.register(Box::new(dequeued_total.clone()))?;
        registry.register(Box::new(priority_insertions_total.clone()))?;
        registry.register(Box::new(rejected_total.clone()))?;
        registry.register(Box::new(capacity.clone()))?;

        Ok(Self {
            depth,
            submitted_total,
            dequeued_total,
            priority_insertions_total,
            rejected_total,
            capacity,
        })
    }
}
