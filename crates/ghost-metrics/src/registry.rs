//! Unified metrics registry for GhostPages.

use prometheus::{Registry, TextEncoder};
use std::sync::Arc;

use crate::allocator::AllocatorMetrics;
use crate::health::BackendHealthMetrics;
use crate::migration::MigrationMetrics;
use crate::queue::QueueMetrics;
use crate::replay::ReplayMetrics;

/// Unified metrics registry that holds all metric families.
///
/// This provides a single entry point for registering and gathering
/// all GhostPages metrics.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// Prometheus registry for all metrics.
    pub registry: Arc<Registry>,
    /// Queue metrics.
    pub queue: QueueMetrics,
    /// Migration metrics.
    pub migration: MigrationMetrics,
    /// Replay metrics.
    pub replay: ReplayMetrics,
    /// Allocator metrics.
    pub allocator: AllocatorMetrics,
    /// Backend health metrics.
    pub health: BackendHealthMetrics,
}

impl MetricsRegistry {
    /// Create a new MetricsRegistry with all metric families registered.
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Arc::new(Registry::new());

        let queue = QueueMetrics::new(&registry)?;
        let migration = MigrationMetrics::new(&registry)?;
        let replay = ReplayMetrics::new(&registry)?;
        let allocator = AllocatorMetrics::new(&registry)?;
        let health = BackendHealthMetrics::new(&registry)?;

        Ok(Self {
            registry,
            queue,
            migration,
            replay,
            allocator,
            health,
        })
    }

    /// Gather all metrics in Prometheus text format.
    pub fn gather(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode_to_string(&metric_families)
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new().expect("Failed to create metrics registry")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registry_creation() {
        let registry = MetricsRegistry::new().unwrap();
        let output = registry.gather().unwrap();
        assert!(output.contains("ghostpages_queue_depth"));
        assert!(output.contains("ghostpages_migration_evaluation_cycles_total"));
        assert!(output.contains("ghostpages_replay_ops_total"));
        assert!(output.contains("ghostpages_allocator_allocations_total"));
        assert!(output.contains("ghostpages_backend_health_status"));
    }
}
