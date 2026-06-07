//! Unified metrics registry for GhostPages.

use prometheus::{Registry, TextEncoder};
use std::sync::Arc;

use crate::allocator::AllocatorMetrics;
use crate::health::BackendHealthMetrics;
use crate::hotness::HotnessMetrics;
use crate::migration::MigrationMetrics;
use crate::policy::PolicyMetrics;
use crate::queue::QueueMetrics;
use crate::replay::ReplayMetrics;
use crate::stability::StabilityMetrics;

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
    /// Hotness metrics (DAMON integration).
    pub hotness: HotnessMetrics,
    /// Policy metrics (recommendation tracking).
    pub policy: PolicyMetrics,
    /// Stability metrics (cooldown and flapping detection).
    pub stability: StabilityMetrics,
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
        let hotness = HotnessMetrics::new(&registry)?;
        let policy = PolicyMetrics::new(&registry)?;
        let stability = StabilityMetrics::new(&registry)?;

        Ok(Self {
            registry,
            queue,
            migration,
            replay,
            allocator,
            health,
            hotness,
            policy,
            stability,
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

    #[test]
    fn test_metrics_registry_hotness_metrics() {
        let registry = MetricsRegistry::new().unwrap();
        // Exercise the IntCounterVec so it appears in gathered output
        registry.hotness.record_temperature_transition("cold", "hot");
        let output = registry.gather().unwrap();
        assert!(output.contains("ghostpages_hotness_hot_regions"));
        assert!(output.contains("ghostpages_hotness_warm_regions"));
        assert!(output.contains("ghostpages_hotness_cold_regions"));
        assert!(output.contains("ghostpages_hotness_frozen_regions"));
        assert!(output.contains("ghostpages_hotness_updates_total"));
        assert!(output.contains("ghostpages_hotness_confidence"));
        assert!(output.contains("ghostpages_hotness_temperature_transitions_total"));
    }

    #[test]
    fn test_metrics_registry_policy_metrics() {
        let registry = MetricsRegistry::new().unwrap();
        // Exercise the IntCounterVec so it appears in gathered output
        registry
            .policy
            .record_recommendation(&crate::policy::Recommendation::promote(0.5));
        let output = registry.gather().unwrap();
        assert!(output.contains("ghostpages_policy_recommendations_total"));
        assert!(output.contains("ghostpages_policy_promotions_total"));
        assert!(output.contains("ghostpages_policy_demotions_total"));
        assert!(output.contains("ghostpages_policy_no_action_total"));
        assert!(output.contains("ghostpages_policy_recommendation_confidence"));
        assert!(output.contains(
            "ghostpages_policy_suppressed_recommendations_total"
        ));
        assert!(output.contains("ghostpages_policy_cooldown_hits_total"));
        assert!(output.contains("ghostpages_policy_evaluation_duration_seconds"));
    }

    #[test]
    fn test_metrics_registry_stability_metrics() {
        let registry = MetricsRegistry::new().unwrap();
        let output = registry.gather().unwrap();
        assert!(output.contains("ghostpages_stability_cooldown_active_regions"));
        assert!(output.contains("ghostpages_stability_violations_total"));
        assert!(output.contains("ghostpages_stability_hysteresis_preventions_total"));
        assert!(output.contains("ghostpages_stability_flapping_detected_total"));
    }
}