//! Stability metrics for cooldown and flapping detection.
//!
//! Tracks cooldown state, stability violations, hysteresis events,
//! and flapping detection for the stability subsystem.

use prometheus::{IntCounter, IntGauge, Registry};

/// Metrics for the stability subsystem.
#[derive(Debug, Clone)]
pub struct StabilityMetrics {
    /// Number of regions currently in cooldown.
    pub cooldown_active_regions: IntGauge,
    /// Total number of stability violations detected.
    pub stability_violations_total: IntCounter,
    /// Total number of hysteresis-based action preventions.
    pub hysteresis_preventions_total: IntCounter,
    /// Total number of flapping detections.
    pub flapping_detected_total: IntCounter,
}

impl StabilityMetrics {
    /// Create a new StabilityMetrics instance and register with the given registry.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let cooldown_active_regions = IntGauge::new(
            "ghostpages_stability_cooldown_active_regions",
            "Number of regions currently in cooldown",
        )?;
        let stability_violations_total = IntCounter::new(
            "ghostpages_stability_violations_total",
            "Total number of stability violations detected",
        )?;
        let hysteresis_preventions_total = IntCounter::new(
            "ghostpages_stability_hysteresis_preventions_total",
            "Total number of hysteresis-based action preventions",
        )?;
        let flapping_detected_total = IntCounter::new(
            "ghostpages_stability_flapping_detected_total",
            "Total number of flapping detections",
        )?;

        registry.register(Box::new(cooldown_active_regions.clone()))?;
        registry.register(Box::new(stability_violations_total.clone()))?;
        registry.register(Box::new(hysteresis_preventions_total.clone()))?;
        registry.register(Box::new(flapping_detected_total.clone()))?;

        Ok(Self {
            cooldown_active_regions,
            stability_violations_total,
            hysteresis_preventions_total,
            flapping_detected_total,
        })
    }

    /// Set the number of regions currently in cooldown.
    pub fn set_cooldown_regions(&self, count: usize) {
        self.cooldown_active_regions.set(count as i64);
    }

    /// Increment the cooldown region count by one.
    pub fn increment_cooldown_regions(&self) {
        self.cooldown_active_regions.inc();
    }

    /// Decrement the cooldown region count by one.
    pub fn decrement_cooldown_regions(&self) {
        self.cooldown_active_regions.dec();
    }

    /// Record a stability violation.
    pub fn record_violation(&self) {
        self.stability_violations_total.inc();
    }

    /// Record a hysteresis-based action prevention.
    pub fn record_hysteresis_prevention(&self) {
        self.hysteresis_preventions_total.inc();
    }

    /// Record a flapping detection.
    pub fn record_flapping(&self) {
        self.flapping_detected_total.inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stability_metrics_new() {
        let registry = Registry::new();
        let metrics = StabilityMetrics::new(&registry).unwrap();
        assert_eq!(metrics.cooldown_active_regions.get(), 0);
        assert_eq!(metrics.stability_violations_total.get(), 0);
        assert_eq!(metrics.hysteresis_preventions_total.get(), 0);
        assert_eq!(metrics.flapping_detected_total.get(), 0);
    }

    #[test]
    fn test_set_cooldown_regions() {
        let registry = Registry::new();
        let metrics = StabilityMetrics::new(&registry).unwrap();

        metrics.set_cooldown_regions(5);
        assert_eq!(metrics.cooldown_active_regions.get(), 5);

        metrics.set_cooldown_regions(10);
        assert_eq!(metrics.cooldown_active_regions.get(), 10);
    }

    #[test]
    fn test_increment_decrement_cooldown_regions() {
        let registry = Registry::new();
        let metrics = StabilityMetrics::new(&registry).unwrap();

        metrics.increment_cooldown_regions();
        metrics.increment_cooldown_regions();
        assert_eq!(metrics.cooldown_active_regions.get(), 2);

        metrics.decrement_cooldown_regions();
        assert_eq!(metrics.cooldown_active_regions.get(), 1);
    }

    #[test]
    fn test_record_violation() {
        let registry = Registry::new();
        let metrics = StabilityMetrics::new(&registry).unwrap();

        metrics.record_violation();
        metrics.record_violation();
        metrics.record_violation();
        assert_eq!(metrics.stability_violations_total.get(), 3);
    }

    #[test]
    fn test_record_hysteresis_prevention() {
        let registry = Registry::new();
        let metrics = StabilityMetrics::new(&registry).unwrap();

        metrics.record_hysteresis_prevention();
        assert_eq!(metrics.hysteresis_preventions_total.get(), 1);
    }

    #[test]
    fn test_record_flapping() {
        let registry = Registry::new();
        let metrics = StabilityMetrics::new(&registry).unwrap();

        metrics.record_flapping();
        metrics.record_flapping();
        assert_eq!(metrics.flapping_detected_total.get(), 2);
    }
}