//! Hotness metrics for DAMON hotness integration.
//!
//! Tracks region temperature distribution, update counts, confidence scores,
//! and temperature transitions for observability of the hotness subsystem.

use prometheus::{IntCounter, IntCounterVec, IntGauge, Registry};

use ghost_core::hotness_summary::HotnessSummary;

/// Metrics for the hotness tracking subsystem.
#[derive(Debug, Clone)]
pub struct HotnessMetrics {
    /// Number of hot regions.
    pub hot_regions: IntGauge,
    /// Number of warm regions.
    pub warm_regions: IntGauge,
    /// Number of cold regions.
    pub cold_regions: IntGauge,
    /// Number of frozen regions.
    pub frozen_regions: IntGauge,
    /// Total number of hotness updates processed.
    pub hotness_updates_total: IntCounter,
    /// Current hotness confidence score (scaled by 1000).
    pub hotness_confidence: IntGauge,
    /// Total temperature transitions labeled [from, to].
    pub temperature_transitions_total: IntCounterVec,
}

impl HotnessMetrics {
    /// Create a new HotnessMetrics instance and register with the given registry.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let hot_regions = IntGauge::new(
            "ghostpages_hotness_hot_regions",
            "Number of hot regions",
        )?;
        let warm_regions = IntGauge::new(
            "ghostpages_hotness_warm_regions",
            "Number of warm regions",
        )?;
        let cold_regions = IntGauge::new(
            "ghostpages_hotness_cold_regions",
            "Number of cold regions",
        )?;
        let frozen_regions = IntGauge::new(
            "ghostpages_hotness_frozen_regions",
            "Number of frozen regions",
        )?;
        let hotness_updates_total = IntCounter::new(
            "ghostpages_hotness_updates_total",
            "Total number of hotness updates processed",
        )?;
        let hotness_confidence = IntGauge::new(
            "ghostpages_hotness_confidence",
            "Current hotness confidence score (scaled by 1000)",
        )?;
        let temperature_transitions_total = IntCounterVec::new(
            prometheus::Opts::new(
                "ghostpages_hotness_temperature_transitions_total",
                "Total temperature transitions",
            ),
            &["from", "to"],
        )?;

        registry.register(Box::new(hot_regions.clone()))?;
        registry.register(Box::new(warm_regions.clone()))?;
        registry.register(Box::new(cold_regions.clone()))?;
        registry.register(Box::new(frozen_regions.clone()))?;
        registry.register(Box::new(hotness_updates_total.clone()))?;
        registry.register(Box::new(hotness_confidence.clone()))?;
        registry.register(Box::new(temperature_transitions_total.clone()))?;

        Ok(Self {
            hot_regions,
            warm_regions,
            cold_regions,
            frozen_regions,
            hotness_updates_total,
            hotness_confidence,
            temperature_transitions_total,
        })
    }

    /// Update gauge values from a hotness summary.
    pub fn update_from_summary(&self, summary: &HotnessSummary) {
        self.hot_regions.set(summary.hot_count as i64);
        self.warm_regions.set(summary.warm_count as i64);
        self.cold_regions.set(summary.cold_count as i64);
        self.frozen_regions.set(summary.frozen_count as i64);
        self.hotness_updates_total.inc();
    }

    /// Update the confidence gauge from a confidence score (0.0-1.0).
    ///
    /// The score is scaled by 1000 for integer gauge precision.
    pub fn update_confidence(&self, confidence: f32) {
        let scaled = (confidence.clamp(0.0, 1.0) * 1000.0).round() as i64;
        self.hotness_confidence.set(scaled);
    }

    /// Record a temperature transition from one state to another.
    pub fn record_temperature_transition(&self, from: &str, to: &str) {
        self.temperature_transitions_total
            .with_label_values(&[from, to])
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::hotness_provider::{AddressRange, HotnessSample, HotnessSnapshot, Temperature};

    fn create_snapshot() -> HotnessSnapshot {
        HotnessSnapshot {
            samples: vec![
                HotnessSample {
                    address_range: AddressRange::new(0, 4096),
                    temperature: Temperature::Hot,
                    access_count: 150,
                },
                HotnessSample {
                    address_range: AddressRange::new(4096, 8192),
                    temperature: Temperature::Warm,
                    access_count: 50,
                },
                HotnessSample {
                    address_range: AddressRange::new(8192, 12288),
                    temperature: Temperature::Cold,
                    access_count: 5,
                },
                HotnessSample {
                    address_range: AddressRange::new(12288, 16384),
                    temperature: Temperature::Frozen,
                    access_count: 0,
                },
            ],
            timestamp: 1000,
        }
    }

    #[test]
    fn test_hotness_metrics_new() {
        let registry = Registry::new();
        let metrics = HotnessMetrics::new(&registry).unwrap();
        assert_eq!(metrics.hot_regions.get(), 0);
        assert_eq!(metrics.warm_regions.get(), 0);
        assert_eq!(metrics.cold_regions.get(), 0);
        assert_eq!(metrics.frozen_regions.get(), 0);
    }

    #[test]
    fn test_update_from_summary() {
        let registry = Registry::new();
        let metrics = HotnessMetrics::new(&registry).unwrap();
        let snapshot = create_snapshot();
        let summary = HotnessSummary::from_snapshot(&snapshot);

        metrics.update_from_summary(&summary);

        assert_eq!(metrics.hot_regions.get(), 1);
        assert_eq!(metrics.warm_regions.get(), 1);
        assert_eq!(metrics.cold_regions.get(), 1);
        assert_eq!(metrics.frozen_regions.get(), 1);
        assert_eq!(metrics.hotness_updates_total.get(), 1);
    }

    #[test]
    fn test_update_confidence() {
        let registry = Registry::new();
        let metrics = HotnessMetrics::new(&registry).unwrap();

        metrics.update_confidence(0.75);
        assert_eq!(metrics.hotness_confidence.get(), 750);

        metrics.update_confidence(1.0);
        assert_eq!(metrics.hotness_confidence.get(), 1000);

        metrics.update_confidence(0.0);
        assert_eq!(metrics.hotness_confidence.get(), 0);
    }

    #[test]
    fn test_record_temperature_transition() {
        let registry = Registry::new();
        let metrics = HotnessMetrics::new(&registry).unwrap();

        metrics.record_temperature_transition("cold", "hot");
        metrics.record_temperature_transition("cold", "hot");
        metrics.record_temperature_transition("hot", "warm");

        assert_eq!(
            metrics
                .temperature_transitions_total
                .with_label_values(&["cold", "hot"])
                .get(),
            2
        );
        assert_eq!(
            metrics
                .temperature_transitions_total
                .with_label_values(&["hot", "warm"])
                .get(),
            1
        );
    }
}
