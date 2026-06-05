//! Replay metrics for trace replay operations.

use prometheus::{IntCounter, IntGauge, Histogram, HistogramOpts, Registry};

/// Metrics for trace replay operations.
#[derive(Debug, Clone)]
pub struct ReplayMetrics {
    /// Total number of replay operations started.
    pub replay_ops_total: IntCounter,
    /// Total number of events replayed.
    pub events_replayed_total: IntCounter,
    /// Total number of validation errors during replay.
    pub validation_errors_total: IntCounter,
    /// Total number of replay failures.
    pub failures_total: IntCounter,
    /// Currently active replay operations.
    pub active_replays: IntGauge,
    /// Histogram of replay duration.
    pub replay_duration_seconds: Histogram,
    /// Total number of determinism checks performed.
    pub determinism_checks_total: IntCounter,
    /// Total number of determinism check failures.
    pub determinism_failures_total: IntCounter,
}

impl ReplayMetrics {
    /// Create a new ReplayMetrics instance and register with the given registry.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let replay_ops_total = IntCounter::new(
            "ghostpages_replay_ops_total",
            "Total number of replay operations started",
        )?;
        let events_replayed_total = IntCounter::new(
            "ghostpages_replay_events_replayed_total",
            "Total number of events replayed",
        )?;
        let validation_errors_total = IntCounter::new(
            "ghostpages_replay_validation_errors_total",
            "Total number of validation errors during replay",
        )?;
        let failures_total = IntCounter::new(
            "ghostpages_replay_failures_total",
            "Total number of replay failures",
        )?;
        let active_replays = IntGauge::new(
            "ghostpages_replay_active",
            "Currently active replay operations",
        )?;
        let replay_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "ghostpages_replay_duration_seconds",
                "Replay operation duration in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]),
        )?;
        let determinism_checks_total = IntCounter::new(
            "ghostpages_replay_determinism_checks_total",
            "Total number of determinism checks performed",
        )?;
        let determinism_failures_total = IntCounter::new(
            "ghostpages_replay_determinism_failures_total",
            "Total number of determinism check failures",
        )?;

        registry.register(Box::new(replay_ops_total.clone()))?;
        registry.register(Box::new(events_replayed_total.clone()))?;
        registry.register(Box::new(validation_errors_total.clone()))?;
        registry.register(Box::new(failures_total.clone()))?;
        registry.register(Box::new(active_replays.clone()))?;
        registry.register(Box::new(replay_duration_seconds.clone()))?;
        registry.register(Box::new(determinism_checks_total.clone()))?;
        registry.register(Box::new(determinism_failures_total.clone()))?;

        Ok(Self {
            replay_ops_total,
            events_replayed_total,
            validation_errors_total,
            failures_total,
            active_replays,
            replay_duration_seconds,
            determinism_checks_total,
            determinism_failures_total,
        })
    }
}
