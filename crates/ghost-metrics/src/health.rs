//! Backend health metrics.

use prometheus::{IntCounter, IntGauge, Registry};

/// Metrics for backend health tracking.
#[derive(Debug, Clone)]
pub struct BackendHealthMetrics {
    /// Current health status per tier (0=healthy, 1=degraded, 2=unavailable, 3=recovering).
    pub health_status: IntGauge,
    /// Total number of health check successes.
    pub health_check_successes_total: IntCounter,
    /// Total number of health check failures.
    pub health_check_failures_total: IntCounter,
    /// Total number of backend degradation events.
    pub degradation_events_total: IntCounter,
    /// Total number of backend recovery events.
    pub recovery_events_total: IntCounter,
    /// Total number of recovery attempts.
    pub recovery_attempts_total: IntCounter,
    /// Total number of recovery successes.
    pub recovery_successes_total: IntCounter,
    /// Consecutive failures per tier.
    pub consecutive_failures: IntGauge,
}

impl BackendHealthMetrics {
    /// Create a new BackendHealthMetrics instance and register with the given registry.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let health_status = IntGauge::new(
            "ghostpages_backend_health_status",
            "Current health status per tier (0=healthy, 1=degraded, 2=unavailable, 3=recovering)",
        )?;
        let health_check_successes_total = IntCounter::new(
            "ghostpages_backend_health_check_successes_total",
            "Total number of health check successes",
        )?;
        let health_check_failures_total = IntCounter::new(
            "ghostpages_backend_health_check_failures_total",
            "Total number of health check failures",
        )?;
        let degradation_events_total = IntCounter::new(
            "ghostpages_backend_degradation_events_total",
            "Total number of backend degradation events",
        )?;
        let recovery_events_total = IntCounter::new(
            "ghostpages_backend_recovery_events_total",
            "Total number of backend recovery events",
        )?;
        let recovery_attempts_total = IntCounter::new(
            "ghostpages_backend_recovery_attempts_total",
            "Total number of recovery attempts",
        )?;
        let recovery_successes_total = IntCounter::new(
            "ghostpages_backend_recovery_successes_total",
            "Total number of recovery successes",
        )?;
        let consecutive_failures = IntGauge::new(
            "ghostpages_backend_consecutive_failures",
            "Consecutive failures per tier",
        )?;

        registry.register(Box::new(health_status.clone()))?;
        registry.register(Box::new(health_check_successes_total.clone()))?;
        registry.register(Box::new(health_check_failures_total.clone()))?;
        registry.register(Box::new(degradation_events_total.clone()))?;
        registry.register(Box::new(recovery_events_total.clone()))?;
        registry.register(Box::new(recovery_attempts_total.clone()))?;
        registry.register(Box::new(recovery_successes_total.clone()))?;
        registry.register(Box::new(consecutive_failures.clone()))?;

        Ok(Self {
            health_status,
            health_check_successes_total,
            health_check_failures_total,
            degradation_events_total,
            recovery_events_total,
            recovery_attempts_total,
            recovery_successes_total,
            consecutive_failures,
        })
    }
}
