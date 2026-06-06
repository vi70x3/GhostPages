//! Prometheus exporter module.

use ghost_core::error::GhostResult;
use ghost_core::GhostError;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry};
use std::sync::Arc;

/// Convert a prometheus error into a GhostError.
fn prom_err(err: prometheus::Error) -> GhostError {
    GhostError::Internal(format!("prometheus error: {err}"))
}

/// Prometheus metrics for GhostPages.
///
/// This module provides Prometheus-compatible metrics that can be scraped
/// by a Prometheus server for monitoring and alerting.
#[derive(Debug, Clone)]
pub struct PrometheusMetrics {
    /// Registry containing all metrics.
    pub registry: Arc<Registry>,

    // Operation counters
    /// Total store operations.
    pub store_total: IntCounter,
    /// Total bytes stored.
    pub store_bytes_total: IntCounter,
    /// Total store errors.
    pub store_errors_total: IntCounter,

    // Retrieve counters
    /// Total retrieve operations.
    pub retrieve_total: IntCounter,
    /// Total bytes retrieved.
    pub retrieve_bytes_total: IntCounter,
    /// Total retrieve errors.
    pub retrieve_errors_total: IntCounter,

    // Delete counters
    /// Total delete operations.
    pub delete_total: IntCounter,
    /// Total delete errors.
    pub delete_errors_total: IntCounter,

    // Tier gauges
    /// Tier capacity in bytes.
    pub tier_capacity_bytes: IntGauge,
    /// Tier used bytes.
    pub tier_used_bytes: IntGauge,

    // Pipeline gauges
    /// Current ingress queue depth.
    pub ingress_queue_depth: IntGauge,
    /// Current compression queue depth.
    pub compression_queue_depth: IntGauge,
    /// Current transfer queue depth.
    pub transfer_queue_depth: IntGauge,

    // Latency histograms
    /// Store operation latency.
    pub store_duration_seconds: Histogram,
    /// Retrieve operation latency.
    pub retrieve_duration_seconds: Histogram,

    // PSI (Pressure Stall Information) gauges
    /// Memory pressure 10-second average.
    pub memory_pressure_avg10: IntGauge,
    /// Memory pressure 60-second average.
    pub memory_pressure_avg60: IntGauge,
    /// Memory pressure 300-second average.
    pub memory_pressure_avg300: IntGauge,
    /// I/O pressure 10-second average.
    pub io_pressure_avg10: IntGauge,
}

impl PrometheusMetrics {
    /// Create a new PrometheusMetrics instance with default metrics.
    pub fn new() -> GhostResult<Self> {
        let registry = Arc::new(Registry::new());

        let store_total =
            IntCounter::new("ghostpages_store_total", "Total number of store operations")
                .map_err(prom_err)?;

        let store_bytes_total =
            IntCounter::new("ghostpages_store_bytes_total", "Total bytes stored")
                .map_err(prom_err)?;

        let store_errors_total = IntCounter::new(
            "ghostpages_store_errors_total",
            "Total number of store errors",
        )
        .map_err(prom_err)?;

        let retrieve_total = IntCounter::new(
            "ghostpages_retrieve_total",
            "Total number of retrieve operations",
        )
        .map_err(prom_err)?;

        let retrieve_bytes_total =
            IntCounter::new("ghostpages_retrieve_bytes_total", "Total bytes retrieved")
                .map_err(prom_err)?;

        let retrieve_errors_total = IntCounter::new(
            "ghostpages_retrieve_errors_total",
            "Total number of retrieve errors",
        )
        .map_err(prom_err)?;

        let delete_total = IntCounter::new(
            "ghostpages_delete_total",
            "Total number of delete operations",
        )
        .map_err(prom_err)?;

        let delete_errors_total = IntCounter::new(
            "ghostpages_delete_errors_total",
            "Total number of delete errors",
        )
        .map_err(prom_err)?;

        let tier_capacity_bytes =
            IntGauge::new("ghostpages_tier_capacity_bytes", "Tier capacity in bytes")
                .map_err(prom_err)?;

        let tier_used_bytes =
            IntGauge::new("ghostpages_tier_used_bytes", "Tier used bytes").map_err(prom_err)?;

        let ingress_queue_depth = IntGauge::new(
            "ghostpages_ingress_queue_depth",
            "Current ingress queue depth",
        )
        .map_err(prom_err)?;

        let compression_queue_depth = IntGauge::new(
            "ghostpages_compression_queue_depth",
            "Current compression queue depth",
        )
        .map_err(prom_err)?;

        let transfer_queue_depth = IntGauge::new(
            "ghostpages_transfer_queue_depth",
            "Current transfer queue depth",
        )
        .map_err(prom_err)?;

        let store_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "ghostpages_store_duration_seconds",
                "Store operation latency in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        )
        .map_err(prom_err)?;

        let retrieve_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "ghostpages_retrieve_duration_seconds",
                "Retrieve operation latency in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        )
        .map_err(prom_err)?;

        let memory_pressure_avg10 = IntGauge::new(
            "ghost_memory_pressure_avg10",
            "Memory pressure 10-second average from PSI",
        )
        .map_err(prom_err)?;

        let memory_pressure_avg60 = IntGauge::new(
            "ghost_memory_pressure_avg60",
            "Memory pressure 60-second average from PSI",
        )
        .map_err(prom_err)?;

        let memory_pressure_avg300 = IntGauge::new(
            "ghost_memory_pressure_avg300",
            "Memory pressure 300-second average from PSI",
        )
        .map_err(prom_err)?;

        let io_pressure_avg10 = IntGauge::new(
            "ghost_io_pressure_avg10",
            "IO pressure 10-second average from PSI",
        )
        .map_err(prom_err)?;

        // Register all metrics
        registry
            .register(Box::new(store_total.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(store_bytes_total.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(store_errors_total.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(retrieve_total.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(retrieve_bytes_total.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(retrieve_errors_total.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(delete_total.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(delete_errors_total.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(tier_capacity_bytes.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(tier_used_bytes.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(ingress_queue_depth.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(compression_queue_depth.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(transfer_queue_depth.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(store_duration_seconds.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(retrieve_duration_seconds.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(memory_pressure_avg10.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(memory_pressure_avg60.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(memory_pressure_avg300.clone()))
            .map_err(prom_err)?;
        registry
            .register(Box::new(io_pressure_avg10.clone()))
            .map_err(prom_err)?;

        Ok(Self {
            registry,
            store_total,
            store_bytes_total,
            store_errors_total,
            retrieve_total,
            retrieve_bytes_total,
            retrieve_errors_total,
            delete_total,
            delete_errors_total,
            tier_capacity_bytes,
            tier_used_bytes,
            ingress_queue_depth,
            compression_queue_depth,
            transfer_queue_depth,
            store_duration_seconds,
            retrieve_duration_seconds,
            memory_pressure_avg10,
            memory_pressure_avg60,
            memory_pressure_avg300,
            io_pressure_avg10,
        })
    }
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new().expect("Failed to create Prometheus metrics")
    }
}

/// Prometheus exporter that serves metrics over HTTP.
#[derive(Debug, Clone)]
pub struct PrometheusExporter {
    metrics: PrometheusMetrics,
}

impl PrometheusExporter {
    /// Create a new PrometheusExporter with the given metrics.
    pub fn new(metrics: PrometheusMetrics) -> Self {
        Self { metrics }
    }

    /// Get the Prometheus registry for serving metrics.
    pub fn registry(&self) -> Arc<Registry> {
        self.metrics.registry.clone()
    }

    /// Get a reference to the underlying metrics.
    pub fn metrics(&self) -> &PrometheusMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_metrics_creation() {
        let metrics = PrometheusMetrics::new().unwrap();
        assert_eq!(metrics.store_total.get(), 0);
        assert_eq!(metrics.retrieve_total.get(), 0);
    }

    #[test]
    fn test_prometheus_exporter() {
        let metrics = PrometheusMetrics::new().unwrap();
        let exporter = PrometheusExporter::new(metrics);
        let _registry = exporter.registry();
    }
}
