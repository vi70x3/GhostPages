//! # ghost-metrics
//!
//! Observability and metrics collection for GhostPages.
//!
//! Provides Prometheus-compatible metrics and structured tracing setup.

pub mod collector;
pub mod prometheus;
pub mod tracing;

pub use collector::MetricsCollector;
pub use prometheus::PrometheusExporter;
pub use tracing::init_tracing;
