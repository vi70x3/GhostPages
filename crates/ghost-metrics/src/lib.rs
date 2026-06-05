//! # ghost-metrics
//!
//! Observability and metrics collection for GhostPages.
//!
//! Provides Prometheus-compatible metrics and structured tracing setup.

pub mod allocator;
pub mod collector;
pub mod health;
pub mod migration;
pub mod prometheus;
pub mod queue;
pub mod registry;
pub mod replay;
pub mod tracing;

pub use allocator::AllocatorMetrics;
pub use collector::MetricsCollector;
pub use health::BackendHealthMetrics;
pub use migration::MigrationMetrics;
pub use prometheus::PrometheusExporter;
pub use queue::QueueMetrics;
pub use registry::MetricsRegistry;
pub use replay::ReplayMetrics;
pub use tracing::init_tracing;
