//! # ghost-metrics
//!
//! Observability and metrics collection for GhostPages.
//!
//! Provides Prometheus-compatible metrics and structured tracing setup.

pub mod allocator;
pub mod collector;
pub mod event_bridge;
pub mod health;
pub mod hotness;
pub mod migration;
pub mod policy;
pub mod prometheus;
pub mod queue;
pub mod registry;
pub mod replay;
pub mod stability;
pub mod tracing;

pub use allocator::AllocatorMetrics;
pub use collector::MetricsCollector;
pub use event_bridge::{EventBridgeMetrics, MetricsBridge};
pub use health::BackendHealthMetrics;
pub use hotness::HotnessMetrics;
pub use migration::MigrationMetrics;
pub use policy::{PolicyMetrics, Recommendation, RecommendationAction};
pub use prometheus::PrometheusExporter;
pub use queue::QueueMetrics;
pub use registry::MetricsRegistry;
pub use replay::ReplayMetrics;
pub use stability::StabilityMetrics;
pub use tracing::init_tracing;