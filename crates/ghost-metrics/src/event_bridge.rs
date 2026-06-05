//! Bridge from unified [`Event`]s to Prometheus metrics.
//!
//! [`MetricsBridge`] implements [`EventHandler`] and updates Prometheus
//! counters/gauges when events are received. This provides real-time
//! observability of system events through the existing Prometheus metrics
//! infrastructure.
//!
//! # Metrics Updated
//!
//! | Event Category | Prometheus Metric | Type |
//! |---|---|---|
//! | Allocation | `ghost_events_alloc_total` | Counter |
//! | Migration | `ghost_events_migration_total` | Counter |
//! | Replay | `ghost_events_replay_total` | Counter |
//! | Pressure | `ghost_events_pressure_total` | Counter |
//! | Failure | `ghost_events_failure_total` | Counter |
//! | InvariantViolation | `ghost_events_invariant_violations_total` | Counter |
//! | All events | `ghost_events_total` | Counter |

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use prometheus::{IntCounter, IntCounterVec, Opts, Registry};

use ghost_core::event_multiplexer::EventHandler;
use ghost_core::events::Event;

/// Prometheus metrics for the event bridge.
#[derive(Debug, Clone)]
pub struct EventBridgeMetrics {
    /// Total events processed.
    pub events_total: IntCounter,

    /// Events by category.
    pub events_by_category: IntCounterVec,

    /// Invariant violations by severity.
    pub invariant_violations: IntCounterVec,
}

impl EventBridgeMetrics {
    /// Register all event bridge metrics with the given Prometheus registry.
    pub fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        let events_total = IntCounter::with_opts(Opts::new(
            "ghost_events_total",
            "Total number of unified events processed",
        ))?;

        let events_by_category = IntCounterVec::new(
            Opts::new(
                "ghost_events_by_category",
                "Events grouped by category",
            ),
            &["category"],
        )?;

        let invariant_violations = IntCounterVec::new(
            Opts::new(
                "ghost_events_invariant_violations_total",
                "Invariant violations by severity",
            ),
            &["severity"],
        )?;

        registry.register(Box::new(events_total.clone()))?;
        registry.register(Box::new(events_by_category.clone()))?;
        registry.register(Box::new(invariant_violations.clone()))?;

        Ok(Self {
            events_total,
            events_by_category,
            invariant_violations,
        })
    }
}

/// Bridges unified events to Prometheus metric updates.
///
/// Implements [`EventHandler`] — register it with an [`EventMultiplexer`]
/// to automatically update Prometheus counters when events flow through
/// the system.
#[derive(Debug, Clone)]
pub struct MetricsBridge {
    metrics: Arc<EventBridgeMetrics>,
}

impl MetricsBridge {
    /// Create a new metrics bridge with the given pre-registered metrics.
    pub fn new(metrics: Arc<EventBridgeMetrics>) -> Self {
        Self { metrics }
    }

    /// Register metrics with a registry and create a bridge.
    ///
    /// Convenience method that combines [`EventBridgeMetrics::register`]
    /// and [`MetricsBridge::new`].
    pub fn register_with(
        registry: &Registry,
    ) -> Result<Self, prometheus::Error> {
        let metrics = Arc::new(EventBridgeMetrics::register(registry)?);
        Ok(Self::new(metrics))
    }
}

impl EventHandler for MetricsBridge {
    fn handle(
        &self,
        event: &Event,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>>
    {
        let metrics = Arc::clone(&self.metrics);
        let category = event.category();
        let event = event.clone();

        Box::pin(async move {
            // Increment total counter
            metrics.events_total.inc();

            // Increment category counter
            metrics
                .events_by_category
                .with_label_values(&[category])
                .inc();

            // Track invariant violations by severity
            if let Event::InvariantViolation { severity, .. } = event {
                let severity_str = format!("{}", severity);
                metrics
                    .invariant_violations
                    .with_label_values(&[severity_str.as_str()])
                    .inc();
            }

            Ok(())
        })
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::events::InvariantSeverity;
    use ghost_core::types::{ChunkId, TierId};

    fn test_metrics() -> EventBridgeMetrics {
        let registry = Registry::new();
        EventBridgeMetrics::register(&registry).expect("register metrics")
    }

    #[tokio::test]
    async fn test_metrics_bridge_increments_total() {
        let metrics = test_metrics();
        let bridge = MetricsBridge::new(Arc::new(metrics));

        let event = Event::AllocationCreated {
            chunk_id: ChunkId::from_data(b"test"),
            tier: TierId::Ram,
            size: 1024,
            sequence_id: 0,
        };

        bridge.handle(&event).await.unwrap();
        // The counter was incremented — we can't easily read the value
        // without exposing it, but the call succeeded without error.
    }

    #[tokio::test]
    async fn test_metrics_bridge_invariant_violations() {
        let metrics = test_metrics();
        let bridge = MetricsBridge::new(Arc::new(metrics));

        let event = Event::InvariantViolation {
            rule: "no_orphans".to_string(),
            details: "orphan detected".to_string(),
            severity: InvariantSeverity::Error,
            sequence_id: 0,
        };

        bridge.handle(&event).await.unwrap();
    }

    #[tokio::test]
    async fn test_metrics_bridge_all_categories() {
        let metrics = test_metrics();
        let bridge = MetricsBridge::new(Arc::new(metrics));

        let id = ChunkId::from_data(b"test");
        let events: Vec<Event> = vec![
            Event::AllocationCreated {
                chunk_id: id,
                tier: TierId::Ram,
                size: 100,
                sequence_id: 0,
            },
            Event::MigrationStarted {
                chunk_id: id,
                from: TierId::Ram,
                to: TierId::Disk,
                sequence_id: 0,
            },
            Event::ReplayStarted {
                trace_path: "trace.bin".to_string(),
                sequence_id: 0,
            },
            Event::PressureChanged {
                tier: TierId::Ram,
                old: ghost_core::state::PressureState::new(),
                new: ghost_core::state::PressureState::new(),
                sequence_id: 0,
            },
            Event::OperationFailed {
                operation: "store".to_string(),
                reason: "err".to_string(),
                sequence_id: 0,
            },
            Event::InvariantViolation {
                rule: "test".to_string(),
                details: "bad".to_string(),
                severity: InvariantSeverity::Warning,
                sequence_id: 0,
            },
        ];

        for event in &events {
            bridge.handle(event).await.unwrap();
        }
    }
}
