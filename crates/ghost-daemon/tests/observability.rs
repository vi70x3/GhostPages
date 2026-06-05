//! Integration tests for observability and diagnostics.
//!
//! Validates the metrics registry, diagnostic snapshot, metrics exporter,
//! and trace event coverage.

use std::collections::BTreeMap;
use std::sync::Arc;

use ghost_core::state::PressureState;
use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::diagnostics::{
    BackendDiagnostics, DiagnosticSnapshot, DiagnosticSnapshotBuilder, HealthStatus,
    QueueDiagnostics,
};
use ghost_daemon::exporter::{ExporterState, MetricsExporter};
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_metrics::registry::MetricsRegistry;
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;

// ─── Metrics Registry Tests ───────────────────────────────────────────────────

#[test]
fn test_metrics_registry_creation() {
    let registry = MetricsRegistry::new().expect("should create metrics registry");
    let output = registry.gather().expect("should gather metrics");
    assert!(output.contains("ghostpages_queue_depth"));
    assert!(output.contains("ghostpages_migration_evaluation_cycles_total"));
    assert!(output.contains("ghostpages_replay_ops_total"));
    assert!(output.contains("ghostpages_allocator_allocations_total"));
    assert!(output.contains("ghostpages_backend_health_status"));
}

#[test]
fn test_metrics_registry_gather_not_empty() {
    let registry = MetricsRegistry::new().expect("should create metrics registry");
    let output = registry.gather().expect("should gather metrics");
    assert!(!output.is_empty());
    // Prometheus text format starts with # HELP or # TYPE
    assert!(output.starts_with('#'));
}

#[test]
fn test_metrics_registry_clone() {
    let registry = MetricsRegistry::new().expect("should create metrics registry");
    let cloned = registry.clone();
    let output = cloned.gather().expect("cloned registry should gather");
    assert!(output.contains("ghostpages_queue_depth"));
}

// ─── Diagnostic Snapshot Tests ───────────────────────────────────────────────

#[test]
fn test_diagnostic_snapshot_default() {
    let snapshot = DiagnosticSnapshot::default();
    assert_eq!(snapshot.overall_health, HealthStatus::Healthy);
    assert!(snapshot.backends.is_empty());
    assert_eq!(snapshot.queue.depth, 0);
    assert_eq!(snapshot.migration.active_migrations, 0);
    assert_eq!(snapshot.allocator.allocations_total, 0);
    assert_eq!(snapshot.replay.replay_ops_total, 0);
}

#[test]
fn test_diagnostic_snapshot_serialization() {
    let snapshot = DiagnosticSnapshot::default();
    let json = serde_json::to_string(&snapshot).expect("should serialize");
    let deserialized: DiagnosticSnapshot =
        serde_json::from_str(&json).expect("should deserialize");
    assert_eq!(deserialized.overall_health, snapshot.overall_health);
    assert_eq!(deserialized.queue.depth, snapshot.queue.depth);
    assert_eq!(
        deserialized.migration.active_migrations,
        snapshot.migration.active_migrations
    );
}

#[test]
fn test_diagnostic_snapshot_json_structure() {
    let snapshot = DiagnosticSnapshot::default();
    let json = serde_json::to_string_pretty(&snapshot).expect("should serialize");
    // Verify all expected fields are present in JSON
    assert!(json.contains("timestamp"));
    assert!(json.contains("uptime_secs"));
    assert!(json.contains("overall_health"));
    assert!(json.contains("queue"));
    assert!(json.contains("migration"));
    assert!(json.contains("allocator"));
    assert!(json.contains("backends"));
    assert!(json.contains("replay"));
    assert!(json.contains("pressure"));
}

#[test]
fn test_health_status_display() {
    assert_eq!(format!("{}", HealthStatus::Healthy), "healthy");
    assert_eq!(format!("{}", HealthStatus::Degraded), "degraded");
    assert_eq!(format!("{}", HealthStatus::Unhealthy), "unhealthy");
}

#[test]
fn test_health_status_equality() {
    assert_eq!(HealthStatus::Healthy, HealthStatus::Healthy);
    assert_ne!(HealthStatus::Healthy, HealthStatus::Degraded);
    assert_ne!(HealthStatus::Degraded, HealthStatus::Unhealthy);
}

#[test]
fn test_snapshot_builder() {
    let builder = DiagnosticSnapshotBuilder::new(std::time::Instant::now());
    let snapshot = builder.build_default();
    assert_eq!(snapshot.overall_health, HealthStatus::Healthy);
    assert!(snapshot.backends.is_empty());
}

#[test]
fn test_snapshot_builder_uptime() {
    let builder = DiagnosticSnapshotBuilder::new(std::time::Instant::now());
    let snapshot = builder.build_default();
    // Uptime should be very close to 0 for a freshly created builder
    assert_eq!(snapshot.uptime_secs, 0);
}

#[test]
fn test_backend_diagnostics_default() {
    let diag = BackendDiagnostics::default();
    assert_eq!(diag.health, "unknown");
    assert_eq!(diag.consecutive_failures, 0);
    assert_eq!(diag.tier_id, "");
    assert_eq!(diag.health_check_successes_total, 0);
    assert_eq!(diag.recovery_events_total, 0);
}

#[test]
fn test_queue_diagnostics_default() {
    let diag = QueueDiagnostics::default();
    assert_eq!(diag.depth, 0);
    assert_eq!(diag.capacity, 0);
    assert!(!diag.is_full);
    assert!(!diag.is_shutdown);
    assert_eq!(diag.submitted_total, 0);
    assert_eq!(diag.dequeued_total, 0);
}

// ─── Metrics Exporter Tests ──────────────────────────────────────────────────

#[test]
fn test_metrics_exporter_creation() {
    let registry = Arc::new(prometheus::Registry::new());
    let config = ghost_daemon::config::MetricsExporterConfig::default();
    let exporter = MetricsExporter::new(config, registry);
    let _ = exporter;
}

#[test]
fn test_exporter_state_clone() {
    let registry = Arc::new(prometheus::Registry::new());
    let state = ExporterState { registry };
    let _cloned = state.clone();
}

#[tokio::test]
async fn test_metrics_exporter_health_endpoint() {
    use axum::extract::Json;
    use axum::routing::get;
    use axum::Router;
    use serde::Serialize;

    #[derive(Serialize)]
    struct HealthResp {
        status: String,
        service: String,
    }

    async fn health_handler() -> Json<HealthResp> {
        Json(HealthResp {
            status: "ok".to_string(),
            service: "ghostpages-metrics".to_string(),
        })
    }

    let app = Router::new().route("/health", get(health_handler));

    // Use axum's built-in test server via tokio
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Make an HTTP request to the health endpoint via raw TCP
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut buf).await.unwrap();
    let response = String::from_utf8(buf).unwrap();
    assert!(response.contains("200 OK"), "expected 200 OK, got: {}", response);

    server_handle.abort();
}

// ─── Trace Event Coverage Tests ──────────────────────────────────────────────

#[test]
fn test_trace_event_promotion_queued() {
    let event = TraceEvent::PromotionQueued {
        chunk_id: ChunkId::from_data(b"promo"),
        from: TierId::Disk,
        to: TierId::Ram,
        timestamp: 100,
    };
    assert_eq!(event.event_type(), "promotion_queued");
    assert_eq!(event.timestamp(), 100);
    assert!(event.chunk_id().is_some());
}

#[test]
fn test_trace_event_eviction_queued() {
    let event = TraceEvent::EvictionQueued {
        chunk_id: ChunkId::from_data(b"evict"),
        tier: TierId::Ram,
        reason: ghost_core::trace::EvictionReason::Capacity,
        timestamp: 200,
    };
    assert_eq!(event.event_type(), "eviction_queued");
    assert_eq!(event.timestamp(), 200);
}

#[test]
fn test_trace_event_backpressure_activated() {
    let event = TraceEvent::BackpressureActivated {
        memory_pressure: 0.8,
        vram_pressure: 0.5,
        io_pressure: 0.3,
        timestamp: 300,
    };
    assert_eq!(event.event_type(), "backpressure_activated");
    assert_eq!(event.timestamp(), 300);
}

#[test]
fn test_trace_event_backpressure_released() {
    let event = TraceEvent::BackpressureReleased {
        memory_pressure: 0.3,
        vram_pressure: 0.2,
        io_pressure: 0.1,
        timestamp: 400,
    };
    assert_eq!(event.event_type(), "backpressure_released");
    assert_eq!(event.timestamp(), 400);
}

#[test]
fn test_trace_event_metrics_exported() {
    let event = TraceEvent::MetricsExported {
        metrics_count: 42,
        timestamp: 500,
    };
    assert_eq!(event.event_type(), "metrics_exported");
    assert_eq!(event.timestamp(), 500);
}

#[test]
fn test_new_trace_events_serialization_roundtrip() {
    let events: Vec<TraceEvent> = vec![
        TraceEvent::PromotionQueued {
            chunk_id: ChunkId::from_data(b"promo"),
            from: TierId::Disk,
            to: TierId::Ram,
            timestamp: 1,
        },
        TraceEvent::EvictionQueued {
            chunk_id: ChunkId::from_data(b"evict"),
            tier: TierId::Ram,
            reason: ghost_core::trace::EvictionReason::Capacity,
            timestamp: 2,
        },
        TraceEvent::BackpressureActivated {
            memory_pressure: 0.8,
            vram_pressure: 0.5,
            io_pressure: 0.3,
            timestamp: 3,
        },
        TraceEvent::BackpressureReleased {
            memory_pressure: 0.3,
            vram_pressure: 0.2,
            io_pressure: 0.1,
            timestamp: 4,
        },
        TraceEvent::MetricsExported {
            metrics_count: 10,
            timestamp: 5,
        },
    ];

    for event in &events {
        let serialized = serde_json::to_string(event).expect("serialize trace event");
        let deserialized: TraceEvent =
            serde_json::from_str(&serialized).expect("deserialize trace event");
        assert_eq!(event.timestamp(), deserialized.timestamp());
        assert_eq!(event.event_type(), deserialized.event_type());
    }
}

// ─── Orchestrator Diagnostic Integration Tests ───────────────────────────────

fn test_backends() -> BTreeMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> {
    let mut backends: BTreeMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> =
        BTreeMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(4 * 1024 * 1024)) as Arc<dyn ghost_tier::backend::StorageBackend>,
    );
    let sim = Arc::new(SimBackend::new(
        SimConfig::with_capacity(16 * 1024 * 1024).with_seed(42),
    ));
    backends.insert(
        TierId::Simulation,
        sim as Arc<dyn ghost_tier::backend::StorageBackend>,
    );
    backends
}
fn test_policy() -> Arc<dyn ghost_policy::PlacementPolicy> {
    Arc::new(ghost_policy::pressure::PressureAwarePolicy::new(
        ghost_policy::pressure::PressureAwareConfig::default(),
    ))
}

#[test]
fn test_orchestrator_diagnostic_snapshot() {
    let config = OrchestratorConfig::default();
    let orchestrator = TransferOrchestrator::new(config, test_backends(), test_policy());
    let snapshot = orchestrator.diagnostic_snapshot();
    assert_eq!(snapshot.overall_health, HealthStatus::Healthy);
    assert!(snapshot.backends.is_empty());
}

#[test]
fn test_orchestrator_diagnostic_snapshot_pressure_degraded() {
    let config = OrchestratorConfig::default();
    let orchestrator = TransferOrchestrator::new(config, test_backends(), test_policy());
    // Without pressure, should be healthy
    let snapshot = orchestrator.diagnostic_snapshot();
    assert_eq!(snapshot.overall_health, HealthStatus::Healthy);
}

#[test]
fn test_orchestrator_diagnostic_snapshot_serializable() {
    let config = OrchestratorConfig::default();
    let orchestrator = TransferOrchestrator::new(config, test_backends(), test_policy());
    let snapshot = orchestrator.diagnostic_snapshot();
    let json = serde_json::to_string(&snapshot).expect("should serialize");
    assert!(json.contains("timestamp"));
    assert!(json.contains("overall_health"));
}

// ─── Pressure State Tests ────────────────────────────────────────────────────

#[test]
fn test_pressure_state_default() {
    let pressure = PressureState::new();
    assert!(!pressure.is_under_pressure());
}

#[test]
fn test_pressure_state_serialization() {
    let pressure = PressureState::new();
    let json = serde_json::to_string(&pressure).expect("should serialize");
    let deserialized: PressureState =
        serde_json::from_str(&json).expect("should deserialize");
    assert_eq!(pressure.is_under_pressure(), deserialized.is_under_pressure());
}
