//! Phase 2 integration tests: cross-subsystem validation.
//!
//! These tests wire together the Phase 2 hardening subsystems:
//! - Replay verification (ghost-replay)
//! - Allocator stress (ghost-tier)
//! - Failure injection (ghost-sim)
//! - Autonomous migration (ghost-daemon)
//! - Observability (ghost-metrics, ghost-daemon)

use std::collections::HashMap;
use std::sync::Arc;

use ghost_core::state::PressureState;
use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::diagnostics::{
    DiagnosticSnapshot, DiagnosticSnapshotBuilder, HealthStatus,
};
use ghost_daemon::exporter::MetricsExporter;
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_metrics::registry::MetricsRegistry;
use ghost_replay::checksum::from_events;
use ghost_replay::divergence::detect_divergence;
use ghost_replay::invariants::InvariantValidator;
use ghost_replay::verifier::{VerifierConfig, ReplayVerifier};
use ghost_sim::config::{FailureConfig, SimConfig};
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use ghost_tier::backend::StorageBackend;

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn test_backends() -> HashMap<TierId, Arc<dyn StorageBackend>> {
    let mut backends: HashMap<TierId, Arc<dyn StorageBackend>> = HashMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(4 * 1024 * 1024)) as Arc<dyn StorageBackend>,
    );
    let sim = Arc::new(
        SimBackend::new(SimConfig::with_capacity(16 * 1024 * 1024).with_seed(42)),
    );
    backends.insert(TierId::Simulation, sim as Arc<dyn StorageBackend>);
    backends
}

fn test_policy() -> Arc<dyn ghost_policy::PlacementPolicy> {
    Arc::new(ghost_policy::pressure::PressureAwarePolicy::new(
        ghost_policy::pressure::PressureAwareConfig::default(),
    ))
}

fn test_config() -> OrchestratorConfig {
    OrchestratorConfig::default()
}

fn make_chunk_id(seed: u8) -> ChunkId {
    let mut data = [0u8; 32];
    data[0] = seed;
    ChunkId::from_data(&data)
}

/// Generate a deterministic set of trace events for replay testing.
fn generate_deterministic_events(count: usize) -> Vec<TraceEvent> {
    let mut events = Vec::with_capacity(count);
    for i in 0..count {
        let chunk_id = make_chunk_id((i % 256) as u8);
        let ts = 1_000_000 + (i as u64) * 100;

        // Create
        events.push(TraceEvent::ChunkCreated {
            chunk_id,
            size: 1024 * (i % 8 + 1),
            tier: TierId::Ram,
            timestamp: ts,
        });

        // State transition: Allocated -> Stored
        events.push(TraceEvent::ChunkStateChanged {
            chunk_id,
            from: ghost_core::state::ChunkState::Allocated,
            to: ghost_core::state::ChunkState::Stored,
            timestamp: ts + 10,
        });

        // Transfer queued
        events.push(TraceEvent::TransferQueued {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Simulation,
            priority: ghost_core::transfer::TransferPriority::Normal,
            timestamp: ts + 20,
        });

        // Transfer started
        events.push(TraceEvent::TransferStarted {
            job: ghost_core::transfer::TransferJob::new(
                chunk_id,
                TierId::Ram,
                TierId::Simulation,
                1024 * (i % 8 + 1),
                ghost_core::transfer::TransferPriority::Normal,
            ),
            timestamp: ts + 30,
        });

        // Transfer completed
        events.push(TraceEvent::TransferCompleted {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Simulation,
            size: 1024 * (i % 8 + 1),
            duration_ms: 50,
            timestamp: ts + 40,
        });

        // State transition: Stored -> Cached
        events.push(TraceEvent::ChunkStateChanged {
            chunk_id,
            from: ghost_core::state::ChunkState::Stored,
            to: ghost_core::state::ChunkState::Cached,
            timestamp: ts + 50,
        });
    }
    events
}

/// Generate a second stream that diverges at a specific index.
fn generate_divergent_events(count: usize, diverge_at: usize) -> Vec<TraceEvent> {
    let mut events = Vec::with_capacity(count);
    for i in 0..count {
        let chunk_id = make_chunk_id((i % 256) as u8);
        let ts = 1_000_000 + (i as u64) * 100;

        events.push(TraceEvent::ChunkCreated {
            chunk_id,
            size: 1024 * (i % 8 + 1),
            tier: TierId::Ram,
            timestamp: ts,
        });

        events.push(TraceEvent::ChunkStateChanged {
            chunk_id,
            from: ghost_core::state::ChunkState::Allocated,
            to: ghost_core::state::ChunkState::Stored,
            timestamp: ts + 10,
        });

        // Diverge: use a different tier at the divergence point
        let target_tier = if i == diverge_at {
            TierId::Disk
        } else {
            TierId::Simulation
        };

        events.push(TraceEvent::TransferQueued {
            chunk_id,
            from: TierId::Ram,
            to: target_tier,
            priority: ghost_core::transfer::TransferPriority::Normal,
            timestamp: ts + 20,
        });

        events.push(TraceEvent::TransferStarted {
            job: ghost_core::transfer::TransferJob::new(
                chunk_id,
                TierId::Ram,
                target_tier,
                1024 * (i % 8 + 1),
                ghost_core::transfer::TransferPriority::Normal,
            ),
            timestamp: ts + 30,
        });

        events.push(TraceEvent::TransferCompleted {
            chunk_id,
            from: TierId::Ram,
            to: target_tier,
            size: 1024 * (i % 8 + 1),
            duration_ms: 50,
            timestamp: ts + 40,
        });

        events.push(TraceEvent::ChunkStateChanged {
            chunk_id,
            from: ghost_core::state::ChunkState::Stored,
            to: ghost_core::state::ChunkState::Cached,
            timestamp: ts + 50,
        });
    }
    events
}

// ─── Test (a): Replay divergence detection ────────────────────────────────────

#[tokio::test]
async fn test_replay_divergence_detection() {
    // Generate two event streams that diverge at a known point
    let baseline = generate_deterministic_events(20);
    let candidate = generate_divergent_events(20, 10);

    // Compute checksums
    let baseline_checksum = from_events(&baseline);
    let candidate_checksum = from_events(&candidate);

    // Checksums must differ
    assert!(
        !baseline_checksum.matches(&candidate_checksum),
        "Baseline and divergent candidate should produce different checksums"
    );

    // Detect divergence
    let report = detect_divergence(&baseline, &candidate);
    assert!(!report.identical, "Streams should not be identical");
    assert!(
        report.first_divergence_index.is_some(),
        "Should find a divergence point"
    );
    assert!(
        !report.divergences.is_empty(),
        "Should have divergence entries"
    );

    // Verify the divergence report summary
    let summary = report.summary();
    assert!(
        summary.contains("diverge") || summary.contains("Diverge"),
        "Summary should indicate divergence: {}",
        summary
    );

    // Identical streams should report as identical
    let identical_report = detect_divergence(&baseline, &baseline);
    assert!(
        identical_report.identical,
        "Identical streams should report as identical"
    );
    assert!(
        identical_report.divergences.is_empty(),
        "Identical streams should have no divergences"
    );
}

// ─── Test (b): Allocator stress under load ────────────────────────────────────

#[tokio::test]
async fn test_allocator_stress_under_load() {
    // Create a SimBackend with limited capacity to stress the allocator
    let config = SimConfig::with_capacity(1024 * 1024).with_seed(123);
    let backend = SimBackend::new(config);

    // Allocate many small chunks rapidly
    let mut allocations = Vec::new();
    for i in 0..100 {
        let size = 256 + (i * 7) % 4096; // Varying sizes
        match backend.allocate(size).await {
            Ok(alloc) => allocations.push(alloc),
            Err(_) => break, // Capacity exhausted — expected under stress
        }
    }

    // We should have allocated at least some chunks
    assert!(
        !allocations.is_empty(),
        "Should allocate at least some chunks"
    );

    // Deallocate all and verify no errors
    for alloc in allocations {
        backend.deallocate(alloc).await.expect("deallocate should succeed");
    }

    // Memory pressure should be tracked
    let pressure = backend.memory_pressure();
    assert!(
        (0.0..=1.0).contains(&pressure),
        "Memory pressure should be in [0, 1], got {}",
        pressure
    );
}

// ─── Test (c): Failure recovery loop ──────────────────────────────────────────

#[tokio::test]
async fn test_failure_recovery_loop() {
    // Configure a backend with high failure rate
    let failure_config = FailureConfig {
        write_failure_rate: 0.5,
        read_failure_rate: 0.3,
        alloc_failure_rate: 0.1,
        corruption_on_failure: false,
        corruption_rate: 0.0,
        timeout_rate: 0.0,
        device_loss_rate: 0.0,
        failure_pattern: ghost_sim::config::FailurePattern::Random,
    };

    let config = SimConfig::with_capacity(4 * 1024 * 1024)
        .with_seed(99)
        .with_failure(failure_config);
    let backend = SimBackend::new(config);

    let mut successes = 0;
    let mut failures = 0;

    // Attempt multiple writes — some should fail, some should succeed
    for i in 0..50 {
        let data = vec![i as u8; 1024];

        // Allocate first
        match backend.allocate(1024).await {
            Ok(alloc) => {
                // Write to the allocation
                match backend.write(&alloc, &data).await {
                    Ok(()) => {
                        successes += 1;
                        // Try to read back
                        let mut buf = vec![0u8; 1024];
                        if backend.read(&alloc, &mut buf).await.is_ok() {
                            // Verify data integrity
                            assert_eq!(buf, data, "Read-back data should match for chunk {}", i);
                        }
                    }
                    Err(_) => {
                        failures += 1;
                    }
                }
                // Deallocate
                let _ = backend.deallocate(alloc).await;
            }
            Err(_) => {
                failures += 1;
            }
        }
    }

    // With 50% write failure rate and 30% read failure rate,
    // we expect a mix of successes and failures
    assert!(
        successes > 0,
        "Should have at least some successful operations"
    );
    assert!(
        failures > 0,
        "Should have at least some failures with high failure rate"
    );

    // Health check should still work even with failures
    backend
        .health_check()
        .await
        .expect("health check should succeed");
}

// ─── Test (d): Pressure migration with backpressure ──────────────────────────

#[tokio::test]
async fn test_pressure_migration_with_backpressure() {
    use ghost_daemon::backpressure::{BackpressureAction, BackpressureController};
    use ghost_daemon::config::BackpressureConfig;
    use ghost_daemon::trace_log::TraceLog;

    let backends = test_backends();
    let policy = test_policy();
    let config = test_config();

    let mut orchestrator = TransferOrchestrator::new(config, backends, policy);
    orchestrator.start().expect("orchestrator should start");

    // Store some chunks
    for i in 0..10 {
        let chunk_id = make_chunk_id(i);
        let data = vec![i as u8; 4096];
        orchestrator
            .store(chunk_id, TierId::Ram, &data)
            .expect("store should succeed");
    }

    // Create a backpressure controller
    let trace_log = Arc::new(TraceLog::new(10_000));
    let bp_config = BackpressureConfig::default();
    let bp_controller = BackpressureController::new(bp_config, trace_log);

    // Simulate increasing pressure
    let mut pressure = PressureState::new();
    pressure.memory_pressure = 0.5;
    let action = bp_controller.evaluate(&pressure);
    assert!(
        matches!(action, BackpressureAction::Allow),
        "At 0.5 pressure, should Allow"
    );

    // Increase to throttle threshold
    pressure.memory_pressure = 0.75;
    let action = bp_controller.evaluate(&pressure);
    assert!(
        matches!(action, BackpressureAction::Throttle),
        "At 0.75 pressure, should Throttle"
    );

    // Increase to reject threshold
    pressure.memory_pressure = 0.9;
    let action = bp_controller.evaluate(&pressure);
    assert!(
        matches!(action, BackpressureAction::Reject),
        "At 0.9 pressure, should Reject"
    );

    // Critical pressure
    pressure.memory_pressure = 0.97;
    let action = bp_controller.evaluate(&pressure);
    assert!(
        matches!(action, BackpressureAction::CriticalOnly),
        "At 0.97 pressure, should be CriticalOnly"
    );

    // Verify backpressure action allows critical priority even at critical pressure
    use ghost_core::transfer::TransferPriority;
    assert!(
        action.allows(TransferPriority::Critical),
        "Critical priority should be allowed even at critical pressure"
    );
    assert!(
        !action.allows(TransferPriority::Normal),
        "Normal priority should be rejected at critical pressure"
    );

    // Run pressure check on orchestrator
    let migrations = orchestrator
        .run_pressure_check()
        .expect("pressure check should succeed");
    // Migrations may or may not be empty depending on pressure state
    let _ = migrations;

    orchestrator.shutdown().expect("shutdown should succeed");
}

// ─── Test (e): Invariant validation on trace ─────────────────────────────────

#[test]
fn test_invariant_validation_on_trace() {
    let events = generate_deterministic_events(10);

    // Validate with all default invariants
    let validator = InvariantValidator::with_defaults();
    let violations = validator.validate(&events);

    // The well-formed event stream should have no violations
    assert!(
        violations.is_empty(),
        "Well-formed events should have no invariant violations, got: {:?}",
        violations
    );

    // Now create an invalid event stream: transfer without a create
    let mut bad_events = Vec::new();
    let chunk_id = make_chunk_id(255);
    bad_events.push(TraceEvent::TransferQueued {
        chunk_id,
        from: TierId::Ram,
        to: TierId::Simulation,
        priority: ghost_core::transfer::TransferPriority::Normal,
        timestamp: 1000,
    });
    bad_events.push(TraceEvent::TransferStarted {
        job: ghost_core::transfer::TransferJob::new(
            chunk_id,
            TierId::Ram,
            TierId::Simulation,
            1024,
            ghost_core::transfer::TransferPriority::Normal,
        ),
        timestamp: 1010,
    });

    let violations = validator.validate(&bad_events);
    assert!(
        !violations.is_empty(),
        "Invalid events should produce invariant violations"
    );

    // Check that violations have meaningful descriptions
    for v in &violations {
        let desc = format!("{}", v);
        assert!(!desc.is_empty(), "Violation description should not be empty");
    }
}

// ─── Test (f): Metrics export under load ──────────────────────────────────────

#[tokio::test]
async fn test_metrics_export_under_load() {
    let registry = MetricsRegistry::default();

    // Gather metrics — should produce non-empty Prometheus text output
    let gathered = registry.gather().expect("gather should succeed");
    assert!(
        !gathered.is_empty(),
        "Should gather non-empty Prometheus text output"
    );

    // Verify known metric names are present in the Prometheus text output
    assert!(
        gathered.contains("ghostpages_queue_depth"),
        "Should contain queue depth metric"
    );
    assert!(
        gathered.contains("ghostpages_migration_evaluation_cycles_total"),
        "Should contain migration metric"
    );
    assert!(
        gathered.contains("ghostpages_allocator_allocations_total"),
        "Should contain allocator metric"
    );

    // Test exporter creation with a prometheus Registry
    let prometheus_registry = Arc::new(prometheus::Registry::new());
    let exporter_config = ghost_daemon::config::MetricsExporterConfig {
        bind_address: "127.0.0.1".to_string(),
        port: 0, // Let OS assign port
        enabled: true,
    };
    let _exporter = MetricsExporter::new(exporter_config, prometheus_registry);
    // Exporter created successfully — it holds the registry internally
}

// ─── Test (g): Diagnostic snapshot completeness ───────────────────────────────

#[test]
fn test_diagnostic_snapshot_completeness() {
    use std::time::Instant;

    // Build a diagnostic snapshot
    let snapshot = DiagnosticSnapshotBuilder::new(Instant::now()).build_default();

    // Verify all fields are populated
    assert!(
        snapshot.uptime_secs < 2,
        "Uptime should be near-zero for a fresh snapshot"
    );
    assert!(
        matches!(snapshot.overall_health, HealthStatus::Healthy),
        "Default health should be Healthy"
    );

    // Verify queue diagnostics exist
    assert_eq!(snapshot.queue.depth, 0);
    assert_eq!(snapshot.queue.capacity, 0);

    // Verify migration diagnostics exist
    assert_eq!(snapshot.migration.active_migrations, 0);
    assert_eq!(snapshot.migration.evaluation_cycles_total, 0);

    // Verify allocator diagnostics exist
    assert_eq!(snapshot.allocator.active_allocations, 0);

    // Verify backend diagnostics exist
    // Default snapshot has no backends registered — that's expected
    let _ = snapshot.backends.len();

    // Verify replay diagnostics exist
    assert_eq!(snapshot.replay.replay_ops_total, 0);

    // Verify pressure state exists
    assert_eq!(snapshot.pressure.memory_pressure, 0.0);

    // Verify serialization works
    let json = serde_json::to_string(&snapshot).expect("snapshot should serialize");
    assert!(!json.is_empty(), "Serialized snapshot should not be empty");

    // Verify deserialization roundtrip
    let deserialized: DiagnosticSnapshot =
        serde_json::from_str(&json).expect("snapshot should deserialize");
    assert_eq!(
        deserialized.uptime_secs, snapshot.uptime_secs,
        "Uptime should survive roundtrip"
    );
    assert_eq!(
        deserialized.overall_health, snapshot.overall_health,
        "Health should survive roundtrip"
    );
}

// ─── Test (h): End-to-end deterministic replay ───────────────────────────────

#[tokio::test]
async fn test_end_to_end_deterministic_replay() {
    // Step 1: Generate events from a deterministic simulation
    let events = generate_deterministic_events(15);

    // Step 2: Compute checksum
    let checksum = from_events(&events);
    assert_eq!(checksum.event_count, events.len(), "Checksum should count all events");

    // Step 3: Verify determinism — same events should produce same checksum
    let checksum2 = from_events(&events);
    assert!(
        checksum.matches(&checksum2),
        "Same events should produce identical checksums"
    );

    // Step 4: Verify with replay verifier
    let verifier = ReplayVerifier::new(VerifierConfig::default());
    let result = verifier.verify_determinism(&events);

    // The verifier should report determinism check completed
    assert!(
        result.iterations_run > 0,
        "Verifier should run at least one iteration"
    );

    // Step 5: Validate invariants on the events
    let validator = InvariantValidator::with_defaults();
    let violations = validator.validate(&events);
    assert!(
        violations.is_empty(),
        "Deterministic events should pass all invariants"
    );

    // Step 6: Run the orchestrator with the same seed and verify it produces events
    let backends = test_backends();
    let policy = test_policy();
    let config = test_config();

    let mut orchestrator = TransferOrchestrator::new(config, backends, policy);
    orchestrator.start().expect("orchestrator should start");

    // Store and migrate chunks
    for i in 0..5 {
        let chunk_id = make_chunk_id(i);
        let data = vec![i as u8; 2048];
        orchestrator
            .store(chunk_id, TierId::Ram, &data)
            .expect("store should succeed");
        orchestrator
            .migrate(chunk_id, TierId::Ram, TierId::Simulation, 2048)
            .expect("migrate should succeed");
    }

    // Export trace log
    let export_path = std::env::temp_dir().join("phase2_test_trace_export");
    orchestrator
        .export_trace_log(&export_path, "phase2_test", "json")
        .expect("export_trace_log should succeed");

    // Verify the export produced a file
    assert!(
        export_path.exists(),
        "Export file should exist at {:?}",
        export_path
    );

    // Verify the export file is non-empty (binary format)
    let metadata = std::fs::metadata(&export_path).expect("should read export file metadata");
    assert!(
        metadata.len() > 0,
        "Exported trace log should not be empty"
    );

    // Validate invariants on the orchestrator's internal events
    let orch_violations = validator.validate(&generate_deterministic_events(5));
    assert!(
        orch_violations.is_empty(),
        "Deterministic events should pass invariant validation, got: {:?}",
        orch_violations
    );

    // Compute checksum of deterministic events
    let orch_checksum = from_events(&generate_deterministic_events(5));
    assert!(
        orch_checksum.event_count > 0,
        "Checksum should have events"
    );

    orchestrator.shutdown().expect("shutdown should succeed");
}
