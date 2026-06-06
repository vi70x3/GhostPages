//! Integration tests for ghost-linux replay infrastructure.
//!
//! Tests the full pipeline: scan → record → replay → verify.

use std::sync::Arc;
use std::time::Duration;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::time::DeterministicTimeProvider;
use ghost_linux::*;

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn test_time_provider() -> Arc<dyn ghost_core::time::TimeProvider> {
    Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        Duration::from_secs(1),
    ))
}

fn test_emitter() -> EventEmitter {
    let (tx, _rx) = tokio::sync::mpsc::channel(256);
    EventEmitter::new(tx)
}

// ─── Test: Full System Scan ──────────────────────────────────────────────────

#[test]
fn test_full_system_scan() {
    let mut scanner = SystemScanner::new(
        test_time_provider(),
        test_emitter(),
        42,
    );

    let snapshot = scanner.scan().expect("scan should succeed");

    // All observation layers should produce data
    assert!(snapshot.psi.is_some(), "PSI should produce data");
    assert!(snapshot.meminfo.is_some(), "meminfo should produce data");
    assert!(snapshot.vmstat.is_some(), "vmstat should produce data");
    assert!(snapshot.swap.is_some(), "swap should produce data");
    assert!(snapshot.zram.is_some(), "zram should produce data");
    assert!(snapshot.tier_inventory.is_some(), "tier inventory should produce data");

    // Verify PSI has all three resources
    let psi = snapshot.psi.as_ref().unwrap();
    assert_eq!(psi.len(), 3, "PSI should have memory, I/O, CPU samples");

    // Verify meminfo has reasonable values
    let meminfo = snapshot.meminfo.as_ref().unwrap();
    assert!(meminfo.total_kb > 0, "total memory should be > 0");

    // Verify tier inventory has at least DRAM
    let tiers = snapshot.tier_inventory.as_ref().unwrap();
    assert!(!tiers.is_empty(), "should have at least one tier");
    assert!(
        tiers.iter().any(|t| t.kind == TierKind::Dram),
        "should have DRAM tier"
    );
}

// ─── Test: Scan Emits All Events ─────────────────────────────────────────────

#[test]
fn test_scan_emits_all_events() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let emitter = EventEmitter::new(tx);

    let mut scanner = SystemScanner::new(
        test_time_provider(),
        emitter,
        42,
    );

    let _snapshot = scanner.scan().expect("scan should succeed");

    // Collect all emitted events
    let mut events = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        events.push(rec.event);
    }

    // Should have events from multiple subsystems
    let has_psi = events.iter().any(|e| {
        matches!(e, Event::MemoryPressureChanged { .. }) || matches!(e, Event::IoPressureChanged { .. })
    });
    let has_meminfo = events.iter().any(|e| matches!(e, Event::MemoryStatsChanged { .. }));
    let has_swap = events.iter().any(|e| matches!(e, Event::SwapTopologyChanged { .. }));
    let has_zram = events.iter().any(|e| matches!(e, Event::ZramUtilizationChanged { .. }));
    let has_tier = events.iter().any(|e| matches!(e, Event::TierInventoryChanged { .. }));
    let has_vmstat = events.iter().any(|e| matches!(e, Event::VmstatChanged { .. }));

    assert!(has_psi, "scan should emit PSI events, got {:?}", events);
    assert!(has_meminfo, "scan should emit meminfo events");
    assert!(has_swap, "scan should emit swap events");
    assert!(has_zram, "scan should emit zram events");
    assert!(has_tier, "scan should emit tier inventory events");
    assert!(has_vmstat, "scan should emit vmstat events");
}

// ─── Test: Record and Replay ─────────────────────────────────────────────────

#[test]
fn test_record_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let record_path = dir.path().join("test_record_and_replay.bin");

    // Phase 1: Scan and record
    let mut scanner = SystemScanner::new(
        test_time_provider(),
        test_emitter(),
        42,
    );

    let mut recorder = LinuxRecorder::new(&record_path).expect("recorder should create");
    let original_snapshot = scanner
        .scan_and_record(&mut recorder)
        .expect("scan and record should succeed");
    recorder.close().expect("recorder should close");

    // Phase 2: Replay
    let mut replayer = LinuxReplayer::new(&record_path).expect("replayer should open");
    replayer.load().expect("replayer should load");

    assert_eq!(replayer.event_count(), 1, "should have one recorded scan");

    let replayed_event = replayer.next().expect("should have an event");
    assert_eq!(replayed_event.timestamp, original_snapshot.timestamp);

    // Verify the replayed event contains snapshot data
    if let Event::PolicyRecommendationGenerated { recommendations, .. } = &replayed_event.event {
        assert!(!recommendations.is_empty(), "should have recommendations");
    } else {
        panic!("expected PolicyRecommendationGenerated event");
    }
}

// ─── Test: Replay Determinism ────────────────────────────────────────────────

#[test]
fn test_replay_determinism() {
    let dir = tempfile::tempdir().unwrap();
    let path1 = dir.path().join("replay1.bin");
    let path2 = dir.path().join("replay2.bin");

    // Record same workload twice
    for path in [&path1, &path2] {
        let mut scanner = SystemScanner::new(
            test_time_provider(),
            test_emitter(),
            42,
        );
        let mut recorder = LinuxRecorder::new(path).expect("recorder should create");
        let _ = scanner.scan_and_record(&mut recorder).expect("scan should succeed");
        recorder.close().expect("recorder should close");
    }

    // Load both replays
    let mut replayer1 = LinuxReplayer::new(&path1).expect("replayer1 should open");
    replayer1.load().expect("replayer1 should load");

    let mut replayer2 = LinuxReplayer::new(&path2).expect("replayer2 should open");
    replayer2.load().expect("replayer2 should load");

    // Verify identical replays
    let result = replayer1.verify_against(&replayer2);
    assert!(result.passed(), "replays should be deterministic: {:?}", result);
    assert!(result.events_match, "events should match");
    assert!(result.ordering_match, "ordering should match");
}

// ─── Test: Pressure to Recommendation Flow ───────────────────────────────────

#[test]
fn test_pressure_to_recommendation_flow() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let emitter = EventEmitter::new(tx);

    let mut scanner = SystemScanner::new(
        test_time_provider(),
        emitter,
        42,
    );

    let snapshot = scanner.scan().expect("scan should succeed");

    // The policy runtime should produce recommendations
    assert!(
        !snapshot.recommendations.is_empty(),
        "scan should produce policy recommendations"
    );

    // Verify recommendations are valid strings
    for rec in &snapshot.recommendations {
        assert!(!rec.is_empty(), "recommendation should not be empty");
    }

    // Verify a PolicyRecommendationGenerated event was emitted
    let mut found_policy_event = false;
    while let Ok(rec) = rx.try_recv() {
        if matches!(rec.event, Event::PolicyRecommendationGenerated { .. }) {
            found_policy_event = true;
        }
    }
    assert!(found_policy_event, "should have emitted policy recommendation event");
}

// ─── Test: Tier Inventory from Scan ─────────────────────────────────────────

#[test]
fn test_tier_inventory_from_scan() {
    let mut scanner = SystemScanner::new(
        test_time_provider(),
        test_emitter(),
        42,
    );

    let snapshot = scanner.scan().expect("scan should succeed");

    let tiers = snapshot.tier_inventory.as_ref().expect("should have tier inventory");

    // Should have at least DRAM and Simulation tiers
    assert!(tiers.len() >= 2, "should have at least 2 tiers");

    // DRAM tier should be present
    let dram = tiers.iter().find(|t| t.kind == TierKind::Dram);
    assert!(dram.is_some(), "should have DRAM tier");

    let dram = dram.unwrap();
    assert_eq!(dram.name, "DRAM");
    

    // Simulation tier should be present
    let sim = tiers.iter().find(|t| t.kind == TierKind::Simulated);
    assert!(sim.is_some(), "should have Simulation tier");
}

// ─── Test: Event Ordering Preserved ──────────────────────────────────────────

#[test]
fn test_event_ordering_preserved() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let emitter = EventEmitter::new(tx);

    let mut scanner = SystemScanner::new(
        test_time_provider(),
        emitter,
        42,
    );

    let _snapshot = scanner.scan().expect("scan should succeed");

    // Collect all events
    let mut events = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        events.push(rec.sequence_id);
    }

    // Verify sequence IDs are monotonically increasing
    for window in events.windows(2) {
        assert!(
            window[0] <= window[1],
            "sequence IDs should be non-decreasing: {:?}",
            events
        );
    }
}

// ─── Test: Cross-Component Consistency ───────────────────────────────────────

#[test]
fn test_cross_component_consistency() {
    let mut scanner = SystemScanner::new(
        test_time_provider(),
        test_emitter(),
        42,
    );

    let snapshot = scanner.scan().expect("scan should succeed");

    // PSI pressure should be consistent with tier pressure
    if let (Some(psi_samples), Some(tiers)) = (&snapshot.psi, &snapshot.tier_inventory) {
        let memory_psi = psi_samples
            .iter()
            .find(|s| s.resource == PsiResource::Memory);

        if let Some(psi) = memory_psi {
            let dram = tiers.iter().find(|t| t.kind == TierKind::Dram);
            if let Some(dram) = dram {
                // PSI avg10 should be a valid pressure value (0-100%)
                assert!(psi.avg10 >= 0.0, "PSI avg10 should be non-negative");
                assert!(psi.avg10 <= 100.0, "PSI avg10 should be <= 100");

                // DRAM utilization should be consistent with pressure
                let utilization = dram.utilization();
                assert!(utilization >= 0.0 && utilization <= 1.0,
                    "DRAM utilization should be between 0 and 1");
            }
        }
    }

    // Policy recommendations should be consistent with system state
    if !snapshot.recommendations.is_empty() {
        // If there's high pressure, we should have eviction/migration recommendations
        if let Some(psi_samples) = &snapshot.psi {
            let memory_psi = psi_samples.iter().find(|s| s.resource == PsiResource::Memory);
            if let Some(psi) = memory_psi {
                if psi.avg10 > 10.0 {
                    // Critical pressure should produce non-NoAction recommendations
                    let has_action = snapshot.recommendations.iter().any(|r| {
                        !r.contains("NoAction") && !r.contains("no_action")
                    });
                    assert!(has_action,
                        "critical pressure should produce action recommendations, got: {:?}",
                        snapshot.recommendations
                    );
                }
            }
        }
    }
}
