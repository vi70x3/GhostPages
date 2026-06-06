//! End-to-end replay tests for ghost-linux.
//!
//! Tests the complete observation → record → replay → verify pipeline
//! with realistic scenarios.

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
    let (tx, _rx) = tokio::sync::mpsc::channel(512);
    EventEmitter::new(tx)
}

fn record_scans(dir: &std::path::Path, seed: u64, count: usize) -> std::path::PathBuf {
    let path = dir.join(format!("e2e_replay_{}_{}.bin", seed, count));
    let mut scanner = SystemScanner::new(
        test_time_provider(),
        test_emitter(),
        seed,
    );
    let mut recorder = LinuxRecorder::new(&path).expect("recorder should create");

    for _ in 0..count {
        let _ = scanner
            .scan_and_record(&mut recorder)
            .expect("scan should succeed");
    }

    recorder.close().expect("recorder should close");
    path
}

// ─── Test: E2E Observation Replay ────────────────────────────────────────────

#[test]
fn test_e2e_observation_replay() {
    let dir = tempfile::tempdir().unwrap();
    let record_path = dir.path().join("observation_replay.bin");

    // Phase 1: Full observation → record
    let mut scanner = SystemScanner::new(
        test_time_provider(),
        test_emitter(),
        42,
    );

    let mut recorder = LinuxRecorder::new(&record_path).expect("recorder should create");
    let original_snapshot = scanner
        .scan_and_record(&mut recorder)
        .expect("scan should succeed");
    recorder.close().expect("recorder should close");

    // Phase 2: Replay
    let mut replayer = LinuxReplayer::new(&record_path).expect("replayer should open");
    replayer.load().expect("replayer should load");

    assert_eq!(replayer.event_count(), 1);

    let replayed = replayer.next().expect("should have event");

    // Verify timestamp matches
    assert_eq!(replayed.timestamp, original_snapshot.timestamp);

    // Verify the event contains snapshot data
    if let Event::PolicyRecommendationGenerated { recommendations, .. } = &replayed.event {
        assert!(!recommendations.is_empty());
        // The recommendation should contain JSON snapshot data
        let payload: serde_json::Value = serde_json::from_str(&recommendations[0])
            .expect("recommendation should be valid JSON");

        assert!(payload.get("timestamp").is_some());
        assert!(payload.get("psi").is_some());
        assert!(payload.get("meminfo").is_some());
        assert!(payload.get("vmstat").is_some());
        assert!(payload.get("swap").is_some());
        assert!(payload.get("zram").is_some());
        assert!(payload.get("tier_inventory").is_some());
    } else {
        panic!("expected PolicyRecommendationGenerated event");
    }
}

// ─── Test: E2E Policy Replay ─────────────────────────────────────────────────

#[test]
fn test_e2e_policy_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path1 = dir.path().join("policy1.bin");
    let path2 = dir.path().join("policy2.bin");

    // Record system state + recommendations twice with same seed
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

    // Replay both
    let mut replayer1 = LinuxReplayer::new(&path1).expect("replayer1 should open");
    replayer1.load().expect("replayer1 should load");

    let mut replayer2 = LinuxReplayer::new(&path2).expect("replayer2 should open");
    replayer2.load().expect("replayer2 should load");

    // Verify same recommendations
    let result = replayer1.verify_against(&replayer2);
    assert!(result.passed(), "policy replay should be deterministic: {:?}", result);
    assert!(result.recommendation_match, "recommendations should match");
}

// ─── Test: E2E Tier Changes ──────────────────────────────────────────────────

#[test]
fn test_e2e_tier_changes() {
    let dir = tempfile::tempdir().unwrap();
    let record_path = dir.path().join("tier_changes.bin");

    // Record multiple scans to capture tier topology
    let mut scanner = SystemScanner::new(
        test_time_provider(),
        test_emitter(),
        42,
    );

    let mut recorder = LinuxRecorder::new(&record_path).expect("recorder should create");

    // Record 3 scans
    for _ in 0..3 {
        let snapshot = scanner
            .scan_and_record(&mut recorder)
            .expect("scan should succeed");

        // Verify tier inventory is present
        assert!(
            snapshot.tier_inventory.is_some(),
            "scan should produce tier inventory"
        );
    }

    recorder.close().expect("recorder should close");

    // Replay and verify tier topology is consistent
    let mut replayer = LinuxReplayer::new(&record_path).expect("replayer should open");
    replayer.load().expect("replayer should load");

    assert_eq!(replayer.event_count(), 3, "should have 3 recorded scans");

    // Collect all tier inventory data from replay
    let mut tier_snapshots = Vec::new();
    replayer.reset();
    while let Some(event) = replayer.next() {
        if let Event::PolicyRecommendationGenerated { recommendations, .. } = &event.event {
            if let Some(first_rec) = recommendations.first() {
                if let Ok(payload) = serde_json::from_str::<serde_json::Value>(first_rec) {
                    if let Some(tiers) = payload.get("tier_inventory") {
                        tier_snapshots.push(tiers.clone());
                    }
                }
            }
        }
    }

    assert_eq!(tier_snapshots.len(), 3, "should have 3 tier snapshots");

    // All scans with same seed should produce identical tier topology
    if tier_snapshots.len() >= 2 {
        assert_eq!(
            tier_snapshots[0], tier_snapshots[1],
            "tier topology should be deterministic across scans"
        );
        assert_eq!(
            tier_snapshots[1], tier_snapshots[2],
            "tier topology should be deterministic across all scans"
        );
    }
}

// ─── Test: E2E Multi-Scan Determinism ────────────────────────────────────────

#[test]
fn test_e2e_multi_scan_determinism() {
    let dir = tempfile::tempdir().unwrap();

    // Record 5 scans with same seed
    let path1 = record_scans(dir.path(), 42, 5);
    let path2 = record_scans(dir.path(), 42, 5);

    let mut r1 = LinuxReplayer::new(&path1).unwrap();
    r1.load().unwrap();

    let mut r2 = LinuxReplayer::new(&path2).unwrap();
    r2.load().unwrap();

    assert_eq!(r1.event_count(), 5);
    assert_eq!(r2.event_count(), 5);

    let result = r1.verify_against(&r2);
    assert!(result.passed(), "multi-scan replay should be deterministic");
}

// ─── Test: E2E Different Seeds Produce Different Replays ─────────────────────

#[test]
fn test_e2e_different_seeds_diverge() {
    let dir = tempfile::tempdir().unwrap();

    let path1 = record_scans(dir.path(), 42, 3);
    let path2 = record_scans(dir.path(), 99, 3);

    let mut r1 = LinuxReplayer::new(&path1).unwrap();
    r1.load().unwrap();

    let mut r2 = LinuxReplayer::new(&path2).unwrap();
    r2.load().unwrap();

    assert_eq!(r1.event_count(), 3);
    assert_eq!(r2.event_count(), 3);

    let result = r1.verify_against(&r2);
    assert!(!result.events_match, "different seeds should produce different observations");
    assert!(result.divergence_point.is_some(), "should find divergence point");
}
