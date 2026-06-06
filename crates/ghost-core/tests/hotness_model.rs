//! Hotness model integration tests.
//!
//! Tests the Temperature enum extension methods and integration between
//! hotness components (summary, confidence, history).

use ghost_core::hotness_provider::{AddressRange, HotnessSample, HotnessSnapshot, Temperature};
use ghost_core::hotness_confidence::{ConfidenceLevel, HotnessConfidence};
use ghost_core::hotness_history::{AccessTrend, HotnessHistory, TemperatureTrend};
use ghost_core::hotness_summary::HotnessSummary;
use ghost_core::types::TierId;

// ─── Helper Functions ─────────────────────────────────────────────────────────

fn make_sample(addr: u64, temp: Temperature, access_count: u64) -> HotnessSample {
    HotnessSample {
        address_range: AddressRange::new(addr, addr + 0x1000),
        access_count,
        temperature: temp,
    }
}

fn make_snapshot(samples: Vec<HotnessSample>, timestamp: u64) -> HotnessSnapshot {
    HotnessSnapshot { samples, timestamp }
}

// ─── Temperature Enum Tests ────────────────────────────────────────────────────

/// Test Temperature::to_tier conversion.
#[test]
fn test_temperature_to_tier() {
    // Hot and Warm map to RAM tier
    assert_eq!(Temperature::Hot.to_tier(), TierId::Ram);
    assert_eq!(Temperature::Warm.to_tier(), TierId::Ram);

    // Cold and Frozen map to Disk tier
    assert_eq!(Temperature::Cold.to_tier(), TierId::Disk);
    assert_eq!(Temperature::Frozen.to_tier(), TierId::Disk);
}

/// Test Temperature::value returns correct ordinal values.
#[test]
fn test_temperature_value() {
    // Note: value() returns semantic ordering (Hot=3 is "hotter" than Cold=1)
    assert_eq!(Temperature::Hot.value(), 3);
    assert_eq!(Temperature::Warm.value(), 2);
    assert_eq!(Temperature::Cold.value(), 1);
    assert_eq!(Temperature::Frozen.value(), 0);
}

/// Test Temperature::is_active returns true for Hot and Warm.
#[test]
fn test_temperature_is_active() {
    assert!(Temperature::Hot.is_active());
    assert!(Temperature::Warm.is_active());
    assert!(!Temperature::Cold.is_active());
    assert!(!Temperature::Frozen.is_active());
}

/// Test Temperature::is_inactive returns true for Cold and Frozen.
#[test]
fn test_temperature_is_inactive() {
    assert!(!Temperature::Hot.is_inactive());
    assert!(!Temperature::Warm.is_inactive());
    assert!(Temperature::Cold.is_inactive());
    assert!(Temperature::Frozen.is_inactive());
}

/// Test Temperature ordering (Hot < Warm < Cold < Frozen due to #[derive(Ord)]).
/// Note: This is the discriminant ordering, not the semantic "hotness" ordering.
#[test]
fn test_temperature_ordering() {
    use std::cmp::Ordering;

    // With #[derive(Ord)], Hot < Warm < Cold < Frozen (discriminant order)
    assert_eq!(Temperature::Hot.partial_cmp(&Temperature::Warm), Some(Ordering::Less));
    assert_eq!(Temperature::Hot.partial_cmp(&Temperature::Cold), Some(Ordering::Less));
    assert_eq!(Temperature::Hot.partial_cmp(&Temperature::Frozen), Some(Ordering::Less));

    // Warm < Cold and Warm < Frozen
    assert_eq!(Temperature::Warm.partial_cmp(&Temperature::Cold), Some(Ordering::Less));
    assert_eq!(Temperature::Warm.partial_cmp(&Temperature::Frozen), Some(Ordering::Less));

    // Cold < Frozen
    assert_eq!(Temperature::Cold.partial_cmp(&Temperature::Frozen), Some(Ordering::Less));

    // Reflexivity
    assert_eq!(Temperature::Hot.partial_cmp(&Temperature::Hot), Some(Ordering::Equal));
    assert_eq!(Temperature::Warm.partial_cmp(&Temperature::Warm), Some(Ordering::Equal));
    assert_eq!(Temperature::Cold.partial_cmp(&Temperature::Cold), Some(Ordering::Equal));
    assert_eq!(Temperature::Frozen.partial_cmp(&Temperature::Frozen), Some(Ordering::Equal));
}

/// Test Temperature ordering with max/min functions.
/// Note: With #[derive(Ord)], Frozen is max and Hot is min.
#[test]
fn test_temperature_ordering_max_min() {
    let temps = [
        Temperature::Frozen,
        Temperature::Cold,
        Temperature::Warm,
        Temperature::Hot,
    ];

    // With #[derive(Ord)], Frozen is the maximum (highest discriminant)
    assert_eq!(temps.iter().max(), Some(&Temperature::Frozen));
    // With #[derive(Ord)], Hot is the minimum (lowest discriminant)
    assert_eq!(temps.iter().min(), Some(&Temperature::Hot));
}

/// Test Temperature::from_access_count.
#[test]
fn test_temperature_from_access_count() {
    assert_eq!(Temperature::from_access_count(100), Temperature::Hot);
    assert_eq!(Temperature::from_access_count(50), Temperature::Warm);
    assert_eq!(Temperature::from_access_count(10), Temperature::Cold);
    assert_eq!(Temperature::from_access_count(0), Temperature::Frozen);
    assert_eq!(Temperature::from_access_count(5), Temperature::Cold);
}

// ─── HotnessSummary Integration Tests ────────────────────────────────────────

/// Test HotnessSummary with various temperature distributions.
#[test]
fn test_hotness_summary_integration() {
    // Create a snapshot with mixed temperatures
    let samples = vec![
        make_sample(0x1000, Temperature::Hot, 100),
        make_sample(0x2000, Temperature::Hot, 90),
        make_sample(0x3000, Temperature::Warm, 50),
        make_sample(0x4000, Temperature::Cold, 10),
        make_sample(0x5000, Temperature::Frozen, 0),
    ];
    let snapshot = make_snapshot(samples, 100);

    let summary = HotnessSummary::from_snapshot(&snapshot);

    // Verify counts
    assert_eq!(summary.hot_count, 2);
    assert_eq!(summary.warm_count, 1);
    assert_eq!(summary.cold_count, 1);
    assert_eq!(summary.frozen_count, 1);

    // Verify dominant temperature
    assert_eq!(summary.dominant_temperature(), Temperature::Hot);

    // Verify workload classification (2 hot + 1 warm = 3 active out of 5 = 60%)
    assert!(summary.is_hot_workload());
    assert!(!summary.is_cold_workload());

    // Verify active/inactive counts
    assert_eq!(summary.active_count(), 3);
    assert_eq!(summary.inactive_count(), 2);
    assert_eq!(summary.active_percentage(), 60.0);
    assert_eq!(summary.inactive_percentage(), 40.0);
}

/// Test HotnessSummary with all frozen (coldest workload).
#[test]
fn test_hotness_summary_all_frozen() {
    let samples = vec![
        make_sample(0x1000, Temperature::Frozen, 0),
        make_sample(0x2000, Temperature::Frozen, 0),
        make_sample(0x3000, Temperature::Frozen, 0),
    ];
    let snapshot = make_snapshot(samples, 100);

    let summary = HotnessSummary::from_snapshot(&snapshot);

    assert_eq!(summary.dominant_temperature(), Temperature::Frozen);
    assert!(!summary.is_hot_workload());
    assert!(summary.is_cold_workload());
    assert_eq!(summary.active_count(), 0);
    assert_eq!(summary.inactive_count(), 3);
}

// ─── HotnessConfidence Integration Tests ──────────────────────────────────────

/// Test HotnessConfidence calculation with no history.
#[test]
fn test_confidence_no_history() {
    let samples = vec![
        make_sample(0x1000, Temperature::Hot, 100),
        make_sample(0x2000, Temperature::Hot, 90),
    ];
    let snapshot = make_snapshot(samples, 100);

    let confidence = HotnessConfidence::calculate(&snapshot, &[]);

    // With only current snapshot, confidence should be lower
    assert!(confidence.score < 1.0);
    assert!(confidence.score >= 0.0);
}

/// Test HotnessConfidence calculation with stable history.
#[test]
fn test_confidence_with_stable_history() {
    // Create multiple snapshots with similar temperatures
    let samples1 = vec![make_sample(0x1000, Temperature::Hot, 100)];
    let samples2 = vec![make_sample(0x1000, Temperature::Hot, 95)];
    let samples3 = vec![make_sample(0x1000, Temperature::Hot, 105)];

    let snapshot1 = make_snapshot(samples1, 100);
    let snapshot2 = make_snapshot(samples2, 200);
    let snapshot3 = make_snapshot(samples3, 300);

    let history = vec![snapshot1, snapshot2];
    let confidence = HotnessConfidence::calculate(&snapshot3, &history);

    // With stable history, confidence should be higher
    assert!(confidence.score > 0.5);
}

/// Test ConfidenceLevel thresholds.
#[test]
fn test_confidence_level_thresholds() {
    assert_eq!(ConfidenceLevel::High.min_score(), 0.8);
    assert_eq!(ConfidenceLevel::Medium.min_score(), 0.5);
    assert_eq!(ConfidenceLevel::Low.min_score(), 0.2);
}

// ─── HotnessHistory Integration Tests ─────────────────────────────────────────

/// Test HotnessHistory with warming trend.
/// Uses 4 samples to avoid flapping (with 3 samples and 2 changes, it would be flapping).
#[test]
fn test_history_warming_trend() {
    let mut history = HotnessHistory::new(10);
    let region = AddressRange::new(0x1000, 0x2000);

    // Add 4 snapshots with increasing temperatures (only 2 changes, not flapping)
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Frozen, 0)],
        100,
    ));
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Frozen, 0)],
        200,
    ));
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Cold, 5)],
        300,
    ));
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Warm, 50)],
        400,
    ));

    match history.get_temperature_trend(&region) {
        TemperatureTrend::Warming(_, _) => {},
        other => panic!("Expected Warming, got {:?}", other),
    }
}

/// Test HotnessHistory with cooling trend.
/// Uses 4 samples to avoid flapping.
#[test]
fn test_history_cooling_trend() {
    let mut history = HotnessHistory::new(10);
    let region = AddressRange::new(0x1000, 0x2000);

    // Add 4 snapshots with decreasing temperatures (only 2 changes, not flapping)
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Hot, 100)],
        100,
    ));
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Hot, 100)],
        200,
    ));
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Warm, 50)],
        300,
    ));
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Cold, 10)],
        400,
    ));

    match history.get_temperature_trend(&region) {
        TemperatureTrend::Cooling(_, _) => {},
        other => panic!("Expected Cooling, got {:?}", other),
    }
}

/// Test HotnessHistory with increasing access trend.
/// Uses more samples and smoother progression to avoid volatility.
#[test]
fn test_history_access_increasing_trend() {
    let mut history = HotnessHistory::new(10);
    let region = AddressRange::new(0x1000, 0x2000);

    // Add 4 snapshots with increasing access counts (slope must exceed 10% of mean)
    // With counts [100, 200, 300, 400], mean=250, threshold=25, slope=100
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Hot, 100)],
        100,
    ));
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Hot, 200)],
        200,
    ));
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Hot, 300)],
        300,
    ));
    history.push(make_snapshot(
        vec![make_sample(0x1000, Temperature::Hot, 400)],
        400,
    ));

    assert_eq!(history.get_access_trend(&region), AccessTrend::Increasing);
}

/// Test HotnessHistory respects max_snapshots limit.
#[test]
fn test_history_max_snapshots_limit() {
    let mut history = HotnessHistory::new(3);

    for i in 0..5 {
        history.push(make_snapshot(
            vec![make_sample(0x1000, Temperature::Hot, i as u64 * 10)],
            i as u64 * 100,
        ));
    }

    // Should only keep the last 3 snapshots
    assert_eq!(history.len(), 3);
}

// ─── Cross-Component Integration Tests ────────────────────────────────────────

/// Test integration between HotnessSummary and HotnessConfidence.
#[test]
fn test_summary_confidence_integration() {
    let samples = vec![
        make_sample(0x1000, Temperature::Hot, 100),
        make_sample(0x2000, Temperature::Hot, 90),
        make_sample(0x3000, Temperature::Warm, 50),
    ];
    let snapshot = make_snapshot(samples, 100);

    let summary = HotnessSummary::from_snapshot(&snapshot);
    let confidence = HotnessConfidence::calculate(&snapshot, &[]);

    // Hot workload should have reasonable confidence
    assert!(summary.is_hot_workload());
    assert!(confidence.score >= 0.0);
    assert!(confidence.score <= 1.0);
}

/// Test integration between HotnessHistory and HotnessConfidence.
#[test]
fn test_history_confidence_integration() {
    let mut history = HotnessHistory::new(5);
    let region = AddressRange::new(0x1000, 0x2000);

    // Build up history with stable temperatures
    for i in 0..3 {
        history.push(make_snapshot(
            vec![make_sample(0x1000, Temperature::Hot, 100)],
            i as u64 * 100,
        ));
    }

    // Create current snapshot
    let current = make_snapshot(
        vec![make_sample(0x1000, Temperature::Hot, 100)],
        300,
    );

    let confidence = HotnessConfidence::calculate(&current, history.snapshots());

    // With history, confidence should be higher
    assert!(confidence.score > 0.5);

    // Temperature trend should be stable
    match history.get_temperature_trend(&region) {
        TemperatureTrend::Stable(_) => {},
        other => panic!("Expected Stable, got {:?}", other),
    }
}

/// Test full pipeline: create snapshot, add to history, get summary and confidence.
#[test]
fn test_full_pipeline() {
    // Create initial history
    let mut history = HotnessHistory::new(10);

    // Add historical snapshots
    for i in 0..3 {
        history.push(make_snapshot(
            vec![
                make_sample(0x1000, Temperature::Hot, 100),
                make_sample(0x2000, Temperature::Warm, 50),
                make_sample(0x3000, Temperature::Cold, 10),
            ],
            i as u64 * 100,
        ));
    }

    // Create current snapshot
    let current = make_snapshot(
        vec![
            make_sample(0x1000, Temperature::Hot, 100),
            make_sample(0x2000, Temperature::Hot, 90),
            make_sample(0x3000, Temperature::Warm, 60),
        ],
        300,
    );

    // Get summary
    let summary = HotnessSummary::from_snapshot(&current);
    assert_eq!(summary.dominant_temperature(), Temperature::Hot);
    assert!(summary.is_hot_workload());

    // Get confidence
    let confidence = HotnessConfidence::calculate(&current, history.snapshots());
    assert!(confidence.score > 0.5);

    // Add to history
    history.push(current);

    // Verify history length
    assert_eq!(history.len(), 4);
}