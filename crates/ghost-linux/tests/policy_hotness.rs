//! Integration tests for hotness-aware policy evaluation.
//!
//! These tests verify that the policy engine correctly incorporates
//! hotness data into recommendations with confidence scoring.

use ghost_core::hotness_confidence::{ConfidenceFactor, HotnessConfidence};
use ghost_core::hotness_provider::{AddressRange, HotnessSample, HotnessSnapshot, Temperature};
use ghost_core::hotness_summary::HotnessSummary;
use ghost_core::state::PressureState;
use ghost_core::types::{ChunkId, TierId};

use ghost_linux::policy::Recommendation;
use ghost_linux::policy_rules::PolicyRules;
use ghost_linux::policy_rules::SystemState;

// ─── Helpers ────────────────────────────────────────────────────────────────────

fn hot_sample(start: u64, end: u64, temp: Temperature, access_count: u64) -> HotnessSample {
    HotnessSample {
        address_range: AddressRange::new(start, end),
        temperature: temp,
        access_count,
    }
}

fn make_snapshot(samples: Vec<HotnessSample>, timestamp: u64) -> HotnessSnapshot {
    HotnessSnapshot {
        samples,
        timestamp,
    }
}

fn make_confidence(score: f32) -> HotnessConfidence {
    HotnessConfidence {
        score,
        factors: vec![
            ConfidenceFactor::SampleCount(10),
            ConfidenceFactor::ObservationDuration(120),
            ConfidenceFactor::AccessStability(0.1),
            ConfidenceFactor::TemperatureStability(0.05),
        ],
    }
}

fn low_pressure_state() -> SystemState {
    SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.2,
            ..Default::default()
        },
        dram_utilization: 0.3,
        swap_utilization: 0.1,
        zram_utilization: Some(0.2),
        io_pressure: PressureState::new(),
        hotness_summary: None,
        hotness_confidence: None,
    }
}

// ─── Test: Hotness-aware evaluation ─────────────────────────────────────────────

/// Test: Hotness data influences recommendations when pressure is low
/// and confidence is high enough to produce actionable hotness recommendations.
#[test]
fn test_hotness_aware_evaluation() {
    // Use rules with high hotness weight so hotness can influence decisions
    let rules = PolicyRules::with_hotness(
        0.8, // hotness_weight
        0.2, // pressure_weight
        0.3, // min_confidence
        TierId::Ram,
        TierId::Disk,
    );

    // Create a snapshot with many frozen regions (> 50% threshold)
    let samples = vec![
        hot_sample(0x1000, 0x2000, Temperature::Frozen, 0),
        hot_sample(0x2000, 0x3000, Temperature::Frozen, 0),
        hot_sample(0x3000, 0x4000, Temperature::Frozen, 0),
        hot_sample(0x4000, 0x5000, Temperature::Frozen, 1),
        hot_sample(0x5000, 0x6000, Temperature::Cold, 3),
        hot_sample(0x6000, 0x7000, Temperature::Hot, 100),
    ];
    let snapshot = make_snapshot(samples, 100);
    let summary = HotnessSummary::from_snapshot(&snapshot);
    let confidence = make_confidence(0.85);

    // Frozen = 4/6 ≈ 67% > 50% threshold
    assert!(summary.frozen_percentage > 50.0);

    // Low pressure so hotness is the primary signal
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.2,
            ..Default::default()
        },
        dram_utilization: 0.3,
        swap_utilization: 0.1,
        zram_utilization: Some(0.2),
        io_pressure: PressureState::new(),
        hotness_summary: Some(summary),
        hotness_confidence: Some(confidence),
    };

    let recs = rules.evaluate(&state);

    // Should have hotness-based recommendations since pressure is low
    // and hotness weight is high
    assert!(
        !recs.is_empty(),
        "should produce recommendations with hotness data"
    );

    // Check that at least one recommendation has hotness-related factors
    let has_hotness_factor = recs.iter().any(|r| {
        r.factors().iter().any(|f| f.contains("hotness") || f.contains("frozen_regions"))
    });
    assert!(
        has_hotness_factor,
        "at least one recommendation should reference hotness factors, got {:?}",
        recs
    );
}

// ─── Test: Confidence filtering ─────────────────────────────────────────────────

/// Test: Low confidence hotness data is ignored.
#[test]
fn test_confidence_filtering() {
    let rules = PolicyRules::new();

    // Create a snapshot with many hot regions
    let samples = vec![
        hot_sample(0x1000, 0x2000, Temperature::Hot, 150),
        hot_sample(0x2000, 0x3000, Temperature::Hot, 120),
        hot_sample(0x3000, 0x4000, Temperature::Hot, 100),
        hot_sample(0x4000, 0x5000, Temperature::Hot, 80),
    ];
    let snapshot = make_snapshot(samples, 100);
    let summary = HotnessSummary::from_snapshot(&snapshot);

    // Low confidence (below min_confidence threshold of 0.3)
    let low_confidence = make_confidence(0.1);

    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
        hotness_summary: Some(summary),
        hotness_confidence: Some(low_confidence),
    };

    let recs = rules.evaluate(&state);

    // Should only have pressure-based recommendations
    // No hotness-based recommendations because confidence is too low
    let has_hotness_factor = recs.iter().any(|r| {
        r.factors().iter().any(|f| f.contains("hotness") || f.contains("hot_regions"))
    });
    assert!(
        !has_hotness_factor,
        "low confidence hotness should not produce hotness-based recommendations, got {:?}",
        recs
    );
}

// ─── Test: Pressure-hotness merge ───────────────────────────────────────────────

/// Test: Conflicting recommendations are merged correctly.
/// Pressure takes precedence when it has higher confidence.
#[test]
fn test_pressure_hotness_merge() {
    let rules = PolicyRules::new();

    // Create a snapshot with many frozen regions (> 50% threshold)
    let samples = vec![
        hot_sample(0x1000, 0x2000, Temperature::Frozen, 0),
        hot_sample(0x2000, 0x3000, Temperature::Frozen, 0),
        hot_sample(0x3000, 0x4000, Temperature::Frozen, 1),
        hot_sample(0x4000, 0x5000, Temperature::Cold, 3),
        hot_sample(0x5000, 0x6000, Temperature::Hot, 100),
    ];
    let snapshot = make_snapshot(samples, 100);
    let summary = HotnessSummary::from_snapshot(&snapshot);
    let confidence = make_confidence(0.9);

    // Frozen = 3/5 = 60% > 50% threshold
    assert!(summary.frozen_percentage > 50.0);

    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
        hotness_summary: Some(summary),
        hotness_confidence: Some(confidence),
    };

    let recs = rules.evaluate(&state);

    // Pressure says MoveToZram (high pressure + ZRAM available)
    // Hotness says MoveToDiskSwap (frozen regions > 50%)
    // These conflict — pressure should win because it has higher confidence (1.0 vs weighted 0.27)
    assert!(
        recs.iter()
            .any(|r| matches!(r, Recommendation::MoveToZram { .. })),
        "pressure-based MoveToZram should be present, got {:?}",
        recs
    );

    // MoveToDiskSwap should NOT be present because pressure wins the conflict
    assert!(
        !recs
            .iter()
            .any(|r| matches!(r, Recommendation::MoveToDiskSwap { .. })),
        "hotness-based MoveToDiskSwap should lose to pressure-based MoveToZram, got {:?}",
        recs
    );
}

// ─── Test: Hot regions prefer DRAM ──────────────────────────────────────────────

/// Test: Hot regions get PromoteToDram recommendation when pressure is low.
#[test]
fn test_hot_regions_prefer_dram() {
    // Use rules with high hotness weight so hotness can influence decisions
    let rules = PolicyRules::with_hotness(
        0.8, // hotness_weight
        0.2, // pressure_weight
        0.3, // min_confidence
        TierId::Ram,
        TierId::Disk,
    );

    // Create a snapshot with many hot regions (> 25% threshold)
    let samples = vec![
        hot_sample(0x1000, 0x2000, Temperature::Hot, 200),
        hot_sample(0x2000, 0x3000, Temperature::Hot, 180),
        hot_sample(0x3000, 0x4000, Temperature::Hot, 150),
        hot_sample(0x4000, 0x5000, Temperature::Warm, 60),
        hot_sample(0x5000, 0x6000, Temperature::Cold, 5),
        hot_sample(0x6000, 0x7000, Temperature::Cold, 2),
        hot_sample(0x7000, 0x8000, Temperature::Frozen, 0),
        hot_sample(0x8000, 0x9000, Temperature::Warm, 30),
        hot_sample(0x9000, 0xA000, Temperature::Cold, 1),
        hot_sample(0xA000, 0xB000, Temperature::Frozen, 0),
    ];
    let snapshot = make_snapshot(samples, 100);
    let summary = HotnessSummary::from_snapshot(&snapshot);
    let confidence = make_confidence(0.9);

    // Hot = 3/10 = 30% > 25% threshold
    assert!(summary.hot_percentage > 25.0);

    // Use low pressure so hotness is the primary signal
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.2,
            ..Default::default()
        },
        dram_utilization: 0.3,
        swap_utilization: 0.1,
        zram_utilization: Some(0.2),
        io_pressure: PressureState::new(),
        hotness_summary: Some(summary),
        hotness_confidence: Some(confidence),
    };

    let recs = rules.evaluate(&state);

    // Should have a PromoteToDram recommendation from hotness
    let promote_count = recs
        .iter()
        .filter(|r| matches!(r, Recommendation::PromoteToDram { .. }))
        .count();
    assert!(
        promote_count > 0,
        "should have at least one PromoteToDram recommendation, got {:?}",
        recs
    );

    // Verify the PromoteToDram has hotness factors
    let hotness_promote = recs.iter().find(|r| matches!(r, Recommendation::PromoteToDram { .. }));
    if let Some(Recommendation::PromoteToDram { factors, confidence, .. }) = hotness_promote {
        assert!(
            factors.iter().any(|f| f.contains("hot_regions")),
            "PromoteToDram should reference hot_regions factor, got {:?}",
            factors
        );
        assert!(
            *confidence > 0.0 && *confidence <= 1.0,
            "confidence should be between 0 and 1, got {}",
            confidence
        );
    }
}

// ─── Test: Frozen regions prefer disk ───────────────────────────────────────────

/// Test: Frozen regions get MoveToDisk recommendation when pressure is low.
#[test]
fn test_frozen_regions_prefer_disk() {
    // Use rules with high hotness weight so hotness can influence decisions
    let rules = PolicyRules::with_hotness(
        0.8, // hotness_weight
        0.2, // pressure_weight
        0.3, // min_confidence
        TierId::Ram,
        TierId::Disk,
    );

    // Create a snapshot with many frozen regions (> 50% threshold)
    let samples = vec![
        hot_sample(0x1000, 0x2000, Temperature::Frozen, 0),
        hot_sample(0x2000, 0x3000, Temperature::Frozen, 0),
        hot_sample(0x3000, 0x4000, Temperature::Frozen, 0),
        hot_sample(0x4000, 0x5000, Temperature::Frozen, 1),
        hot_sample(0x5000, 0x6000, Temperature::Cold, 2),
        hot_sample(0x6000, 0x7000, Temperature::Cold, 5),
        hot_sample(0x7000, 0x8000, Temperature::Warm, 30),
    ];
    let snapshot = make_snapshot(samples, 100);
    let summary = HotnessSummary::from_snapshot(&snapshot);
    let confidence = make_confidence(0.85);

    // Frozen = 4/7 ≈ 57% > 50% threshold
    assert!(summary.frozen_percentage > 50.0);

    // Low pressure so hotness is the primary signal
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.2,
            ..Default::default()
        },
        dram_utilization: 0.3,
        swap_utilization: 0.1,
        zram_utilization: Some(0.2),
        io_pressure: PressureState::new(),
        hotness_summary: Some(summary),
        hotness_confidence: Some(confidence),
    };

    let recs = rules.evaluate(&state);

    // Should have a MoveToDiskSwap recommendation from hotness
    let disk_count = recs
        .iter()
        .filter(|r| matches!(r, Recommendation::MoveToDiskSwap { .. }))
        .count();
    assert!(
        disk_count > 0,
        "should have at least one MoveToDiskSwap recommendation for frozen regions, got {:?}",
        recs
    );

    // Verify the MoveToDiskSwap has frozen factors
    let frozen_disk = recs.iter().find(|r| matches!(r, Recommendation::MoveToDiskSwap { .. }));
    if let Some(Recommendation::MoveToDiskSwap { factors, confidence, .. }) = frozen_disk {
        assert!(
            factors.iter().any(|f| f.contains("frozen_regions")),
            "MoveToDiskSwap should reference frozen_regions factor, got {:?}",
            factors
        );
        assert!(
            *confidence > 0.0 && *confidence <= 1.0,
            "confidence should be between 0 and 1, got {}",
            confidence
        );
    }
}

// ─── Test: No hotness data ──────────────────────────────────────────────────────

/// Test: Falls back to pressure-only when no hotness data.
#[test]
fn test_no_hotness_data() {
    let rules = PolicyRules::new();

    // State without hotness data
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
        hotness_summary: None,
        hotness_confidence: None,
    };

    let recs = rules.evaluate(&state);

    // Should still produce pressure-based recommendations
    assert!(
        recs.iter()
            .any(|r| matches!(r, Recommendation::MoveToZram { .. })),
        "should produce MoveToZram for high pressure with ZRAM, got {:?}",
        recs
    );

    // No hotness factors should appear
    let has_hotness_factor = recs.iter().any(|r| {
        r.factors().iter().any(|f| f.contains("hotness") || f.contains("hot_regions") || f.contains("frozen_regions"))
    });
    assert!(
        !has_hotness_factor,
        "no hotness factors should appear without hotness data, got {:?}",
        recs
    );
}

// ─── Test: Confidence scoring ───────────────────────────────────────────────────

/// Test: Recommendations include confidence scores.
#[test]
fn test_confidence_scoring() {
    // Use rules with high hotness weight
    let rules = PolicyRules::with_hotness(
        0.8, // hotness_weight
        0.2, // pressure_weight
        0.3, // min_confidence
        TierId::Ram,
        TierId::Disk,
    );

    // Create a snapshot with many frozen regions (> 50% threshold)
    let samples = vec![
        hot_sample(0x1000, 0x2000, Temperature::Frozen, 0),
        hot_sample(0x2000, 0x3000, Temperature::Frozen, 0),
        hot_sample(0x3000, 0x4000, Temperature::Frozen, 0),
        hot_sample(0x4000, 0x5000, Temperature::Frozen, 1),
        hot_sample(0x5000, 0x6000, Temperature::Cold, 2),
        hot_sample(0x6000, 0x7000, Temperature::Cold, 5),
        hot_sample(0x7000, 0x8000, Temperature::Warm, 30),
    ];
    let snapshot = make_snapshot(samples, 100);
    let summary = HotnessSummary::from_snapshot(&snapshot);
    let confidence = make_confidence(0.75);

    // Low pressure so hotness is the primary signal
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.2,
            ..Default::default()
        },
        dram_utilization: 0.3,
        swap_utilization: 0.1,
        zram_utilization: Some(0.2),
        io_pressure: PressureState::new(),
        hotness_summary: Some(summary),
        hotness_confidence: Some(confidence),
    };

    let recs = rules.evaluate(&state);

    // All recommendations should have confidence scores
    for rec in &recs {
        let conf = rec.confidence();
        assert!(
            conf >= 0.0 && conf <= 1.0,
            "confidence should be between 0 and 1, got {} for {:?}",
            conf,
            rec
        );
    }

    // Hotness-based recommendations should have weighted confidence (< 1.0)
    let hotness_recs: Vec<_> = recs
        .iter()
        .filter(|r| {
            r.factors()
                .iter()
                .any(|f| f.contains("hotness_confidence") || f.contains("frozen_regions"))
        })
        .collect();

    assert!(
        !hotness_recs.is_empty(),
        "should have at least one hotness-based recommendation, got {:?}",
        recs
    );

    for rec in &hotness_recs {
        assert!(
            rec.confidence() < 1.0,
            "hotness-based recommendation should have confidence < 1.0 (weighted), got {} for {:?}",
            rec.confidence(),
            rec
        );
        assert!(
            rec.confidence() > 0.0,
            "hotness-based recommendation should have confidence > 0.0, got {} for {:?}",
            rec.confidence(),
            rec
        );
    }
}
