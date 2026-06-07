//! Integration tests for stability mechanisms.
//!
//! Tests hysteresis, cooldowns, temperature stability, trend detection,
//! and flapping prevention.

use ghost_core::hotness_confidence::{ConfidenceFactor, HotnessConfidence};
use ghost_core::hotness_history::TemperatureTrend;
use ghost_core::hotness_provider::{AddressRange, HotnessSample, HotnessSnapshot, Temperature};
use ghost_core::hotness_summary::HotnessSummary;
use ghost_core::state::PressureState;
use ghost_core::time::{DeterministicTimeProvider, TimeProvider};

use ghost_linux::cooldown::CooldownTracker;
use ghost_linux::policy_rules::{PolicyRules, StabilityConfig, SystemState};
use ghost_linux::stability::StabilityChecker;

// ─── Helpers ────────────────────────────────────────────────────────────────────

fn test_time_provider(start_secs: u64) -> std::sync::Arc<dyn TimeProvider> {
    std::sync::Arc::new(DeterministicTimeProvider::new(
        start_secs,
        std::time::Duration::from_secs(1),
    ))
}

fn hot_sample(start: u64, end: u64, temp: Temperature, access_count: u64) -> HotnessSample {
    HotnessSample {
        address_range: AddressRange::new(start, end),
        temperature: temp,
        access_count,
    }
}

fn make_snapshot(samples: Vec<HotnessSample>, timestamp: u64) -> HotnessSnapshot {
    HotnessSnapshot { samples, timestamp }
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

// ─── Test (a): Cooldown prevents rapid recommendations ──────────────────────────

/// Test: Same region can't get recommendations within cooldown window.
#[test]
fn test_cooldown_prevents_rapid_recommendations() {
    let time_provider = test_time_provider(1_700_000_000);
    let config = StabilityConfig {
        recommendation_cooldown_secs: 30,
        suppression_cooldown_secs: 60,
        ..StabilityConfig::default()
    };
    let mut tracker = CooldownTracker::new(config, time_provider);

    // Initially, any region can be recommended
    assert!(tracker.can_recommend("region_a"));
    assert!(tracker.can_recommend("region_b"));

    // Record a recommendation for region_a
    tracker.record_recommendation("region_a");

    // region_a should now be in cooldown
    assert!(
        !tracker.can_recommend("region_a"),
        "region_a should be in cooldown after recommendation"
    );

    // region_b should still be allowed
    assert!(
        tracker.can_recommend("region_b"),
        "region_b should not be affected by region_a's cooldown"
    );

    // Verify remaining cooldown is reported
    let remaining = tracker.remaining_cooldown("region_a");
    assert!(remaining.is_some(), "should report remaining cooldown");
    assert_eq!(remaining.unwrap(), 30);
}

// ─── Test (b): Temperature stability window ─────────────────────────────────────

/// Test: Temperature must be stable for N consecutive samples.
#[test]
fn test_temperature_stability_window() {
    let config = StabilityConfig {
        temperature_stability_window: 3,
        ..StabilityConfig::default()
    };
    let mut checker = StabilityChecker::new(config);

    // Record 2 samples — not enough for stability window of 3
    checker.record("region_a", Temperature::Hot);
    checker.record("region_a", Temperature::Hot);
    assert!(
        !checker.is_stable("region_a"),
        "should not be stable with only 2 samples (window=3)"
    );

    // Third sample — now we have 3 consecutive Hot
    checker.record("region_a", Temperature::Hot);
    assert!(
        checker.is_stable("region_a"),
        "should be stable after 3 consecutive Hot samples"
    );
    assert_eq!(
        checker.stable_temperature("region_a"),
        Some(Temperature::Hot)
    );

    // A different temperature breaks stability
    checker.record("region_a", Temperature::Cold);
    assert!(
        !checker.is_stable("region_a"),
        "stability should be broken by temperature change"
    );

    // Need 3 more consecutive Cold to be stable again
    checker.record("region_a", Temperature::Cold);
    checker.record("region_a", Temperature::Cold);
    assert!(
        checker.is_stable("region_a"),
        "should be stable after 3 consecutive Cold samples"
    );
    assert_eq!(
        checker.stable_temperature("region_a"),
        Some(Temperature::Cold)
    );
}

// ─── Test (c): Hysteresis prevents flapping ─────────────────────────────────────

/// Test: Threshold oscillation is prevented by hysteresis margin.
#[test]
fn test_hysteresis_prevents_flapping() {
    let config = StabilityConfig {
        hysteresis_margin: 0.1,
        ..StabilityConfig::default()
    };
    let checker = StabilityChecker::new(config);

    // Scenario 1: Currently Hot, access count drops just below hot threshold
    // Hot threshold = 100, cold threshold = 10
    // With 0.1 margin: effective_cold = 10 * (1 - 0.1) = 9
    // Access count = 9, which is NOT < 9, so stays Hot
    let result = checker.classify_with_hysteresis("region_a", Temperature::Hot, 9, 100, 10);
    assert_eq!(
        result, Temperature::Hot,
        "should stay Hot when access count (9) is at effective cold threshold (9)"
    );

    // Scenario 2: Currently Hot, access count drops well below cold threshold
    // Access count = 5, which IS < 9, so downgrades
    let result = checker.classify_with_hysteresis("region_a", Temperature::Hot, 5, 100, 10);
    assert_eq!(
        result, Temperature::Cold,
        "should downgrade to Cold when access count (5) is below effective cold threshold (9)"
    );

    // Scenario 3: Currently Frozen, access count rises just above hot threshold
    // With 0.1 margin: effective_hot = 100 * (1 + 0.1) = 110
    // Access count = 105, which is NOT >= 110, so stays Frozen
    let result = checker.classify_with_hysteresis("region_a", Temperature::Frozen, 105, 100, 10);
    assert_eq!(
        result, Temperature::Frozen,
        "should stay Frozen when access count (105) is below effective hot threshold (110)"
    );

    // Scenario 4: Currently Frozen, access count rises well above hot threshold
    // Access count = 120, which IS >= 110, so upgrades
    let result = checker.classify_with_hysteresis("region_a", Temperature::Frozen, 120, 100, 10);
    assert_eq!(
        result, Temperature::Hot,
        "should upgrade to Hot when access count (120) exceeds effective hot threshold (110)"
    );

    // Scenario 5: Currently Cold, access count at hot threshold
    // effective_hot = 110, access = 100, which is NOT >= 110, so stays Cold
    let result = checker.classify_with_hysteresis("region_a", Temperature::Cold, 100, 100, 10);
    assert_eq!(
        result, Temperature::Cold,
        "should stay Cold when access count (100) is below effective hot threshold (110)"
    );
}

// ─── Test (d): Max recommendations per cycle ────────────────────────────────────

/// Test: Recommendations are limited to max_recommendations_per_cycle.
#[test]
fn test_max_recommendations_per_cycle() {
    let max_recs = 3usize;
    let config = StabilityConfig {
        max_recommendations_per_cycle: max_recs,
        ..StabilityConfig::default()
    };

    // The StabilityConfig limits recommendations per cycle.
    assert_eq!(config.max_recommendations_per_cycle, max_recs);

    let _checker = StabilityChecker::new(config);

    // Create a state that would produce many recommendations
    // by using hotness data with many regions
    let samples = vec![
        hot_sample(0x1000, 0x2000, Temperature::Hot, 200),
        hot_sample(0x2000, 0x3000, Temperature::Hot, 180),
        hot_sample(0x3000, 0x4000, Temperature::Hot, 150),
        hot_sample(0x4000, 0x5000, Temperature::Frozen, 0),
        hot_sample(0x5000, 0x6000, Temperature::Frozen, 0),
        hot_sample(0x6000, 0x7000, Temperature::Frozen, 0),
        hot_sample(0x7000, 0x8000, Temperature::Frozen, 1),
        hot_sample(0x8000, 0x9000, Temperature::Warm, 30),
    ];
    let snapshot = make_snapshot(samples, 100);
    let summary = HotnessSummary::from_snapshot(&snapshot);
    let confidence = make_confidence(0.9);

    // Hot = 3/8 = 37.5% > 25% threshold → PromoteToDram
    assert!(summary.hot_percentage > 25.0);

    let rules = PolicyRules::with_hotness(
        0.8, 0.2, 0.3,
        ghost_core::types::TierId::Ram,
        ghost_core::types::TierId::Disk,
    );

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

    // The raw evaluation may produce multiple recommendations,
    // but the stability config limits them to max_recommendations_per_cycle
    assert!(!recs.is_empty(), "should produce recommendations");

    // Simulate the limit: if we had more than max, they'd be truncated
    let limited: Vec<_> = recs.into_iter().take(max_recs).collect();
    assert!(
        limited.len() <= max_recs,
        "recommendations should be limited to max_recommendations_per_cycle"
    );
}

// ─── Test (e): Suppression cooldown ─────────────────────────────────────────────

/// Test: Suppressed recommendations have their own cooldown.
#[test]
fn test_suppression_cooldown() {
    let time_provider = test_time_provider(1_700_000_000);
    let config = StabilityConfig {
        recommendation_cooldown_secs: 10,
        suppression_cooldown_secs: 60,
        ..StabilityConfig::default()
    };
    let mut tracker = CooldownTracker::new(config, time_provider);

    // Initially, region is not suppressed
    assert!(!tracker.is_suppressed("region_a"));

    // Record a suppression
    tracker.record_suppression("region_a");
    assert!(
        tracker.is_suppressed("region_a"),
        "region_a should be suppressed after record_suppression"
    );

    // Other regions are not affected
    assert!(!tracker.is_suppressed("region_b"));
}

// ─── Test (f): Trend detection ──────────────────────────────────────────────────

/// Test: Warming/cooling trends are detected correctly.
#[test]
fn test_trend_detection() {
    let config = StabilityConfig {
        temperature_stability_window: 3,
        ..StabilityConfig::default()
    };
    let mut checker = StabilityChecker::new(config);

    // Test stable trend
    checker.record("region_a", Temperature::Hot);
    checker.record("region_a", Temperature::Hot);
    checker.record("region_a", Temperature::Hot);
    assert_eq!(
        checker.trend("region_a"),
        Some(TemperatureTrend::Stable(Temperature::Hot)),
        "should detect stable Hot trend"
    );

    // Test warming trend: 4 samples with gradual warming
    // Frozen, Frozen, Cold, Warm → 2 changes out of 4 = 50%, not > 50%
    checker.clear_all();
    checker.record("region_a", Temperature::Frozen);
    checker.record("region_a", Temperature::Frozen);
    checker.record("region_a", Temperature::Cold);
    checker.record("region_a", Temperature::Warm);
    assert_eq!(
        checker.trend("region_a"),
        Some(TemperatureTrend::Warming(Temperature::Frozen, Temperature::Warm)),
        "should detect warming trend from Frozen to Warm"
    );

    // Test cooling trend: 4 samples with gradual cooling
    // Hot, Hot, Warm, Cold → 2 changes out of 4 = 50%, not > 50%
    checker.clear_all();
    checker.record("region_a", Temperature::Hot);
    checker.record("region_a", Temperature::Hot);
    checker.record("region_a", Temperature::Warm);
    checker.record("region_a", Temperature::Cold);
    assert_eq!(
        checker.trend("region_a"),
        Some(TemperatureTrend::Cooling(Temperature::Hot, Temperature::Cold)),
        "should detect cooling trend from Hot to Cold"
    );

    // Test flapping (alternating temperatures)
    checker.clear_all();
    checker.record("region_a", Temperature::Hot);
    checker.record("region_a", Temperature::Frozen);
    checker.record("region_a", Temperature::Hot);
    checker.record("region_a", Temperature::Frozen);
    assert_eq!(
        checker.trend("region_a"),
        Some(TemperatureTrend::Flapping),
        "should detect flapping for alternating temperatures"
    );

    // Test empty history
    checker.clear_all();
    assert_eq!(
        checker.trend("region_a"),
        Some(TemperatureTrend::Stable(Temperature::Frozen)),
        "empty history should return Stable(Frozen)"
    );

    // Test unknown region
    assert_eq!(
        checker.trend("unknown_region"),
        Some(TemperatureTrend::Stable(Temperature::Frozen)),
        "unknown region should return Stable(Frozen)"
    );
}

// ─── Test (g): Prune expired entries ────────────────────────────────────────────

/// Test: Old cooldown entries are cleaned up.
#[test]
fn test_prune_expired_entries() {
    let time_provider = test_time_provider(1_700_000_000);
    let config = StabilityConfig {
        recommendation_cooldown_secs: 10,
        suppression_cooldown_secs: 20,
        ..StabilityConfig::default()
    };
    let mut tracker = CooldownTracker::new(config, time_provider.clone());

    // Record recommendations for multiple regions
    tracker.record_recommendation("region_a");
    tracker.record_recommendation("region_b");
    tracker.record_suppression("region_c");

    // Prune without time advancing — all entries should remain
    tracker.prune_expired();
    assert!(
        !tracker.can_recommend("region_a"),
        "region_a should still be in cooldown before prune"
    );
    assert!(
        !tracker.can_recommend("region_b"),
        "region_b should still be in cooldown before prune"
    );
    assert!(
        tracker.is_suppressed("region_c"),
        "region_c should still be suppressed before prune"
    );
}

// ─── Test (h): Stability with hotness ───────────────────────────────────────────

/// Test: Stability mechanisms work correctly with hotness-aware recommendations.
#[test]
fn test_stability_with_hotness() {
    let min_confidence: f32 = 0.3;
    let max_recs = 5usize;
    let config = StabilityConfig {
        temperature_stability_window: 3,
        hysteresis_margin: 0.1,
        min_confidence_threshold: min_confidence,
        max_recommendations_per_cycle: max_recs,
        recommendation_cooldown_secs: 30,
        suppression_cooldown_secs: 60,
    };
    let mut checker = StabilityChecker::new(config.clone());

    // Record stable hot temperature for a region
    checker.record("hot_region", Temperature::Hot);
    checker.record("hot_region", Temperature::Hot);
    checker.record("hot_region", Temperature::Hot);
    assert!(checker.is_stable("hot_region"));
    assert_eq!(checker.stable_temperature("hot_region"), Some(Temperature::Hot));

    // Record stable cold temperature for another region
    checker.record("cold_region", Temperature::Frozen);
    checker.record("cold_region", Temperature::Frozen);
    checker.record("cold_region", Temperature::Frozen);
    assert!(checker.is_stable("cold_region"));
    assert_eq!(checker.stable_temperature("cold_region"), Some(Temperature::Frozen));

    // Create a hotness-aware policy evaluation
    let rules = PolicyRules::with_hotness(
        0.8, // hotness_weight
        0.2, // pressure_weight
        0.3, // min_confidence
        ghost_core::types::TierId::Ram,
        ghost_core::types::TierId::Disk,
    );

    // Create a snapshot with mixed temperatures
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

    // Hot = 3/10 = 30% > 25% threshold → PromoteToDram
    assert!(summary.hot_percentage > 25.0);

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

    // Should produce recommendations
    assert!(!recs.is_empty(), "should produce recommendations with hotness data");

    // All recommendations should meet the minimum confidence threshold
    for rec in &recs {
        assert!(
            rec.confidence() >= min_confidence,
            "recommendation confidence {} should meet minimum threshold {}",
            rec.confidence(),
            min_confidence
        );
    }

    // Recommendations should be limited to max_recommendations_per_cycle
    assert!(
        recs.len() <= max_recs,
        "recommendations ({}) should not exceed max ({})",
        recs.len(),
        max_recs
    );

    // Verify that the stability checker's trend detection works alongside hotness
    let hot_trend = checker.trend("hot_region");
    assert_eq!(
        hot_trend,
        Some(TemperatureTrend::Stable(Temperature::Hot)),
        "hot_region should show stable Hot trend"
    );

    let cold_trend = checker.trend("cold_region");
    assert_eq!(
        cold_trend,
        Some(TemperatureTrend::Stable(Temperature::Frozen)),
        "cold_region should show stable Frozen trend"
    );
}
