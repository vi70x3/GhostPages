//! Integration tests for DAMON hotness metrics.
//!
//! Verifies that HotnessMetrics, PolicyMetrics, and StabilityMetrics
//! correctly reflect subsystem state without requiring a Prometheus server.

use ghost_core::hotness_provider::{AddressRange, HotnessSample, HotnessSnapshot, Temperature};
use ghost_core::hotness_summary::HotnessSummary;
use ghost_metrics::hotness::HotnessMetrics;
use ghost_metrics::policy::{PolicyMetrics, Recommendation};
use ghost_metrics::stability::StabilityMetrics;
use prometheus::Registry;
use std::time::Duration;

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn create_sample(temp: Temperature, access_count: u64) -> HotnessSample {
    HotnessSample {
        address_range: AddressRange::new(0, 4096),
        temperature: temp,
        access_count,
    }
}

fn create_mixed_snapshot() -> HotnessSnapshot {
    HotnessSnapshot {
        samples: vec![
            HotnessSample {
                address_range: AddressRange::new(0, 4096),
                temperature: Temperature::Hot,
                access_count: 150,
            },
            HotnessSample {
                address_range: AddressRange::new(4096, 8192),
                temperature: Temperature::Warm,
                access_count: 50,
            },
            HotnessSample {
                address_range: AddressRange::new(8192, 12288),
                temperature: Temperature::Cold,
                access_count: 5,
            },
            HotnessSample {
                address_range: AddressRange::new(12288, 16384),
                temperature: Temperature::Frozen,
                access_count: 0,
            },
        ],
        timestamp: 1000,
    }
}

// ─── Hotness Metrics Tests ────────────────────────────────────────────────────

#[test]
fn test_hotness_metrics_update() {
    let registry = Registry::new();
    let metrics = HotnessMetrics::new(&registry).unwrap();

    // Initial state: all gauges at 0
    assert_eq!(metrics.hot_regions.get(), 0);
    assert_eq!(metrics.warm_regions.get(), 0);
    assert_eq!(metrics.cold_regions.get(), 0);
    assert_eq!(metrics.frozen_regions.get(), 0);
    assert_eq!(metrics.hotness_updates_total.get(), 0);

    // Create a mixed snapshot and update metrics
    let snapshot = create_mixed_snapshot();
    let summary = HotnessSummary::from_snapshot(&snapshot);
    metrics.update_from_summary(&summary);

    // Verify gauges reflect the summary
    assert_eq!(metrics.hot_regions.get(), 1);
    assert_eq!(metrics.warm_regions.get(), 1);
    assert_eq!(metrics.cold_regions.get(), 1);
    assert_eq!(metrics.frozen_regions.get(), 1);
    assert_eq!(metrics.hotness_updates_total.get(), 1);

    // Update again with an all-hot snapshot
    let all_hot = HotnessSnapshot {
        samples: vec![
            create_sample(Temperature::Hot, 200),
            create_sample(Temperature::Hot, 300),
            create_sample(Temperature::Hot, 100),
        ],
        timestamp: 2000,
    };
    let summary = HotnessSummary::from_snapshot(&all_hot);
    metrics.update_from_summary(&summary);

    assert_eq!(metrics.hot_regions.get(), 3);
    assert_eq!(metrics.warm_regions.get(), 0);
    assert_eq!(metrics.cold_regions.get(), 0);
    assert_eq!(metrics.frozen_regions.get(), 0);
    assert_eq!(metrics.hotness_updates_total.get(), 2);
}

#[test]
fn test_hotness_metrics_confidence() {
    let registry = Registry::new();
    let metrics = HotnessMetrics::new(&registry).unwrap();

    // Initial confidence is 0
    assert_eq!(metrics.hotness_confidence.get(), 0);

    // Update with various confidence values
    metrics.update_confidence(0.5);
    assert_eq!(metrics.hotness_confidence.get(), 500);

    metrics.update_confidence(0.123);
    assert_eq!(metrics.hotness_confidence.get(), 123);

    metrics.update_confidence(1.0);
    assert_eq!(metrics.hotness_confidence.get(), 1000);

    // Clamp values outside [0, 1]
    metrics.update_confidence(-0.5);
    assert_eq!(metrics.hotness_confidence.get(), 0);

    metrics.update_confidence(1.5);
    assert_eq!(metrics.hotness_confidence.get(), 1000);
}

#[test]
fn test_temperature_transition_counting() {
    let registry = Registry::new();
    let metrics = HotnessMetrics::new(&registry).unwrap();

    // Record various transitions
    metrics.record_temperature_transition("frozen", "cold");
    metrics.record_temperature_transition("cold", "warm");
    metrics.record_temperature_transition("warm", "hot");
    metrics.record_temperature_transition("hot", "warm");
    metrics.record_temperature_transition("warm", "cold");
    metrics.record_temperature_transition("cold", "frozen");

    // Record duplicates
    metrics.record_temperature_transition("cold", "warm");
    metrics.record_temperature_transition("cold", "warm");

    // Verify counts
    assert_eq!(
        metrics
            .temperature_transitions_total
            .with_label_values(&["frozen", "cold"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .temperature_transitions_total
            .with_label_values(&["cold", "warm"])
            .get(),
        3
    );
    assert_eq!(
        metrics
            .temperature_transitions_total
            .with_label_values(&["warm", "hot"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .temperature_transitions_total
            .with_label_values(&["hot", "warm"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .temperature_transitions_total
            .with_label_values(&["warm", "cold"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .temperature_transitions_total
            .with_label_values(&["cold", "frozen"])
            .get(),
        1
    );
}

// ─── Policy Metrics Tests ─────────────────────────────────────────────────────

#[test]
fn test_policy_metrics_record() {
    let registry = Registry::new();
    let metrics = PolicyMetrics::new(&registry).unwrap();

    // Initial state
    assert_eq!(metrics.promotions_total.get(), 0);
    assert_eq!(metrics.demotions_total.get(), 0);
    assert_eq!(metrics.no_action_total.get(), 0);

    // Record a promotion
    metrics.record_recommendation(&Recommendation::promote(0.85));
    assert_eq!(metrics.promotions_total.get(), 1);
    assert_eq!(metrics.recommendation_confidence.get(), 850);
    assert_eq!(
        metrics
            .recommendations_total
            .with_label_values(&["promote"])
            .get(),
        1
    );

    // Record a demotion
    metrics.record_recommendation(&Recommendation::demote(0.72));
    assert_eq!(metrics.demotions_total.get(), 1);
    assert_eq!(metrics.recommendation_confidence.get(), 720);
    assert_eq!(
        metrics
            .recommendations_total
            .with_label_values(&["demote"])
            .get(),
        1
    );

    // Record no-action
    metrics.record_recommendation(&Recommendation::no_action(0.95));
    assert_eq!(metrics.no_action_total.get(), 1);
    assert_eq!(metrics.recommendation_confidence.get(), 950);
    assert_eq!(
        metrics
            .recommendations_total
            .with_label_values(&["no_action"])
            .get(),
        1
    );

    // Record multiple promotions
    metrics.record_recommendation(&Recommendation::promote(0.6));
    metrics.record_recommendation(&Recommendation::promote(0.7));
    assert_eq!(metrics.promotions_total.get(), 3);
}

#[test]
fn test_policy_metrics_suppression_and_cooldown() {
    let registry = Registry::new();
    let metrics = PolicyMetrics::new(&registry).unwrap();

    // Record suppressions
    metrics.record_suppression();
    metrics.record_suppression();
    metrics.record_suppression();
    assert_eq!(metrics.suppressed_recommendations_total.get(), 3);

    // Record cooldown hits
    metrics.record_cooldown_hit();
    metrics.record_cooldown_hit();
    assert_eq!(metrics.cooldown_hits_total.get(), 2);
}

#[test]
fn test_evaluation_duration_histogram() {
    let registry = Registry::new();
    let metrics = PolicyMetrics::new(&registry).unwrap();

    // Record various durations
    metrics.record_evaluation_duration(Duration::from_micros(100));
    metrics.record_evaluation_duration(Duration::from_micros(500));
    metrics.record_evaluation_duration(Duration::from_millis(1));
    metrics.record_evaluation_duration(Duration::from_millis(5));
    metrics.record_evaluation_duration(Duration::from_millis(50));

    // Histogram sample count should be 5
    let histogram_count = metrics.evaluation_duration_seconds.get_sample_count();
    assert_eq!(histogram_count, 5);

    let histogram_sum = metrics.evaluation_duration_seconds.get_sample_sum();
    // 100µs + 500µs + 1ms + 5ms + 50ms = 0.0001 + 0.0005 + 0.001 + 0.005 + 0.05 = 0.0566
    assert!((histogram_sum - 0.0566).abs() < 0.001);
}

// ─── Stability Metrics Tests ──────────────────────────────────────────────────

#[test]
fn test_stability_metrics_cooldown() {
    let registry = Registry::new();
    let metrics = StabilityMetrics::new(&registry).unwrap();

    // Initial state
    assert_eq!(metrics.cooldown_active_regions.get(), 0);

    // Set cooldown regions directly
    metrics.set_cooldown_regions(5);
    assert_eq!(metrics.cooldown_active_regions.get(), 5);

    // Increment/decrement
    metrics.increment_cooldown_regions();
    assert_eq!(metrics.cooldown_active_regions.get(), 6);

    metrics.increment_cooldown_regions();
    assert_eq!(metrics.cooldown_active_regions.get(), 7);

    metrics.decrement_cooldown_regions();
    assert_eq!(metrics.cooldown_active_regions.get(), 6);

    // Update to a new value
    metrics.set_cooldown_regions(0);
    assert_eq!(metrics.cooldown_active_regions.get(), 0);
}

#[test]
fn test_stability_metrics_violations() {
    let registry = Registry::new();
    let metrics = StabilityMetrics::new(&registry).unwrap();

    // Record stability violations
    metrics.record_violation();
    metrics.record_violation();
    assert_eq!(metrics.stability_violations_total.get(), 2);

    // Record hysteresis preventions
    metrics.record_hysteresis_prevention();
    assert_eq!(metrics.hysteresis_preventions_total.get(), 1);

    // Record flapping detections
    metrics.record_flapping();
    metrics.record_flapping();
    metrics.record_flapping();
    assert_eq!(metrics.flapping_detected_total.get(), 3);
}

// ─── Cross-Metric Integration Tests ───────────────────────────────────────────

#[test]
fn test_all_metrics_registered_in_registry() {
    let registry = ghost_metrics::MetricsRegistry::new().unwrap();

    // Exercise IntCounterVec metrics so they appear in gathered output
    registry.hotness.record_temperature_transition("cold", "hot");
    registry
        .policy
        .record_recommendation(&ghost_metrics::policy::Recommendation::promote(0.5));

    let output = registry.gather().unwrap();

    // Verify all hotness metrics appear in gathered output
    assert!(output.contains("ghostpages_hotness_hot_regions"));
    assert!(output.contains("ghostpages_hotness_warm_regions"));
    assert!(output.contains("ghostpages_hotness_cold_regions"));
    assert!(output.contains("ghostpages_hotness_frozen_regions"));
    assert!(output.contains("ghostpages_hotness_updates_total"));
    assert!(output.contains("ghostpages_hotness_confidence"));
    assert!(output.contains("ghostpages_hotness_temperature_transitions_total"));

    // Verify all policy metrics appear
    assert!(output.contains("ghostpages_policy_recommendations_total"));
    assert!(output.contains("ghostpages_policy_promotions_total"));
    assert!(output.contains("ghostpages_policy_demotions_total"));
    assert!(output.contains("ghostpages_policy_no_action_total"));
    assert!(output.contains("ghostpages_policy_recommendation_confidence"));
    assert!(output.contains(
        "ghostpages_policy_suppressed_recommendations_total"
    ));
    assert!(output.contains("ghostpages_policy_cooldown_hits_total"));
    assert!(output.contains("ghostpages_policy_evaluation_duration_seconds"));

    // Verify all stability metrics appear
    assert!(output.contains("ghostpages_stability_cooldown_active_regions"));
    assert!(output.contains("ghostpages_stability_violations_total"));
    assert!(output.contains("ghostpages_stability_hysteresis_preventions_total"));
    assert!(output.contains("ghostpages_stability_flapping_detected_total"));
}

#[test]
fn test_end_to_end_metrics_flow() {
    // Simulate a full cycle: hotness update → policy evaluation → stability check
    let registry = Registry::new();
    let hotness = HotnessMetrics::new(&registry).unwrap();
    let policy = PolicyMetrics::new(&registry).unwrap();
    let stability = StabilityMetrics::new(&registry).unwrap();

    // 1. Hotness tracker processes a snapshot
    let snapshot = create_mixed_snapshot();
    let summary = HotnessSummary::from_snapshot(&snapshot);
    hotness.update_from_summary(&summary);
    hotness.update_confidence(0.87);

    // 2. Policy runtime evaluates and emits a recommendation
    policy.record_recommendation(&Recommendation::promote(0.87));
    policy.record_evaluation_duration(Duration::from_micros(250));

    // 3. Cooldown tracker blocks a recommendation
    policy.record_cooldown_hit();
    stability.increment_cooldown_regions();

    // 4. Stability checker detects flapping
    stability.record_flapping();

    // Verify all metrics are consistent
    assert_eq!(hotness.hot_regions.get(), 1);
    assert_eq!(hotness.hotness_confidence.get(), 870);
    assert_eq!(policy.promotions_total.get(), 1);
    assert_eq!(policy.cooldown_hits_total.get(), 1);
    assert_eq!(stability.cooldown_active_regions.get(), 1);
    assert_eq!(stability.flapping_detected_total.get(), 1);
}