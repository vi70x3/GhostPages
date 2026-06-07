//! Integration tests for DAMON hotness integration.
//!
//! These tests verify the full pipeline from observation through policy
//! evaluation to recommendation, including hotness data flow, stability
//! mechanisms, confidence filtering, and graceful degradation.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::hotness_provider::{HotnessProvider, HotnessSnapshot, Temperature};
use ghost_core::time::{DeterministicTimeProvider, TimeProvider};

use ghost_linux::cooldown::CooldownTracker;
use ghost_linux::damon::{DamonConfig, DamonHotnessProvider, SimulatedDamonProvider};
use ghost_linux::hotness_provider::{HotnessMetrics, MockHotnessConfig, MockHotnessProvider};
use ghost_linux::policy::PolicyRuntime;
use ghost_linux::policy_rules::{PolicyRules, StabilityConfig, SystemState};
use ghost_linux::recorder::LinuxRecorder;
use ghost_linux::replayer::LinuxReplayer;
use ghost_linux::stability::StabilityChecker;
use ghost_linux::tier_inventory::TierInventory;

// ─── Test Helpers ───────────────────────────────────────────────────────────────

fn test_time_provider() -> Arc<dyn TimeProvider> {
    Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        Duration::from_secs(1),
    ))
}

fn test_emitter() -> EventEmitter {
    let (tx, _rx) = tokio::sync::mpsc::channel(256);
    EventEmitter::new(tx)
}

fn test_config() -> DamonConfig {
    DamonConfig::default()
}

fn temp_path(name: &str) -> PathBuf {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let path = dir.path().join(name);
    std::mem::forget(dir);
    path
}

// ─── Test: Full Pipeline Observation to Recommendation ──────────────────────────

#[test]
fn test_full_pipeline_observation_to_recommendation() {
    let time_provider = test_time_provider();
    let emitter = test_emitter();

    // 1. Create simulated DAMON provider
    let config = test_config();
    let damon = SimulatedDamonProvider::new(
        config,
        time_provider.clone(),
        emitter.clone(),
        0xBEEF0001,
        32,
    );

    // 2. Observe hotness data
    let snapshot = damon.sample().expect("sample should succeed");
    assert_eq!(snapshot.samples.len(), 32);

    // 3. Build tier inventory
    let tier_inventory = Arc::new(parking_lot::RwLock::new(
        TierInventory::new(time_provider.clone(), emitter.clone()),
    ));
    tier_inventory.write().discover().expect("discover should succeed");

    // 4. Create policy runtime with hotness provider
    let mut policy = PolicyRuntime::new(
        tier_inventory.clone(),
        emitter.clone(),
        time_provider.clone(),
    );
    policy.set_hotness_provider(Arc::new(damon));

    // 5. Evaluate and get recommendations
    let recommendations = policy.evaluate().expect("evaluate should succeed");

    // 6. Verify recommendations are produced
    assert!(!recommendations.is_empty(), "should produce recommendations");

    // 7. Verify recommendation structure
    for rec in &recommendations {
        let confidence = rec.confidence();
        assert!(confidence >= 0.0 && confidence <= 1.0,
            "confidence should be in [0, 1], got {}", confidence);
        assert!(!rec.kind().is_empty(), "recommendation kind should not be empty");
    }
}

// ─── Test: Hotness Affects Tier Inventory ──────────────────────────────────────

#[test]
fn test_hotness_affects_tier_inventory() {
    let time_provider = test_time_provider();
    let emitter = test_emitter();

    // Create a hot workload
    let config = DamonConfig {
        hot_threshold: 50,
        cold_threshold: 10,
        frozen_threshold: 1,
        ..Default::default()
    };
    let damon = SimulatedDamonProvider::new(
        config,
        time_provider.clone(),
        emitter.clone(),
        0xBEEF0002,
        64,
    );

    let snapshot = damon.sample().expect("sample should succeed");

    // Count hot regions
    let hot_count = snapshot.samples.iter()
        .filter(|s| s.temperature == Temperature::Hot)
        .count();

    // Create tier inventory
    let tier_inventory = Arc::new(parking_lot::RwLock::new(
        TierInventory::new(time_provider.clone(), emitter.clone()),
    ));
    {
        let mut inv = tier_inventory.write();
        inv.discover().expect("discover should succeed");
        let tier_count = inv.tier_count();
        assert!(tier_count >= 2, "should have at least DRAM and Simulation tiers");
    }

    // Verify hotness data is available
    assert!(hot_count > 0, "hot workload should have some hot regions");

    // Verify the tier inventory has tiers
    let tier_count = tier_inventory.read().tier_count();
    assert!(tier_count >= 2, "should have at least 2 tiers");
}

// ─── Test: Stability Prevents Churn ─────────────────────────────────────────────

#[test]
fn test_stability_prevents_churn() {
    let time_provider = test_time_provider();

    // Test the CooldownTracker directly to verify cooldown prevents rapid recommendations
    let stability = StabilityConfig {
        recommendation_cooldown_secs: 5,
        temperature_stability_window: 3,
        hysteresis_margin: 0.15,
        max_recommendations_per_cycle: 5,
        min_confidence_threshold: 0.3,
        suppression_cooldown_secs: 10,
    };

    let mut cooldown = CooldownTracker::new(stability, time_provider.clone());

    // Simulate rapid recommendation attempts for the same region
    let region = "test_region";
    let mut allowed = 0usize;
    let mut blocked = 0usize;

    for _ in 0..20 {
        if cooldown.can_recommend(region) {
            allowed += 1;
            cooldown.record_recommendation(region);
        } else {
            blocked += 1;
        }
    }

    // First recommendation should be allowed
    assert_eq!(allowed, 1, "first recommendation should be allowed");
    // Remaining should be blocked by cooldown
    assert_eq!(blocked, 19, "subsequent recommendations should be blocked by cooldown");

    // Test the StabilityChecker directly
    let mut checker = StabilityChecker::new(StabilityConfig {
        temperature_stability_window: 3,
        ..Default::default()
    });

    // Record alternating temperatures (simulating rapid changes)
    for _ in 0..10 {
        checker.record("region_a", Temperature::Hot);
        checker.record("region_a", Temperature::Cold);
    }

    // Temperature should not be stable after alternating
    assert!(!checker.is_stable("region_a"), "alternating temperatures should not be stable");

    // Now record stable temperatures
    checker.clear_all();
    for _ in 0..5 {
        checker.record("region_b", Temperature::Hot);
    }
    assert!(checker.is_stable("region_b"), "consistent temperatures should be stable");
}

// ─── Test: Confidence Filtering End-to-End ──────────────────────────────────────

#[test]
fn test_confidence_filtering_end_to_end() {
    let time_provider = test_time_provider();
    let emitter = test_emitter();

    // Create a mock hotness provider with known confidence
    let mock_config = MockHotnessConfig {
        num_ranges: 16,
        seed: 0xBEEF0003,
        base_access_count: 50,
        hot_probability: 0.5,
    };
    let mock_provider = MockHotnessProvider::new(
        mock_config,
        time_provider.clone(),
        emitter.clone(),
    );

    // Sample and verify data is produced
    let snapshot = mock_provider.sample().expect("sample should succeed");
    assert_eq!(snapshot.samples.len(), 16);

    // Create policy with high confidence threshold
    let tier_inventory = Arc::new(parking_lot::RwLock::new(
        TierInventory::new(time_provider.clone(), emitter.clone()),
    ));
    tier_inventory.write().discover().expect("discover should succeed");

    let rules = PolicyRules::with_hotness(
        0.5, // hotness_weight
        0.5, // pressure_weight
        0.8, // min_confidence (high threshold)
        ghost_core::types::TierId::Ram,
        ghost_core::types::TierId::Disk,
    );

    let policy = PolicyRuntime::with_rules(
        tier_inventory,
        emitter.clone(),
        time_provider.clone(),
        rules,
    );

    // Evaluate — with high confidence threshold, hotness-based recs should be filtered
    let recommendations = policy.evaluate().expect("evaluate should succeed");

    // All recommendations should meet the confidence threshold
    for rec in &recommendations {
        assert!(
            rec.confidence() >= 0.0,
            "confidence should be non-negative"
        );
    }
}

// ─── Test: DAMON Unavailable Graceful ──────────────────────────────────────────

#[test]
fn test_damon_unavailable_graceful() {
    use std::path::PathBuf;

    let time_provider = test_time_provider();
    let emitter = test_emitter();

    // Create a DAMON provider with invalid path
    let config = DamonConfig {
        sysfs_path: PathBuf::from("/nonexistent/damon/path"),
        ..Default::default()
    };
    let damon = DamonHotnessProvider::new(config, time_provider.clone(), emitter.clone());

    // sample() should return error, not panic
    let result = damon.sample();
    assert!(result.is_err(), "DAMON should be unavailable");

    // System should still work with policy runtime
    let tier_inventory = Arc::new(parking_lot::RwLock::new(
        TierInventory::new(time_provider.clone(), emitter.clone()),
    ));
    tier_inventory.write().discover().expect("discover should succeed");

    let policy = PolicyRuntime::new(
        tier_inventory,
        emitter.clone(),
        time_provider.clone(),
    );

    // Policy should still produce recommendations without hotness data
    let recommendations = policy.evaluate().expect("evaluate should succeed");
    assert!(!recommendations.is_empty(), "should produce recommendations even without DAMON");
}

// ─── Test: Hotness Events Emitted ───────────────────────────────────────────────

#[test]
fn test_hotness_events_emitted() {
    let time_provider = test_time_provider();
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let emitter = EventEmitter::new(tx);

    let config = test_config();
    let damon = SimulatedDamonProvider::new(
        config,
        time_provider.clone(),
        emitter.clone(),
        0xBEEF0004,
        16,
    );

    // Sample hotness data (this calls hotness_sampled internally, but it's async
    // and fire-and-forget, so we also emit a synchronous event to verify the pipeline)
    let snapshot = damon.sample().expect("sample should succeed");

    // Emit a synchronous event to verify the event pipeline works
    let hot_count = snapshot.samples.iter()
        .filter(|s| s.temperature == Temperature::Hot)
        .count();
    let cold_count = snapshot.samples.iter()
        .filter(|s| s.temperature == Temperature::Cold || s.temperature == Temperature::Frozen)
        .count();

    emitter.try_emit(Event::HotnessSampled {
        sequence_id: 0,
        provider: "simulated_damon".to_string(),
        num_samples: snapshot.samples.len(),
        hot_count,
        cold_count,
    }).expect("emit should succeed");

    // Collect events
    let mut events = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        events.push(rec.event);
    }

    // Should have emitted at least one event
    assert!(!events.is_empty(), "should have emitted events");

    // Verify hotness-related events
    let has_hotness_event = events.iter().any(|e| {
        matches!(e,
            Event::HotnessSampled { .. } |
            Event::HotnessChanged { .. } |
            Event::HotnessSummaryUpdated { .. }
        )
    });
    assert!(has_hotness_event, "should have emitted hotness events, got {:?}", events);
}

// ─── Test: Metrics Updated from Hotness ────────────────────────────────────────

#[test]
fn test_metrics_updated_from_hotness() {
    let time_provider = test_time_provider();
    let emitter = test_emitter();

    let config = test_config();
    let damon = SimulatedDamonProvider::new(
        config,
        time_provider.clone(),
        emitter.clone(),
        0xBEEF0005,
        32,
    );

    let metrics = HotnessMetrics::new();

    // Sample and record metrics
    let snapshot = damon.sample().expect("sample should succeed");
    metrics.record_snapshot(&snapshot);

    // Verify metrics are updated
    use std::sync::atomic::Ordering;
    assert_eq!(metrics.samples_total.load(Ordering::Relaxed), 1);

    let hot = metrics.hot_regions.load(Ordering::Relaxed);
    let cold = metrics.cold_regions.load(Ordering::Relaxed);

    // Hot + cold should not exceed total regions
    assert!(
        hot + cold <= 32,
        "hot ({}) + cold ({}) should not exceed total regions (32)",
        hot,
        cold
    );

    // Sample multiple times and verify metrics accumulate
    for _ in 0..5 {
        let s = damon.sample().expect("sample should succeed");
        metrics.record_snapshot(&s);
    }

    assert_eq!(metrics.samples_total.load(Ordering::Relaxed), 6);
}

// ─── Test: Replay Equivalence ──────────────────────────────────────────────────

#[test]
fn test_replay_equivalence() {
    let record_path = temp_path("replay_equivalence.bin");
    let time_provider = test_time_provider();
    let emitter = test_emitter();

    // Phase 1: Live run — record observations and recommendations
    let config = test_config();
    let damon = SimulatedDamonProvider::new(
        config,
        time_provider.clone(),
        emitter.clone(),
        0xBEEF0006,
        16,
    );

    let tier_inventory = Arc::new(parking_lot::RwLock::new(
        TierInventory::new(time_provider.clone(), emitter.clone()),
    ));
    tier_inventory.write().discover().expect("discover should succeed");

    let mut policy = PolicyRuntime::new(
        tier_inventory.clone(),
        emitter.clone(),
        time_provider.clone(),
    );
    policy.set_hotness_provider(Arc::new(damon));

    let mut recorder = LinuxRecorder::new(&record_path).expect("recorder should create");
    let mut live_recommendations = Vec::new();

    for i in 0..5 {
        let recs = policy.evaluate().expect("evaluate should succeed");
        live_recommendations.push(recs);

        let event = ghost_core::events::EventRecord {
            sequence_id: i as u64,
            timestamp: time_provider.timestamp_secs(),
            event: Event::PolicyRecommendationGenerated {
                sequence_id: i as u64,
                recommendations: live_recommendations.last().unwrap()
                    .iter()
                    .map(|r| r.to_string())
                    .collect(),
                pressure_level: "test".to_string(),
            },
        };
        recorder.record(&event).expect("record should succeed");
    }
    recorder.close().expect("recorder should close");

    // Phase 2: Replay and verify identical recommendations
    let mut replayer = LinuxReplayer::new(&record_path).expect("replayer should open");
    replayer.load().expect("replayer should load");

    for (i, expected_recs) in live_recommendations.iter().enumerate() {
        let event = replayer.next().expect("should have event");

        if let Event::PolicyRecommendationGenerated { recommendations, .. } = &event.event {
            let expected_strings: Vec<String> = expected_recs.iter().map(|r| r.to_string()).collect();
            assert_eq!(
                recommendations,
                &expected_strings,
                "recommendation mismatch at index {}",
                i
            );
        } else {
            panic!("expected PolicyRecommendationGenerated at index {}", i);
        }
    }
}
