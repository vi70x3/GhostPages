//! Stress tests for DAMON hotness integration.
//!
//! These tests verify that the system handles high load, rapid changes,
//! and edge cases without performance degradation or data loss.
//!
//! All tests are designed to complete within 30 seconds.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ghost_core::emitter::EventEmitter;
use ghost_core::events::{Event, EventRecord};
use ghost_core::hotness_provider::{HotnessProvider, HotnessSnapshot, Temperature};
use ghost_core::time::{DeterministicTimeProvider, TimeProvider};

use ghost_linux::cooldown::CooldownTracker;
use ghost_linux::damon::{DamonConfig, SimulatedDamonProvider};
use ghost_linux::hotness_provider::{HotnessMetrics, MockHotnessConfig, MockHotnessProvider};
use ghost_linux::policy_rules::{PolicyRules, StabilityConfig};
use ghost_linux::recorder::LinuxRecorder;
use ghost_linux::replayer::LinuxReplayer;
use ghost_linux::stability::StabilityChecker;
use ghost_linux::tier_inventory::TierInventory;

// ─── Test Helpers ───────────────────────────────────────────────────────────────

fn test_time_provider(start: u64) -> Arc<dyn TimeProvider> {
    Arc::new(DeterministicTimeProvider::new(start, Duration::from_millis(100)))
}

fn test_emitter() -> EventEmitter {
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
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

// ─── Test: Large Region Count ───────────────────────────────────────────────────

#[test]
fn test_large_region_count() {
    let start = Instant::now();
    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();

    // Test with 1000+ regions
    let region_counts = [1000, 1500, 2000];

    for &num_regions in &region_counts {
        let config = test_config();
        let damon = SimulatedDamonProvider::new(
            config,
            time_provider.clone(),
            emitter.clone(),
            0xBEEF0007,
            num_regions,
        );

        let sample_start = Instant::now();
        let snapshot = damon.sample().expect("sample should succeed");
        let sample_duration = sample_start.elapsed();

        assert_eq!(snapshot.samples.len(), num_regions,
            "should produce {} samples", num_regions);

        // Verify sampling completes within reasonable time (< 5s for any count)
        assert!(
            sample_duration < Duration::from_secs(5),
            "sampling {} regions took {:?}, expected < 5s",
            num_regions,
            sample_duration
        );

        // Verify temperature distribution is reasonable
        let hot_count = snapshot.samples.iter().filter(|s| s.temperature == Temperature::Hot).count();
        let frozen_count = snapshot.samples.iter().filter(|s| s.temperature == Temperature::Frozen).count();

        // With 1000+ regions, we should have a mix
        assert!(hot_count > 0, "should have some hot regions");
        assert!(frozen_count > 0, "should have some frozen regions");
    }

    // Total test should complete within 30s
    assert!(
        start.elapsed() < Duration::from_secs(30),
        "large region count test took too long: {:?}",
        start.elapsed()
    );
}

// ─── Test: Rapid Workload Changes ───────────────────────────────────────────────

#[test]
fn test_rapid_workload_changes() {
    let start = Instant::now();
    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();

    // Create multiple providers with different configurations to simulate rapid changes
    let hot_config = DamonConfig {
        hot_threshold: 30,
        cold_threshold: 5,
        frozen_threshold: 1,
        ..Default::default()
    };
    let cold_config = DamonConfig {
        hot_threshold: 200,
        cold_threshold: 100,
        frozen_threshold: 1,
        ..Default::default()
    };

    let hot_damon = SimulatedDamonProvider::new(
        hot_config,
        time_provider.clone(),
        emitter.clone(),
        0xBEEF0008,
        64,
    );
    let cold_damon = SimulatedDamonProvider::new(
        cold_config,
        time_provider.clone(),
        emitter.clone(),
        0xBEEF0009,
        64,
    );

    // Test the StabilityChecker with rapid temperature changes
    let mut stability_checker = StabilityChecker::new(StabilityConfig {
        temperature_stability_window: 3,
        hysteresis_margin: 0.2,
        ..Default::default()
    });

    // Test the CooldownTracker with rapid recommendation attempts
    let cooldown_config = StabilityConfig {
        recommendation_cooldown_secs: 2,
        suppression_cooldown_secs: 4,
        ..Default::default()
    };
    let mut cooldown = CooldownTracker::new(cooldown_config, time_provider.clone());

    let mut stable_count = 0usize;
    let mut unstable_count = 0usize;
    let mut cooldown_blocks = 0usize;
    let mut cooldown_allows = 0usize;

    // Alternate between hot and cold workloads rapidly
    for i in 0..50 {
        let snapshot = if i % 2 == 0 {
            hot_damon.sample().expect("hot sample should succeed")
        } else {
            cold_damon.sample().expect("cold sample should succeed")
        };

        // Track temperature stability per region
        for (idx, sample) in snapshot.samples.iter().enumerate().take(4) {
            let region_key = format!("region_{}", idx);
            stability_checker.record(&region_key, sample.temperature);

            if stability_checker.is_stable(&region_key) {
                stable_count += 1;
            } else {
                unstable_count += 1;
            }
        }

        // Test cooldown with per-region keys
        let region_key = format!("region_{}", i % 4);
        if cooldown.can_recommend(&region_key) {
            cooldown_allows += 1;
            cooldown.record_recommendation(&region_key);
        } else {
            cooldown_blocks += 1;
        }

        // Verify snapshot is valid
        assert_eq!(snapshot.samples.len(), 64);
    }

    // With alternating hot/cold, most regions should be unstable
    assert!(
        unstable_count > stable_count,
        "rapid changes should produce more unstable than stable readings (unstable={}, stable={})",
        unstable_count,
        stable_count
    );

    // Cooldown should have blocked some recommendations
    assert!(
        cooldown_blocks > 0,
        "cooldown should block some rapid recommendations, got {} blocks out of 50",
        cooldown_blocks
    );

    // Total test should complete within 30s
    assert!(
        start.elapsed() < Duration::from_secs(30),
        "rapid workload changes test took too long: {:?}",
        start.elapsed()
    );
}

// ─── Test: Pressure Spike During Hotness ────────────────────────────────────────

#[test]
fn test_pressure_spike_during_hotness() {
    let start = Instant::now();
    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();

    let config = test_config();
    let damon = SimulatedDamonProvider::new(
        config,
        time_provider.clone(),
        emitter.clone(),
        0xBEEF000A,
        128,
    );

    let path = temp_path("pressure_spike_hotness.bin");
    let mut recorder = LinuxRecorder::new(&path).expect("recorder should create");

    let mut max_pressure = 0.0f64;
    let mut hotness_during_spike = Vec::new();

    for i in 0..100 {
        let snapshot = damon.sample().expect("sample should succeed");

        // Simulate pressure spike in the middle
        let pressure = match i {
            40..=60 => {
                let spike = 0.5 + (i - 40) as f64 * 0.0225; // Ramp up to 0.95
                spike.min(0.95)
            }
            61..=80 => {
                let decay = 0.95 - (i - 60) as f64 * 0.0375; // Ramp down to 0.2
                decay.max(0.2)
            }
            _ => 0.2,
        };

        if pressure > max_pressure {
            max_pressure = pressure;
        }

        // During pressure spike, record hotness data
        if pressure > 0.7 {
            let hot_count = snapshot.samples.iter()
                .filter(|s| s.temperature == Temperature::Hot)
                .count();
            hotness_during_spike.push(hot_count);
        }

        let event = EventRecord {
            sequence_id: i as u64,
            timestamp: snapshot.timestamp,
            event: Event::MemoryPressureChanged {
                sequence_id: i as u64,
                level: ghost_core::state::PressureState::new(),
                avg10: pressure * 100.0,
                avg60: pressure * 80.0,
                avg300: pressure * 60.0,
                total: 1000,
            },
        };
        recorder.record(&event).expect("record should succeed");
    }
    recorder.close().expect("recorder should close");

    // Verify pressure spike occurred
    assert!(max_pressure >= 0.9, "should have pressure spike >= 0.9, got {}", max_pressure);

    // Verify hotness data was recorded during spike
    assert!(!hotness_during_spike.is_empty(), "should have hotness data during pressure spike");

    // Replay and verify
    let mut replayer = LinuxReplayer::new(&path).expect("replayer should open");
    replayer.load().expect("replayer should load");
    assert_eq!(replayer.event_count(), 100);

    // Total test should complete within 30s
    assert!(
        start.elapsed() < Duration::from_secs(30),
        "pressure spike during hotness test took too long: {:?}",
        start.elapsed()
    );
}

// ─── Test: Sustained High Event Volume ──────────────────────────────────────────

#[test]
fn test_sustained_high_event_volume() {
    let start = Instant::now();
    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();

    let config = test_config();
    let damon = SimulatedDamonProvider::new(
        config,
        time_provider.clone(),
        emitter.clone(),
        0xBEEF000B,
        256,
    );

    let path = temp_path("sustained_volume.bin");
    let mut recorder = LinuxRecorder::new(&path).expect("recorder should create");

    let num_events = 500;
    let mut recorded_events = 0usize;

    let record_start = Instant::now();

    for i in 0..num_events {
        let snapshot = damon.sample().expect("sample should succeed");

        // Record multiple events per sample to increase volume
        for (j, sample) in snapshot.samples.iter().take(4).enumerate() {
            let event = EventRecord {
                sequence_id: (i * 4 + j) as u64,
                timestamp: snapshot.timestamp,
                event: Event::HotnessSummaryUpdated {
                    sequence_id: (i * 4 + j) as u64,
                    hot: if sample.temperature == Temperature::Hot { 1 } else { 0 },
                    warm: if sample.temperature == Temperature::Warm { 1 } else { 0 },
                    cold: if sample.temperature == Temperature::Cold { 1 } else { 0 },
                    frozen: if sample.temperature == Temperature::Frozen { 1 } else { 0 },
                },
            };
            recorder.record(&event).expect("record should succeed");
            recorded_events += 1;
        }
    }

    recorder.close().expect("recorder should close");
    let record_duration = record_start.elapsed();

    // Verify all events were recorded
    assert_eq!(recorded_events, num_events * 4, "should record all events");

    // Verify recording completes within reasonable time
    assert!(
        record_duration < Duration::from_secs(10),
        "recording {} events took {:?}, expected < 10s",
        recorded_events,
        record_duration
    );

    // Replay and verify no events were dropped
    let mut replayer = LinuxReplayer::new(&path).expect("replayer should open");
    replayer.load().expect("replayer should load");

    assert_eq!(
        replayer.event_count(),
        recorded_events,
        "replayed event count should match recorded count"
    );

    // Verify all events can be read
    let replay_start = Instant::now();
    let mut replayed_count = 0;
    while replayer.next().is_some() {
        replayed_count += 1;
    }
    let replay_duration = replay_start.elapsed();

    assert_eq!(replayed_count, recorded_events, "should replay all events");

    // Verify replay completes within reasonable time
    assert!(
        replay_duration < Duration::from_secs(5),
        "replaying {} events took {:?}, expected < 5s",
        recorded_events,
        replay_duration
    );

    // Total test should complete within 30s
    assert!(
        start.elapsed() < Duration::from_secs(30),
        "sustained high event volume test took too long: {:?}",
        start.elapsed()
    );
}
