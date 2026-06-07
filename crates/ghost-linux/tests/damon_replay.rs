//! Replay tests for DAMON hotness integration.
//!
//! These tests verify that hotness data can be recorded and replayed
//! deterministically, producing identical results across runs.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::{Event, EventRecord};
use ghost_core::hotness_provider::{HotnessProvider, HotnessSnapshot, Temperature};
use ghost_core::time::{DeterministicTimeProvider, TimeProvider};
use ghost_core::types::ChunkId;

use ghost_linux::damon::{DamonConfig, SimulatedDamonProvider};
use ghost_linux::recorder::LinuxRecorder;
use ghost_linux::replayer::{LinuxReplayer, ReplayVerificationResult};

// ─── Test Helpers ───────────────────────────────────────────────────────────────

fn test_time_provider(start: u64) -> Arc<dyn TimeProvider> {
    Arc::new(DeterministicTimeProvider::new(
        start,
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
    // Leak the dir so the file persists for the test
    std::mem::forget(dir);
    path
}

/// Record a series of hotness samples to a file.
fn record_hotness_samples(
    path: &PathBuf,
    seed: u64,
    num_regions: usize,
    num_samples: usize,
) -> Vec<HotnessSnapshot> {
    let config = test_config();
    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();
    let provider = SimulatedDamonProvider::new(config, time_provider.clone(), emitter, seed, num_regions);

    let mut recorder = LinuxRecorder::new(path).expect("recorder should create");
    let mut snapshots = Vec::with_capacity(num_samples);

    for i in 0..num_samples {
        let snapshot = provider.sample().expect("sample should succeed");
        let first = snapshot.samples.first();
        let chunk_id = ChunkId::from_data(&i.to_le_bytes());
        let old_temp = format!("{:?}", first.map(|s| s.temperature).unwrap_or(Temperature::Frozen));
        let new_temp = format!("{:?}", first.map(|s| s.temperature).unwrap_or(Temperature::Warm));

        let event = EventRecord {
            sequence_id: i as u64,
            timestamp: snapshot.timestamp,
            event: Event::HotnessChanged {
                sequence_id: i as u64,
                chunk_id,
                old_temp,
                new_temp,
            },
        };
        recorder.record(&event).expect("record should succeed");
        snapshots.push(snapshot);
    }

    recorder.close().expect("recorder should close");
    snapshots
}

// ─── Test: Hotness Stream Replay ───────────────────────────────────────────────

#[test]
fn test_hotness_stream_replay() {
    let path = temp_path("hotness_stream_replay.bin");
    let seed = 0xDEADBEEF;
    let num_regions = 16;
    let num_samples = 10;

    // Phase 1: Record hotness samples
    let original_snapshots = record_hotness_samples(&path, seed, num_regions, num_samples);

    // Phase 2: Replay and verify
    let mut replayer = LinuxReplayer::new(&path).expect("replayer should open");
    replayer.load().expect("replayer should load");

    assert_eq!(
        replayer.event_count(),
        num_samples,
        "should have recorded {} events",
        num_samples
    );

    // Verify each replayed event matches the original
    for (i, original_snapshot) in original_snapshots.iter().enumerate() {
        let event = replayer.next().expect("should have event");
        let expected_seq = (i + 1) as u64;
        assert_eq!(event.sequence_id, expected_seq, "sequence ID mismatch at index {}", i);

        if let Event::HotnessChanged { chunk_id, new_temp, .. } = &event.event {
            let expected_chunk = ChunkId::from_data(&i.to_le_bytes());
            assert_eq!(*chunk_id, expected_chunk, "chunk_id mismatch at index {}", i);
            let expected_temp = format!("{:?}", original_snapshot.samples.first().map(|s| s.temperature).unwrap_or(Temperature::Warm));
            assert_eq!(new_temp, &expected_temp, "temperature mismatch at index {}", i);
        } else {
            panic!("expected HotnessChanged event at index {}", i);
        }
    }

    // No more events
    assert!(replayer.next().is_none(), "should have no more events");
}

// ─── Test: Recommendation Replay ───────────────────────────────────────────────

#[test]
fn test_recommendation_replay() {
    let path = temp_path("recommendation_replay.bin");
    let seed = 0xDEAD004D;
    let num_regions = 32;
    let num_samples = 5;

    let config = test_config();
    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();
    let provider = SimulatedDamonProvider::new(config, time_provider.clone(), emitter, seed, num_regions);

    let mut recorder = LinuxRecorder::new(&path).expect("recorder should create");
    let mut original_recommendations: Vec<Vec<String>> = Vec::new();

    for i in 0..num_samples {
        let snapshot = provider.sample().expect("sample should succeed");

        // Generate recommendations based on snapshot
        let hot_count = snapshot.samples.iter().filter(|s| s.temperature == Temperature::Hot).count();
        let cold_count = snapshot.samples.iter().filter(|s| s.temperature == Temperature::Cold || s.temperature == Temperature::Frozen).count();

        let mut recommendations = Vec::new();
        if hot_count > num_regions / 4 {
            recommendations.push(format!("promote_hot:{}:regions", hot_count));
        }
        if cold_count > num_regions / 2 {
            recommendations.push(format!("evict_cold:{}:regions", cold_count));
        }
        if recommendations.is_empty() {
            recommendations.push("no_action".to_string());
        }

        let event = EventRecord {
            sequence_id: i as u64,
            timestamp: snapshot.timestamp,
            event: Event::PolicyRecommendationGenerated {
                sequence_id: i as u64,
                recommendations: recommendations.clone(),
                pressure_level: "test".to_string(),
            },
        };
        recorder.record(&event).expect("record should succeed");
        original_recommendations.push(recommendations);
    }
    recorder.close().expect("recorder should close");

    // Replay and verify recommendations match
    let mut replayer = LinuxReplayer::new(&path).expect("replayer should open");
    replayer.load().expect("replayer should load");

    for (i, expected_recs) in original_recommendations.iter().enumerate() {
        let event = replayer.next().expect("should have event");

        if let Event::PolicyRecommendationGenerated { recommendations, .. } = &event.event {
            assert_eq!(
                recommendations,
                expected_recs,
                "recommendations mismatch at index {}",
                i
            );
        } else {
            panic!("expected PolicyRecommendationGenerated at index {}", i);
        }
    }
}

// ─── Test: Deterministic Replay ─────────────────────────────────────────────────

#[test]
fn test_deterministic_replay() {
    let path1 = temp_path("deterministic_replay_1.bin");
    let path2 = temp_path("deterministic_replay_2.bin");
    let seed = 0xC0DE;
    let num_regions = 16;
    let num_samples = 8;

    // Record the same workload twice with the same seed
    record_hotness_samples(&path1, seed, num_regions, num_samples);
    record_hotness_samples(&path2, seed, num_regions, num_samples);

    // Load both replays
    let mut replayer1 = LinuxReplayer::new(&path1).expect("replayer1 should open");
    replayer1.load().expect("replayer1 should load");

    let mut replayer2 = LinuxReplayer::new(&path2).expect("replayer2 should open");
    replayer2.load().expect("replayer2 should load");

    // Verify identical replays
    let result: ReplayVerificationResult = replayer1.verify_against(&replayer2);
    assert!(result.passed(), "replays should be deterministic: {:?}", result);
    assert!(result.events_match, "events should match");
    assert!(result.ordering_match, "ordering should match");
    assert!(result.recommendation_match, "recommendations should match");
    assert!(result.divergence_point.is_none(), "should have no divergence");
}

// ─── Test: Hot Workload Fixture ─────────────────────────────────────────────────

#[test]
fn test_hot_workload_fixture() {
    let path = temp_path("hot_workload_fixture.bin");

    // Generate a hot workload: use low hot_threshold so most regions are hot
    let config = DamonConfig {
        hot_threshold: 50,
        cold_threshold: 10,
        frozen_threshold: 1,
        ..Default::default()
    };
    let seed = 0x484F5400; // "HOT\0"
    let num_regions = 32;
    let num_samples = 10;

    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();
    let provider = SimulatedDamonProvider::new(config, time_provider, emitter, seed, num_regions);

    let mut recorder = LinuxRecorder::new(&path).expect("recorder should create");
    let mut total_hot = 0usize;
    let mut total_samples = 0usize;

    for i in 0..num_samples {
        let snapshot = provider.sample().expect("sample should succeed");
        let hot_count = snapshot.samples.iter().filter(|s| s.temperature == Temperature::Hot).count();
        total_hot += hot_count;
        total_samples += snapshot.samples.len();

        let event = EventRecord {
            sequence_id: i as u64,
            timestamp: snapshot.timestamp,
            event: Event::HotnessSummaryUpdated {
                sequence_id: i as u64,
                hot: hot_count,
                warm: 0,
                cold: 0,
                frozen: 0,
            },
        };
        recorder.record(&event).expect("record should succeed");
    }
    recorder.close().expect("recorder should close");

    // Verify: with low threshold, most regions should be hot
    let hot_ratio = total_hot as f64 / total_samples as f64;
    assert!(
        hot_ratio > 0.4,
        "hot workload should have >40% hot regions, got {:.1}%",
        hot_ratio * 100.0
    );

    // Replay and verify
    let mut replayer = LinuxReplayer::new(&path).expect("replayer should open");
    replayer.load().expect("replayer should load");
    assert_eq!(replayer.event_count(), num_samples);

    // Verify all replayed events have hot temperature
    for i in 0..num_samples {
        let event = replayer.next().expect("should have event");
        if let Event::HotnessSummaryUpdated { hot, .. } = &event.event {
            assert!(*hot > 0, "hot workload should produce hot counts > 0 at index {}", i);
        }
    }
}

// ─── Test: Cold Workload Fixture ────────────────────────────────────────────────

#[test]
fn test_cold_workload_fixture() {
    let path = temp_path("cold_workload_fixture.bin");

    // Generate a cold workload: use high hot_threshold so most regions are cold/frozen
    let config = DamonConfig {
        hot_threshold: 250,
        cold_threshold: 250,
        frozen_threshold: 1,
        ..Default::default()
    };
    let seed = 0x434F4C44; // "COLD"
    let num_regions = 32;
    let num_samples = 10;

    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();
    let provider = SimulatedDamonProvider::new(config, time_provider, emitter, seed, num_regions);

    let mut recorder = LinuxRecorder::new(&path).expect("recorder should create");
    let mut total_cold = 0usize;
    let mut total_samples = 0usize;

    for i in 0..num_samples {
        let snapshot = provider.sample().expect("sample should succeed");
        let cold_count = snapshot.samples.iter()
            .filter(|s| s.temperature == Temperature::Cold || s.temperature == Temperature::Frozen)
            .count();
        total_cold += cold_count;
        total_samples += snapshot.samples.len();

        let event = EventRecord {
            sequence_id: i as u64,
            timestamp: snapshot.timestamp,
            event: Event::HotnessSummaryUpdated {
                sequence_id: i as u64,
                hot: 0,
                warm: 0,
                cold: cold_count,
                frozen: 0,
            },
        };
        recorder.record(&event).expect("record should succeed");
    }
    recorder.close().expect("recorder should close");

    // Verify: with high threshold, most regions should be cold
    let cold_ratio = total_cold as f64 / total_samples as f64;
    assert!(
        cold_ratio > 0.4,
        "cold workload should have >40% cold regions, got {:.1}%",
        cold_ratio * 100.0
    );

    // Replay and verify
    let mut replayer = LinuxReplayer::new(&path).expect("replayer should open");
    replayer.load().expect("replayer should load");
    assert_eq!(replayer.event_count(), num_samples);
}

// ─── Test: Mixed Temperature Fixture ────────────────────────────────────────────

#[test]
fn test_mixed_temperature_fixture() {
    let path = temp_path("mixed_temperature_fixture.bin");

    // Use default thresholds for a balanced mix
    let config = test_config();
    let seed = 0x4D495854; // "MIXT"
    let num_regions = 64; // More regions for better distribution
    let num_samples = 10;

    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();
    let provider = SimulatedDamonProvider::new(config, time_provider, emitter, seed, num_regions);

    let mut recorder = LinuxRecorder::new(&path).expect("recorder should create");
    let mut total_hot = 0usize;
    let mut total_warm = 0usize;
    let mut total_cold = 0usize;
    let mut total_frozen = 0usize;

    for i in 0..num_samples {
        let snapshot = provider.sample().expect("sample should succeed");
        total_hot += snapshot.samples.iter().filter(|s| s.temperature == Temperature::Hot).count();
        total_warm += snapshot.samples.iter().filter(|s| s.temperature == Temperature::Warm).count();
        total_cold += snapshot.samples.iter().filter(|s| s.temperature == Temperature::Cold).count();
        total_frozen += snapshot.samples.iter().filter(|s| s.temperature == Temperature::Frozen).count();

        let event = EventRecord {
            sequence_id: i as u64,
            timestamp: snapshot.timestamp,
            event: Event::HotnessSummaryUpdated {
                sequence_id: i as u64,
                hot: total_hot,
                warm: total_warm,
                cold: total_cold,
                frozen: total_frozen,
            },
        };
        recorder.record(&event).expect("record should succeed");
    }
    recorder.close().expect("recorder should close");

    // Verify: mixed workload should have at least 2 different temperature categories
    let categories_with_data = [
        total_hot > 0,
        total_warm > 0,
        total_cold > 0,
        total_frozen > 0,
    ]
    .iter()
    .filter(|&&x| x)
    .count();
    assert!(
        categories_with_data >= 2,
        "mixed workload should have at least 2 temperature categories, got {} (hot={}, warm={}, cold={}, frozen={})",
        categories_with_data,
        total_hot,
        total_warm,
        total_cold,
        total_frozen
    );

    // Replay and verify
    let mut replayer = LinuxReplayer::new(&path).expect("replayer should open");
    replayer.load().expect("replayer should load");
    assert_eq!(replayer.event_count(), num_samples);
}

// ─── Test: Pressure Spike Fixture ───────────────────────────────────────────────

#[test]
fn test_pressure_spike_fixture() {
    let path = temp_path("pressure_spike_fixture.bin");
    let seed = 0x5053504B; // "PSPK"
    let num_regions = 32;
    let num_samples = 20;

    let config = test_config();
    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();
    let provider = SimulatedDamonProvider::new(config, time_provider, emitter, seed, num_regions);

    let mut recorder = LinuxRecorder::new(&path).expect("recorder should create");
    let mut pressure_values: Vec<f64> = Vec::new();

    for i in 0..num_samples {
        let snapshot = provider.sample().expect("sample should succeed");

        // Simulate pressure spike in the middle of the trace
        let pressure = if i >= 8 && i <= 12 {
            0.95 // Critical pressure spike
        } else if i >= 6 && i <= 14 {
            0.75 // Elevated pressure around spike
        } else {
            0.2 // Normal pressure
        };
        pressure_values.push(pressure);

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

    // Verify pressure spike pattern
    let max_pressure = pressure_values.iter().cloned().fold(0.0f64, f64::max);
    assert!(max_pressure >= 0.9, "should have pressure spike >= 0.9");

    // Replay and verify pressure values
    let mut replayer = LinuxReplayer::new(&path).expect("replayer should open");
    replayer.load().expect("replayer should load");

    for (i, expected_pressure) in pressure_values.iter().enumerate() {
        let event = replayer.next().expect("should have event");
        if let Event::MemoryPressureChanged { avg10, .. } = &event.event {
            let replayed_pressure = avg10 / 100.0;
            assert!(
                (replayed_pressure - expected_pressure).abs() < 0.01,
                "pressure mismatch at index {}: expected {}, got {}",
                i,
                expected_pressure,
                replayed_pressure
            );
        } else {
            panic!("expected MemoryPressureChanged at index {}", i);
        }
    }
}

// ─── Test: Replay with Stability ────────────────────────────────────────────────

#[test]
fn test_replay_with_stability() {
    let path = temp_path("replay_stability.bin");
    let seed = 0x5741424C; // "WABL" (wobble)
    let num_regions = 16;
    let num_samples = 30;

    let config = test_config();
    let time_provider = test_time_provider(1_700_000_000);
    let emitter = test_emitter();
    let provider = SimulatedDamonProvider::new(config, time_provider, emitter, seed, num_regions);

    let mut recorder = LinuxRecorder::new(&path).expect("recorder should create");
    let mut recommendation_count = 0usize;
    let mut last_recommendation_step: Option<usize> = None;
    let cooldown_steps = 3; // Minimum steps between recommendations

    for i in 0..num_samples {
        let snapshot = provider.sample().expect("sample should succeed");
        let hot_count = snapshot.samples.iter().filter(|s| s.temperature == Temperature::Hot).count();

        // Apply cooldown: only recommend if enough steps have passed
        let should_recommend = if hot_count > num_regions / 3 {
            match last_recommendation_step {
                None => true,
                Some(last) => i - last >= cooldown_steps,
            }
        } else {
            false
        };

        if should_recommend {
            recommendation_count += 1;
            last_recommendation_step = Some(i);
        }

        let event = EventRecord {
            sequence_id: i as u64,
            timestamp: snapshot.timestamp,
            event: Event::PolicyRecommendationGenerated {
                sequence_id: i as u64,
                recommendations: if should_recommend {
                    vec![format!("promote:{}:hot_regions", hot_count)]
                } else {
                    vec!["no_action".to_string()]
                },
                pressure_level: "test".to_string(),
            },
        };
        recorder.record(&event).expect("record should succeed");
    }
    recorder.close().expect("recorder should close");

    // Verify cooldown was applied: should have fewer recommendations than hot steps
    assert!(
        recommendation_count < num_samples / 2,
        "cooldown should limit recommendations, got {} out of {} steps",
        recommendation_count,
        num_samples
    );

    // Replay and verify cooldown behavior is preserved
    let mut replayer = LinuxReplayer::new(&path).expect("replayer should open");
    replayer.load().expect("replayer should load");

    let mut replayed_recommendations = 0usize;
    let mut replayed_last_rec_step: Option<usize> = None;
    let mut cooldown_violations = 0usize;

    for i in 0..num_samples {
        let event = replayer.next().expect("should have event");
        if let Event::PolicyRecommendationGenerated { recommendations, .. } = &event.event {
            let is_recommendation = !recommendations.iter().any(|r| r == "no_action");
            if is_recommendation {
                replayed_recommendations += 1;
                if let Some(last) = replayed_last_rec_step {
                    if i - last < cooldown_steps {
                        cooldown_violations += 1;
                    }
                }
                replayed_last_rec_step = Some(i);
            }
        }
    }

    assert_eq!(
        replayed_recommendations, recommendation_count,
        "replayed recommendations should match original"
    );
    assert_eq!(
        cooldown_violations, 0,
        "cooldown violations should be 0 during replay"
    );
}
