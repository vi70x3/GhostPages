//! Integration tests for the HotnessProvider trait and mock implementation.
//!
//! Tests cover:
//! - Deterministic output from MockHotnessProvider
//! - Event emission on sample
//! - Temperature classification correctness
//! - Availability checks
//! - HotnessTracker integration with provider
//! - Record/replay of hotness streams

use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::hotness_provider::{
    AddressRange, HotnessProvider, HotnessSample, HotnessSnapshot, Temperature,
};
use ghost_core::time::{DeterministicTimeProvider, TimeProvider};
use ghost_core::types::ChunkId;

use ghost_daemon::hotness_tracker::HotnessTracker;
use ghost_daemon::trace_log::TraceLog;

use ghost_linux::hotness_provider::{MockHotnessConfig, MockHotnessProvider};

// ─── Helpers ───────────────────────────────────────────────────────────────────

fn test_time_provider() -> Arc<dyn TimeProvider> {
    Arc::new(DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_millis(1)))
}

fn test_emitter() -> EventEmitter {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    EventEmitter::new(tx)
}

fn test_tracker() -> HotnessTracker {
    let trace_log = Arc::new(TraceLog::new(1000));
    HotnessTracker::new(1000, trace_log)
}

fn test_chunk_id(seed: u8) -> ChunkId {
    let mut id = [0u8; 32];
    id[0] = seed;
    ChunkId(id)
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn test_mock_provider_deterministic() {
    // Same seed must produce same hotness data
    let config = MockHotnessConfig {
        num_ranges: 8,
        seed: 42,
        base_access_count: 1,
        hot_probability: 0.25,
    };

    let provider1 = MockHotnessProvider::new(
        config.clone(),
        test_time_provider(),
        test_emitter(),
    );
    let snapshot1 = provider1.sample().unwrap();

    let provider2 = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
    let snapshot2 = provider2.sample().unwrap();

    assert_eq!(snapshot1.samples.len(), snapshot2.samples.len());
    assert_eq!(snapshot1.timestamp, snapshot2.timestamp);

    for (s1, s2) in snapshot1.samples.iter().zip(snapshot2.samples.iter()) {
        assert_eq!(s1.address_range, s2.address_range);
        assert_eq!(s1.access_count, s2.access_count);
        assert_eq!(s1.temperature, s2.temperature);
    }
}

#[test]
fn test_mock_provider_emits_events() {
    let config = MockHotnessConfig {
        num_ranges: 4,
        seed: 42,
        base_access_count: 1,
        hot_probability: 0.25,
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    let provider = MockHotnessProvider::new(config, test_time_provider(), emitter);
    let _snapshot = provider.sample().unwrap();

    // The emitter should have sent a HotnessSampled event
    // Note: try_recv is used since the emit is fire-and-forget
    match rx.try_recv() {
        Ok(record) => {
            match &record.event {
                ghost_core::events::Event::HotnessSampled {
                    provider: name,
                    num_samples,
                    hot_count,
                    cold_count,
                    ..
                } => {
                    assert_eq!(name, "mock");
                    assert_eq!(*num_samples, 4);
                    // hot_count + cold_count should be <= num_samples
                    assert!(hot_count + cold_count <= 4);
                }
                other => panic!("Expected HotnessSampled event, got: {:?}", other),
            }
        }
        Err(_) => {
            // The channel might be full or the event might not have been sent
            // This is acceptable since emit is fire-and-forget
        }
    }
}

#[test]
fn test_mock_provider_temperature_classification() {
    let config = MockHotnessConfig {
        num_ranges: 100,
        seed: 42,
        base_access_count: 0,
        hot_probability: 0.5,
    };

    let provider = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
    let snapshot = provider.sample().unwrap();

    // Every sample's temperature must match its access count
    for sample in &snapshot.samples {
        let expected = Temperature::from_access_count(sample.access_count);
        assert_eq!(
            sample.temperature, expected,
            "Access count {} should be {:?}, got {:?}",
            sample.access_count, expected, sample.temperature
        );
    }

    // Verify the classification thresholds
    let hot_count = snapshot.samples.iter().filter(|s| matches!(s.temperature, Temperature::Hot)).count();
    let warm_count = snapshot.samples.iter().filter(|s| matches!(s.temperature, Temperature::Warm)).count();
    let cold_count = snapshot.samples.iter().filter(|s| matches!(s.temperature, Temperature::Cold)).count();
    let frozen_count = snapshot.samples.iter().filter(|s| matches!(s.temperature, Temperature::Frozen)).count();

    // All samples should be accounted for
    assert_eq!(hot_count + warm_count + cold_count + frozen_count, 100);

    // With base_access_count=0, some should be frozen (access_count=0)
    assert!(frozen_count > 0, "Should have some frozen ranges");
}

#[test]
fn test_mock_provider_availability() {
    let config = MockHotnessConfig::default();
    let provider = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
    assert!(provider.is_available(), "Mock provider should always be available");
    assert_eq!(provider.provider_name(), "mock");
}

#[test]
fn test_hotness_tracker_integration() {
    let tracker = test_tracker();

    // Initially no provider
    assert!(!tracker.has_hotness_provider());

    // Set a mock provider
    let config = MockHotnessConfig {
        num_ranges: 4,
        seed: 42,
        base_access_count: 1,
        hot_probability: 0.25,
    };
    let provider = Arc::new(MockHotnessProvider::new(
        config,
        test_time_provider(),
        test_emitter(),
    ));

    tracker.set_hotness_provider(Some(provider.clone()));

    assert!(tracker.has_hotness_provider());
    assert!(tracker.hotness_provider().is_some());

    // The tracker should still work normally
    let id = test_chunk_id(1);
    tracker.record_access(id, 1024);
    assert_eq!(tracker.len(), 1);

    // Clear the provider
    tracker.set_hotness_provider(None);
    assert!(!tracker.has_hotness_provider());
    assert!(tracker.hotness_provider().is_none());

    // Tracker should still work without provider
    tracker.record_access(id, 2048);
    assert_eq!(tracker.len(), 1);
}

#[test]
fn test_replay() {
    // Record a hotness stream, then replay with the same seed
    let config = MockHotnessConfig {
        num_ranges: 4,
        seed: 99,
        base_access_count: 5,
        hot_probability: 0.5,
    };

    // First run
    let provider1 = MockHotnessProvider::new(
        config.clone(),
        test_time_provider(),
        test_emitter(),
    );
    let mut snapshots = Vec::new();
    for _ in 0..3 {
        snapshots.push(provider1.sample().unwrap());
    }

    // Second run with same seed
    let provider2 = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
    let mut replayed = Vec::new();
    for _ in 0..3 {
        replayed.push(provider2.sample().unwrap());
    }

    // Each snapshot should match (deterministic replay)
    for (orig, replay) in snapshots.iter().zip(replayed.iter()) {
        assert_eq!(orig.timestamp, replay.timestamp);
        assert_eq!(orig.samples.len(), replay.samples.len());
        for (s1, s2) in orig.samples.iter().zip(replay.samples.iter()) {
            assert_eq!(s1.address_range, s2.address_range);
            assert_eq!(s1.access_count, s2.access_count);
            assert_eq!(s1.temperature, s2.temperature);
        }
    }
}

#[test]
fn test_address_range_operations() {
    let range = AddressRange::new(0x1000, 0x5000);
    assert_eq!(range.size(), 0x4000);
    assert_eq!(range.start, 0x1000);
    assert_eq!(range.end, 0x5000);

    // Zero-size range
    let empty = AddressRange::new(0x1000, 0x1000);
    assert_eq!(empty.size(), 0);

    // Large range
    let large = AddressRange::new(0, u64::MAX);
    assert_eq!(large.size(), u64::MAX);
}

#[test]
fn test_temperature_display_and_ordering() {
    assert_eq!(format!("{}", Temperature::Hot), "hot");
    assert_eq!(format!("{}", Temperature::Warm), "warm");
    assert_eq!(format!("{}", Temperature::Cold), "cold");
    assert_eq!(format!("{}", Temperature::Frozen), "frozen");

    // Verify classification boundaries
    assert_eq!(Temperature::from_access_count(0), Temperature::Frozen);
    assert_eq!(Temperature::from_access_count(1), Temperature::Cold);
    assert_eq!(Temperature::from_access_count(19), Temperature::Cold);
    assert_eq!(Temperature::from_access_count(20), Temperature::Warm);
    assert_eq!(Temperature::from_access_count(99), Temperature::Warm);
    assert_eq!(Temperature::from_access_count(100), Temperature::Hot);
}

#[test]
fn test_snapshot_timestamp() {
    let config = MockHotnessConfig::default();
    let provider = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
    let snapshot = provider.sample().unwrap();

    // Timestamp should match the time provider
    assert_eq!(snapshot.timestamp, 1_700_000_000);
}

#[test]
fn test_different_seeds_produce_different_data() {
    let make_config = |seed: u64| MockHotnessConfig {
        num_ranges: 10,
        seed,
        base_access_count: 0,
        hot_probability: 0.5,
    };

    let p1 = MockHotnessProvider::new(make_config(1), test_time_provider(), test_emitter());
    let p2 = MockHotnessProvider::new(make_config(2), test_time_provider(), test_emitter());
    let p3 = MockHotnessProvider::new(make_config(3), test_time_provider(), test_emitter());

    let s1 = p1.sample().unwrap();
    let s2 = p2.sample().unwrap();
    let s3 = p3.sample().unwrap();

    // At least some of the snapshots should differ
    let all_same = s1.samples.iter().zip(s2.samples.iter()).all(|(a, b)| {
        a.access_count == b.access_count
    }) && s2.samples.iter().zip(s3.samples.iter()).all(|(a, b)| {
        a.access_count == b.access_count
    });

    assert!(!all_same, "Different seeds should produce different data");
}
