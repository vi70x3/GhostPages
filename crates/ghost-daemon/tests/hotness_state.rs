//! Integration tests for HotnessState runtime state integration.
//!
//! Tests the HotnessTracker's HotnessState struct, sampling, region
//! classification, and integration with the hotness monitoring system.

use std::sync::Arc;

use ghost_core::hotness_provider::{
    AddressRange, HotnessProvider, HotnessSample, HotnessSnapshot, Temperature,
};
use ghost_daemon::hotness_tracker::HotnessTracker;
use ghost_daemon::trace_log::TraceLog;

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn test_trace_log() -> Arc<TraceLog> {
    Arc::new(TraceLog::new(1000))
}

/// A minimal mock hotness provider for testing.
struct TestHotnessProvider {
    snapshot: HotnessSnapshot,
}

impl HotnessProvider for TestHotnessProvider {
    fn sample(&self) -> Result<HotnessSnapshot, ghost_core::error::GhostError> {
        Ok(self.snapshot.clone())
    }

    fn name(&self) -> &'static str {
        "test"
    }
}

fn make_snapshot(samples: Vec<HotnessSample>) -> HotnessSnapshot {
    HotnessSnapshot {
        samples,
        timestamp: 1_700_000_000,
    }
}

fn make_sample(start: u64, access_count: u64) -> HotnessSample {
    HotnessSample {
        address_range: AddressRange::new(start, start + 0x1000),
        access_count,
        temperature: Temperature::from_access_count(access_count),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[test]
fn test_hotness_tracker_with_provider() {
    let tracker = HotnessTracker::new(1000, test_trace_log());

    // Create a provider with mixed temperature samples
    let samples = vec![
        make_sample(0x1000, 150),  // Hot (>= 100)
        make_sample(0x2000, 50),   // Warm (>= 20)
        make_sample(0x3000, 5),    // Cold (>= 1)
        make_sample(0x4000, 0),    // Frozen (0)
    ];
    let provider = Arc::new(TestHotnessProvider {
        snapshot: make_snapshot(samples),
    });

    tracker.set_hotness_provider(Some(provider));
    assert!(tracker.has_hotness_provider());

    // Sample hotness
    let state = tracker.sample_hotness();
    assert!(state.is_some());

    let state = state.unwrap();
    assert_eq!(state.summary.total_regions, 4);
    assert_eq!(state.summary.hot_count, 1);
    assert_eq!(state.summary.warm_count, 1);
    assert_eq!(state.summary.cold_count, 1);
    assert_eq!(state.summary.frozen_count, 1);
    assert_eq!(state.last_update, 1_700_000_000);
}

#[test]
fn test_hotness_tracker_without_provider() {
    let tracker = HotnessTracker::new(1000, test_trace_log());

    // No provider configured
    assert!(!tracker.has_hotness_provider());

    // sample_hotness should return None
    let state = tracker.sample_hotness();
    assert!(state.is_none());

    // get_hotness_state should return empty state
    let state = tracker.get_hotness_state();
    assert_eq!(state.summary.total_regions, 0);
    assert_eq!(state.last_update, 0);
}

#[test]
fn test_get_hot_regions() {
    let tracker = HotnessTracker::new(1000, test_trace_log());

    let samples = vec![
        make_sample(0x1000, 150),  // Hot
        make_sample(0x2000, 50),   // Warm
        make_sample(0x3000, 5),    // Cold
        make_sample(0x4000, 0),    // Frozen
    ];
    let provider = Arc::new(TestHotnessProvider {
        snapshot: make_snapshot(samples),
    });

    tracker.set_hotness_provider(Some(provider));
    tracker.sample_hotness();

    let hot_regions = tracker.get_hot_regions();
    assert_eq!(hot_regions.len(), 2); // Hot + Warm

    // Verify the regions are correctly classified
    let temps: Vec<Temperature> = hot_regions.iter().map(|(_, t)| *t).collect();
    assert!(temps.contains(&Temperature::Hot));
    assert!(temps.contains(&Temperature::Warm));
    assert!(!temps.contains(&Temperature::Cold));
    assert!(!temps.contains(&Temperature::Frozen));
}

#[test]
fn test_get_cold_regions() {
    let tracker = HotnessTracker::new(1000, test_trace_log());

    let samples = vec![
        make_sample(0x1000, 150),  // Hot
        make_sample(0x2000, 50),   // Warm
        make_sample(0x3000, 5),    // Cold
        make_sample(0x4000, 0),    // Frozen
    ];
    let provider = Arc::new(TestHotnessProvider {
        snapshot: make_snapshot(samples),
    });

    tracker.set_hotness_provider(Some(provider));
    tracker.sample_hotness();

    let cold_regions = tracker.get_cold_regions();
    assert_eq!(cold_regions.len(), 2); // Cold + Frozen

    let temps: Vec<Temperature> = cold_regions.iter().map(|(_, t)| *t).collect();
    assert!(temps.contains(&Temperature::Cold));
    assert!(temps.contains(&Temperature::Frozen));
    assert!(!temps.contains(&Temperature::Hot));
    assert!(!temps.contains(&Temperature::Warm));
}

#[test]
fn test_hotness_state_integration() {
    let tracker = HotnessTracker::new(1000, test_trace_log());

    // Verify initial state is empty
    let initial_state = tracker.get_hotness_state();
    assert_eq!(initial_state.summary.total_regions, 0);

    // Set up provider and sample
    let samples = vec![
        make_sample(0x1000, 200),  // Hot
        make_sample(0x2000, 200),  // Hot
        make_sample(0x3000, 30),   // Warm
        make_sample(0x4000, 0),    // Frozen
    ];
    let provider = Arc::new(TestHotnessProvider {
        snapshot: make_snapshot(samples),
    });

    tracker.set_hotness_provider(Some(provider));

    // Sample multiple times to build history
    for _ in 0..3 {
        tracker.sample_hotness();
    }

    let state = tracker.get_hotness_state();
    assert_eq!(state.summary.total_regions, 4);
    assert_eq!(state.summary.hot_count, 2);
    assert_eq!(state.summary.warm_count, 1);
    assert_eq!(state.summary.frozen_count, 1);

    // Verify history was built
    assert!(state.history.len() > 0);

    // Verify confidence was calculated
    // With 4 samples, confidence should be > 0
    assert!(state.confidence.score >= 0.0);
    assert!(state.confidence.score <= 1.0);
}

#[test]
fn test_hotness_state_empty() {
    let state = ghost_daemon::hotness_tracker::HotnessState::empty();
    assert_eq!(state.summary.total_regions, 0);
    assert_eq!(state.last_update, 0);
    assert!(state.history.is_empty());
}

#[test]
fn test_hotness_tracker_sampling_interval() {
    use std::time::Duration;

    let tracker = HotnessTracker::new(1000, test_trace_log());
    assert_eq!(tracker.sampling_interval(), Duration::from_secs(60));

    // Test with custom interval
    let custom = HotnessTracker::with_sampling_interval(
        1000,
        test_trace_log(),
        Duration::from_secs(30),
    );
    assert_eq!(custom.sampling_interval(), Duration::from_secs(30));
}
