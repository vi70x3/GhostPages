//! Mock hotness provider for DAMON integration testing.
//!
//! This module provides [`MockHotnessProvider`] — a deterministic hotness
//! data generator for testing and simulation. The trait definition lives in
//! `ghost-core` so that `ghost-daemon` can consume it without a circular
//! dependency on `ghost-linux`.

use std::sync::Arc;

use parking_lot::Mutex;
use rand::distributions::{Distribution, Uniform};
use rand::SeedableRng;
use rand::rngs::StdRng;

use ghost_core::emitter::EventEmitter;
use ghost_core::hotness_provider::{
    AddressRange, HotnessProvider, HotnessSample, HotnessSnapshot, Temperature,
};
use ghost_core::time::TimeProvider;

// ─── MockHotnessConfig ─────────────────────────────────────────────────────────

/// Configuration for the mock hotness provider.
#[derive(Debug, Clone)]
pub struct MockHotnessConfig {
    /// Number of address ranges to generate.
    pub num_ranges: usize,
    /// Seed for deterministic random number generation.
    pub seed: u64,
    /// Base access count (minimum accesses per range).
    pub base_access_count: u64,
    /// Probability (0.0–1.0) that a range will be classified as "hot".
    pub hot_probability: f32,
}

impl Default for MockHotnessConfig {
    fn default() -> Self {
        Self {
            num_ranges: 16,
            seed: 42,
            base_access_count: 1,
            hot_probability: 0.25,
        }
    }
}

// ─── MockHotnessProvider ───────────────────────────────────────────────────────

/// Mock hotness provider that generates deterministic hotness data.
///
/// Uses a seeded [`StdRng`] behind a [`Mutex`] for reproducible output
/// across calls. Each call to `sample()` advances the RNG state, producing
/// a new snapshot while maintaining determinism from the initial seed.
pub struct MockHotnessProvider {
    config: MockHotnessConfig,
    rng: Mutex<StdRng>,
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
}

impl MockHotnessProvider {
    /// Create a new mock hotness provider.
    ///
    /// # Arguments
    ///
    /// * `config` — Configuration controlling the generated data.
    /// * `time_provider` — Time source for timestamping snapshots.
    /// * `event_emitter` — Emitter for hotness events.
    pub fn new(
        config: MockHotnessConfig,
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        let rng = StdRng::seed_from_u64(config.seed);
        Self {
            config,
            rng: Mutex::new(rng),
            time_provider,
            event_emitter,
        }
    }

    /// Generate a single address range with deterministic addresses.
    fn generate_address_range(&self, index: usize) -> AddressRange {
        // Use a base address with 4KB-aligned ranges
        let base = 0x7f00_0000_0000 + (index as u64 * 0x1000);
        let size = 0x1000; // 4KB pages
        AddressRange::new(base, base + size)
    }

    /// Generate a random access count using the configured distribution.
    fn generate_access_count(&self, rng: &mut StdRng) -> u64 {
        let range = Uniform::from(0..200u64);
        self.config.base_access_count + range.sample(rng)
    }
}

impl HotnessProvider for MockHotnessProvider {
    fn sample(&self) -> Result<HotnessSnapshot, ghost_core::error::GhostError> {
        let mut rng = self.rng.lock();
        let timestamp = self.time_provider.timestamp_secs();

        let mut samples = Vec::with_capacity(self.config.num_ranges);
        let mut hot_count = 0usize;
        let mut cold_count = 0usize;

        for i in 0..self.config.num_ranges {
            let address_range = self.generate_address_range(i);
            let access_count = self.generate_access_count(&mut rng);
            let temperature = Temperature::from_access_count(access_count);

            match temperature {
                Temperature::Hot => hot_count += 1,
                Temperature::Cold | Temperature::Frozen => cold_count += 1,
                _ => {}
            }

            samples.push(HotnessSample {
                address_range,
                access_count,
                temperature,
            });
        }

        // Emit hotness sampled event (fire-and-forget)
        let _ = self.event_emitter.hotness_sampled(
            self.provider_name(),
            samples.len(),
            hot_count,
            cold_count,
        );

        Ok(HotnessSnapshot {
            samples,
            timestamp,
        })
    }

    fn provider_name(&self) -> &str {
        "mock"
    }

    fn is_available(&self) -> bool {
        true
    }
}

// ─── Hotness Metrics ───────────────────────────────────────────────────────────

/// Prometheus metrics for hotness tracking.
#[derive(Debug)]
pub struct HotnessMetrics {
    /// Total number of hotness samples collected.
    pub samples_total: std::sync::atomic::AtomicU64,
    /// Number of hot memory regions currently tracked.
    pub hot_regions: std::sync::atomic::AtomicU64,
    /// Number of cold memory regions currently tracked.
    pub cold_regions: std::sync::atomic::AtomicU64,
}

impl HotnessMetrics {
    /// Create a new hotness metrics instance.
    pub fn new() -> Self {
        Self {
            samples_total: std::sync::atomic::AtomicU64::new(0),
            hot_regions: std::sync::atomic::AtomicU64::new(0),
            cold_regions: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Record a hotness sample snapshot.
    pub fn record_snapshot(&self, snapshot: &HotnessSnapshot) {
        use std::sync::atomic::Ordering;

        self.samples_total.fetch_add(1, Ordering::Relaxed);

        let mut hot = 0u64;
        let mut cold = 0u64;
        for sample in &snapshot.samples {
            match sample.temperature {
                Temperature::Hot => hot += 1,
                Temperature::Cold | Temperature::Frozen => cold += 1,
                _ => {}
            }
        }
        self.hot_regions.store(hot, Ordering::Relaxed);
        self.cold_regions.store(cold, Ordering::Relaxed);
    }
}

impl Default for HotnessMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::time::DeterministicTimeProvider;

    fn test_time_provider() -> Arc<dyn TimeProvider> {
        Arc::new(DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_millis(1)))
    }

    fn test_emitter() -> EventEmitter {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        EventEmitter::new(tx)
    }

    #[test]
    fn test_mock_provider_available() {
        let config = MockHotnessConfig::default();
        let provider = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
        assert!(provider.is_available());
    }

    #[test]
    fn test_mock_provider_name() {
        let config = MockHotnessConfig::default();
        let provider = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
        assert_eq!(provider.provider_name(), "mock");
    }

    #[test]
    fn test_mock_provider_sample_returns_data() {
        let config = MockHotnessConfig {
            num_ranges: 8,
            seed: 42,
            base_access_count: 1,
            hot_probability: 0.25,
        };
        let provider = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
        let snapshot = provider.sample().unwrap();

        assert_eq!(snapshot.samples.len(), 8);
        assert_eq!(snapshot.timestamp, 1_700_000_000);
    }

    #[test]
    fn test_mock_provider_deterministic() {
        let config = MockHotnessConfig {
            num_ranges: 4,
            seed: 123,
            base_access_count: 5,
            hot_probability: 0.5,
        };

        let provider1 = MockHotnessProvider::new(config.clone(), test_time_provider(), test_emitter());
        let snapshot1 = provider1.sample().unwrap();

        let provider2 = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
        let snapshot2 = provider2.sample().unwrap();

        // Same seed should produce same first sample
        assert_eq!(snapshot1.samples.len(), snapshot2.samples.len());
        for (s1, s2) in snapshot1.samples.iter().zip(snapshot2.samples.iter()) {
            assert_eq!(s1.address_range, s2.address_range);
            assert_eq!(s1.access_count, s2.access_count);
            assert_eq!(s1.temperature, s2.temperature);
        }
    }

    #[test]
    fn test_mock_provider_different_seeds() {
        let config1 = MockHotnessConfig {
            num_ranges: 4,
            seed: 1,
            base_access_count: 1,
            hot_probability: 0.5,
        };
        let config2 = MockHotnessConfig {
            num_ranges: 4,
            seed: 2,
            base_access_count: 1,
            hot_probability: 0.5,
        };

        let provider1 = MockHotnessProvider::new(config1, test_time_provider(), test_emitter());
        let snapshot1 = provider1.sample().unwrap();

        let provider2 = MockHotnessProvider::new(config2, test_time_provider(), test_emitter());
        let snapshot2 = provider2.sample().unwrap();

        // Different seeds should produce different data
        let any_different = snapshot1.samples.iter().zip(snapshot2.samples.iter()).any(
            |(s1, s2)| s1.access_count != s2.access_count,
        );
        assert!(any_different, "Different seeds should produce different access counts");
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

        // Verify all samples have valid temperature classifications
        for sample in &snapshot.samples {
            let expected = Temperature::from_access_count(sample.access_count);
            assert_eq!(sample.temperature, expected);
        }

        // With 100 ranges and base_access_count=0, we should have a mix
        let hot_count = snapshot.samples.iter().filter(|s| s.temperature == Temperature::Hot).count();
        let frozen_count = snapshot.samples.iter().filter(|s| s.temperature == Temperature::Frozen).count();
        assert!(hot_count > 0 || frozen_count > 0, "Should have some variety in temperatures");
    }

    #[test]
    fn test_mock_hotness_config_default() {
        let config = MockHotnessConfig::default();
        assert_eq!(config.num_ranges, 16);
        assert_eq!(config.seed, 42);
        assert_eq!(config.base_access_count, 1);
        assert!((config.hot_probability - 0.25).abs() < f32::EPSILON);
    }

    #[test]
    fn test_hotness_metrics() {
        let metrics = HotnessMetrics::new();
        let config = MockHotnessConfig {
            num_ranges: 10,
            seed: 42,
            base_access_count: 0,
            hot_probability: 0.5,
        };
        let provider = MockHotnessProvider::new(config, test_time_provider(), test_emitter());
        let snapshot = provider.sample().unwrap();

        metrics.record_snapshot(&snapshot);

        assert_eq!(metrics.samples_total.load(std::sync::atomic::Ordering::Relaxed), 1);
        // Just verify the gauges are set to reasonable values
        let hot = metrics.hot_regions.load(std::sync::atomic::Ordering::Relaxed);
        let cold = metrics.cold_regions.load(std::sync::atomic::Ordering::Relaxed);
        assert!(hot + cold <= 10, "Hot + cold should not exceed total samples");
    }
}
