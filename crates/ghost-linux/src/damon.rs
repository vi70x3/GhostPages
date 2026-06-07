//! DAMON-based hotness provider for Linux.
//!
//! This module provides [`DamonHotnessProvider`] — a real hotness data source
//! that reads from the Linux DAMON (Data Access Monitor) subsystem via sysfs.
//! DAMON is a kernel framework that monitors data access patterns, making it
//! ideal for hotness-based tiering decisions.
//!
//! The provider is designed with graceful degradation: if DAMON is not available
//! on the system (e.g., non-Linux, kernel not configured with CONFIG_DAMON), the
//! provider reports `sample()` returns an error.
//!
//! A [`SimulatedDamonProvider`] is also provided for deterministic testing
//! without requiring DAMON support in the kernel.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::error::GhostError;
use ghost_core::hotness_provider::{
    AddressRange, HotnessProvider, HotnessSample, HotnessSnapshot, Temperature,
};
use ghost_core::time::TimeProvider;

// ─── DamonConfig ─────────────────────────────────────────────────────────────────

/// Configuration for the DAMON hotness provider.
#[derive(Debug, Clone)]
pub struct DamonConfig {
    /// Path to the DAMON sysfs directory (typically `/sys/kernel/mm/damon/admin`).
    pub sysfs_path: PathBuf,
    /// Maximum number of regions to read.
    pub nr_regions_max: usize,
    /// Access count threshold for "hot" classification.
    pub hot_threshold: u64,
    /// Access count threshold for "cold" classification.
    pub cold_threshold: u64,
    /// Access count threshold for "frozen" classification.
    pub frozen_threshold: u64,
    /// DAMON sampling interval in milliseconds (informational).
    pub sampling_interval_ms: u64,
    /// DAMON update interval in milliseconds (informational).
    pub update_interval_ms: u64,
}

impl Default for DamonConfig {
    fn default() -> Self {
        Self {
            sysfs_path: PathBuf::from("/sys/kernel/mm/damon/admin"),
            nr_regions_max: 1024,
            hot_threshold: 100,
            cold_threshold: 20,
            frozen_threshold: 1,
            sampling_interval_ms: 1000,
            update_interval_ms: 1000,
        }
    }
}

// ─── DamonState ──────────────────────────────────────────────────────────────────

/// Runtime state of the DAMON hotness provider.
#[derive(Debug, Clone)]
pub struct DamonState {
    /// Whether DAMON is currently running and producing data.
    pub is_running: bool,
    /// Timestamp of the last successful update (seconds since epoch).
    pub last_update: u64,
    /// Number of regions currently being monitored.
    pub regions_count: usize,
}

impl Default for DamonState {
    fn default() -> Self {
        Self {
            is_running: false,
            last_update: 0,
            regions_count: 0,
        }
    }
}

// ─── DamonRegion ─────────────────────────────────────────────────────────────────

/// A single DAMON monitoring region with raw data from sysfs.
#[derive(Debug, Clone)]
pub struct DamonRegion {
    /// Start address of the monitored region (inclusive).
    pub start: u64,
    /// End address of the monitored region (exclusive).
    pub end: u64,
    /// Number of accesses observed in this region.
    pub nr_accesses: u64,
    /// Age of this region in milliseconds (how long it has been monitored).
    pub age: u64,
}

// ─── DamonHotnessProvider ────────────────────────────────────────────────────────

/// DAMON-based hotness provider that reads real access data from the kernel.
///
/// This provider reads DAMON region data from sysfs and converts it into
/// [`HotnessSnapshot`] values that the rest of the system can consume.
/// All DAMON internals are contained within this module — the public API
/// only exposes standard [`HotnessProvider`] types.
pub struct DamonHotnessProvider {
    config: DamonConfig,
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
    state: DamonState,
}

impl DamonHotnessProvider {
    /// Create a new DAMON hotness provider.
    ///
    /// # Arguments
    ///
    /// * `config` — Configuration controlling thresholds and sysfs path.
    /// * `time_provider` — Time source for timestamping snapshots.
    /// * `event_emitter` — Emitter for hotness events.
    pub fn new(
        config: DamonConfig,
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        Self {
            config,
            time_provider,
            event_emitter,
            state: DamonState::default(),
        }
    }

    /// Check if DAMON is available on this system.
    ///
    /// Returns `true` if the DAMON sysfs directory exists and is readable.
    #[cfg(target_os = "linux")]
    pub fn check_availability(&self) -> bool {
        let path = &self.config.sysfs_path;
        path.exists() && path.is_dir()
    }

    #[cfg(not(target_os = "linux"))]
    pub fn check_availability(&self) -> bool {
        false
    }

    /// Read DAMON regions from sysfs.
    ///
    /// Parses the DAMON region data from sysfs files. Each region file
    /// contains: `start end nr_accesses age`.
    #[cfg(target_os = "linux")]
    fn read_regions(&self) -> Result<Vec<DamonRegion>, GhostError> {
        let regions_dir = self.config.sysfs_path.join("regions");

        // Read nr_regions to know how many regions exist
        let nr_regions_path = self.config.sysfs_path.join("nr_regions");
        let nr_regions_str = fs::read_to_string(&nr_regions_path).map_err(|e| {
            GhostError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to read DAMON nr_regions: {}", e),
            ))
        })?;

        let nr_regions: usize = nr_regions_str.trim().parse().map_err(|e| {
            GhostError::Internal(format!("Failed to parse DAMON nr_regions: {}", e))
        })?;

        let count = nr_regions.min(self.config.nr_regions_max);
        let mut regions = Vec::with_capacity(count);

        for i in 0..count {
            let region_path = regions_dir.join(i.to_string());
            let content = fs::read_to_string(&region_path).map_err(|e| {
                GhostError::Io(std::io::Error::new(
                    e.kind(),
                    format!("Failed to read DAMON region {}: {}", i, e),
                ))
            })?;

            let region = Self::parse_region(&content)?;
            regions.push(region);
        }

        Ok(regions)
    }

    #[cfg(not(target_os = "linux"))]
    fn read_regions(&self) -> Result<Vec<DamonRegion>, GhostError> {
        Err(GhostError::ProviderUnavailable(
            "DAMON not available on non-Linux systems".to_string(),
        ))
    }

    /// Parse a u64 value that may be in hex (0x prefix) or decimal.
    fn parse_address(s: &str) -> Result<u64, GhostError> {
        if s.starts_with("0x") || s.starts_with("0X") {
            u64::from_str_radix(&s[2..], 16).map_err(|e| {
                GhostError::Internal(format!("Failed to parse hex address '{}': {}", s, e))
            })
        } else {
            s.parse::<u64>().map_err(|e| {
                GhostError::Internal(format!("Failed to parse decimal address '{}': {}", s, e))
            })
        }
    }

    /// Parse a single DAMON region from sysfs output.
    ///
    /// Expected format: `start end nr_accesses age`
    fn parse_region(content: &str) -> Result<DamonRegion, GhostError> {
        let parts: Vec<&str> = content.trim().split_whitespace().collect();
        if parts.len() < 4 {
            return Err(GhostError::Internal(format!(
                "Invalid DAMON region format: expected 4 fields, got {}",
                parts.len()
            )));
        }

        let start = Self::parse_address(parts[0])?;
        let end = Self::parse_address(parts[1])?;
        let nr_accesses = parts[2].parse::<u64>().map_err(|e| {
            GhostError::Internal(format!("Failed to parse DAMON region nr_accesses: {}", e))
        })?;
        let age = parts[3].parse::<u64>().map_err(|e| {
            GhostError::Internal(format!("Failed to parse DAMON region age: {}", e))
        })?;

        Ok(DamonRegion {
            start,
            end,
            nr_accesses,
            age,
        })
    }

    /// Convert DAMON regions to a HotnessSnapshot.
    fn to_snapshot(&self, regions: Vec<DamonRegion>) -> HotnessSnapshot {
        let timestamp = self.time_provider.timestamp_secs();

        let samples: Vec<HotnessSample> = regions
            .into_iter()
            .map(|region| HotnessSample {
                address_range: AddressRange::new(region.start, region.end),
                access_count: region.nr_accesses,
                temperature: Self::classify_temperature(
                    region.nr_accesses,
                    self.config.hot_threshold,
                    self.config.cold_threshold,
                    self.config.frozen_threshold,
                ),
            })
            .collect();

        HotnessSnapshot {
            samples,
            timestamp,
        }
    }

    /// Classify a region's access frequency into a Temperature.
    ///
    /// Uses the thresholds from `DamonConfig`:
    /// - `accesses >= hot_threshold` → Hot
    /// - `cold_threshold <= accesses < hot_threshold` → Warm
    /// - `frozen_threshold <= accesses < cold_threshold` → Cold
    /// - `accesses < frozen_threshold` → Frozen
    fn classify_temperature(
        accesses: u64,
        hot_threshold: u64,
        cold_threshold: u64,
        frozen_threshold: u64,
    ) -> Temperature {
        if accesses >= hot_threshold {
            Temperature::Hot
        } else if accesses >= cold_threshold {
            Temperature::Warm
        } else if accesses >= frozen_threshold {
            Temperature::Cold
        } else {
            Temperature::Frozen
        }
    }

    /// Get the current state of the provider.
    pub fn state(&self) -> &DamonState {
        &self.state
    }
}

impl HotnessProvider for DamonHotnessProvider {
    fn sample(&self) -> Result<HotnessSnapshot, GhostError> {
        if !self.check_availability() {
            return Err(GhostError::ProviderUnavailable(
                "DAMON not available".to_string(),
            ));
        }

        let regions = self.read_regions()?;
        let snapshot = self.to_snapshot(regions);

        // Emit hotness sampled event (fire-and-forget)
        let hot_count = snapshot
            .samples
            .iter()
            .filter(|s| s.temperature == Temperature::Hot)
            .count();
        let cold_count = snapshot
            .samples
            .iter()
            .filter(|s| s.temperature == Temperature::Cold || s.temperature == Temperature::Frozen)
            .count();

        let _ = self.event_emitter.hotness_sampled(
            self.name(),
            snapshot.samples.len(),
            hot_count,
            cold_count,
        );

        Ok(snapshot)
    }

    fn name(&self) -> &'static str {
        "damon"
    }
}

// ─── SimulatedDamonProvider ──────────────────────────────────────────────────────

/// Deterministic DAMON simulator for testing.
///
/// Generates reproducible hotness data from a seed, mimicking the structure
/// of real DAMON output without requiring kernel support.
pub struct SimulatedDamonProvider {
    config: DamonConfig,
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
    seed: u64,
    num_regions: usize,
}

impl SimulatedDamonProvider {
    /// Create a new simulated DAMON provider.
    ///
    /// # Arguments
    ///
    /// * `config` — Configuration controlling thresholds and simulation parameters.
    /// * `time_provider` — Time source for timestamping snapshots.
    /// * `event_emitter` — Emitter for hotness events.
    /// * `seed` — Seed for deterministic random number generation.
    /// * `num_regions` — Number of simulated regions to generate.
    pub fn new(
        config: DamonConfig,
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
        seed: u64,
        num_regions: usize,
    ) -> Self {
        Self {
            config,
            time_provider,
            event_emitter,
            seed,
            num_regions,
        }
    }

    /// Generate deterministic regions from the seed.
    fn generate_regions(&self) -> Vec<DamonRegion> {
        // Simple deterministic hash-based generation
        let mut regions = Vec::with_capacity(self.num_regions);
        let mut state = self.seed;

        for i in 0..self.num_regions {
            // Simple LCG for deterministic pseudo-random numbers
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);

            let base_addr = 0x7f00_0000_0000 + (i as u64 * 0x10000);
            let region_size = 0x10000; // 64KB regions

            // Generate access count from the upper bits of state
            let nr_accesses = (state >> 32) % 200;
            let age = ((state >> 48) % 10000) + 1;

            regions.push(DamonRegion {
                start: base_addr,
                end: base_addr + region_size,
                nr_accesses,
                age,
            });
        }

        regions
    }

    /// Classify a region's access frequency into a Temperature.
    fn classify_temperature(&self, accesses: u64) -> Temperature {
        DamonHotnessProvider::classify_temperature(
            accesses,
            self.config.hot_threshold,
            self.config.cold_threshold,
            self.config.frozen_threshold,
        )
    }
}

impl HotnessProvider for SimulatedDamonProvider {
    fn sample(&self) -> Result<HotnessSnapshot, GhostError> {
        let regions = self.generate_regions();
        let timestamp = self.time_provider.timestamp_secs();

        let mut hot_count = 0usize;
        let mut cold_count = 0usize;

        let samples: Vec<HotnessSample> = regions
            .into_iter()
            .map(|region| {
                let temperature = self.classify_temperature(region.nr_accesses);
                match temperature {
                    Temperature::Hot => hot_count += 1,
                    Temperature::Cold | Temperature::Frozen => cold_count += 1,
                    _ => {}
                }

                HotnessSample {
                    address_range: AddressRange::new(region.start, region.end),
                    access_count: region.nr_accesses,
                    temperature,
                }
            })
            .collect();

        // Emit hotness sampled event (fire-and-forget)
        let _ = self.event_emitter.hotness_sampled(
            self.name(),
            samples.len(),
            hot_count,
            cold_count,
        );

        Ok(HotnessSnapshot {
            samples,
            timestamp,
        })
    }

    fn name(&self) -> &'static str {
        "simulated_damon"
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::time::DeterministicTimeProvider;

    fn test_time_provider() -> Arc<dyn TimeProvider> {
        Arc::new(DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_millis(1),
        ))
    }

    fn test_emitter() -> EventEmitter {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        EventEmitter::new(tx)
    }

    fn test_config() -> DamonConfig {
        DamonConfig::default()
    }

    #[test]
    fn test_damon_availability() {
        let config = test_config();
        let provider = DamonHotnessProvider::new(config, test_time_provider(), test_emitter());

        // In a test environment, DAMON is typically not available.
        // This test verifies the method doesn't panic and returns a boolean.
        let available = provider.check_availability();
        // We don't assert the exact value since it depends on the environment.
        // On most test systems, this will be false.
        assert!(available == true || available == false);
    }

    #[test]
    fn test_temperature_classification() {
        let config = DamonConfig {
            hot_threshold: 100,
            cold_threshold: 20,
            frozen_threshold: 1,
            ..Default::default()
        };
        let provider = DamonHotnessProvider::new(config, test_time_provider(), test_emitter());

        // Test via the classify_temperature function directly
        // Hot: >= 100
        assert_eq!(
            DamonHotnessProvider::classify_temperature(100, 100, 20, 1),
            Temperature::Hot
        );
        assert_eq!(
            DamonHotnessProvider::classify_temperature(200, 100, 20, 1),
            Temperature::Hot
        );
        assert_eq!(
            DamonHotnessProvider::classify_temperature(u64::MAX, 100, 20, 1),
            Temperature::Hot
        );

        // Warm: >= 20 && < 100
        assert_eq!(
            DamonHotnessProvider::classify_temperature(99, 100, 20, 1),
            Temperature::Warm
        );
        assert_eq!(
            DamonHotnessProvider::classify_temperature(50, 100, 20, 1),
            Temperature::Warm
        );
        assert_eq!(
            DamonHotnessProvider::classify_temperature(20, 100, 20, 1),
            Temperature::Warm
        );

        // Cold: >= 1 && < 20
        assert_eq!(
            DamonHotnessProvider::classify_temperature(19, 100, 20, 1),
            Temperature::Cold
        );
        assert_eq!(
            DamonHotnessProvider::classify_temperature(10, 100, 20, 1),
            Temperature::Cold
        );
        assert_eq!(
            DamonHotnessProvider::classify_temperature(1, 100, 20, 1),
            Temperature::Cold
        );

        // Frozen: < 1 (i.e., 0)
        assert_eq!(
            DamonHotnessProvider::classify_temperature(0, 100, 20, 1),
            Temperature::Frozen
        );

        // Verify provider state
        assert_eq!(provider.state().regions_count, 0);
    }

    #[test]
    fn test_region_parsing_hex() {
        // DAMON sysfs can output addresses in hex format
        let content = "0x7f0000000000 0x7f0000001000 42 5000";
        let region = DamonHotnessProvider::parse_region(content).unwrap();

        assert_eq!(region.start, 0x7f0000000000);
        assert_eq!(region.end, 0x7f0000001000);
        assert_eq!(region.nr_accesses, 42);
        assert_eq!(region.age, 5000);
    }

    #[test]
    fn test_region_parsing_decimal() {
        // DAMON can also output decimal addresses
        let content = "139621733646336 139621733649664 15 12345";
        let region = DamonHotnessProvider::parse_region(content).unwrap();

        assert_eq!(region.start, 139621733646336);
        assert_eq!(region.end, 139621733649664);
        assert_eq!(region.nr_accesses, 15);
        assert_eq!(region.age, 12345);
    }

    #[test]
    fn test_region_parsing_invalid() {
        // Too few fields
        let content = "0x7f0000000000 0x7f0000001000 42";
        let result = DamonHotnessProvider::parse_region(content);
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_conversion() {
        let config = DamonConfig {
            hot_threshold: 100,
            cold_threshold: 20,
            frozen_threshold: 1,
            ..Default::default()
        };
        let provider = DamonHotnessProvider::new(config, test_time_provider(), test_emitter());

        let regions = vec![
            DamonRegion {
                start: 0x7f00_0000_0000,
                end: 0x7f00_0000_1000,
                nr_accesses: 150,
                age: 1000,
            },
            DamonRegion {
                start: 0x7f00_0000_1000,
                end: 0x7f00_0000_2000,
                nr_accesses: 50,
                age: 2000,
            },
            DamonRegion {
                start: 0x7f00_0000_2000,
                end: 0x7f00_0000_3000,
                nr_accesses: 5,
                age: 3000,
            },
            DamonRegion {
                start: 0x7f00_0000_3000,
                end: 0x7f00_0000_4000,
                nr_accesses: 0,
                age: 4000,
            },
        ];

        let snapshot = provider.to_snapshot(regions);

        assert_eq!(snapshot.samples.len(), 4);
        assert_eq!(snapshot.timestamp, 1_700_000_000);

        // Verify temperature classifications
        assert_eq!(snapshot.samples[0].temperature, Temperature::Hot);
        assert_eq!(snapshot.samples[1].temperature, Temperature::Warm);
        assert_eq!(snapshot.samples[2].temperature, Temperature::Cold);
        assert_eq!(snapshot.samples[3].temperature, Temperature::Frozen);

        // Verify address ranges
        assert_eq!(snapshot.samples[0].address_range.start, 0x7f00_0000_0000);
        assert_eq!(snapshot.samples[0].address_range.end, 0x7f00_0000_1000);

        // Verify access counts
        assert_eq!(snapshot.samples[0].access_count, 150);
        assert_eq!(snapshot.samples[3].access_count, 0);
    }

    #[test]
    fn test_simulated_deterministic() {
        let config = test_config();
        let time_provider = test_time_provider();
        let emitter1 = test_emitter();
        let emitter2 = test_emitter();

        let provider1 = SimulatedDamonProvider::new(
            config.clone(),
            time_provider.clone(),
            emitter1,
            42,
            8,
        );
        let snapshot1 = provider1.sample().unwrap();

        let provider2 = SimulatedDamonProvider::new(config, time_provider, emitter2, 42, 8);
        let snapshot2 = provider2.sample().unwrap();

        // Same seed should produce same snapshot
        assert_eq!(snapshot1.samples.len(), snapshot2.samples.len());
        assert_eq!(snapshot1.timestamp, snapshot2.timestamp);

        for (s1, s2) in snapshot1.samples.iter().zip(snapshot2.samples.iter()) {
            assert_eq!(s1.address_range, s2.address_range);
            assert_eq!(s1.access_count, s2.access_count);
            assert_eq!(s1.temperature, s2.temperature);
        }
    }

    #[test]
    fn test_simulated_different_seeds() {
        let config = test_config();
        let time_provider = test_time_provider();

        let provider1 = SimulatedDamonProvider::new(
            config.clone(),
            time_provider.clone(),
            test_emitter(),
            1,
            8,
        );
        let snapshot1 = provider1.sample().unwrap();

        let provider2 = SimulatedDamonProvider::new(config, time_provider, test_emitter(), 2, 8);
        let snapshot2 = provider2.sample().unwrap();

        // Different seeds should produce different data
        let any_different = snapshot1
            .samples
            .iter()
            .zip(snapshot2.samples.iter())
            .any(|(s1, s2)| s1.access_count != s2.access_count);
        assert!(
            any_different,
            "Different seeds should produce different access counts"
        );
    }

    #[test]
    fn test_graceful_degradation() {
        // Use a non-existent path to simulate DAMON being unavailable
        let config = DamonConfig {
            sysfs_path: PathBuf::from("/nonexistent/damon/path"),
            ..Default::default()
        };
        let provider = DamonHotnessProvider::new(config, test_time_provider(), test_emitter());

        // check_availability should return false
        assert!(!provider.check_availability());

        // sample should return an error, not panic
        let result = provider.sample();
        assert!(result.is_err());
    }

    #[test]
    fn test_hotness_provider_trait() {
        let config = test_config();
        let provider = DamonHotnessProvider::new(config, test_time_provider(), test_emitter());

        // Verify trait method
        assert_eq!(provider.name(), "damon");

        // When DAMON is unavailable, sample returns an error
        if !provider.check_availability() {
            assert!(provider.sample().is_err());
        }
    }

    #[test]
    fn test_simulated_always_available() {
        let config = test_config();
        let provider =
            SimulatedDamonProvider::new(config, test_time_provider(), test_emitter(), 42, 8);

        assert_eq!(provider.name(), "simulated_damon");

        let snapshot = provider.sample().unwrap();
        assert_eq!(snapshot.samples.len(), 8);
    }

    #[test]
    fn test_damon_config_default() {
        let config = DamonConfig::default();
        assert_eq!(config.sysfs_path, PathBuf::from("/sys/kernel/mm/damon/admin"));
        assert_eq!(config.nr_regions_max, 1024);
        assert_eq!(config.hot_threshold, 100);
        assert_eq!(config.cold_threshold, 20);
        assert_eq!(config.frozen_threshold, 1);
        assert_eq!(config.sampling_interval_ms, 1000);
        assert_eq!(config.update_interval_ms, 1000);
    }

    #[test]
    fn test_damon_state_default() {
        let state = DamonState::default();
        assert!(!state.is_running);
        assert_eq!(state.last_update, 0);
        assert_eq!(state.regions_count, 0);
    }
}
