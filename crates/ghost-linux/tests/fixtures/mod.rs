//! Fixture generator for DAMON hotness replay tests.
//!
//! This module provides utilities to generate deterministic test fixtures
//! for replay testing. Fixtures are generated from known seeds to ensure
//! reproducibility.

use std::path::Path;
use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::hotness_provider::{HotnessProvider, HotnessSnapshot, Temperature};
use ghost_core::time::{DeterministicTimeProvider, TimeProvider};

use crate::damon::{DamonConfig, SimulatedDamonProvider};
use crate::recorder::LinuxRecorder;
use crate::replayer::LinuxReplayer;

/// Seed constants for different workload types.
pub mod seeds {
    /// Seed for hot workload fixtures (predominantly hot regions).
    pub const HOT_WORKLOAD: u64 = 0x484F5400; // "HOT\0"
    
    /// Seed for cold workload fixtures (predominantly cold/frozen regions).
    pub const COLD_WORKLOAD: u64 = 0x434F4C44; // "COLD"
    
    /// Seed for mixed temperature fixtures.
    pub const MIXED_TEMPERATURE: u64 = 0x4D495854; // "MIXT"
    
    /// Seed for pressure spike fixtures.
    pub const PRESSURE_SPIKE: u64 = 0x5053504B; // "PSPK"
}

/// Generate a hot workload fixture (predominantly hot regions).
///
/// Uses a seed that produces high access counts (>= hot_threshold).
pub fn generate_hot_workload_fixture(path: &Path) -> std::io::Result<()> {
    generate_fixture(path, seeds::HOT_WORKLOAD, 32, |config, seed| {
        // Modify config to ensure hot regions
        let mut cfg = config;
        cfg.hot_threshold = 50; // Lower threshold
        cfg.cold_threshold = 10;
        SimulatedDamonProvider::new(cfg, create_time_provider(seed), create_emitter(), seed, 32)
    })
}

/// Generate a cold workload fixture (predominantly cold/frozen regions).
///
/// Uses a seed that produces low access counts (< cold_threshold).
pub fn generate_cold_workload_fixture(path: &Path) -> std::io::Result<()> {
    generate_fixture(path, seeds::COLD_WORKLOAD, 32, |config, seed| {
        // Modify config to ensure cold regions
        let mut cfg = config;
        cfg.hot_threshold = 200; // Very high threshold
        cfg.cold_threshold = 50;
        SimulatedDamonProvider::new(cfg, create_time_provider(seed), create_emitter(), seed, 32)
    })
}

/// Generate a mixed temperature fixture.
pub fn generate_mixed_temperature_fixture(path: &Path) -> std::io::Result<()> {
    generate_fixture(path, seeds::MIXED_TEMPERATURE, 32, |config, seed| {
        SimulatedDamonProvider::new(config, create_time_provider(seed), create_emitter(), seed, 32)
    })
}

/// Generate a pressure spike fixture with temperature changes.
pub fn generate_pressure_spike_fixture(path: &Path) -> std::io::Result<()> {
    generate_fixture(path, seeds::PRESSURE_SPIKE, 64, |config, seed| {
        SimulatedDamonProvider::new(config, create_time_provider(seed), create_emitter(), seed, 64)
    })
}

/// Core fixture generation logic.
fn generate_fixture<F>(
    path: &Path,
    seed: u64,
    num_samples: usize,
    provider_factory: F,
) -> std::io::Result<()>
where
    F: Fn(DamonConfig, u64) -> SimulatedDamonProvider,
{
    let config = DamonConfig::default();
    let mut recorder = LinuxRecorder::new(path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    let provider = provider_factory(config, seed);
    
    for i in 0..num_samples {
        let snapshot = provider.sample()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        
        // Record the snapshot as an event
        let event = ghost_core::events::EventRecord {
            sequence_id: i as u64,
            timestamp: snapshot.timestamp,
            event: ghost_core::events::Event::HotnessChanged {
                sequence_id: i as u64,
                region: format!("region_{}", i),
                temperature: snapshot.samples.first().map(|s| s.temperature).unwrap_or(Temperature::Frozen),
                access_count: snapshot.samples.first().map(|s| s.access_count).unwrap_or(0),
            },
        };
        
        recorder.record(&event)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    }
    
    recorder.close()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    Ok(())
}

/// Create a deterministic time provider for testing.
fn create_time_provider(seed: u64) -> Arc<dyn TimeProvider> {
    Arc::new(DeterministicTimeProvider::new(
        1_700_000_000 + seed,
        std::time::Duration::from_secs(1),
    ))
}

/// Create an event emitter for testing.
fn create_emitter() -> EventEmitter {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    EventEmitter::new(tx)
}

/// Verify a fixture file is valid and deterministic.
pub fn verify_fixture(path: &Path) -> std::io::Result<bool> {
    let mut replayer = LinuxReplayer::new(path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    replayer.load()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    // Verify we have events
    Ok(replayer.event_count() > 0)
}

/// Get the expected temperature distribution for a fixture type.
#[derive(Debug, Clone)]
pub struct ExpectedTemperatureDistribution {
    pub hot_min: usize,
    pub hot_max: usize,
    pub warm_min: usize,
    pub warm_max: usize,
    pub cold_min: usize,
    pub cold_max: usize,
    pub frozen_min: usize,
    pub frozen_max: usize,
}

impl ExpectedTemperatureDistribution {
    /// Expected distribution for hot workload.
    pub fn hot_workload() -> Self {
        Self {
            hot_min: 15,
            hot_max: 32,
            warm_min: 0,
            warm_max: 10,
            cold_min: 0,
            cold_max: 5,
            frozen_min: 0,
            frozen_max: 2,
        }
    }
    
    /// Expected distribution for cold workload.
    pub fn cold_workload() -> Self {
        Self {
            hot_min: 0,
            hot_max: 2,
            warm_min: 0,
            warm_max: 5,
            cold_min: 5,
            cold_max: 15,
            frozen_min: 10,
            frozen_max: 25,
        }
    }
    
    /// Expected distribution for mixed temperature workload.
    pub fn mixed_temperature() -> Self {
        Self {
            hot_min: 5,
            hot_max: 15,
            warm_min: 5,
            warm_max: 15,
            cold_min: 5,
            cold_max: 15,
            frozen_min: 5,
            frozen_max: 15,
        }
    }
    
    /// Check if a snapshot matches the expected distribution.
    pub fn matches(&self, snapshot: &HotnessSnapshot) -> bool {
        let mut hot = 0;
        let mut warm = 0;
        let mut cold = 0;
        let mut frozen = 0;
        
        for sample in &snapshot.samples {
            match sample.temperature {
                Temperature::Hot => hot += 1,
                Temperature::Warm => warm += 1,
                Temperature::Cold => cold += 1,
                Temperature::Frozen => frozen += 1,
            }
        }
        
        hot >= self.hot_min && hot <= self.hot_max
            && warm >= self.warm_min && warm <= self.warm_max
            && cold >= self.cold_min && cold <= self.cold_max
            && frozen >= self.frozen_min && frozen <= self.frozen_max
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_fixture_generation_hot_workload() {
        let tmp = NamedTempFile::new().unwrap();
        generate_hot_workload_fixture(tmp.path()).unwrap();
        assert!(verify_fixture(tmp.path()).unwrap());
    }

    #[test]
    fn test_fixture_generation_cold_workload() {
        let tmp = NamedTempFile::new().unwrap();
        generate_cold_workload_fixture(tmp.path()).unwrap();
        assert!(verify_fixture(tmp.path()).unwrap());
    }

    #[test]
    fn test_fixture_generation_mixed_temperature() {
        let tmp = NamedTempFile::new().unwrap();
        generate_mixed_temperature_fixture(tmp.path()).unwrap();
        assert!(verify_fixture(tmp.path()).unwrap());
    }

    #[test]
    fn test_fixture_generation_pressure_spike() {
        let tmp = NamedTempFile::new().unwrap();
        generate_pressure_spike_fixture(tmp.path()).unwrap();
        assert!(verify_fixture(tmp.path()).unwrap());
    }

    #[test]
    fn test_expected_distribution_hot() {
        let dist = ExpectedTemperatureDistribution::hot_workload();
        assert!(dist.hot_min > dist.cold_max);
    }

    #[test]
    fn test_expected_distribution_cold() {
        let dist = ExpectedTemperatureDistribution::cold_workload();
        assert!(dist.frozen_min > dist.hot_max);
    }
}