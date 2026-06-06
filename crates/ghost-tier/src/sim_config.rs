//! Configuration for the simulation backend.
//!
//! This module defines the configuration types used by `SimBackend` for
//! latency simulation, bandwidth throttling, and failure injection.
//! It is shared between the pure simulation backend and the disk backend's
//! simulation layer.

use std::time::Duration;

/// Configuration for simulated latency.
#[derive(Debug, Clone)]
pub struct LatencyConfig {
    /// Base latency for all operations.
    pub base: Duration,
    /// Per-byte latency factor (added per byte transferred).
    pub per_byte: Duration,
    /// Jitter range as a fraction of base latency (0.0 to 1.0).
    pub jitter_fraction: f64,
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_micros(10),
            per_byte: Duration::from_nanos(1),
            jitter_fraction: 0.1,
        }
    }
}

/// Configuration for simulated bandwidth.
#[derive(Debug, Clone)]
pub struct BandwidthConfig {
    /// Maximum throughput in bytes per second.
    pub bytes_per_second: usize,
}

impl Default for BandwidthConfig {
    fn default() -> Self {
        Self {
            bytes_per_second: 100 * 1024 * 1024, // 100 MB/s
        }
    }
}

/// Pattern of failure injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailurePattern {
    /// Failures occur randomly based on probability.
    Random,
    /// Failures occur in bursts (several in quick succession).
    Burst,
    /// Failure rate increases over time (degrading backend).
    Degrading,
    /// Failures occur intermittently (on/off pattern).
    Intermittent,
    /// One failure triggers cascading failures across operations.
    Cascading,
}

impl Default for FailurePattern {
    fn default() -> Self {
        FailurePattern::Random
    }
}

/// Configuration for failure injection.
#[derive(Debug, Clone)]
pub struct FailureConfig {
    /// Probability of a write operation failing (0.0 to 1.0).
    pub write_failure_rate: f64,
    /// Probability of a read operation failing (0.0 to 1.0).
    pub read_failure_rate: f64,
    /// Probability of an allocation failing (0.0 to 1.0).
    pub alloc_failure_rate: f64,
    /// Whether to simulate corruption on failure.
    pub corruption_on_failure: bool,
    /// Probability of data corruption on write (0.0 to 1.0), independent of failure.
    pub corruption_rate: f64,
    /// Probability of an operation timing out (0.0 to 1.0).
    pub timeout_rate: f64,
    /// Probability of a device loss event (0.0 to 1.0).
    pub device_loss_rate: f64,
    /// Pattern of failure injection.
    pub failure_pattern: FailurePattern,
}

impl Default for FailureConfig {
    fn default() -> Self {
        Self {
            write_failure_rate: 0.0,
            read_failure_rate: 0.0,
            alloc_failure_rate: 0.0,
            corruption_on_failure: false,
            corruption_rate: 0.0,
            timeout_rate: 0.0,
            device_loss_rate: 0.0,
            failure_pattern: FailurePattern::Random,
        }
    }
}

/// Simulation backend configuration.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Total capacity in bytes.
    pub capacity: usize,
    /// Simulated latency configuration.
    pub latency: LatencyConfig,
    /// Simulated bandwidth configuration.
    pub bandwidth: BandwidthConfig,
    /// Failure injection configuration.
    pub failure: FailureConfig,
    /// RNG seed for deterministic behavior.
    pub seed: u64,
    /// Maximum number of concurrent operations.
    pub max_concurrent_ops: usize,
    /// Whether to simulate fragmentation effects.
    pub simulate_fragmentation: bool,
    /// Fragmentation factor (0.0 = none, 1.0 = maximum). Only used if
    /// `simulate_fragmentation` is true.
    pub fragmentation_factor: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            capacity: 1024 * 1024 * 1024, // 1 GiB
            latency: LatencyConfig::default(),
            bandwidth: BandwidthConfig::default(),
            failure: FailureConfig::default(),
            seed: 42,
            max_concurrent_ops: 64,
            simulate_fragmentation: false,
            fragmentation_factor: 0.0,
        }
    }
}

impl SimConfig {
    /// Create a new simulation config with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            ..Default::default()
        }
    }

    /// Set the RNG seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Set the latency configuration.
    pub fn with_latency(mut self, latency: LatencyConfig) -> Self {
        self.latency = latency;
        self
    }

    /// Set the bandwidth configuration.
    pub fn with_bandwidth(mut self, bandwidth: BandwidthConfig) -> Self {
        self.bandwidth = bandwidth;
        self
    }

    /// Set the failure configuration.
    pub fn with_failure(mut self, failure: FailureConfig) -> Self {
        self.failure = failure;
        self
    }

    /// Enable fragmentation simulation with the given factor.
    pub fn with_fragmentation(mut self, factor: f64) -> Self {
        self.simulate_fragmentation = true;
        self.fragmentation_factor = factor.clamp(0.0, 1.0);
        self
    }

    /// Set the failure pattern for injection.
    pub fn with_failure_pattern(mut self, pattern: FailurePattern) -> Self {
        self.failure.failure_pattern = pattern;
        self
    }

    /// Set the corruption rate for the failure configuration.
    pub fn with_corruption_rate(mut self, rate: f64) -> Self {
        self.failure.corruption_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set the timeout rate for the failure configuration.
    pub fn with_timeout_rate(mut self, rate: f64) -> Self {
        self.failure.timeout_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set the device loss rate for the failure configuration.
    pub fn with_device_loss_rate(mut self, rate: f64) -> Self {
        self.failure.device_loss_rate = rate.clamp(0.0, 1.0);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sim_config_default() {
        let config = SimConfig::default();
        assert_eq!(config.capacity, 1024 * 1024 * 1024);
        assert_eq!(config.seed, 42);
        assert!(!config.simulate_fragmentation);
    }

    #[test]
    fn test_sim_config_with_capacity() {
        let config = SimConfig::with_capacity(512);
        assert_eq!(config.capacity, 512);
    }

    #[test]
    fn test_sim_config_builder() {
        let config = SimConfig::default().with_seed(123).with_fragmentation(0.5);
        assert_eq!(config.seed, 123);
        assert!(config.simulate_fragmentation);
        assert!((config.fragmentation_factor - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_fragmentation_clamped() {
        let config = SimConfig::default().with_fragmentation(2.0);
        assert!((config.fragmentation_factor - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_latency_config_default() {
        let latency = LatencyConfig::default();
        assert_eq!(latency.base, Duration::from_micros(10));
        assert!((latency.jitter_fraction - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_failure_pattern_default() {
        let pattern: FailurePattern = Default::default();
        assert_eq!(pattern, FailurePattern::Random);
    }

    #[test]
    fn test_sim_config_with_failure_pattern() {
        let config = SimConfig::default()
            .with_failure_pattern(FailurePattern::Burst);
        assert_eq!(config.failure.failure_pattern, FailurePattern::Burst);
    }

    #[test]
    fn test_sim_config_with_corruption_rate() {
        let config = SimConfig::default().with_corruption_rate(0.3);
        assert!((config.failure.corruption_rate - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sim_config_with_timeout_rate() {
        let config = SimConfig::default().with_timeout_rate(0.2);
        assert!((config.failure.timeout_rate - 0.2).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sim_config_with_device_loss_rate() {
        let config = SimConfig::default().with_device_loss_rate(0.05);
        assert!((config.failure.device_loss_rate - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn test_corruption_rate_clamped() {
        let config = SimConfig::default().with_corruption_rate(1.5);
        assert!((config.failure.corruption_rate - 1.0).abs() < f64::EPSILON);
        let config2 = SimConfig::default().with_corruption_rate(-0.5);
        assert!((config2.failure.corruption_rate).abs() < f64::EPSILON);
    }

    #[test]
    fn test_failure_config_default_new_fields() {
        let failure = FailureConfig::default();
        assert!((failure.corruption_rate).abs() < f64::EPSILON);
        assert!((failure.timeout_rate).abs() < f64::EPSILON);
        assert!((failure.device_loss_rate).abs() < f64::EPSILON);
        assert_eq!(failure.failure_pattern, FailurePattern::Random);
    }
}
