//! Configuration for the disk storage backend.
//!
//! This module defines the configuration types for `DiskBackend`, including
//! disk type profiles (HDD, SSD, NVMe), latency models, and failure injection.

use std::path::PathBuf;
use std::time::Duration;

// ─── Latency Configuration ────────────────────────────────────────────────────

/// Latency model for storage operations.
#[derive(Debug, Clone)]
pub struct LatencyConfig {
    /// Base latency for any operation.
    pub base: Duration,

    /// Per-byte latency (scales with data size).
    pub per_byte: Duration,

    /// Jitter fraction (0.0 = no jitter, 1.0 = up to 100% extra).
    pub jitter_fraction: f64,
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_micros(500),
            per_byte: Duration::from_nanos(100),
            jitter_fraction: 0.2,
        }
    }
}

// ─── Bandwidth Configuration ──────────────────────────────────────────────────

/// Bandwidth model for storage operations.
#[derive(Debug, Clone)]
pub struct BandwidthConfig {
    /// Maximum throughput in bytes per second.
    pub bytes_per_second: u64,
}

impl Default for BandwidthConfig {
    fn default() -> Self {
        Self {
            bytes_per_second: 500 * 1024 * 1024, // 500 MB/s
        }
    }
}

// ─── Failure Configuration ────────────────────────────────────────────────────

/// Failure injection configuration for testing.
#[derive(Debug, Clone)]
pub struct FailureConfig {
    /// Probability of a write failure (0.0 = never, 1.0 = always).
    pub write_failure_rate: f64,

    /// Probability of a read failure (0.0 = never, 1.0 = always).
    pub read_failure_rate: f64,

    /// Probability of silent data corruption (0.0 = never, 1.0 = always).
    pub corruption_rate: f64,
}

impl Default for FailureConfig {
    fn default() -> Self {
        Self {
            write_failure_rate: 0.0,
            read_failure_rate: 0.0,
            corruption_rate: 0.0,
        }
    }
}

// ─── Disk Type ────────────────────────────────────────────────────────────────

/// Type of disk storage, affecting latency and throughput characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DiskType {
    /// Hard disk drive — higher latency, sequential access preferred.
    Hdd,

    /// Solid state drive — lower latency, random access OK.
    #[default]
    Ssd,

    /// NVMe SSD — very low latency, high IOPS.
    Nvme,
}

impl DiskType {
    /// Get the default latency configuration for this disk type.
    pub fn default_latency(&self) -> LatencyConfig {
        match self {
            DiskType::Hdd => LatencyConfig {
                base: Duration::from_millis(5),
                per_byte: Duration::from_micros(10),
                jitter_fraction: 0.5,
            },
            DiskType::Ssd => LatencyConfig {
                base: Duration::from_micros(500),
                per_byte: Duration::from_nanos(100),
                jitter_fraction: 0.2,
            },
            DiskType::Nvme => LatencyConfig {
                base: Duration::from_micros(50),
                per_byte: Duration::from_nanos(10),
                jitter_fraction: 0.1,
            },
        }
    }

    /// Get the default bandwidth configuration for this disk type.
    pub fn default_bandwidth(&self) -> BandwidthConfig {
        match self {
            DiskType::Hdd => BandwidthConfig {
                bytes_per_second: 200 * 1024 * 1024, // 200 MB/s
            },
            DiskType::Ssd => BandwidthConfig {
                bytes_per_second: 500 * 1024 * 1024, // 500 MB/s
            },
            DiskType::Nvme => BandwidthConfig {
                bytes_per_second: 3 * 1024 * 1024 * 1024, // 3 GB/s
            },
        }
    }
}

// ─── Disk Configuration ───────────────────────────────────────────────────────

/// Configuration for the disk storage backend.
#[derive(Debug, Clone)]
pub struct DiskConfig {
    /// Base path for storing chunk files.
    pub base_path: PathBuf,

    /// Total capacity in bytes.
    pub capacity: usize,

    /// Latency configuration.
    pub latency: LatencyConfig,

    /// Bandwidth configuration.
    pub bandwidth: BandwidthConfig,

    /// Failure injection configuration.
    pub failure: FailureConfig,

    /// Type of disk (affects default latency/bandwidth).
    pub disk_type: DiskType,

    /// Maximum number of concurrent I/O operations.
    pub max_concurrent_ops: usize,

    /// Maximum queue depth for I/O operations.
    pub max_queue_depth: usize,

    /// Whether to enable fsync after writes.
    pub fsync_enabled: bool,

    /// Whether to use atomic write operations (temp file + rename).
    pub atomic_writes: bool,

    /// RNG seed for deterministic behavior (None for random).
    pub seed: Option<u64>,
}

impl Default for DiskConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("/var/lib/ghostpages/data"),
            capacity: 100 * 1024 * 1024 * 1024, // 100 GB
            latency: LatencyConfig::default(),
            bandwidth: BandwidthConfig::default(),
            failure: FailureConfig::default(),
            disk_type: DiskType::default(),
            max_concurrent_ops: 64,
            max_queue_depth: 256,
            fsync_enabled: true,
            atomic_writes: true,
            seed: None,
        }
    }
}

impl DiskConfig {
    /// Create a new disk config with the given base path and capacity.
    pub fn new(base_path: PathBuf, capacity: usize) -> Self {
        Self {
            base_path,
            capacity,
            ..Default::default()
        }
    }

    /// Set the disk type and apply its default latency/bandwidth.
    pub fn with_disk_type(mut self, disk_type: DiskType) -> Self {
        self.disk_type = disk_type;
        self.latency = disk_type.default_latency();
        self.bandwidth = disk_type.default_bandwidth();
        self
    }

    /// Set the RNG seed for deterministic behavior.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Enable or disable atomic writes.
    pub fn with_atomic_writes(mut self, enabled: bool) -> Self {
        self.atomic_writes = enabled;
        self
    }

    /// Enable or disable fsync.
    pub fn with_fsync(mut self, enabled: bool) -> Self {
        self.fsync_enabled = enabled;
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

    /// Set the maximum concurrent operations.
    pub fn with_max_concurrent_ops(mut self, max_ops: usize) -> Self {
        self.max_concurrent_ops = max_ops;
        self
    }

    /// Set the maximum queue depth.
    pub fn with_max_queue_depth(mut self, max_depth: usize) -> Self {
        self.max_queue_depth = max_depth;
        self
    }
}

// ─── Builder ──────────────────────────────────────────────────────────────────

/// Builder for creating DiskConfig with method chaining.
#[derive(Debug, Clone, Default)]
pub struct DiskConfigBuilder {
    config: DiskConfig,
}

impl DiskConfigBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            config: DiskConfig::default(),
        }
    }

    /// Set the base path.
    pub fn base_path(mut self, path: PathBuf) -> Self {
        self.config.base_path = path;
        self
    }

    /// Set the capacity in bytes.
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.config.capacity = capacity;
        self
    }

    /// Set the disk type.
    pub fn disk_type(mut self, disk_type: DiskType) -> Self {
        self.config = self.config.with_disk_type(disk_type);
        self
    }

    /// Set the RNG seed.
    pub fn seed(mut self, seed: u64) -> Self {
        self.config.seed = Some(seed);
        self
    }

    /// Enable atomic writes.
    pub fn atomic_writes(mut self, enabled: bool) -> Self {
        self.config.atomic_writes = enabled;
        self
    }

    /// Enable fsync.
    pub fn fsync(mut self, enabled: bool) -> Self {
        self.config.fsync_enabled = enabled;
        self
    }

    /// Build the final DiskConfig.
    pub fn build(self) -> DiskConfig {
        self.config
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_type_default() {
        assert_eq!(DiskType::default(), DiskType::Ssd);
    }

    #[test]
    fn test_disk_type_latency() {
        let hdd_latency = DiskType::Hdd.default_latency();
        let ssd_latency = DiskType::Ssd.default_latency();
        let nvme_latency = DiskType::Nvme.default_latency();

        // NVMe should have lowest latency
        assert!(nvme_latency.base < ssd_latency.base);
        // SSD should have lower latency than HDD
        assert!(ssd_latency.base < hdd_latency.base);
    }

    #[test]
    fn test_disk_type_bandwidth() {
        let hdd_bandwidth = DiskType::Hdd.default_bandwidth();
        let ssd_bandwidth = DiskType::Ssd.default_bandwidth();
        let nvme_bandwidth = DiskType::Nvme.default_bandwidth();

        // NVMe should have highest bandwidth
        assert!(nvme_bandwidth.bytes_per_second > ssd_bandwidth.bytes_per_second);
        // SSD should have higher bandwidth than HDD
        assert!(ssd_bandwidth.bytes_per_second > hdd_bandwidth.bytes_per_second);
    }

    #[test]
    fn test_disk_config_default() {
        let config = DiskConfig::default();
        assert_eq!(config.capacity, 100 * 1024 * 1024 * 1024);
        assert!(config.atomic_writes);
        assert!(config.fsync_enabled);
    }

    #[test]
    fn test_disk_config_builder() {
        let config = DiskConfigBuilder::new()
            .base_path(PathBuf::from("/tmp/test"))
            .capacity(1024 * 1024)
            .disk_type(DiskType::Nvme)
            .seed(42)
            .atomic_writes(false)
            .fsync(false)
            .build();

        assert_eq!(config.base_path, PathBuf::from("/tmp/test"));
        assert_eq!(config.capacity, 1024 * 1024);
        assert_eq!(config.disk_type, DiskType::Nvme);
        assert_eq!(config.seed, Some(42));
        assert!(!config.atomic_writes);
        assert!(!config.fsync_enabled);
    }

    #[test]
    fn test_disk_config_with_disk_type() {
        let config = DiskConfig::default().with_disk_type(DiskType::Hdd);
        assert_eq!(config.disk_type, DiskType::Hdd);
        // Should have applied HDD defaults
        assert!(config.latency.base >= Duration::from_millis(1));
    }

    #[test]
    fn test_disk_config_with_seed() {
        let config = DiskConfig::default().with_seed(123);
        assert_eq!(config.seed, Some(123));
    }
}
