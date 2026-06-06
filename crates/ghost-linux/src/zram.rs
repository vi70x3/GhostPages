//! ZRAM (compressed RAM disk) awareness for Linux.
//!
//! Provides read-only observation of Linux ZRAM devices via `/sys/block/zram*/`.
//! Supports both real reads on Linux and deterministic simulation for
//! testing and replay.
//!
//! ZRAM devices represent a tier between DRAM and swap:
//! - Faster than disk swap (compressed RAM)
//! - Slower than DRAM (decompression overhead)
//! - Compression ratio tracked as efficiency metric
use serde::{Serialize, Deserialize};

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::SeedableRng;

use ghost_core::emitter::EventEmitter;
use ghost_core::error::{GhostError, GhostResult};
use ghost_core::events::Event;
use ghost_core::time::TimeProvider;

/// A single ZRAM device's statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZramDevice {
    /// Device name (e.g., "zram0").
    pub name: String,

    /// Original (uncompressed) data size in kB.
    pub orig_size_kb: u64,

    /// Compressed data size in kB.
    pub comp_size_kb: u64,

    /// Total memory used in kB.
    pub mem_used_total_kb: u64,

    /// Maximum number of compression streams.
    pub max_comp_streams: u32,

    /// Compression algorithm name (e.g., "lzo", "zstd").
    pub comp_algorithm: String,
}

impl ZramDevice {
    /// Calculate compression ratio (original / compressed).
    ///
    /// Returns `None` if compressed size is 0 (no data stored).
    pub fn compression_ratio(&self) -> Option<f32> {
        if self.comp_size_kb == 0 {
            None
        } else {
            Some(self.orig_size_kb as f32 / self.comp_size_kb as f32)
        }
    }
}

/// Complete ZRAM snapshot across all devices.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZramSnapshot {
    /// All ZRAM devices found.
    pub devices: Vec<ZramDevice>,

    /// Total original (uncompressed) size across all devices in kB.
    pub total_orig_kb: u64,

    /// Total compressed size across all devices in kB.
    pub total_comp_kb: u64,

    /// Overall compression ratio (total_orig / total_comp).
    /// `None` if no data is stored.
    pub compression_ratio: f32,

    /// Timestamp when the snapshot was taken (seconds since epoch).
    pub timestamp: u64,
}

/// Reader for Linux ZRAM devices via `/sys/block/zram*/`.
///
/// Reads from `/sys/block/zramN/` on Linux. On non-Linux platforms
/// or when no ZRAM devices exist, returns a graceful error.
pub struct ZramReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
}

impl ZramReader {
    /// Create a new ZRAM reader.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        Self {
            time_provider,
            event_emitter,
        }
    }

    /// Read ZRAM statistics from `/sys/block/zram*/`.
    ///
    /// On Linux, discovers all zram devices and reads their stats.
    /// On non-Linux platforms, returns `GhostError::Internal`.
    pub fn read(&self) -> GhostResult<ZramSnapshot> {
        #[cfg(target_os = "linux")]
        {
            let devices = self.discover_devices()?;
            if devices.is_empty() {
                return Err(GhostError::Internal(
                    "No ZRAM devices found at /sys/block/zram*".to_string(),
                ));
            }

            let snapshot = self.build_snapshot(devices);
            self.emit_events(&snapshot);
            Ok(snapshot)
        }

        #[cfg(not(target_os = "linux"))]
        {
            Err(GhostError::Internal(
                "ZRAM is only available on Linux".to_string(),
            ))
        }
    }

    /// Read a single ZRAM device by name.
    ///
    /// `name` should be the device name without path (e.g., "zram0").
    pub fn read_device(&self, name: &str) -> GhostResult<ZramDevice> {
        #[cfg(target_os = "linux")]
        {
            let base = PathBuf::from(format!("/sys/block/{}", name));
            if !base.exists() {
                return Err(GhostError::Internal(
                    format!("ZRAM device {} not found", name),
                ));
            }

            let orig_size = self.parse_sysfs_value(&base, "orig_size")? / 1024;
            let comp_size = self.parse_sysfs_value(&base, "compr_data_size")? / 1024;
            let mem_used = self.parse_sysfs_value(&base, "mem_used_total")? / 1024;
            let max_streams = self.parse_sysfs_value(&base, "max_comp_streams")? as u32;
            let algorithm = self.read_sysfs_string(&base, "comp_algorithm")?;

            Ok(ZramDevice {
                name: name.to_string(),
                orig_size_kb: orig_size,
                comp_size_kb: comp_size,
                mem_used_total_kb: mem_used,
                max_comp_streams: max_streams,
                comp_algorithm: algorithm,
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            Err(GhostError::Internal(
                "ZRAM is only available on Linux".to_string(),
            ))
        }
    }

    /// Discover all ZRAM devices in `/sys/block/`.
    #[cfg(target_os = "linux")]
    fn discover_devices(&self) -> GhostResult<Vec<ZramDevice>> {
        let mut devices = Vec::new();
        let block_dir = Path::new("/sys/block");

        if !block_dir.exists() {
            return Ok(devices);
        }

        for entry in fs::read_dir(block_dir).map_err(GhostError::Io)? {
            let entry = entry.map_err(GhostError::Io)?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.starts_with("zram") {
                let path = entry.path();
                if let Ok(device) = self.read_device_from_path(&path, &name_str) {
                    devices.push(device);
                }
            }
        }

        // Sort by device name for deterministic ordering
        devices.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(devices)
    }

    /// Read a ZRAM device from a sysfs path.
    #[cfg(target_os = "linux")]
    fn read_device_from_path(&self, path: &Path, name: &str) -> GhostResult<ZramDevice> {
        let orig_size = self.parse_sysfs_value(path, "orig_size")? / 1024;
        let comp_size = self.parse_sysfs_value(path, "compr_data_size")? / 1024;
        let mem_used = self.parse_sysfs_value(path, "mem_used_total")? / 1024;
        let max_streams = self.parse_sysfs_value(path, "max_comp_streams")? as u32;
        let algorithm = self.read_sysfs_string(path, "comp_algorithm")?;

        Ok(ZramDevice {
            name: name.to_string(),
            orig_size_kb: orig_size,
            comp_size_kb: comp_size,
            mem_used_total_kb: mem_used,
            max_comp_streams: max_streams,
            comp_algorithm: algorithm,
        })
    }

    /// Parse a numeric sysfs attribute value (returns bytes).
    #[cfg(target_os = "linux")]
    fn parse_sysfs_value(&self, device_path: &Path, attr: &str) -> GhostResult<u64> {
        let path = device_path.join(attr);
        let content = fs::read_to_string(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                GhostError::Internal(
                    format!("ZRAM attribute {} not found at {:?}", attr, path),
                )
            } else {
                GhostError::Io(e)
            }
        })?;

        content
            .trim()
            .parse::<u64>()
            .map_err(|_| GhostError::Internal(
                format!("Failed to parse ZRAM attribute {}: '{}'", attr, content.trim()),
            ))
    }

    /// Read a string sysfs attribute value.
    #[cfg(target_os = "linux")]
    fn read_sysfs_string(&self, device_path: &Path, attr: &str) -> GhostResult<String> {
        let path = device_path.join(attr);
        let content = fs::read_to_string(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                GhostError::Internal(
                    format!("ZRAM attribute {} not found at {:?}", attr, path),
                )
            } else {
                GhostError::Io(e)
            }
        })?;
        Ok(content.trim().to_string())
    }

    /// Build a snapshot from a list of devices.
    fn build_snapshot(&self, devices: Vec<ZramDevice>) -> ZramSnapshot {
        let total_orig_kb = devices.iter().map(|d| d.orig_size_kb).sum();
        let total_comp_kb = devices.iter().map(|d| d.comp_size_kb).sum();
        let compression_ratio = if total_comp_kb > 0 {
            total_orig_kb as f32 / total_comp_kb as f32
        } else {
            0.0
        };

        ZramSnapshot {
            devices,
            total_orig_kb,
            total_comp_kb,
            compression_ratio,
            timestamp: self.time_provider.timestamp_secs(),
        }
    }

    /// Emit ZRAM events for the snapshot.
    fn emit_events(&self, snapshot: &ZramSnapshot) {
        for device in &snapshot.devices {
            let ratio = device.compression_ratio().unwrap_or(0.0);
            let _ = self.event_emitter.try_emit(Event::ZramUtilizationChanged {
                sequence_id: 0,
                device: device.name.clone(),
                orig_kb: device.orig_size_kb,
                comp_kb: device.comp_size_kb,
                ratio,
            });
        }

        let tiers: Vec<String> = snapshot.devices.iter().map(|d| d.name.clone()).collect();
        let _ = self.event_emitter.try_emit(Event::TierInventoryChanged {
            sequence_id: 0,
            tiers,
        });
    }
}

/// Deterministic ZRAM reader for testing and replay.
///
/// Generates deterministic ZRAM state from a seed. When a simulation
/// file exists, reads from it; otherwise generates pseudo-random but
/// deterministic ZRAM data.
pub struct SimulatedZramReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
    seed: u64,
    simulation_path: Option<String>,
}

impl SimulatedZramReader {
    /// Create a new simulated ZRAM reader.
    ///
    /// # Arguments
    /// * `time_provider` — Time source for timestamps.
    /// * `event_emitter` — Emitter for ZRAM events.
    /// * `seed` — Deterministic seed for value generation.
    /// * `simulation_path` — Optional path to a file containing ZRAM data.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
        seed: u64,
        simulation_path: Option<String>,
    ) -> Self {
        Self {
            time_provider,
            event_emitter,
            seed,
            simulation_path,
        }
    }

    /// Read simulated ZRAM data.
    ///
    /// If a simulation file is set and exists, reads from it.
    /// Otherwise generates deterministic values from the seed.
    pub fn read(&self) -> GhostResult<ZramSnapshot> {
        // Try simulation file first
        if let Some(ref path) = self.simulation_path {
            if Path::new(path).exists() {
                let content = fs::read_to_string(path).map_err(GhostError::Io)?;
                return self.parse(&content);
            }
        }

        // Generate deterministic values from seed
        let mut rng = StdRng::seed_from_u64(self.seed);
        use rand::Rng;

        let device_count: usize = rng.gen_range(1..=3);
        let mut devices = Vec::new();

        let algorithms = ["lzo", "zstd", "lz4", "deflate"];

        for i in 0..device_count {
            let name = format!("zram{}", i);
            let orig_size_kb: u64 = rng.gen_range(256_000..8_388_608); // 256MB - 8GB
            let ratio: f64 = rng.gen_range(1.5..4.0);
            let comp_size_kb: u64 = (orig_size_kb as f64 / ratio) as u64;
            let mem_used_total_kb: u64 = rng.gen_range(0..=orig_size_kb);
            let max_comp_streams: u32 = rng.gen_range(1..=8);
            let algo_idx: usize = rng.gen_range(0..algorithms.len());
            let comp_algorithm = algorithms[algo_idx].to_string();

            devices.push(ZramDevice {
                name,
                orig_size_kb,
                comp_size_kb,
                mem_used_total_kb,
                max_comp_streams,
                comp_algorithm,
            });
        }

        let total_orig_kb = devices.iter().map(|d| d.orig_size_kb).sum();
        let total_comp_kb = devices.iter().map(|d| d.comp_size_kb).sum();
        let compression_ratio = if total_comp_kb > 0 {
            total_orig_kb as f32 / total_comp_kb as f32
        } else {
            0.0
        };

        let snapshot = ZramSnapshot {
            devices,
            total_orig_kb,
            total_comp_kb,
            compression_ratio,
            timestamp: self.time_provider.timestamp_secs(),
        };

        // Emit events
        let reader = ZramReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        reader.emit_events(&snapshot);

        Ok(snapshot)
    }

    /// Parse ZRAM data from a string (for simulation file replay).
    ///
    /// Expected format (one block per device):
    /// ```text
    /// zram0
    ///   orig_size: 4194304
    ///   comp_size: 1048576
    ///   mem_used_total: 2097152
    ///   max_comp_streams: 2
    ///   comp_algorithm: zstd
    /// ```
    pub fn parse(&self, content: &str) -> GhostResult<ZramSnapshot> {
        let mut devices = Vec::new();
        let mut lines = content.lines().peekable();

        while let Some(line) = lines.next() {
            let name = line.trim();
            if name.is_empty() {
                continue;
            }

            let mut orig_size_kb = 0u64;
            let mut comp_size_kb = 0u64;
            let mut mem_used_total_kb = 0u64;
            let mut max_comp_streams = 0u32;
            let mut comp_algorithm = String::new();

            while let Some(sub_line) = lines.peek() {
                let trimmed = sub_line.trim();
                if trimmed.is_empty() || !sub_line.starts_with(' ') {
                    break;
                }
                let trimmed = trimmed.trim();
                if let Some((key, value)) = trimmed.split_once(':') {
                    let key = key.trim();
                    let value = value.trim();
                    match key {
                        "orig_size" => {
                            orig_size_kb = value.parse::<u64>().unwrap_or(0);
                        }
                        "comp_size" => {
                            comp_size_kb = value.parse::<u64>().unwrap_or(0);
                        }
                        "mem_used_total" => {
                            mem_used_total_kb = value.parse::<u64>().unwrap_or(0);
                        }
                        "max_comp_streams" => {
                            max_comp_streams = value.parse::<u32>().unwrap_or(0);
                        }
                        "comp_algorithm" => {
                            comp_algorithm = value.to_string();
                        }
                        _ => {}
                    }
                }
                lines.next();
            }

            devices.push(ZramDevice {
                name: name.to_string(),
                orig_size_kb,
                comp_size_kb,
                mem_used_total_kb,
                max_comp_streams,
                comp_algorithm,
            });
        }

        let total_orig_kb = devices.iter().map(|d| d.orig_size_kb).sum();
        let total_comp_kb = devices.iter().map(|d| d.comp_size_kb).sum();
        let compression_ratio = if total_comp_kb > 0 {
            total_orig_kb as f32 / total_comp_kb as f32
        } else {
            0.0
        };

        Ok(ZramSnapshot {
            devices,
            total_orig_kb,
            total_comp_kb,
            compression_ratio,
            timestamp: self.time_provider.timestamp_secs(),
        })
    }
}

/// Prometheus metrics for ZRAM.
pub mod metrics {
    use prometheus::{Gauge, Opts, Registry};

    use ghost_core::error::{GhostError, GhostResult};

    /// Container for all ZRAM metrics.
    pub struct ZramMetrics {
        /// Compression ratio gauge.
        pub compression_ratio: Gauge,
        /// Used bytes gauge.
        pub used_bytes: Gauge,
        /// Original bytes gauge.
        pub original_bytes: Gauge,
        /// Device count gauge.
        pub device_count: Gauge,
    }

    /// Register ZRAM metrics with the given registry.
    pub fn register(registry: &Registry) -> GhostResult<ZramMetrics> {
        let compression_ratio = Gauge::with_opts(
            Opts::new(
                "ghost_zram_compression_ratio",
                "ZRAM compression ratio (original / compressed)",
            )
        ).map_err(|e| GhostError::Internal(e.to_string()))?;

        let used_bytes = Gauge::with_opts(
            Opts::new(
                "ghost_zram_used_bytes",
                "ZRAM total memory used in bytes",
            )
        ).map_err(|e| GhostError::Internal(e.to_string()))?;

        let original_bytes = Gauge::with_opts(
            Opts::new(
                "ghost_zram_original_bytes",
                "ZRAM total original (uncompressed) bytes",
            )
        ).map_err(|e| GhostError::Internal(e.to_string()))?;

        let device_count = Gauge::with_opts(
            Opts::new(
                "ghost_zram_device_count",
                "Number of ZRAM devices",
            )
        ).map_err(|e| GhostError::Internal(e.to_string()))?;

        registry.register(Box::new(compression_ratio.clone())).map_err(|e| GhostError::Internal(e.to_string()))?;
        registry.register(Box::new(used_bytes.clone())).map_err(|e| GhostError::Internal(e.to_string()))?;
        registry.register(Box::new(original_bytes.clone())).map_err(|e| GhostError::Internal(e.to_string()))?;
        registry.register(Box::new(device_count.clone())).map_err(|e| GhostError::Internal(e.to_string()))?;

        Ok(ZramMetrics {
            compression_ratio,
            used_bytes,
            original_bytes,
            device_count,
        })
    }

    /// Update ZRAM metrics from a snapshot.
    pub fn update_from_snapshot(
        metrics: &ZramMetrics,
        snapshot: &super::ZramSnapshot,
    ) {
        metrics.compression_ratio.set(snapshot.compression_ratio as f64);
        metrics.used_bytes.set(snapshot.total_comp_kb as f64 * 1024.0);
        metrics.original_bytes.set(snapshot.total_orig_kb as f64 * 1024.0);
        metrics.device_count.set(snapshot.devices.len() as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_ratio_calculation() {
        let device = ZramDevice {
            name: "zram0".to_string(),
            orig_size_kb: 4_194_304, // 4GB
            comp_size_kb: 1_048_576, // 1GB
            mem_used_total_kb: 2_097_152,
            max_comp_streams: 2,
            comp_algorithm: "zstd".to_string(),
        };

        let ratio = device.compression_ratio().unwrap();
        assert!((ratio - 4.0).abs() < 0.01, "Expected ratio ~4.0, got {}", ratio);
    }

    #[test]
    fn test_compression_ratio_zero_comp() {
        let device = ZramDevice {
            name: "zram0".to_string(),
            orig_size_kb: 0,
            comp_size_kb: 0,
            mem_used_total_kb: 0,
            max_comp_streams: 1,
            comp_algorithm: "lzo".to_string(),
        };

        assert!(device.compression_ratio().is_none());
    }

    #[test]
    fn test_simulated_deterministic() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader1 = SimulatedZramReader::new(
            clock.clone(),
            emitter.clone(),
            42,
            None,
        );
        let reader2 = SimulatedZramReader::new(
            clock,
            emitter,
            42,
            None,
        );

        let snapshot1 = reader1.read().unwrap();
        let snapshot2 = reader2.read().unwrap();

        assert_eq!(snapshot1.devices.len(), snapshot2.devices.len());
        assert_eq!(snapshot1.total_orig_kb, snapshot2.total_orig_kb);
        assert_eq!(snapshot1.total_comp_kb, snapshot2.total_comp_kb);
        assert_eq!(snapshot1.compression_ratio, snapshot2.compression_ratio);

        for (d1, d2) in snapshot1.devices.iter().zip(snapshot2.devices.iter()) {
            assert_eq!(d1.name, d2.name);
            assert_eq!(d1.orig_size_kb, d2.orig_size_kb);
            assert_eq!(d1.comp_size_kb, d2.comp_size_kb);
            assert_eq!(d1.mem_used_total_kb, d2.mem_used_total_kb);
            assert_eq!(d1.max_comp_streams, d2.max_comp_streams);
            assert_eq!(d1.comp_algorithm, d2.comp_algorithm);
        }
    }

    #[test]
    fn test_simulated_different_seeds() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader1 = SimulatedZramReader::new(
            clock.clone(),
            emitter.clone(),
            42,
            None,
        );
        let reader2 = SimulatedZramReader::new(
            clock,
            emitter,
            99,
            None,
        );

        let snapshot1 = reader1.read().unwrap();
        let snapshot2 = reader2.read().unwrap();

        // Different seeds should produce different values
        assert_ne!(snapshot1.total_orig_kb, snapshot2.total_orig_kb);
    }

    #[test]
    fn test_parse_simulation_format() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader = SimulatedZramReader::new(clock, emitter, 42, None);

        let content = "\
zram0
  orig_size: 4194304
  comp_size: 1048576
  mem_used_total: 2097152
  max_comp_streams: 2
  comp_algorithm: zstd
zram1
  orig_size: 2097152
  comp_size: 524288
  mem_used_total: 1048576
  max_comp_streams: 4
  comp_algorithm: lzo
";

        let snapshot = reader.parse(content).unwrap();

        assert_eq!(snapshot.devices.len(), 2);
        assert_eq!(snapshot.devices[0].name, "zram0");
        assert_eq!(snapshot.devices[0].orig_size_kb, 4_194_304);
        assert_eq!(snapshot.devices[0].comp_size_kb, 1_048_576);
        assert_eq!(snapshot.devices[0].mem_used_total_kb, 2_097_152);
        assert_eq!(snapshot.devices[0].max_comp_streams, 2);
        assert_eq!(snapshot.devices[0].comp_algorithm, "zstd");

        assert_eq!(snapshot.devices[1].name, "zram1");
        assert_eq!(snapshot.devices[1].comp_algorithm, "lzo");

        assert_eq!(snapshot.total_orig_kb, 4_194_304 + 2_097_152);
        assert_eq!(snapshot.total_comp_kb, 1_048_576 + 524_288);
    }

    #[test]
    fn test_emits_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader = SimulatedZramReader::new(clock, emitter, 42, None);
        let snapshot = reader.read().unwrap();

        // Collect all emitted events
        let mut events = Vec::new();
        while let Ok(rec) = rx.try_recv() {
            events.push(rec.event);
        }

        // Should have ZramUtilizationChanged events for each device
        // plus one TierInventoryChanged event
        let zram_events: Vec<_> = events.iter()
            .filter(|e| matches!(e, Event::ZramUtilizationChanged { .. }))
            .collect();
        let tier_events: Vec<_> = events.iter()
            .filter(|e| matches!(e, Event::TierInventoryChanged { .. }))
            .collect();

        assert_eq!(zram_events.len(), snapshot.devices.len());
        assert_eq!(tier_events.len(), 1);

        // Verify the tier inventory event contains device names
        if let Event::TierInventoryChanged { tiers, .. } = &tier_events[0] {
            assert_eq!(tiers.len(), snapshot.devices.len());
            for (i, device) in snapshot.devices.iter().enumerate() {
                assert_eq!(tiers[i], device.name);
            }
        } else {
            panic!("Expected TierInventoryChanged event");
        }
    }

    #[test]
    fn test_replay() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        // Record phase
        let reader1 = SimulatedZramReader::new(clock.clone(), emitter.clone(), 42, None);
        let original = reader1.read().unwrap();

        // Collect emitted events
        let mut events = Vec::new();
        while let Ok(rec) = rx.try_recv() {
            events.push(rec.event);
        }

        // Replay phase — same seed should produce identical values
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
        let emitter2 = EventEmitter::new(tx2);
        let reader2 = SimulatedZramReader::new(clock, emitter2, 42, None);
        let replayed = reader2.read().unwrap();

        // Verify identical snapshots
        assert_eq!(original.devices.len(), replayed.devices.len());
        assert_eq!(original.total_orig_kb, replayed.total_orig_kb);
        assert_eq!(original.total_comp_kb, replayed.total_comp_kb);
        assert_eq!(original.compression_ratio, replayed.compression_ratio);

        for (d1, d2) in original.devices.iter().zip(replayed.devices.iter()) {
            assert_eq!(d1.name, d2.name);
            assert_eq!(d1.orig_size_kb, d2.orig_size_kb);
            assert_eq!(d1.comp_size_kb, d2.comp_size_kb);
            assert_eq!(d1.mem_used_total_kb, d2.mem_used_total_kb);
            assert_eq!(d1.max_comp_streams, d2.max_comp_streams);
            assert_eq!(d1.comp_algorithm, d2.comp_algorithm);
        }

        // Verify events were emitted during replay too
        let replay_events: Vec<_> = std::iter::from_fn(|| rx2.try_recv().ok())
            .map(|r| r.event)
            .collect();
        assert_eq!(replay_events.len(), events.len());
    }

    #[test]
    fn test_tier_inventory_includes_zram() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader = SimulatedZramReader::new(clock, emitter, 42, None);
        let snapshot = reader.read().unwrap();

        // Drain events
        let mut tier_event_found = false;
        while let Ok(rec) = rx.try_recv() {
            if let Event::TierInventoryChanged { tiers, .. } = &rec.event {
                tier_event_found = true;
                // Verify all ZRAM devices appear in tier inventory
                for device in &snapshot.devices {
                    assert!(
                        tiers.contains(&device.name),
                        "ZRAM device {} not in tier inventory {:?}",
                        device.name,
                        tiers
                    );
                }
            }
        }
        assert!(tier_event_found, "Expected TierInventoryChanged event");
    }
}
