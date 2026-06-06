//! Swap device discovery for Linux.
//!
//! Provides read-only observation of Linux swap devices via `/proc/swaps`.
//! Supports both real reads on Linux and deterministic simulation for
//! testing and replay.

use serde::{Serialize, Deserialize};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::SeedableRng;

use ghost_core::emitter::EventEmitter;
use ghost_core::error::{GhostError, GhostResult};
use ghost_core::events::Event;
use ghost_core::time::TimeProvider;

/// Kind of swap device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SwapKind {
    /// Swap file (e.g., `/swapfile`).
    File,

    /// Swap partition (e.g., `/dev/sda2`).
    Partition,

    /// Unknown or unrecognized swap type.
    Unknown,
}

impl std::fmt::Display for SwapKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SwapKind::File => write!(f, "file"),
            SwapKind::Partition => write!(f, "partition"),
            SwapKind::Unknown => write!(f, "unknown"),
        }
    }
}

/// A single swap device entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwapDevice {
    /// Device name or file path.
    pub name: String,

    /// Kind of swap device (file, partition, or unknown).
    pub kind: SwapKind,

    /// Swap priority (higher = preferred by kernel).
    pub priority: i32,

    /// Total size in kB.
    pub size_kb: u64,

    /// Used space in kB.
    pub used_kb: u64,
}

/// Complete swap topology snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwapTopology {
    /// All swap devices.
    pub devices: Vec<SwapDevice>,

    /// Total swap space across all devices in kB.
    pub total_kb: u64,

    /// Used swap space across all devices in kB.
    pub used_kb: u64,

    /// Timestamp when the snapshot was taken (seconds since epoch).
    pub timestamp: u64,
}

/// Reader for Linux `/proc/swaps`.
///
/// Reads from `/proc/swaps` on Linux. On non-Linux platforms
/// or when the file is absent, returns a graceful error.
pub struct SwapReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
}

impl SwapReader {
    /// Create a new swap reader.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        Self {
            time_provider,
            event_emitter,
        }
    }

    /// Read swap topology from `/proc/swaps`.
    ///
    /// On Linux, reads from `/proc/swaps`.
    /// On non-Linux platforms, returns `GhostError::Internal`.
    pub fn read(&self) -> GhostResult<SwapTopology> {
        #[cfg(target_os = "linux")]
        {
            let content = fs::read_to_string("/proc/swaps").map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    GhostError::Internal(
                        "/proc/swaps not found. Kernel may lack CONFIG_PROC_FS".to_string(),
                    )
                } else {
                    GhostError::Io(e)
                }
            })?;

            let topology = self.parse(&content)?;
            self.emit_events(&topology);
            Ok(topology)
        }

        #[cfg(not(target_os = "linux"))]
        {
            Err(GhostError::Internal(
                "/proc/swaps is only available on Linux".to_string(),
            ))
        }
    }

    /// Read swap topology from a file (for testing/replay).
    pub fn read_from_file(path: &str) -> GhostResult<SwapTopology> {
        let content = fs::read_to_string(path).map_err(GhostError::Io)?;
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = SwapReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );
        reader.parse(&content)
    }

    /// Parse `/proc/swaps` content into a [`SwapTopology`].
    ///
    /// Expected format:
    /// ```text
    /// Filename                                Type            Size    Used    Priority
    /// /dev/sda2                               partition       8388608 0       -1
    /// /swapfile                               file            2097152 1024    0
    /// ```
    pub fn parse(&self, content: &str) -> GhostResult<SwapTopology> {
        let mut devices = Vec::new();
        let mut lines = content.lines();

        // Skip header line
        if let Some(header) = lines.next() {
            if !header.contains("Filename") {
                // No header — process this line too
                if let Some(device) = self.parse_line(header)? {
                    devices.push(device);
                }
            }
        }

        for line in lines {
            if let Some(device) = self.parse_line(line)? {
                devices.push(device);
            }
        }

        let total_kb = devices.iter().map(|d| d.size_kb).sum();
        let used_kb = devices.iter().map(|d| d.used_kb).sum();

        Ok(SwapTopology {
            devices,
            total_kb,
            used_kb,
            timestamp: self.time_provider.timestamp_secs(),
        })
    }

    /// Parse a single line from `/proc/swaps`.
    fn parse_line(&self, line: &str) -> GhostResult<Option<SwapDevice>> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 4 {
            return Ok(None);
        }

        let name = parts[0].to_string();
        let kind = self.detect_kind(parts.get(1).unwrap_or(&""), &name);
        let size_kb = parts.get(2).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        let used_kb = parts.get(3).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        let priority = parts.get(4).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);

        Ok(Some(SwapDevice {
            name,
            kind,
            priority,
            size_kb,
            used_kb,
        }))
    }

    /// Detect swap kind from type field and name.
    fn detect_kind(&self, type_field: &str, name: &str) -> SwapKind {
        match type_field.to_lowercase().as_str() {
            "file" => SwapKind::File,
            "partition" => SwapKind::Partition,
            _ => {
                // Fallback: infer from name
                if name.starts_with("/dev/") {
                    SwapKind::Partition
                } else if name.contains(".swap") || name.contains("swapfile") {
                    SwapKind::File
                } else {
                    SwapKind::Unknown
                }
            }
        }
    }

    /// Emit swap events for the topology.
    pub fn emit_events(&self, topology: &SwapTopology) {
        let device_names: Vec<String> = topology.devices.iter().map(|d| d.name.clone()).collect();

        let _ = self.event_emitter.try_emit(Event::SwapTopologyChanged {
            sequence_id: 0,
            devices: device_names,
            total_kb: topology.total_kb,
            used_kb: topology.used_kb,
        });

        for device in &topology.devices {
            let _ = self.event_emitter.try_emit(Event::SwapUtilizationChanged {
                sequence_id: 0,
                device: device.name.clone(),
                used_kb: device.used_kb,
                total_kb: device.size_kb,
            });
        }
    }
}

/// Deterministic swap reader for testing and replay.
///
/// Generates deterministic swap topology from a seed. When a simulation
/// file exists, reads from it; otherwise generates pseudo-random but
/// deterministic swap data.
pub struct SimulatedSwapReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
    seed: u64,
    simulation_path: Option<String>,
}

impl SimulatedSwapReader {
    /// Create a new simulated swap reader.
    ///
    /// # Arguments
    /// * `time_provider` — Time source for timestamps.
    /// * `event_emitter` — Emitter for swap events.
    /// * `seed` — Deterministic seed for value generation.
    /// * `simulation_path` — Optional path to a file containing swap data.
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

    /// Read simulated swap data.
    ///
    /// If a simulation file is set and exists, reads from it.
    /// Otherwise generates deterministic values from the seed.
    pub fn read(&self) -> GhostResult<SwapTopology> {
        // Try simulation file first
        if let Some(ref path) = self.simulation_path {
            if Path::new(path).exists() {
                let content = fs::read_to_string(path).map_err(GhostError::Io)?;
                let reader = SwapReader::new(
                    self.time_provider.clone(),
                    self.event_emitter.clone(),
                );
                return reader.parse(&content);
            }
        }

        // Generate deterministic values from seed
        let mut rng = StdRng::seed_from_u64(self.seed);
        use rand::Rng;

        let device_count: usize = rng.gen_range(1..=4);
        let mut devices = Vec::new();

        for i in 0..device_count {
            let is_partition: bool = rng.gen_bool(0.5);
            let name = if is_partition {
                format!("/dev/swap{}", i)
            } else {
                format!("/swapfile{}", i)
            };

            let kind = if is_partition {
                SwapKind::Partition
            } else {
                SwapKind::File
            };

            let size_kb: u64 = rng.gen_range(512_000..16_000_000); // 512MB - 16GB
            let used_kb: u64 = rng.gen_range(0..=size_kb);
            let priority: i32 = rng.gen_range(-32768..32767);

            devices.push(SwapDevice {
                name,
                kind,
                priority,
                size_kb,
                used_kb,
            });
        }

        // Sort by priority descending (higher = preferred)
        devices.sort_by_key(|d| -d.priority);

        let total_kb = devices.iter().map(|d| d.size_kb).sum();
        let used_kb = devices.iter().map(|d| d.used_kb).sum();

        let topology = SwapTopology {
            devices,
            total_kb,
            used_kb,
            timestamp: self.time_provider.timestamp_secs(),
        };

        // Emit events
        let reader = SwapReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        reader.emit_events(&topology);

        Ok(topology)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_line_basic() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = SwapReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let device = reader
            .parse_line("/dev/sda2                               partition       8388608 0       -1")
            .unwrap()
            .unwrap();

        assert_eq!(device.name, "/dev/sda2");
        assert_eq!(device.kind, SwapKind::Partition);
        assert_eq!(device.size_kb, 8_388_608);
        assert_eq!(device.used_kb, 0);
        assert_eq!(device.priority, -1);
    }

    #[test]
    fn test_parse_line_file() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = SwapReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let device = reader
            .parse_line("/swapfile                               file            2097152 1024    0")
            .unwrap()
            .unwrap();

        assert_eq!(device.name, "/swapfile");
        assert_eq!(device.kind, SwapKind::File);
        assert_eq!(device.size_kb, 2_097_152);
        assert_eq!(device.used_kb, 1024);
        assert_eq!(device.priority, 0);
    }

    #[test]
    fn test_parse_line_short() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = SwapReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        // Too few fields
        assert!(reader.parse_line("/dev/sda2 partition 8388608").unwrap().is_none());
        assert!(reader.parse_line("").unwrap().is_none());
    }

    #[test]
    fn test_detect_kind_from_type_field() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = SwapReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        assert_eq!(reader.detect_kind("partition", "/dev/sda2"), SwapKind::Partition);
        assert_eq!(reader.detect_kind("file", "/swapfile"), SwapKind::File);
        assert_eq!(reader.detect_kind("FILE", "/swapfile"), SwapKind::File);
        assert_eq!(reader.detect_kind("PARTITION", "/dev/sda2"), SwapKind::Partition);
    }

    #[test]
    fn test_detect_kind_fallback_from_name() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = SwapReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        // Unknown type field, but name starts with /dev/
        assert_eq!(reader.detect_kind("unknown", "/dev/sda2"), SwapKind::Partition);
        // Unknown type field, name contains swapfile
        assert_eq!(reader.detect_kind("unknown", "/swapfile"), SwapKind::File);
        // Unknown type field, name contains .swap
        assert_eq!(reader.detect_kind("unknown", "/var/swap.swap"), SwapKind::File);
        // Truly unknown
        assert_eq!(reader.detect_kind("unknown", "/something"), SwapKind::Unknown);
    }

    #[test]
    fn test_swap_kind_display() {
        assert_eq!(format!("{}", SwapKind::File), "file");
        assert_eq!(format!("{}", SwapKind::Partition), "partition");
        assert_eq!(format!("{}", SwapKind::Unknown), "unknown");
    }
}
