//! Memory statistics collector for Linux.
//!
//! Provides read-only observation of Linux kernel memory metrics via
//! `/proc/meminfo`. Supports both real reads on Linux and deterministic
//! simulation for testing and replay.

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

/// A single memory statistics snapshot from `/proc/meminfo`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MeminfoSnapshot {
    /// Total system memory in kB.
    pub total_kb: u64,
    /// Available memory in kB (estimate of memory available for starting new applications).
    pub available_kb: u64,
    /// Free memory in kB.
    pub free_kb: u64,
    /// Buffers in kB (file metadata cache).
    pub buffers_kb: u64,
    /// Cached memory in kB (page cache).
    pub cached_kb: u64,
    /// Total swap space in kB.
    pub swap_total_kb: u64,
    /// Free swap space in kB.
    pub swap_free_kb: u64,
    /// Active memory in kB (recently used, not reclaimable).
    pub active_kb: u64,
    /// Inactive memory in kB (not recently used, reclaimable).
    pub inactive_kb: u64,
    /// Dirty memory in kB (waiting to be written to disk).
    pub dirty_kb: u64,
    /// Writeback memory in kB (actively being written to disk).
    pub writeback_kb: u64,
    /// Timestamp when the snapshot was taken (seconds since epoch).
    pub timestamp: u64,
}

/// Reader for Linux `/proc/meminfo`.
///
/// Reads from `/proc/meminfo` on Linux. On non-Linux platforms
/// or when the file is absent, returns a graceful error.
pub struct MeminfoReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
}

impl MeminfoReader {
    /// Create a new meminfo reader.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        Self {
            time_provider,
            event_emitter,
        }
    }

    /// Read memory statistics from `/proc/meminfo`.
    ///
    /// On Linux, reads from `/proc/meminfo`.
    /// On non-Linux platforms, returns `GhostError::Internal` with a
    /// message indicating meminfo is unavailable.
    pub fn read(&self) -> GhostResult<MeminfoSnapshot> {
        #[cfg(target_os = "linux")]
        {
            let content = fs::read_to_string("/proc/meminfo").map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    GhostError::Internal(
                        "/proc/meminfo not found. Kernel may lack CONFIG_PROC_FS".to_string(),
                    )
                } else {
                    GhostError::Io(e)
                }
            })?;

            let snapshot = self.parse(&content)?;
            self.emit_memory_stats_event(&snapshot);
            Ok(snapshot)
        }

        #[cfg(not(target_os = "linux"))]
        {
            Err(GhostError::Internal(
                "/proc/meminfo is only available on Linux".to_string(),
            ))
        }
    }

    /// Parse `/proc/meminfo` content into a [`MeminfoSnapshot`].
    ///
    /// Expected format per line:
    /// ```text
    /// MemTotal:       16384000 kB
    /// MemFree:         8192000 kB
    /// MemAvailable:   12288000 kB
    /// Buffers:          512000 kB
    /// Cached:          4096000 kB
    /// SwapTotal:       8388608 kB
    /// SwapFree:        8388608 kB
    /// Active:          6144000 kB
    /// Inactive:        3072000 kB
    /// Dirty:             16384 kB
    /// Writeback:         8192 kB
    /// ```
    pub fn parse(&self, content: &str) -> GhostResult<MeminfoSnapshot> {
        let mut total_kb = None;
        let mut available_kb = None;
        let mut free_kb = None;
        let mut buffers_kb = None;
        let mut cached_kb = None;
        let mut swap_total_kb = None;
        let mut swap_free_kb = None;
        let mut active_kb = None;
        let mut inactive_kb = None;
        let mut dirty_kb = None;
        let mut writeback_kb = None;

        for line in content.lines() {
            if let Some((key, value)) = self.parse_line(line) {
                match key.as_str() {
                    "MemTotal" => total_kb = Some(value),
                    "MemAvailable" => available_kb = Some(value),
                    "MemFree" => free_kb = Some(value),
                    "Buffers" => buffers_kb = Some(value),
                    "Cached" => cached_kb = Some(value),
                    "SwapTotal" => swap_total_kb = Some(value),
                    "SwapFree" => swap_free_kb = Some(value),
                    "Active" => active_kb = Some(value),
                    "Inactive" => inactive_kb = Some(value),
                    "Dirty" => dirty_kb = Some(value),
                    "Writeback" => writeback_kb = Some(value),
                    _ => {} // ignore unknown keys
                }
            }
        }

        Ok(MeminfoSnapshot {
            total_kb: total_kb.unwrap_or(0),
            available_kb: available_kb.unwrap_or(0),
            free_kb: free_kb.unwrap_or(0),
            buffers_kb: buffers_kb.unwrap_or(0),
            cached_kb: cached_kb.unwrap_or(0),
            swap_total_kb: swap_total_kb.unwrap_or(0),
            swap_free_kb: swap_free_kb.unwrap_or(0),
            active_kb: active_kb.unwrap_or(0),
            inactive_kb: inactive_kb.unwrap_or(0),
            dirty_kb: dirty_kb.unwrap_or(0),
            writeback_kb: writeback_kb.unwrap_or(0),
            timestamp: self.time_provider.timestamp_secs(),
        })
    }

    /// Parse a single line from `/proc/meminfo`.
    ///
    /// Returns `Some((key, value_in_kb))` if the line matches the expected format.
    fn parse_line(&self, line: &str) -> Option<(String, u64)> {
        // Format: "Key:       <value> kB"
        let (key, rest) = line.split_once(':')?;
        let key = key.trim().to_string();

        // Extract the numeric value (ignore unit suffix)
        let value_part = rest.trim();
        let value_str = value_part.split_whitespace().next()?;
        let value = value_str.parse::<u64>().ok()?;

        Some((key, value))
    }

    /// Emit a `MemoryStatsChanged` event.
    fn emit_memory_stats_event(&self, snapshot: &MeminfoSnapshot) {
        let swap_used_kb = snapshot.swap_total_kb.saturating_sub(snapshot.swap_free_kb);
        let event = Event::MemoryStatsChanged {
            sequence_id: 0,
            total_kb: snapshot.total_kb,
            available_kb: snapshot.available_kb,
            swap_used_kb,
            dirty_kb: snapshot.dirty_kb,
        };
        let _ = self.event_emitter.try_emit(event);
    }
}

/// Deterministic meminfo reader for testing and replay.
///
/// Reads meminfo values from a file or generates deterministic values based on
/// a seed. When the simulation file exists, it reads from it; otherwise
/// it generates pseudo-random but deterministic meminfo values.
pub struct SimulatedMeminfoReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
    seed: u64,
    simulation_path: Option<String>,
}

impl SimulatedMeminfoReader {
    /// Create a new simulated meminfo reader.
    ///
    /// # Arguments
    /// * `time_provider` — Time source for timestamps.
    /// * `event_emitter` — Emitter for memory events.
    /// * `seed` — Deterministic seed for value generation.
    /// * `simulation_path` — Optional path to a file containing meminfo data.
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

    /// Read simulated meminfo data.
    ///
    /// If a simulation file is set and exists, reads from it.
    /// Otherwise generates deterministic values from the seed.
    pub fn read(&self) -> GhostResult<MeminfoSnapshot> {
        // Try simulation file first
        if let Some(ref path) = self.simulation_path {
            if Path::new(path).exists() {
                let content = fs::read_to_string(path).map_err(GhostError::Io)?;
                let reader = MeminfoReader::new(
                    self.time_provider.clone(),
                    self.event_emitter.clone(),
                );
                return reader.parse(&content);
            }
        }

        // Generate deterministic values from seed
        let mut rng = StdRng::seed_from_u64(self.seed);

        use rand::Rng;
        let total_kb: u64 = rng.gen_range(4_000_000..32_000_000); // 4-32 GB
        let free_kb: u64 = rng.gen_range(0..total_kb);
        let available_kb: u64 = rng.gen_range(free_kb..total_kb);
        let buffers_kb: u64 = rng.gen_range(0..total_kb / 10);
        let cached_kb: u64 = rng.gen_range(0..total_kb / 5);
        let swap_total_kb: u64 = rng.gen_range(0..16_000_000); // 0-16 GB
        let swap_free_kb: u64 = rng.gen_range(0..=swap_total_kb);
        let active_kb: u64 = rng.gen_range(0..total_kb);
        let inactive_kb: u64 = rng.gen_range(0..total_kb);
        let dirty_kb: u64 = rng.gen_range(0..1_000_000);
        let writeback_kb: u64 = rng.gen_range(0..500_000);

        let snapshot = MeminfoSnapshot {
            total_kb,
            available_kb,
            free_kb,
            buffers_kb,
            cached_kb,
            swap_total_kb,
            swap_free_kb,
            active_kb,
            inactive_kb,
            dirty_kb,
            writeback_kb,
            timestamp: self.time_provider.timestamp_secs(),
        };

        // Emit event
        let reader = MeminfoReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        reader.emit_memory_stats_event(&snapshot);

        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_line_basic() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = MeminfoReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let result = reader.parse_line("MemTotal:       16384000 kB");
        assert_eq!(result, Some(("MemTotal".to_string(), 16_384_000)));

        let result = reader.parse_line("MemAvailable:   12288000 kB");
        assert_eq!(result, Some(("MemAvailable".to_string(), 12_288_000)));

        let result = reader.parse_line("Buffers:          512000 kB");
        assert_eq!(result, Some(("Buffers".to_string(), 512_000)));
    }

    #[test]
    fn test_parse_line_invalid() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = MeminfoReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        // No colon
        assert_eq!(reader.parse_line("MemTotal 16384000 kB"), None);

        // Non-numeric value
        assert_eq!(reader.parse_line("MemTotal:       abc kB"), None);

        // Empty line
        assert_eq!(reader.parse_line(""), None);
    }

    #[test]
    fn test_parse_full_meminfo() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = MeminfoReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let content = "\
MemTotal:       16384000 kB
MemFree:         8192000 kB
MemAvailable:   12288000 kB
Buffers:          512000 kB
Cached:          4096000 kB
SwapTotal:       8388608 kB
SwapFree:        8388608 kB
Active:          6144000 kB
Inactive:        3072000 kB
Dirty:             16384 kB
Writeback:         8192 kB
HugePages_Total:       0
HugePages_Free:        0
Hugepagesize:       2048 kB";

        let snapshot = reader.parse(content).unwrap();

        assert_eq!(snapshot.total_kb, 16_384_000);
        assert_eq!(snapshot.available_kb, 12_288_000);
        assert_eq!(snapshot.free_kb, 8_192_000);
        assert_eq!(snapshot.buffers_kb, 512_000);
        assert_eq!(snapshot.cached_kb, 4_096_000);
        assert_eq!(snapshot.swap_total_kb, 8_388_608);
        assert_eq!(snapshot.swap_free_kb, 8_388_608);
        assert_eq!(snapshot.active_kb, 6_144_000);
        assert_eq!(snapshot.inactive_kb, 3_072_000);
        assert_eq!(snapshot.dirty_kb, 16_384);
        assert_eq!(snapshot.writeback_kb, 8_192);
    }

    #[test]
    fn test_parse_partial_meminfo() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = MeminfoReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        // Only some fields present
        let content = "\
MemTotal:       16384000 kB
MemFree:         8192000 kB";

        let snapshot = reader.parse(content).unwrap();

        assert_eq!(snapshot.total_kb, 16_384_000);
        assert_eq!(snapshot.free_kb, 8_192_000);
        // Missing fields default to 0
        assert_eq!(snapshot.available_kb, 0);
        assert_eq!(snapshot.buffers_kb, 0);
        assert_eq!(snapshot.cached_kb, 0);
    }

    #[test]
    fn test_simulated_deterministic() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader1 = SimulatedMeminfoReader::new(
            clock.clone(),
            emitter.clone(),
            42,
            None,
        );
        let reader2 = SimulatedMeminfoReader::new(
            clock,
            emitter,
            42,
            None,
        );

        let snapshot1 = reader1.read().unwrap();
        let snapshot2 = reader2.read().unwrap();

        assert_eq!(snapshot1.total_kb, snapshot2.total_kb);
        assert_eq!(snapshot1.available_kb, snapshot2.available_kb);
        assert_eq!(snapshot1.free_kb, snapshot2.free_kb);
        assert_eq!(snapshot1.buffers_kb, snapshot2.buffers_kb);
        assert_eq!(snapshot1.cached_kb, snapshot2.cached_kb);
        assert_eq!(snapshot1.swap_total_kb, snapshot2.swap_total_kb);
        assert_eq!(snapshot1.swap_free_kb, snapshot2.swap_free_kb);
        assert_eq!(snapshot1.active_kb, snapshot2.active_kb);
        assert_eq!(snapshot1.inactive_kb, snapshot2.inactive_kb);
        assert_eq!(snapshot1.dirty_kb, snapshot2.dirty_kb);
        assert_eq!(snapshot1.writeback_kb, snapshot2.writeback_kb);
    }

    #[test]
    fn test_simulated_different_seeds() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader1 = SimulatedMeminfoReader::new(
            clock.clone(),
            emitter.clone(),
            42,
            None,
        );
        let reader2 = SimulatedMeminfoReader::new(
            clock,
            emitter,
            99,
            None,
        );

        let snapshot1 = reader1.read().unwrap();
        let snapshot2 = reader2.read().unwrap();

        // Different seeds should produce different values
        assert_ne!(snapshot1.total_kb, snapshot2.total_kb);
    }

    #[test]
    fn test_meminfo_emits_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader = SimulatedMeminfoReader::new(clock, emitter, 42, None);
        let snapshot = reader.read().unwrap();

        // Should have received a MemoryStatsChanged event
        let record = rx.try_recv().expect("should have received an event");
        match record.event {
            Event::MemoryStatsChanged {
                sequence_id: 0,
                total_kb,
                available_kb,
                swap_used_kb,
                dirty_kb,
            } => {
                assert_eq!(total_kb, snapshot.total_kb);
                assert_eq!(available_kb, snapshot.available_kb);
                assert_eq!(dirty_kb, snapshot.dirty_kb);
                let expected_swap_used = snapshot.swap_total_kb - snapshot.swap_free_kb;
                assert_eq!(swap_used_kb, expected_swap_used);
            }
            other => panic!("expected MemoryStatsChanged, got {:?}", other),
        }
    }

    #[test]
    fn test_meminfo_replay() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        // Record phase
        let reader1 = SimulatedMeminfoReader::new(clock.clone(), emitter.clone(), 42, None);
        let original = reader1.read().unwrap();

        // Collect emitted events
        let mut events = Vec::new();
        while let Ok(rec) = rx.try_recv() {
            events.push(rec.event);
        }

        // Replay phase — same seed should produce identical values
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
        let emitter2 = EventEmitter::new(tx2);
        let reader2 = SimulatedMeminfoReader::new(clock, emitter2, 42, None);
        let replayed = reader2.read().unwrap();

        // Verify identical snapshots
        assert_eq!(original.total_kb, replayed.total_kb);
        assert_eq!(original.available_kb, replayed.available_kb);
        assert_eq!(original.free_kb, replayed.free_kb);
        assert_eq!(original.buffers_kb, replayed.buffers_kb);
        assert_eq!(original.cached_kb, replayed.cached_kb);
        assert_eq!(original.swap_total_kb, replayed.swap_total_kb);
        assert_eq!(original.swap_free_kb, replayed.swap_free_kb);
        assert_eq!(original.active_kb, replayed.active_kb);
        assert_eq!(original.inactive_kb, replayed.inactive_kb);
        assert_eq!(original.dirty_kb, replayed.dirty_kb);
        assert_eq!(original.writeback_kb, replayed.writeback_kb);

        // Verify events were emitted during replay too
        let replay_events: Vec<_> = std::iter::from_fn(|| rx2.try_recv().ok())
            .map(|r| r.event)
            .collect();
        assert_eq!(replay_events.len(), events.len());
    }
}
