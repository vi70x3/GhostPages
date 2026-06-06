//! VM statistics collector for Linux.
//!
//! Provides read-only observation of Linux kernel virtual memory metrics via
//! `/proc/vmstat`. Supports both real reads on Linux and deterministic
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

/// A single VM statistics snapshot from `/proc/vmstat`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VmstatSnapshot {
    /// Pages scanned by kswapd (background reclaim).
    pub pgscan_kswapd: u64,
    /// Pages scanned by direct reclaim.
    pub pgscan_direct: u64,
    /// Pages stolen by kswapd.
    pub pgsteal_kswapd: u64,
    /// Pages stolen by direct reclaim.
    pub pgsteal_direct: u64,
    /// Number of OOM kills.
    pub oom_kill: u64,
    /// Pages swapped in.
    pub pswpin: u64,
    /// Pages swapped out.
    pub pswpout: u64,
    /// Total page faults.
    pub pgfault: u64,
    /// Major page faults.
    pub pgmajfault: u64,
    /// Timestamp when the snapshot was taken (seconds since epoch).
    pub timestamp: u64,
}

/// Reader for Linux `/proc/vmstat`.
///
/// Reads from `/proc/vmstat` on Linux. On non-Linux platforms
/// or when the file is absent, returns a graceful error.
pub struct VmstatReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
}

impl VmstatReader {
    /// Create a new vmstat reader.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        Self {
            time_provider,
            event_emitter,
        }
    }

    /// Read VM statistics from `/proc/vmstat`.
    ///
    /// On Linux, reads from `/proc/vmstat`.
    /// On non-Linux platforms, returns `GhostError::Internal` with a
    /// message indicating vmstat is unavailable.
    pub fn read(&self) -> GhostResult<VmstatSnapshot> {
        #[cfg(target_os = "linux")]
        {
            let content = fs::read_to_string("/proc/vmstat").map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    GhostError::Internal(
                        "/proc/vmstat not found. Kernel may lack CONFIG_PROC_FS".to_string(),
                    )
                } else {
                    GhostError::Io(e)
                }
            })?;

            let snapshot = self.parse(&content)?;
            self.emit_vmstat_event(&snapshot);
            Ok(snapshot)
        }

        #[cfg(not(target_os = "linux"))]
        {
            Err(GhostError::Internal(
                "/proc/vmstat is only available on Linux".to_string(),
            ))
        }
    }

    /// Parse `/proc/vmstat` content into a [`VmstatSnapshot`].
    ///
    /// Expected format per line:
    /// ```text
    /// pgscan_kswapd 12345
    /// pgscan_direct 67890
    /// pgsteal_kswapd 1234
    /// pgsteal_direct 5678
    /// oom_kill 0
    /// pswpin 100
    /// pswpout 200
    /// pgfault 1234567
    /// pgmajfault 890
    /// ```
    pub fn parse(&self, content: &str) -> GhostResult<VmstatSnapshot> {
        let mut pgscan_kswapd = None;
        let mut pgscan_direct = None;
        let mut pgsteal_kswapd = None;
        let mut pgsteal_direct = None;
        let mut oom_kill = None;
        let mut pswpin = None;
        let mut pswpout = None;
        let mut pgfault = None;
        let mut pgmajfault = None;

        for line in content.lines() {
            if let Some((key, value)) = self.parse_line(line) {
                match key.as_str() {
                    "pgscan_kswapd" => pgscan_kswapd = Some(value),
                    "pgscan_direct" => pgscan_direct = Some(value),
                    "pgsteal_kswapd" => pgsteal_kswapd = Some(value),
                    "pgsteal_direct" => pgsteal_direct = Some(value),
                    "oom_kill" => oom_kill = Some(value),
                    "pswpin" => pswpin = Some(value),
                    "pswpout" => pswpout = Some(value),
                    "pgfault" => pgfault = Some(value),
                    "pgmajfault" => pgmajfault = Some(value),
                    _ => {} // ignore unknown keys
                }
            }
        }

        Ok(VmstatSnapshot {
            pgscan_kswapd: pgscan_kswapd.unwrap_or(0),
            pgscan_direct: pgscan_direct.unwrap_or(0),
            pgsteal_kswapd: pgsteal_kswapd.unwrap_or(0),
            pgsteal_direct: pgsteal_direct.unwrap_or(0),
            oom_kill: oom_kill.unwrap_or(0),
            pswpin: pswpin.unwrap_or(0),
            pswpout: pswpout.unwrap_or(0),
            pgfault: pgfault.unwrap_or(0),
            pgmajfault: pgmajfault.unwrap_or(0),
            timestamp: self.time_provider.timestamp_secs(),
        })
    }

    /// Parse a single line from `/proc/vmstat`.
    ///
    /// Returns `Some((key, value))` if the line matches the expected format.
    fn parse_line(&self, line: &str) -> Option<(String, u64)> {
        // Format: "key value"
        let mut parts = line.split_whitespace();
        let key = parts.next()?.to_string();
        let value = parts.next()?.parse::<u64>().ok()?;

        Some((key, value))
    }

    /// Emit a `VmstatChanged` event.
    fn emit_vmstat_event(&self, snapshot: &VmstatSnapshot) {
        let event = Event::VmstatChanged {
            sequence_id: 0,
            pgscan_kswapd: snapshot.pgscan_kswapd,
            pgscan_direct: snapshot.pgscan_direct,
            oom_kill: snapshot.oom_kill,
            pswpin: snapshot.pswpin,
            pswpout: snapshot.pswpout,
        };
        let _ = self.event_emitter.try_emit(event);
    }
}

/// Deterministic vmstat reader for testing and replay.
///
/// Reads vmstat values from a file or generates deterministic values based on
/// a seed. When the simulation file exists, it reads from it; otherwise
/// it generates pseudo-random but deterministic vmstat values.
pub struct SimulatedVmstatReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
    seed: u64,
    simulation_path: Option<String>,
}

impl SimulatedVmstatReader {
    /// Create a new simulated vmstat reader.
    ///
    /// # Arguments
    /// * `time_provider` — Time source for timestamps.
    /// * `event_emitter` — Emitter for vmstat events.
    /// * `seed` — Deterministic seed for value generation.
    /// * `simulation_path` — Optional path to a file containing vmstat data.
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

    /// Read simulated vmstat data.
    ///
    /// If a simulation file is set and exists, reads from it.
    /// Otherwise generates deterministic values from the seed.
    pub fn read(&self) -> GhostResult<VmstatSnapshot> {
        // Try simulation file first
        if let Some(ref path) = self.simulation_path {
            if Path::new(path).exists() {
                let content = fs::read_to_string(path).map_err(GhostError::Io)?;
                let reader = VmstatReader::new(
                    self.time_provider.clone(),
                    self.event_emitter.clone(),
                );
                return reader.parse(&content);
            }
        }

        // Generate deterministic values from seed
        let mut rng = StdRng::seed_from_u64(self.seed.wrapping_mul(31));

        use rand::Rng;
        let pgscan_kswapd: u64 = rng.gen_range(0..1_000_000);
        let pgscan_direct: u64 = rng.gen_range(0..1_000_000);
        let pgsteal_kswapd: u64 = rng.gen_range(0..pgscan_kswapd);
        let pgsteal_direct: u64 = rng.gen_range(0..pgscan_direct);
        let oom_kill: u64 = rng.gen_range(0..100);
        let pswpin: u64 = rng.gen_range(0..10_000);
        let pswpout: u64 = rng.gen_range(0..10_000);
        let pgfault: u64 = rng.gen_range(0..100_000_000);
        let pgmajfault: u64 = rng.gen_range(0..pgfault);

        let snapshot = VmstatSnapshot {
            pgscan_kswapd,
            pgscan_direct,
            pgsteal_kswapd,
            pgsteal_direct,
            oom_kill,
            pswpin,
            pswpout,
            pgfault,
            pgmajfault,
            timestamp: self.time_provider.timestamp_secs(),
        };

        // Emit event
        let reader = VmstatReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        reader.emit_vmstat_event(&snapshot);

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
        let reader = VmstatReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let result = reader.parse_line("pgscan_kswapd 12345");
        assert_eq!(result, Some(("pgscan_kswapd".to_string(), 12345)));

        let result = reader.parse_line("oom_kill 0");
        assert_eq!(result, Some(("oom_kill".to_string(), 0)));
    }

    #[test]
    fn test_parse_line_invalid() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = VmstatReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        // No value
        assert_eq!(reader.parse_line("pgscan_kswapd"), None);

        // Non-numeric value
        assert_eq!(reader.parse_line("pgscan_kswapd abc"), None);

        // Empty line
        assert_eq!(reader.parse_line(""), None);
    }

    #[test]
    fn test_parse_full_vmstat() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = VmstatReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let content = "\
pgscan_kswapd 12345
pgscan_direct 67890
pgsteal_kswapd 1234
pgsteal_direct 5678
oom_kill 0
pswpin 100
pswpout 200
pgfault 1234567
pgmajfault 890
nr_free_pages 123456
nr_inactive_anon 789
nr_active_anon 456";

        let snapshot = reader.parse(content).unwrap();

        assert_eq!(snapshot.pgscan_kswapd, 12345);
        assert_eq!(snapshot.pgscan_direct, 67890);
        assert_eq!(snapshot.pgsteal_kswapd, 1234);
        assert_eq!(snapshot.pgsteal_direct, 5678);
        assert_eq!(snapshot.oom_kill, 0);
        assert_eq!(snapshot.pswpin, 100);
        assert_eq!(snapshot.pswpout, 200);
        assert_eq!(snapshot.pgfault, 1234567);
        assert_eq!(snapshot.pgmajfault, 890);
    }

    #[test]
    fn test_parse_partial_vmstat() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = VmstatReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        // Only some fields present
        let content = "\
pgscan_kswapd 12345
oom_kill 0";

        let snapshot = reader.parse(content).unwrap();

        assert_eq!(snapshot.pgscan_kswapd, 12345);
        assert_eq!(snapshot.oom_kill, 0);
        // Missing fields default to 0
        assert_eq!(snapshot.pgscan_direct, 0);
        assert_eq!(snapshot.pgsteal_kswapd, 0);
        assert_eq!(snapshot.pgsteal_direct, 0);
        assert_eq!(snapshot.pswpin, 0);
        assert_eq!(snapshot.pswpout, 0);
        assert_eq!(snapshot.pgfault, 0);
        assert_eq!(snapshot.pgmajfault, 0);
    }

    #[test]
    fn test_simulated_deterministic() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader1 = SimulatedVmstatReader::new(
            clock.clone(),
            emitter.clone(),
            42,
            None,
        );
        let reader2 = SimulatedVmstatReader::new(
            clock,
            emitter,
            42,
            None,
        );

        let snapshot1 = reader1.read().unwrap();
        let snapshot2 = reader2.read().unwrap();

        assert_eq!(snapshot1.pgscan_kswapd, snapshot2.pgscan_kswapd);
        assert_eq!(snapshot1.pgscan_direct, snapshot2.pgscan_direct);
        assert_eq!(snapshot1.pgsteal_kswapd, snapshot2.pgsteal_kswapd);
        assert_eq!(snapshot1.pgsteal_direct, snapshot2.pgsteal_direct);
        assert_eq!(snapshot1.oom_kill, snapshot2.oom_kill);
        assert_eq!(snapshot1.pswpin, snapshot2.pswpin);
        assert_eq!(snapshot1.pswpout, snapshot2.pswpout);
        assert_eq!(snapshot1.pgfault, snapshot2.pgfault);
        assert_eq!(snapshot1.pgmajfault, snapshot2.pgmajfault);
    }

    #[test]
    fn test_simulated_different_seeds() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader1 = SimulatedVmstatReader::new(
            clock.clone(),
            emitter.clone(),
            42,
            None,
        );
        let reader2 = SimulatedVmstatReader::new(
            clock,
            emitter,
            99,
            None,
        );

        let snapshot1 = reader1.read().unwrap();
        let snapshot2 = reader2.read().unwrap();

        // Different seeds should produce different values
        assert_ne!(snapshot1.pgscan_kswapd, snapshot2.pgscan_kswapd);
    }

    #[test]
    fn test_vmstat_emits_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        let reader = SimulatedVmstatReader::new(clock, emitter, 42, None);
        let snapshot = reader.read().unwrap();

        // Should have received a VmstatChanged event
        let record = rx.try_recv().expect("should have received an event");
        match record.event {
            Event::VmstatChanged {
                sequence_id: 0,
                pgscan_kswapd,
                pgscan_direct,
                oom_kill,
                pswpin,
                pswpout,
            } => {
                assert_eq!(pgscan_kswapd, snapshot.pgscan_kswapd);
                assert_eq!(pgscan_direct, snapshot.pgscan_direct);
                assert_eq!(oom_kill, snapshot.oom_kill);
                assert_eq!(pswpin, snapshot.pswpin);
                assert_eq!(pswpout, snapshot.pswpout);
            }
            other => panic!("expected VmstatChanged, got {:?}", other),
        }
    }

    #[test]
    fn test_vmstat_replay() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ));

        // Record phase
        let reader1 = SimulatedVmstatReader::new(clock.clone(), emitter.clone(), 42, None);
        let original = reader1.read().unwrap();

        // Collect emitted events
        let mut events = Vec::new();
        while let Ok(rec) = rx.try_recv() {
            events.push(rec.event);
        }

        // Replay phase — same seed should produce identical values
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
        let emitter2 = EventEmitter::new(tx2);
        let reader2 = SimulatedVmstatReader::new(clock, emitter2, 42, None);
        let replayed = reader2.read().unwrap();

        // Verify identical snapshots
        assert_eq!(original.pgscan_kswapd, replayed.pgscan_kswapd);
        assert_eq!(original.pgscan_direct, replayed.pgscan_direct);
        assert_eq!(original.pgsteal_kswapd, replayed.pgsteal_kswapd);
        assert_eq!(original.pgsteal_direct, replayed.pgsteal_direct);
        assert_eq!(original.oom_kill, replayed.oom_kill);
        assert_eq!(original.pswpin, replayed.pswpin);
        assert_eq!(original.pswpout, replayed.pswpout);
        assert_eq!(original.pgfault, replayed.pgfault);
        assert_eq!(original.pgmajfault, replayed.pgmajfault);

        // Verify events were emitted during replay too
        let replay_events: Vec<_> = std::iter::from_fn(|| rx2.try_recv().ok())
            .map(|r| r.event)
            .collect();
        assert_eq!(replay_events.len(), events.len());
    }
}
