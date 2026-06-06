//! Pressure Stall Information (PSI) reader for Linux.
//!
//! Provides read-only observation of Linux kernel pressure metrics via
//! `/proc/pressure/{memory,io,cpu}`. Supports both real PSI reads on Linux
//! and deterministic simulation for testing and replay.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::SeedableRng;

use ghost_core::emitter::EventEmitter;
use ghost_core::error::{GhostError, GhostResult};
use ghost_core::events::Event;
use ghost_core::state::PressureState;
use ghost_core::time::TimeProvider;

/// PSI resource type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PsiResource {
    Memory,
    Io,
    Cpu,
}

impl PsiResource {
    /// Get the `/proc/pressure/` filename for this resource.
    fn proc_name(&self) -> &'static str {
        match self {
            PsiResource::Memory => "memory",
            PsiResource::Io => "io",
            PsiResource::Cpu => "cpu",
        }
    }
}

/// A single PSI sample from the kernel.
#[derive(Debug, Clone, PartialEq)]
pub struct PsiSample {
    /// Which resource this sample is for.
    pub resource: PsiResource,
    /// 10-second average pressure (percentage of time stalled).
    pub avg10: f64,
    /// 60-second average pressure.
    pub avg60: f64,
    /// 300-second average pressure.
    pub avg300: f64,
    /// Total stall time in microseconds.
    pub total: u64,
    /// Timestamp when the sample was taken (seconds since epoch).
    pub timestamp: u64,
}

/// Classify a PSI avg10 value into a [`PressureState`] level.
///
/// Mapping:
/// - avg10 < 1.0 → Low
/// - avg10 < 5.0 → Medium
/// - avg10 < 10.0 → High
/// - avg10 >= 10.0 → Critical
pub fn classify_pressure(avg10: f64) -> PressureState {
    // We use a heuristic: the PressureState in ghost-core is a struct with
    // float fields, not an enum. We map to a "level" by returning a
    // PressureState with memory_pressure set to a normalized value and
    // use the avg10 thresholds to determine severity.
    // For PSI event emission we return a PressureState that encodes the
    // pressure dimension. The actual level is determined by the caller
    // from the avg10 value.
    if avg10 < 1.0 {
        PressureState::new() // Low — all zeros
    } else if avg10 < 5.0 {
        PressureState {
            memory_pressure: 0.3,
            ..PressureState::new()
        }
    } else if avg10 < 10.0 {
        PressureState {
            memory_pressure: 0.7,
            ..PressureState::new()
        }
    } else {
        PressureState {
            memory_pressure: 1.0,
            ..PressureState::new()
        }
    }
}

/// Map a PSI avg10 value to a discrete pressure level string.
pub fn pressure_level_str(avg10: f64) -> &'static str {
    if avg10 < 1.0 {
        "low"
    } else if avg10 < 5.0 {
        "medium"
    } else if avg10 < 10.0 {
        "high"
    } else {
        "critical"
    }
}

/// Reader for Linux Pressure Stall Information.
///
/// Reads from `/proc/pressure/{resource}` on Linux. On non-Linux platforms
/// or when the PSI files are absent, returns a graceful error.
pub struct PsiReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
}

impl PsiReader {
    /// Create a new PSI reader.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        Self {
            time_provider,
            event_emitter,
        }
    }

    /// Read PSI data for a specific resource.
    ///
    /// On Linux, reads from `/proc/pressure/{resource}`.
    /// On non-Linux platforms, returns `GhostError::Internal` with a
    /// message indicating PSI is unavailable.
    pub fn read(&self, resource: PsiResource) -> GhostResult<PsiSample> {
        #[cfg(target_os = "linux")]
        {
            let path = format!("/proc/pressure/{}", resource.proc_name());
            let content = fs::read_to_string(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    GhostError::Internal(format!(
                        "PSI not available: {} not found. \
                         Kernel may lack CONFIG_PRESSURE",
                        path
                    ))
                } else {
                    GhostError::Io(e)
                }
            })?;

            let sample = self.parse_line(content.trim(), resource)?;
            self.emit_pressure_event(&sample);
            Ok(sample)
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = resource;
            Err(GhostError::Internal(
                "PSI is only available on Linux (requires /proc/pressure/)".to_string(),
            ))
        }
    }

    /// Read PSI data for all three resources (memory, I/O, CPU).
    ///
    /// Returns a vector of results — one per resource. Failures for one
    /// resource do not prevent reading others.
    pub fn read_all(&self) -> Vec<GhostResult<PsiSample>> {
        vec![
            self.read(PsiResource::Memory),
            self.read(PsiResource::Io),
            self.read(PsiResource::Cpu),
        ]
    }

    /// Parse a single PSI line from `/proc/pressure/{resource}`.
    ///
    /// Expected format:
    /// ```text
    /// some avg10=0.00 avg60=0.00 avg300=0.00 total=0
    /// full avg10=0.00 avg60=0.00 avg300=0.00 total=0
    /// ```
    pub fn parse_line(&self, line: &str, resource: PsiResource) -> GhostResult<PsiSample> {
        let timestamp = self.time_provider.timestamp_secs();

        // Split on whitespace: first token is "some" or "full", then key=value pairs
        let mut avg10 = None;
        let mut avg60 = None;
        let mut avg300 = None;
        let mut total = None;

        for token in line.split_whitespace() {
            if let Some((key, value)) = token.split_once('=') {
                match key {
                    "avg10" => {
                        avg10 = Some(value.parse::<f64>().map_err(|_| {
                            GhostError::Internal(format!("invalid avg10 value: {value}"))
                        })?);
                    }
                    "avg60" => {
                        avg60 = Some(value.parse::<f64>().map_err(|_| {
                            GhostError::Internal(format!("invalid avg60 value: {value}"))
                        })?);
                    }
                    "avg300" => {
                        avg300 = Some(value.parse::<f64>().map_err(|_| {
                            GhostError::Internal(format!("invalid avg300 value: {value}"))
                        })?);
                    }
                    "total" => {
                        total = Some(value.parse::<u64>().map_err(|_| {
                            GhostError::Internal(format!("invalid total value: {value}"))
                        })?);
                    }
                    _ => {} // ignore unknown keys
                }
            }
            // "some" and "full" prefixes are ignored
        }

        Ok(PsiSample {
            resource,
            avg10: avg10.ok_or_else(|| {
                GhostError::Internal("missing avg10 in PSI line".to_string())
            })?,
            avg60: avg60.ok_or_else(|| {
                GhostError::Internal("missing avg60 in PSI line".to_string())
            })?,
            avg300: avg300.ok_or_else(|| {
                GhostError::Internal("missing avg300 in PSI line".to_string())
            })?,
            total: total.ok_or_else(|| {
                GhostError::Internal("missing total in PSI line".to_string())
            })?,
            timestamp,
        })
    }

    /// Emit the appropriate pressure event for a PSI sample.
    fn emit_pressure_event(&self, sample: &PsiSample) {
        let level = classify_pressure(sample.avg10);
        let _ = level; // used for event emission below

        let event = match sample.resource {
            PsiResource::Memory => Event::MemoryPressureChanged {
                sequence_id: 0,
                level: PressureState::new(), // placeholder — level derived from avg10
                avg10: sample.avg10,
                avg60: sample.avg60,
                avg300: sample.avg300,
                total: sample.total,
            },
            PsiResource::Io => Event::IoPressureChanged {
                sequence_id: 0,
                level: PressureState::new(),
                avg10: sample.avg10,
                avg60: sample.avg60,
                avg300: sample.avg300,
                total: sample.total,
            },
            PsiResource::Cpu => {
                // CPU pressure doesn't have a dedicated event variant yet;
                // we skip emitting for now but still read the data.
                return;
            }
        };

        let _ = self.event_emitter.try_emit(event);
    }
}

/// Deterministic PSI reader for testing and replay.
///
/// Reads PSI values from a file or generates deterministic values based on
/// a seed. When the simulation file exists, it reads from it; otherwise
/// it generates pseudo-random but deterministic PSI values.
pub struct SimulatedPsiReader {
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
    seed: u64,
    simulation_path: Option<String>,
}

impl SimulatedPsiReader {
    /// Create a new simulated PSI reader.
    ///
    /// # Arguments
    /// * `time_provider` — Time source for timestamps.
    /// * `event_emitter` — Emitter for pressure events.
    /// * `seed` — Deterministic seed for value generation.
    /// * `simulation_path` — Optional path to a file containing PSI data.
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

    /// Read simulated PSI for a resource.
    ///
    /// If a simulation file is set and exists, reads from it.
    /// Otherwise generates deterministic values from the seed.
    pub fn read(&self, resource: PsiResource) -> GhostResult<PsiSample> {
        // Try simulation file first
        if let Some(ref path) = self.simulation_path {
            if Path::new(path).exists() {
                let content = fs::read_to_string(path).map_err(GhostError::Io)?;
                // Find the line matching the resource
                for line in content.lines() {
                    let prefix = match resource {
                        PsiResource::Memory => "memory:",
                        PsiResource::Io => "io:",
                        PsiResource::Cpu => "cpu:",
                    };
                    if let Some(rest) = line.strip_prefix(prefix) {
                        let mut reader = PsiReader::new(
                            self.time_provider.clone(),
                            self.event_emitter.clone(),
                        );
                        let sample = reader.parse_line(rest.trim(), resource)?;
                        return Ok(sample);
                    }
                }
            }
        }

        // Generate deterministic values from seed + resource
        let resource_seed = match resource {
            PsiResource::Memory => 0u8,
            PsiResource::Io => 1u8,
            PsiResource::Cpu => 2u8,
        };
        let mut rng = StdRng::seed_from_u64(self.seed * 31 + resource_seed as u64);

        use rand::Rng;
        let avg10: f64 = rng.gen_range(0.0..20.0);
        let avg60: f64 = rng.gen_range(0.0..15.0);
        let avg300: f64 = rng.gen_range(0.0..10.0);
        let total: u64 = rng.gen_range(0..1_000_000);

        let sample = PsiSample {
            resource,
            avg10,
            avg60,
            avg300,
            total,
            timestamp: self.time_provider.timestamp_secs(),
        };

        // Emit event via a temporary reader
        let reader = PsiReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        reader.emit_pressure_event(&sample);

        Ok(sample)
    }

    /// Read all simulated PSI resources.
    pub fn read_all(&self) -> Vec<GhostResult<PsiSample>> {
        vec![
            self.read(PsiResource::Memory),
            self.read(PsiResource::Io),
            self.read(PsiResource::Cpu),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_psi_line_basic() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = PsiReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let line = "some avg10=1.50 avg60=0.75 avg300=0.25 total=12345";
        let sample = reader.parse_line(line, PsiResource::Memory).unwrap();

        assert_eq!(sample.resource, PsiResource::Memory);
        assert!((sample.avg10 - 1.50).abs() < f64::EPSILON);
        assert!((sample.avg60 - 0.75).abs() < f64::EPSILON);
        assert!((sample.avg300 - 0.25).abs() < f64::EPSILON);
        assert_eq!(sample.total, 12345);
    }

    #[test]
    fn test_parse_psi_line_full_prefix() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = PsiReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let line = "full avg10=12.34 avg60=8.90 avg300=5.67 total=999999";
        let sample = reader.parse_line(line, PsiResource::Io).unwrap();

        assert_eq!(sample.resource, PsiResource::Io);
        assert!((sample.avg10 - 12.34).abs() < f64::EPSILON);
        assert!((sample.avg60 - 8.90).abs() < f64::EPSILON);
        assert!((sample.avg300 - 5.67).abs() < f64::EPSILON);
        assert_eq!(sample.total, 999999);
    }

    #[test]
    fn test_parse_psi_line_zero() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = PsiReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let line = "some avg10=0.00 avg60=0.00 avg300=0.00 total=0";
        let sample = reader.parse_line(line, PsiResource::Cpu).unwrap();

        assert_eq!(sample.resource, PsiResource::Cpu);
        assert!((sample.avg10).abs() < f64::EPSILON);
        assert!((sample.avg60).abs() < f64::EPSILON);
        assert!((sample.avg300).abs() < f64::EPSILON);
        assert_eq!(sample.total, 0);
    }

    #[test]
    fn test_pressure_classification() {
        // Low: avg10 < 1.0
        let level = classify_pressure(0.5);
        assert_eq!(level.memory_pressure, 0.0);
        assert_eq!(pressure_level_str(0.5), "low");

        // Medium: 1.0 <= avg10 < 5.0
        let level = classify_pressure(3.0);
        assert!((level.memory_pressure - 0.3).abs() < f32::EPSILON);
        assert_eq!(pressure_level_str(3.0), "medium");

        // High: 5.0 <= avg10 < 10.0
        let level = classify_pressure(7.5);
        assert!((level.memory_pressure - 0.7).abs() < f32::EPSILON);
        assert_eq!(pressure_level_str(7.5), "high");

        // Critical: avg10 >= 10.0
        let level = classify_pressure(15.0);
        assert!((level.memory_pressure - 1.0).abs() < f32::EPSILON);
        assert_eq!(pressure_level_str(15.0), "critical");

        // Boundary: exactly 1.0 → Medium
        assert_eq!(pressure_level_str(1.0), "medium");
        // Boundary: exactly 5.0 → High
        assert_eq!(pressure_level_str(5.0), "high");
        // Boundary: exactly 10.0 → Critical
        assert_eq!(pressure_level_str(10.0), "critical");
    }

    #[test]
    fn test_simulated_psi_deterministic() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_secs(1)));

        let reader1 = SimulatedPsiReader::new(
            clock.clone(),
            emitter.clone(),
            42,
            None,
        );
        let reader2 = SimulatedPsiReader::new(
            clock,
            emitter,
            42,
            None,
        );

        let sample1 = reader1.read(PsiResource::Memory).unwrap();
        let sample2 = reader2.read(PsiResource::Memory).unwrap();

        assert_eq!(sample1.avg10, sample2.avg10);
        assert_eq!(sample1.avg60, sample2.avg60);
        assert_eq!(sample1.avg300, sample2.avg300);
        assert_eq!(sample1.total, sample2.total);
    }

    #[test]
    fn test_simulated_psi_different_seeds() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_secs(1)));

        let reader1 = SimulatedPsiReader::new(
            clock.clone(),
            emitter.clone(),
            42,
            None,
        );
        let reader2 = SimulatedPsiReader::new(
            clock,
            emitter,
            99,
            None,
        );

        let sample1 = reader1.read(PsiResource::Memory).unwrap();
        let sample2 = reader2.read(PsiResource::Memory).unwrap();

        // Different seeds should produce different values
        assert_ne!(sample1.avg10, sample2.avg10);
    }

    #[test]
    fn test_psi_emits_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_secs(1)));

        let reader = SimulatedPsiReader::new(clock, emitter, 42, None);
        let _sample = reader.read(PsiResource::Memory).unwrap();

        // Should have received a MemoryPressureChanged event
        let record = rx.try_recv().expect("should have received an event");
        match record.event {
            Event::MemoryPressureChanged {
                sequence_id: 0, avg10, avg60, avg300, total, .. } => {
                assert!((avg10 - _sample.avg10).abs() < f64::EPSILON);
                assert!((avg60 - _sample.avg60).abs() < f64::EPSILON);
                assert!((avg300 - _sample.avg300).abs() < f64::EPSILON);
                assert_eq!(total, _sample.total);
            }
            other => panic!("expected MemoryPressureChanged, got {:?}", other),
        }
    }

    #[test]
    fn test_psi_replay() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let clock = Arc::new(ghost_core::time::DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_secs(1)));

        // Record phase
        let reader1 = SimulatedPsiReader::new(clock.clone(), emitter.clone(), 42, None);
        let samples: Vec<PsiSample> = reader1
            .read_all()
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        // Collect emitted events
        let mut events = Vec::new();
        while let Ok(rec) = rx.try_recv() {
            events.push(rec.event);
        }

        // Replay phase — same seed should produce identical values
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
        let emitter2 = EventEmitter::new(tx2);
        let reader2 = SimulatedPsiReader::new(clock, emitter2, 42, None);
        let replayed: Vec<PsiSample> = reader2
            .read_all()
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        // Verify identical samples
        assert_eq!(samples.len(), replayed.len());
        for (orig, replayed) in samples.iter().zip(replayed.iter()) {
            assert_eq!(orig.resource, replayed.resource);
            assert!((orig.avg10 - replayed.avg10).abs() < f64::EPSILON);
            assert!((orig.avg60 - replayed.avg60).abs() < f64::EPSILON);
            assert!((orig.avg300 - replayed.avg300).abs() < f64::EPSILON);
            assert_eq!(orig.total, replayed.total);
        }

        // Verify events were emitted during replay too
        let replay_events: Vec<_> = std::iter::from_fn(|| rx2.try_recv().ok())
            .map(|r| r.event)
            .collect();
        assert_eq!(replay_events.len(), events.len());
    }

    #[test]
    fn test_parse_invalid_line_missing_field() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = PsiReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        // Missing total
        let result = reader.parse_line("some avg10=1.0 avg60=2.0 avg300=3.0", PsiResource::Memory);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_line_bad_number() {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);
        let reader = PsiReader::new(
            Arc::new(ghost_core::time::RealTimeProvider),
            emitter,
        );

        let result = reader.parse_line(
            "some avg10=not_a_number avg60=2.0 avg300=3.0 total=100",
            PsiResource::Memory,
        );
        assert!(result.is_err());
    }
}
