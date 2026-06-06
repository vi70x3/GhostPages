//! Linux observation replayer.
//!
//! Replays Linux system observations from a binary file produced by
//! [`LinuxRecorder`]. Supports verification against another replayer
//! to confirm deterministic replay.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::events::EventRecord;

use crate::recorder::{MAGIC_END, MAGIC_HEADER, RECORD_FORMAT_VERSION};

/// Result of verifying one replay against another.
#[derive(Debug, Clone)]
pub struct ReplayVerificationResult {
    /// Whether all events match between the two replays.
    pub events_match: bool,

    /// Whether event ordering (sequence IDs) matches.
    pub ordering_match: bool,

    /// Whether recommendations (policy output) match.
    pub recommendation_match: bool,

    /// The index of the first divergence, if any.
    pub divergence_point: Option<usize>,
}

impl ReplayVerificationResult {
    /// Check if the verification passed completely.
    pub fn passed(&self) -> bool {
        self.events_match && self.ordering_match && self.recommendation_match
    }
}

/// Replays Linux observations from a binary file.
pub struct LinuxReplayer {
    reader: BufReader<File>,
    events: Vec<EventRecord>,
    current_index: AtomicU64,
}

impl LinuxReplayer {
    /// Create a new replayer from the given file path.
    pub fn new(path: &Path) -> GhostResult<Self> {
        let file = File::open(path).map_err(GhostError::Io)?;
        let reader = BufReader::new(file);

        Ok(Self {
            reader,
            events: Vec::new(),
            current_index: AtomicU64::new(0),
        })
    }

    /// Load all events from the file into memory.
    pub fn load(&mut self) -> GhostResult<()> {
        // Read and validate header
        let mut header = [0u8; 4];
        self.reader.read_exact(&mut header).map_err(|e| {
            GhostError::Internal(format!(
                "failed to read header magic: {} (file may be empty or corrupted)",
                e
            ))
        })?;

        if &header != MAGIC_HEADER {
            return Err(GhostError::Internal(format!(
                "invalid header magic: expected {:?}, got {:?}",
                MAGIC_HEADER, header
            )));
        }

        let mut version = [0u8; 1];
        self.reader.read_exact(&mut version).map_err(GhostError::Io)?;
        if version[0] != RECORD_FORMAT_VERSION {
            return Err(GhostError::Internal(format!(
                "unsupported format version: {} (expected {})",
                version[0], RECORD_FORMAT_VERSION
            )));
        }

        let mut ts_bytes = [0u8; 8];
        self.reader.read_exact(&mut ts_bytes).map_err(GhostError::Io)?;
        let _file_timestamp = u64::from_le_bytes(ts_bytes);

        // Read events until we hit the end marker
        loop {
            let mut len_bytes = [0u8; 4];
            match self.reader.read_exact(&mut len_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(GhostError::Io(e)),
            }

            // Check if these 4 bytes are the end marker itself
            if &len_bytes == MAGIC_END {
                break;
            }

            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read the event payload
            let mut payload = vec![0u8; len];
            self.reader.read_exact(&mut payload).map_err(GhostError::Io)?;

            let record: EventRecord = bincode::deserialize(&payload).map_err(|e| {
                GhostError::Internal(format!(
                    "failed to deserialize event at offset {}: {}",
                    self.events.len(),
                    e
                ))
            })?;

            self.events.push(record);
        }

        Ok(())
    }

    /// Get the next event in sequence.
    pub fn next(&mut self) -> Option<&EventRecord> {
        let idx = self.current_index.load(Ordering::SeqCst) as usize;
        if idx < self.events.len() {
            self.current_index.fetch_add(1, Ordering::SeqCst);
            Some(&self.events[idx])
        } else {
            None
        }
    }

    /// Reset to the beginning for re-replay.
    pub fn reset(&mut self) {
        self.current_index.store(0, Ordering::SeqCst);
    }

    /// Get the total number of loaded events.
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Get all loaded events.
    pub fn events(&self) -> &[EventRecord] {
        &self.events
    }

    /// Verify this replay against another replayer.
    ///
    /// Compares events by serializing them to bytes and comparing the bytes,
    /// since `Event` does not implement `PartialEq`.
    pub fn verify_against(&self, other: &LinuxReplayer) -> ReplayVerificationResult {
        // Compare events by serializing to bytes
        let self_bytes: Vec<Vec<u8>> = self
            .events
            .iter()
            .filter_map(|e| bincode::serialize(e).ok())
            .collect();
        let other_bytes: Vec<Vec<u8>> = other
            .events
            .iter()
            .filter_map(|e| bincode::serialize(e).ok())
            .collect();

        let events_match = self_bytes.len() == other_bytes.len()
            && self_bytes.iter().zip(other_bytes.iter()).all(|(a, b)| a == b);

        let ordering_match = self.events.len() == other.events.len()
            && self
                .events
                .iter()
                .zip(other.events.iter())
                .all(|(a, b)| a.sequence_id == b.sequence_id);

        // Check recommendation events specifically
        let recommendation_match = {
            let self_recs: Vec<_> = self
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.event,
                        ghost_core::events::Event::PolicyRecommendationGenerated { .. }
                    )
                })
                .collect();
            let other_recs: Vec<_> = other
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.event,
                        ghost_core::events::Event::PolicyRecommendationGenerated { .. }
                    )
                })
                .collect();

            self_recs.len() == other_recs.len()
                && self_recs
                    .iter()
                    .zip(other_recs.iter())
                    .all(|(a, b)| {
                        let a_bytes = bincode::serialize(&a.event).ok();
                        let b_bytes = bincode::serialize(&b.event).ok();
                        a_bytes == b_bytes
                    })
        };

        // Find divergence point
        let divergence_point = if !events_match {
            self_bytes
                .iter()
                .zip(other_bytes.iter())
                .position(|(a, b)| a != b)
        } else if !ordering_match {
            self.events
                .iter()
                .zip(other.events.iter())
                .position(|(a, b)| a.sequence_id != b.sequence_id)
        } else {
            None
        };

        ReplayVerificationResult {
            events_match,
            ordering_match,
            recommendation_match,
            divergence_point,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::events::Event;
    use tempfile::NamedTempFile;
    use std::io::Write;

    fn create_test_event(seq: u64) -> EventRecord {
        EventRecord {
            sequence_id: seq,
            timestamp: 1_700_000_000,
            event: Event::MemoryStatsChanged {
                sequence_id: seq,
                total_kb: 16_000_000,
                available_kb: 8_000_000,
                swap_used_kb: 1_000_000,
                dirty_kb: 100_000,
            },
        }
    }

    #[test]
    fn test_replayer_empty_file() {
        let tmp = NamedTempFile::new().unwrap();
        let mut file = std::fs::File::create(tmp.path()).unwrap();
        file.write_all(MAGIC_HEADER).unwrap();
        file.write_all(&[RECORD_FORMAT_VERSION]).unwrap();
        file.write_all(&0u64.to_le_bytes()).unwrap();
        file.write_all(MAGIC_END).unwrap();
        drop(file);

        let mut replayer = LinuxReplayer::new(tmp.path()).unwrap();
        replayer.load().unwrap();
        assert_eq!(replayer.event_count(), 0);
    }

    #[test]
    fn test_replayer_invalid_magic() {
        let tmp = NamedTempFile::new().unwrap();
        let mut file = std::fs::File::create(tmp.path()).unwrap();
        file.write_all(b"BAD!").unwrap();
        drop(file);

        let mut replayer = LinuxReplayer::new(tmp.path()).unwrap();
        assert!(replayer.load().is_err());
    }

    #[test]
    fn test_replayer_next_and_reset() {
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut recorder = crate::recorder::LinuxRecorder::new(tmp.path()).unwrap();
            for i in 0..3 {
                let event = create_test_event(i + 1);
                recorder.record(&event).unwrap();
            }
            recorder.close().unwrap();
        }

        let mut replayer = LinuxReplayer::new(tmp.path()).unwrap();
        replayer.load().unwrap();
        assert_eq!(replayer.event_count(), 3);

        let e1 = replayer.next().unwrap();
        assert_eq!(e1.sequence_id, 1);

        let e2 = replayer.next().unwrap();
        assert_eq!(e2.sequence_id, 2);

        let e3 = replayer.next().unwrap();
        assert_eq!(e3.sequence_id, 3);

        assert!(replayer.next().is_none());

        replayer.reset();
        let e1_again = replayer.next().unwrap();
        assert_eq!(e1_again.sequence_id, 1);
    }

    #[test]
    fn test_verify_identical_replays() {
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut recorder = crate::recorder::LinuxRecorder::new(tmp.path()).unwrap();
            for i in 0..3 {
                let event = create_test_event(i + 1);
                recorder.record(&event).unwrap();
            }
            recorder.close().unwrap();
        }

        let mut replayer1 = LinuxReplayer::new(tmp.path()).unwrap();
        replayer1.load().unwrap();

        let mut replayer2 = LinuxReplayer::new(tmp.path()).unwrap();
        replayer2.load().unwrap();

        let result = replayer1.verify_against(&replayer2);
        assert!(result.passed());
        assert!(result.events_match);
        assert!(result.ordering_match);
        assert!(result.recommendation_match);
        assert!(result.divergence_point.is_none());
    }

    #[test]
    fn test_verify_different_replays() {
        let tmp1 = NamedTempFile::new().unwrap();
        let tmp2 = NamedTempFile::new().unwrap();

        {
            let mut recorder = crate::recorder::LinuxRecorder::new(tmp1.path()).unwrap();
            let event = create_test_event(1);
            recorder.record(&event).unwrap();
            recorder.close().unwrap();
        }

        {
            let mut recorder = crate::recorder::LinuxRecorder::new(tmp2.path()).unwrap();
            let event = EventRecord {
                sequence_id: 1,
                timestamp: 1_700_000_000,
                event: Event::MemoryPressureChanged {
                    sequence_id: 1,
                    level: ghost_core::state::PressureState::new(),
                    avg10: 5.0,
                    avg60: 3.0,
                    avg300: 1.0,
                    total: 1000,
                },
            };
            recorder.record(&event).unwrap();
            recorder.close().unwrap();
        }

        let mut replayer1 = LinuxReplayer::new(tmp1.path()).unwrap();
        replayer1.load().unwrap();

        let mut replayer2 = LinuxReplayer::new(tmp2.path()).unwrap();
        replayer2.load().unwrap();

        let result = replayer1.verify_against(&replayer2);
        assert!(!result.passed());
        assert!(!result.events_match);
        assert!(result.divergence_point.is_some());
    }
}
