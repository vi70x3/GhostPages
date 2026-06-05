//! Divergence detection for replay validation.
//!
//! Compares baseline vs candidate event streams to pinpoint exact
//! divergence locations and classify the type of divergence.

use std::fmt;

use ghost_core::trace::TraceEvent;
use ghost_core::types::ChunkId;

use crate::checksum::{classify_event, hash_event, EventHash, HashCategory};

/// Types of divergence that can be detected between two event streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DivergenceType {
    /// Streams have different lengths.
    LengthMismatch {
        /// Number of events in the baseline stream.
        baseline_len: usize,
        /// Number of events in the candidate stream.
        candidate_len: usize,
    },
    /// An event at the same index has different content.
    ContentMismatch {
        /// Index of the mismatching event.
        index: usize,
        /// Category of the mismatching event.
        category: HashCategory,
    },
    /// An event has a different timestamp.
    TimestampMismatch {
        /// Index of the mismatching event.
        index: usize,
        /// Timestamp in the baseline stream.
        baseline_ts: u64,
        /// Timestamp in the candidate stream.
        candidate_ts: u64,
    },
    /// An event references a different chunk.
    ChunkIdMismatch {
        /// Index of the mismatching event.
        index: usize,
        /// Chunk ID in the baseline stream.
        baseline_chunk: Option<ChunkId>,
        /// Chunk ID in the candidate stream.
        candidate_chunk: Option<ChunkId>,
    },
    /// An event has a different type.
    TypeMismatch {
        /// Index of the mismatching event.
        index: usize,
        /// Event type in the baseline stream.
        baseline_type: &'static str,
        /// Event type in the candidate stream.
        candidate_type: &'static str,
    },
}

impl fmt::Display for DivergenceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DivergenceType::LengthMismatch {
                baseline_len,
                candidate_len,
            } => {
                write!(
                    f,
                    "LengthMismatch: baseline has {} events, candidate has {}",
                    baseline_len, candidate_len
                )
            }
            DivergenceType::ContentMismatch { index, category } => {
                write!(
                    f,
                    "ContentMismatch at index {} (category: {})",
                    index, category
                )
            }
            DivergenceType::TimestampMismatch {
                index,
                baseline_ts,
                candidate_ts,
            } => {
                write!(
                    f,
                    "TimestampMismatch at index {}: baseline={}, candidate={}",
                    index, baseline_ts, candidate_ts
                )
            }
            DivergenceType::ChunkIdMismatch {
                index,
                baseline_chunk,
                candidate_chunk,
            } => {
                write!(
                    f,
                    "ChunkIdMismatch at index {}: baseline={:?}, candidate={:?}",
                    index, baseline_chunk, candidate_chunk
                )
            }
            DivergenceType::TypeMismatch {
                index,
                baseline_type,
                candidate_type,
            } => {
                write!(
                    f,
                    "TypeMismatch at index {}: baseline={}, candidate={}",
                    index, baseline_type, candidate_type
                )
            }
        }
    }
}

/// Report of divergence between two event streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DivergenceReport {
    /// Whether the streams are identical.
    pub identical: bool,
    /// List of divergences found.
    pub divergences: Vec<DivergenceType>,
    /// Index of the first divergence.
    pub first_divergence_index: Option<usize>,
    /// Number of events compared before divergence was found.
    pub events_compared: usize,
}

impl DivergenceReport {
    /// Creates a report indicating identical streams.
    pub fn identical(events_compared: usize) -> Self {
        Self {
            identical: true,
            divergences: Vec::new(),
            first_divergence_index: None,
            events_compared,
        }
    }

    /// Creates a report with divergences.
    pub fn divergent(
        divergences: Vec<DivergenceType>,
        events_compared: usize,
    ) -> Self {
        let first_index = divergences.iter().find_map(|d| match d {
            DivergenceType::LengthMismatch { .. } => None,
            DivergenceType::ContentMismatch { index, .. } => Some(*index),
            DivergenceType::TimestampMismatch { index, .. } => Some(*index),
            DivergenceType::ChunkIdMismatch { index, .. } => Some(*index),
            DivergenceType::TypeMismatch { index, .. } => Some(*index),
        });
        Self {
            identical: false,
            divergences,
            first_divergence_index: first_index,
            events_compared,
        }
    }

    /// Returns a human-readable summary of the divergence report.
    pub fn summary(&self) -> String {
        if self.identical {
            format!(
                "Streams are identical ({} events compared)",
                self.events_compared
            )
        } else {
            format!(
                "Streams diverge at index {} ({} total divergences, {} events compared)",
                self.first_divergence_index.unwrap_or(0),
                self.divergences.len(),
                self.events_compared
            )
        }
    }
}

impl fmt::Display for DivergenceReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.summary())
    }
}

/// Detects divergences between a baseline and candidate event stream.
///
/// Compares events pairwise by index, checking type, timestamp, chunk ID,
/// and content hash. Returns a `DivergenceReport` with all found
/// divergences.
pub fn detect_divergence(
    baseline: &[TraceEvent],
    candidate: &[TraceEvent],
) -> DivergenceReport {
    let mut divergences = Vec::new();
    let min_len = baseline.len().min(candidate.len());
    let mut events_compared = 0;

    // Check length mismatch
    if baseline.len() != candidate.len() {
        divergences.push(DivergenceType::LengthMismatch {
            baseline_len: baseline.len(),
            candidate_len: candidate.len(),
        });
    }

    // Compare events pairwise
    for i in 0..min_len {
        events_compared = i + 1;
        let b = &baseline[i];
        let c = &candidate[i];

        // Check event type
        let b_type = b.event_type();
        let c_type = c.event_type();
        if b_type != c_type {
            divergences.push(DivergenceType::TypeMismatch {
                index: i,
                baseline_type: b_type,
                candidate_type: c_type,
            });
            continue; // Skip further checks if types differ
        }

        // Check timestamp
        let b_ts = b.timestamp();
        let c_ts = c.timestamp();
        if b_ts != c_ts {
            divergences.push(DivergenceType::TimestampMismatch {
                index: i,
                baseline_ts: b_ts,
                candidate_ts: c_ts,
            });
        }

        // Check chunk ID
        let b_chunk = b.chunk_id();
        let c_chunk = c.chunk_id();
        if b_chunk != c_chunk {
            divergences.push(DivergenceType::ChunkIdMismatch {
                index: i,
                baseline_chunk: b_chunk,
                candidate_chunk: c_chunk,
            });
        }

        // Check content hash
        let b_hash = hash_event(b, i);
        let c_hash = hash_event(c, i);
        if b_hash != c_hash {
            divergences.push(DivergenceType::ContentMismatch {
                index: i,
                category: classify_event(b),
            });
        }
    }

    if divergences.is_empty() {
        DivergenceReport::identical(events_compared.max(min_len))
    } else {
        DivergenceReport::divergent(divergences, events_compared)
    }
}

/// Compares two trace files and returns a divergence report.
pub fn compare_traces(
    baseline_path: &std::path::Path,
    candidate_path: &std::path::Path,
) -> ghost_core::error::GhostResult<DivergenceReport> {
    let mut baseline_reader = crate::reader::TraceReader::open(baseline_path)?;
    let mut candidate_reader = crate::reader::TraceReader::open(candidate_path)?;
    let baseline_events = baseline_reader.read_all()?;
    let candidate_events = candidate_reader.read_all()?;
    Ok(detect_divergence(&baseline_events, &candidate_events))
}

/// Returns per-event hashes for both streams, useful for detailed comparison.
pub fn hash_comparison(
    baseline: &[TraceEvent],
    candidate: &[TraceEvent],
) -> (Vec<EventHash>, Vec<EventHash>) {
    let baseline_hashes = crate::checksum::hash_events(baseline);
    let candidate_hashes = crate::checksum::hash_events(candidate);
    (baseline_hashes, candidate_hashes)
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::ChunkState;
    use ghost_core::types::TierId;

    fn sample_events() -> Vec<TraceEvent> {
        vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1000,
                size: 5,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1001,
                from: ChunkState::Allocated,
                to: ChunkState::Stored,
            },
            TraceEvent::TransferStarted {
                timestamp: 1002,
                job: ghost_core::transfer::TransferJob::new(
                    ChunkId::from_data(b"hello"),
                    TierId::Ram,
                    TierId::Disk,
                    5,
                    ghost_core::transfer::TransferPriority::Normal,
                ),
            },
        ]
    }

    #[test]
    fn test_identical_streams() {
        let events = sample_events();
        let report = detect_divergence(&events, &events);
        assert!(report.identical);
        assert!(report.divergences.is_empty());
        assert_eq!(report.events_compared, 3);
    }

    #[test]
    fn test_length_mismatch() {
        let baseline = sample_events();
        let candidate = vec![baseline[0].clone()];
        let report = detect_divergence(&baseline, &candidate);
        assert!(!report.identical);
        assert!(report
            .divergences
            .iter()
            .any(|d| matches!(d, DivergenceType::LengthMismatch { .. })));
    }

    #[test]
    fn test_content_mismatch() {
        let baseline = sample_events();
        let mut candidate = sample_events();
        candidate[0] = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"different"),
            timestamp: 1000,
            size: 5,
            tier: TierId::Ram,
        };
        let report = detect_divergence(&baseline, &candidate);
        assert!(!report.identical);
        assert_eq!(report.first_divergence_index, Some(0));
        assert!(report
            .divergences
            .iter()
            .any(|d| matches!(d, DivergenceType::ContentMismatch { .. })));
    }

    #[test]
    fn test_timestamp_mismatch() {
        let baseline = sample_events();
        let mut candidate = sample_events();
        if let TraceEvent::ChunkStateChanged { timestamp, .. } = &mut candidate[1] {
            *timestamp = 9999;
        }
        let report = detect_divergence(&baseline, &candidate);
        assert!(!report.identical);
        assert!(report
            .divergences
            .iter()
            .any(|d| matches!(d, DivergenceType::TimestampMismatch { .. })));
    }

    #[test]
    fn test_type_mismatch() {
        let baseline = sample_events();
        let mut candidate = sample_events();
        candidate[0] = TraceEvent::ChunkDeleted {
            chunk_id: ChunkId::from_data(b"hello"),
            timestamp: 1000,
            tier: TierId::Ram,
        };
        let report = detect_divergence(&baseline, &candidate);
        assert!(!report.identical);
        assert!(report
            .divergences
            .iter()
            .any(|d| matches!(d, DivergenceType::TypeMismatch { .. })));
    }

    #[test]
    fn test_empty_streams() {
        let report = detect_divergence(&[], &[]);
        assert!(report.identical);
        assert_eq!(report.events_compared, 0);
    }

    #[test]
    fn test_divergence_report_summary_identical() {
        let report = DivergenceReport::identical(10);
        let summary = report.summary();
        assert!(summary.contains("identical"));
        assert!(summary.contains("10"));
    }

    #[test]
    fn test_divergence_report_summary_divergent() {
        let divergences = vec![DivergenceType::ContentMismatch {
            index: 5,
            category: HashCategory::Content,
        }];
        let report = DivergenceReport::divergent(divergences, 10);
        let summary = report.summary();
        assert!(summary.contains("diverge"));
        assert!(summary.contains("5"));
    }

    #[test]
    fn test_divergence_type_display() {
        let div = DivergenceType::LengthMismatch {
            baseline_len: 10,
            candidate_len: 8,
        };
        let display = format!("{}", div);
        assert!(display.contains("LengthMismatch"));
    }

    #[test]
    fn test_compare_traces_files() {
        use tempfile::NamedTempFile;
        use crate::writer::TraceWriter;
        use crate::format::TraceMetadata;

        let events = sample_events();

        let tmp1 = NamedTempFile::new().unwrap();
        let tmp2 = NamedTempFile::new().unwrap();

        for tmp in [&tmp1, &tmp2] {
            let mut writer = TraceWriter::create(tmp.path(), 0).unwrap();
            writer.write_events(&events).unwrap();
            writer
                .close(TraceMetadata {
                    total_events: 3,
                    total_chunks: 1,
                    tier_ids: vec![TierId::Ram, TierId::Disk],
                    time_range: (1000, 1002),
                    policy_name: "test".to_string(),
                    config_summary: "test".to_string(),
                })
                .unwrap();
        }

        let report = compare_traces(tmp1.path(), tmp2.path()).unwrap();
        assert!(report.identical);
    }

    #[test]
    fn test_hash_comparison() {
        let baseline = sample_events();
        let candidate = sample_events();
        let (b_hashes, c_hashes) = hash_comparison(&baseline, &candidate);
        assert_eq!(b_hashes.len(), c_hashes.len());
        for (b, c) in b_hashes.iter().zip(c_hashes.iter()) {
            assert_eq!(b.hash, c.hash);
        }
    }

    #[test]
    fn test_first_divergence_index() {
        let baseline = sample_events();
        let mut candidate = sample_events();
        // Modify event at index 1
        candidate[1] = TraceEvent::ChunkStateChanged {
            chunk_id: ChunkId::from_data(b"hello"),
            timestamp: 1001,
            from: ChunkState::Stored, // different from baseline
            to: ChunkState::Cached,
        };
        let report = detect_divergence(&baseline, &candidate);
        assert_eq!(report.first_divergence_index, Some(1));
    }
}
