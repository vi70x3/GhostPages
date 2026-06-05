//! Deterministic checksum engine for replay validation.
//!
//! Provides blake3-based content hashing of event streams with tiered
//! hash categories (content, migration, state) to enable deterministic
//! replay verification and precise divergence detection.

use std::fmt;
use std::path::Path;

use blake3::Hasher;
use ghost_core::trace::TraceEvent;
use ghost_core::types::ChunkId;

use crate::reader::TraceReader;

/// Tiered hash categories for classifying replay events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HashCategory {
    /// Chunk creation, deletion, and metadata events.
    Content,
    /// Transfer, migration, and eviction events.
    Migration,
    /// State machine transitions and pressure events.
    State,
    /// Policy, daemon, IPC, and other infrastructure events.
    Other,
}

impl fmt::Display for HashCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HashCategory::Content => write!(f, "content"),
            HashCategory::Migration => write!(f, "migration"),
            HashCategory::State => write!(f, "state"),
            HashCategory::Other => write!(f, "other"),
        }
    }
}

/// Formats a byte slice as a hex string (first 8 bytes).
fn hex8(bytes: &[u8]) -> String {
    let len = bytes.len().min(8);
    let mut s = String::with_capacity(len * 2);
    for &b in &bytes[..len] {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Deterministic checksum over an event stream.
///
/// Contains tiered hashes that can be compared across replays to detect
/// divergence. The `total` hash covers all events and is order-sensitive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayChecksum {
    /// Combined hash of all events (order-sensitive).
    pub total: [u8; 32],
    /// Hash of content events (ChunkCreated, ChunkDeleted).
    pub content: [u8; 32],
    /// Hash of migration events (transfers, evictions).
    pub migration: [u8; 32],
    /// Hash of state events (state changes, pressure).
    pub state: [u8; 32],
    /// Hash of other events (policy, daemon, IPC).
    pub other: [u8; 32],
    /// Number of events hashed.
    pub event_count: usize,
}

impl ReplayChecksum {
    /// Creates a new checksum with zero hashes and zero count.
    pub fn new() -> Self {
        Self {
            total: [0u8; 32],
            content: [0u8; 32],
            migration: [0u8; 32],
            state: [0u8; 32],
            other: [0u8; 32],
            event_count: 0,
        }
    }

    /// Returns true if all hashes match between this and another checksum.
    pub fn matches(&self, other: &Self) -> bool {
        self.total == other.total
            && self.content == other.content
            && self.migration == other.migration
            && self.state == other.state
            && self.other == other.other
    }

    /// Returns a human-readable summary of the checksum.
    pub fn summary(&self) -> String {
        format!(
            "ReplayChecksum {{ events: {}, total: {}, content: {}, migration: {}, state: {}, other: {} }}",
            self.event_count,
            hex8(&self.total),
            hex8(&self.content),
            hex8(&self.migration),
            hex8(&self.state),
            hex8(&self.other),
        )
    }
}

impl Default for ReplayChecksum {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ReplayChecksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.summary())
    }
}

/// Individual event hash with metadata for pinpointing divergence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventHash {
    /// Index of the event in the stream.
    pub index: usize,
    /// Timestamp of the event.
    pub timestamp: u64,
    /// Category of the event.
    pub category: HashCategory,
    /// blake3 hash of the event content.
    pub hash: [u8; 32],
    /// Chunk ID if the event has one.
    pub chunk_id: Option<ChunkId>,
}

impl fmt::Display for EventHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let chunk = self
            .chunk_id
            .map(|id| id.short_hex())
            .unwrap_or_default();
        write!(
            f,
            "EventHash {{ idx: {}, ts: {}, cat: {}, chunk: {}, hash: {} }}",
            self.index,
            self.timestamp,
            self.category,
            chunk,
            hex8(&self.hash),
        )
    }
}

/// Classifies a trace event into a hash category.
pub fn classify_event(event: &TraceEvent) -> HashCategory {
    match event {
        TraceEvent::ChunkCreated { .. } | TraceEvent::ChunkDeleted { .. } => {
            HashCategory::Content
        }
        TraceEvent::TransferQueued { .. }
        | TraceEvent::TransferStarted { .. }
        | TraceEvent::TransferCompleted { .. }
        | TraceEvent::TransferFailed { .. }
        | TraceEvent::TransferRetry { .. }
        | TraceEvent::TransferCancelled { .. }
        | TraceEvent::Eviction { .. } => HashCategory::Migration,
        TraceEvent::ChunkStateChanged { .. }
        | TraceEvent::PressureSample { .. }
        | TraceEvent::PressureAlert { .. } => HashCategory::State,
        _ => HashCategory::Other,
    }
}

/// Hashes a single event deterministically using blake3.
///
/// The hash is computed over the bincode-serialized event, prefixed with
/// the event type index and timestamp for domain separation.
pub fn hash_event(event: &TraceEvent, index: usize) -> [u8; 32] {
    let mut hasher = Hasher::new();

    // Domain separation: include event type index
    let type_index = event_type_index(event);
    hasher.update(&type_index.to_le_bytes());

    // Include timestamp for temporal ordering
    let ts = event.timestamp();
    hasher.update(&ts.to_le_bytes());

    // Include event index for positional uniqueness
    let idx_bytes = (index as u64).to_le_bytes();
    hasher.update(&idx_bytes);

    // Include the bincode-serialized event payload
    if let Ok(bytes) = bincode::serialize(event) {
        hasher.update(&bytes);
    }

    let hash = hasher.finalize();
    *hash.as_bytes()
}

/// Hashes a slice of events and returns their individual hashes.
pub fn hash_events(events: &[TraceEvent]) -> Vec<EventHash> {
    events
        .iter()
        .enumerate()
        .map(|(i, event)| EventHash {
            index: i,
            timestamp: event.timestamp(),
            category: classify_event(event),
            hash: hash_event(event, i),
            chunk_id: event.chunk_id(),
        })
        .collect()
}

/// Computes a full `ReplayChecksum` from a slice of events.
pub fn from_events(events: &[TraceEvent]) -> ReplayChecksum {
    if events.is_empty() {
        return ReplayChecksum::new();
    }

    let mut total_hasher = Hasher::new();
    let mut content_hasher = Hasher::new();
    let mut migration_hasher = Hasher::new();
    let mut state_hasher = Hasher::new();
    let mut other_hasher = Hasher::new();

    for (i, event) in events.iter().enumerate() {
        let event_hash = hash_event(event, i);

        // Always include in total
        total_hasher.update(&event_hash);

        // Also include in category-specific hasher
        let category = classify_event(event);
        match category {
            HashCategory::Content => {
                content_hasher.update(&event_hash);
            }
            HashCategory::Migration => {
                migration_hasher.update(&event_hash);
            }
            HashCategory::State => {
                state_hasher.update(&event_hash);
            }
            HashCategory::Other => {
                other_hasher.update(&event_hash);
            }
        }
    }

    ReplayChecksum {
        total: *total_hasher.finalize().as_bytes(),
        content: *content_hasher.finalize().as_bytes(),
        migration: *migration_hasher.finalize().as_bytes(),
        state: *state_hasher.finalize().as_bytes(),
        other: *other_hasher.finalize().as_bytes(),
        event_count: events.len(),
    }
}

/// Computes a `ReplayChecksum` from a trace file on disk.
pub fn from_file(path: &Path) -> ghost_core::error::GhostResult<ReplayChecksum> {
    let mut reader = TraceReader::open(path)?;
    let events = reader.read_all()?;
    Ok(from_events(&events))
}

/// Returns the event type index for domain separation in hashing.
/// This mirrors the `event_type_index` function in the writer module.
fn event_type_index(event: &TraceEvent) -> u16 {
    match event {
        TraceEvent::ChunkCreated { .. } => 0,
        TraceEvent::ChunkDeleted { .. } => 1,
        TraceEvent::ChunkStateChanged { .. } => 2,
        TraceEvent::TransferQueued { .. } => 3,
        TraceEvent::TransferStarted { .. } => 4,
        TraceEvent::TransferCompleted { .. } => 5,
        TraceEvent::TransferFailed { .. } => 6,
        TraceEvent::TransferRetry { .. } => 7,
        TraceEvent::TransferCancelled { .. } => 8,
        TraceEvent::PressureSample { .. } => 9,
        TraceEvent::PressureAlert { .. } => 10,
        TraceEvent::Eviction { .. } => 11,
        TraceEvent::PolicyDecision { .. } => 12,
        TraceEvent::DaemonStarted { .. } => 13,
        TraceEvent::DaemonStopping { .. } => 14,
        TraceEvent::BackendRegistered { .. } => 15,
        TraceEvent::WorkerSpawned { .. } => 16,
        TraceEvent::WorkerStopped { .. } => 17,
        TraceEvent::IpcRequestReceived { .. } => 18,
        TraceEvent::IpcResponseSent { .. } => 19,
        TraceEvent::IpcConnectionAccepted { .. } => 20,
        TraceEvent::IpcConnectionClosed { .. } => 21,
        TraceEvent::CompressionStarted { .. } => 22,
        TraceEvent::CompressionCompleted { .. } => 23,
        TraceEvent::DecompressionStarted { .. } => 24,
        TraceEvent::DecompressionCompleted { .. } => 25,
    }
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
            TraceEvent::TransferCompleted {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1005,
                from: TierId::Ram,
                to: TierId::Disk,
                size: 5,
                duration_ms: 3,
            },
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1010,
                tier: TierId::Disk,
                reason: ghost_core::trace::EvictionReason::Capacity,
            },
        ]
    }

    #[test]
    fn test_from_events_produces_deterministic_checksum() {
        let events = sample_events();
        let checksum1 = from_events(&events);
        let checksum2 = from_events(&events);
        assert_eq!(checksum1, checksum2);
        assert!(checksum1.matches(&checksum2));
    }

    #[test]
    fn test_different_events_produce_different_checksum() {
        let events1 = sample_events();
        let mut events2 = sample_events();
        // Modify one event
        events2[0] = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"different"),
            timestamp: 1000,
            size: 5,
            tier: TierId::Ram,
        };
        let checksum1 = from_events(&events1);
        let checksum2 = from_events(&events2);
        assert_ne!(checksum1.total, checksum2.total);
        assert!(!checksum1.matches(&checksum2));
    }

    #[test]
    fn test_empty_events_produce_zero_checksum() {
        let checksum = from_events(&[]);
        assert_eq!(checksum.event_count, 0);
        assert_eq!(checksum.total, [0u8; 32]);
    }

    #[test]
    fn test_hash_events_returns_correct_count() {
        let events = sample_events();
        let hashes = hash_events(&events);
        assert_eq!(hashes.len(), 5);
    }

    #[test]
    fn test_classify_event_content() {
        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test"),
            timestamp: 0,
            size: 4,
            tier: TierId::Ram,
        };
        assert_eq!(classify_event(&event), HashCategory::Content);
    }

    #[test]
    fn test_classify_event_migration() {
        let event = TraceEvent::Eviction {
            chunk_id: ChunkId::from_data(b"test"),
            timestamp: 0,
            tier: TierId::Ram,
            reason: ghost_core::trace::EvictionReason::Capacity,
        };
        assert_eq!(classify_event(&event), HashCategory::Migration);
    }

    #[test]
    fn test_classify_event_state() {
        let event = TraceEvent::ChunkStateChanged {
            chunk_id: ChunkId::from_data(b"test"),
            timestamp: 0,
            from: ChunkState::Allocated,
            to: ChunkState::Stored,
        };
        assert_eq!(classify_event(&event), HashCategory::State);
    }

    #[test]
    fn test_classify_event_other() {
        let event = TraceEvent::DaemonStarted {
            timestamp: 0,
        };
        assert_eq!(classify_event(&event), HashCategory::Other);
    }

    #[test]
    fn test_replay_checksum_summary() {
        let events = sample_events();
        let checksum = from_events(&events);
        let summary = checksum.summary();
        assert!(summary.contains("events: 5"));
    }

    #[test]
    fn test_event_hash_display() {
        let events = sample_events();
        let hashes = hash_events(&events);
        let display = format!("{}", hashes[0]);
        assert!(display.contains("EventHash"));
        assert!(display.contains("idx: 0"));
    }

    #[test]
    fn test_checksum_display() {
        let events = sample_events();
        let checksum = from_events(&events);
        let display = format!("{}", checksum);
        assert!(display.contains("ReplayChecksum"));
    }

    #[test]
    fn test_tiered_hashes_differ() {
        let events = sample_events();
        let checksum = from_events(&events);
        // With mixed event types, category hashes should differ
        assert_ne!(checksum.total, checksum.content);
        assert_ne!(checksum.content, checksum.migration);
    }

    #[test]
    fn test_order_matters() {
        let events1 = sample_events();
        let mut events2 = sample_events();
        events2.reverse();
        let checksum1 = from_events(&events1);
        let checksum2 = from_events(&events2);
        assert_ne!(checksum1.total, checksum2.total);
    }

    #[test]
    fn test_hash_event_deterministic() {
        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test"),
            timestamp: 1000,
            size: 4,
            tier: TierId::Ram,
        };
        let hash1 = hash_event(&event, 0);
        let hash2 = hash_event(&event, 0);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_different_indices_produce_different_hashes() {
        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test"),
            timestamp: 1000,
            size: 4,
            tier: TierId::Ram,
        };
        let hash0 = hash_event(&event, 0);
        let hash1 = hash_event(&event, 1);
        assert_ne!(hash0, hash1);
    }

    #[test]
    fn test_from_file() {
        use tempfile::NamedTempFile;
        use crate::writer::TraceWriter;
        use crate::format::TraceMetadata;

        let events = sample_events();
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut writer = TraceWriter::create(&path, 0).unwrap();
        writer.write_events(&events).unwrap();
        writer
            .close(TraceMetadata {
                total_events: 5,
                total_chunks: 1,
                tier_ids: vec![TierId::Ram, TierId::Disk],
                time_range: (1000, 1010),
                policy_name: "test".to_string(),
                config_summary: "test".to_string(),
            })
            .unwrap();

        let checksum = from_file(&path).unwrap();
        assert_eq!(checksum.event_count, 5);
    }
}
