//! Trace file writer for GhostPages replay system.
//!
//! Writes trace events to a binary file with CRC32 checksums.

use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::Path;

use crc32fast::Hasher;

use ghost_core::error::GhostResult;
use ghost_core::trace::TraceEvent;

use crate::format::{TraceFileHeader, TraceMetadata, TraceRecord};

/// Writes trace events to a binary trace file.
///
/// Usage:
/// ```ignore
/// use ghost_replay::writer::TraceWriter;
/// use ghost_replay::format::TraceMetadata;
/// use ghost_core::trace::TraceEvent;
/// use ghost_core::types::TierId;
/// use std::path::Path;
/// let mut writer = TraceWriter::create(Path::new("trace.ghost"), 0)?;
/// writer.write_event(&TraceEvent::ChunkCreated {
///     chunk_id: ghost_core::types::ChunkId::from_data(b"test"),
///     size: 1024,
///     tier: TierId::Ram,
///     timestamp: 0,
/// })?;
/// let metadata = TraceMetadata {
///     total_events: 1,
///     total_chunks: 1,
///     tier_ids: vec![TierId::Ram],
///     time_range: (0, 0),
///     policy_name: "test".to_string(),
///     config_summary: "test".to_string(),
/// };
/// writer.close(metadata)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct TraceWriter {
    writer: BufWriter<File>,
    header: TraceFileHeader,
    record_count: u64,
    hasher: Hasher,
}

impl TraceWriter {
    /// Create a new trace writer that writes to the given path.
    ///
    /// The file is created with a placeholder header. Call `close()` to
    /// finalize the header and write metadata.
    pub fn create(path: &Path, flags: u16) -> GhostResult<Self> {
        let file = File::create(path).map_err(|e| {
            ghost_core::error::GhostError::ReplayError(format!(
                "failed to create trace file: {}",
                e
            ))
        })?;
        let mut writer = BufWriter::new(file);

        // Write a placeholder header (will be updated on close)
        let header = TraceFileHeader::new(flags, 0, 0, 0);
        header.write_to(&mut writer)?;

        Ok(Self {
            writer,
            header,
            record_count: 0,
            hasher: Hasher::new(),
        })
    }

    /// Write a single event to the file.
    pub fn write_event(&mut self, event: &TraceEvent) -> GhostResult<()> {
        let payload = bincode::serialize(event).map_err(|e| {
            ghost_core::error::GhostError::ReplayError(format!("failed to serialize event: {}", e))
        })?;

        // Compute CRC32 of payload
        self.hasher.reset();
        self.hasher.update(&payload);
        let crc = self.hasher.clone().finalize();

        let event_type = event_type_index(event);

        let record = TraceRecord {
            timestamp: event.timestamp(),
            event_type,
            payload_len: payload.len() as u32,
            payload,
            crc32: crc,
        };

        record.write_to(&mut self.writer)?;
        self.record_count += 1;

        Ok(())
    }

    /// Write multiple events to the file.
    pub fn write_events(&mut self, events: &[TraceEvent]) -> GhostResult<()> {
        for event in events {
            self.write_event(event)?;
        }
        Ok(())
    }

    /// Finalize the trace file: update the header with the correct record
    /// count and write the metadata section.
    pub fn close(mut self, metadata: TraceMetadata) -> GhostResult<()> {
        // Write metadata at current position
        let metadata_offset = self.writer.stream_position().map_err(|e| {
            ghost_core::error::GhostError::ReplayError(format!(
                "failed to get stream position: {}",
                e
            ))
        })?;

        metadata.write_to(&mut self.writer)?;

        // Flush before updating header
        self.writer.flush()?;

        // Seek back to start and write the real header
        self.writer.seek(std::io::SeekFrom::Start(0)).map_err(|e| {
            ghost_core::error::GhostError::ReplayError(format!("failed to seek to start: {}", e))
        })?;

        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let final_header = TraceFileHeader::new(
            self.header.flags,
            self.record_count,
            created_at,
            metadata_offset,
        );
        final_header.write_to(&mut self.writer)?;

        self.writer.flush()?;

        Ok(())
    }

    /// Get the number of records written so far.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }
}

/// Get the event type index for a TraceEvent.
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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::flags;
    use ghost_core::state::ChunkState;
    use ghost_core::types::{ChunkId, TierId};
    use tempfile::NamedTempFile;

    #[test]
    fn test_writer_create() {
        let tmp = NamedTempFile::new().unwrap();
        let writer = TraceWriter::create(tmp.path(), 0).unwrap();
        assert_eq!(writer.record_count(), 0);
    }

    #[test]
    fn test_writer_write_single_event() {
        let tmp = NamedTempFile::new().unwrap();
        let mut writer = TraceWriter::create(tmp.path(), flags::HAS_CHECKSUM).unwrap();

        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test"),
            size: 1024,
            tier: TierId::Ram,
            timestamp: 1000,
        };

        writer.write_event(&event).unwrap();
        assert_eq!(writer.record_count(), 1);

        let metadata = TraceMetadata {
            total_events: 1,
            total_chunks: 1,
            tier_ids: vec![TierId::Ram],
            time_range: (1000, 1000),
            policy_name: "test".to_string(),
            config_summary: "test".to_string(),
        };

        writer.close(metadata).unwrap();
    }

    #[test]
    fn test_writer_write_multiple_events() {
        let tmp = NamedTempFile::new().unwrap();
        let mut writer = TraceWriter::create(tmp.path(), flags::HAS_CHECKSUM).unwrap();

        let events = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"chunk1"),
                size: 100,
                tier: TierId::Ram,
                timestamp: 1000,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"chunk1"),
                from: ChunkState::Allocated,
                to: ChunkState::Stored,
                timestamp: 1001,
            },
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"chunk1"),
                tier: TierId::Ram,
                reason: ghost_core::trace::EvictionReason::Capacity,
                timestamp: 1002,
            },
        ];

        writer.write_events(&events).unwrap();
        assert_eq!(writer.record_count(), 3);

        let metadata = TraceMetadata {
            total_events: 3,
            total_chunks: 1,
            tier_ids: vec![TierId::Ram],
            time_range: (1000, 1002),
            policy_name: "test".to_string(),
            config_summary: "test".to_string(),
        };

        writer.close(metadata).unwrap();
    }

    #[test]
    fn test_event_type_index_unique() {
        // Verify all event types have unique indices
        let events: Vec<TraceEvent> = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"a"),
                size: 0,
                tier: TierId::Ram,
                timestamp: 0,
            },
            TraceEvent::ChunkDeleted {
                chunk_id: ChunkId::from_data(b"a"),
                tier: TierId::Ram,
                timestamp: 0,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"a"),
                from: ChunkState::Allocated,
                to: ChunkState::Stored,
                timestamp: 0,
            },
            TraceEvent::TransferQueued {
                chunk_id: ChunkId::from_data(b"a"),
                from: TierId::Ram,
                to: TierId::Disk,
                priority: ghost_core::transfer::TransferPriority::Normal,
                timestamp: 0,
            },
            TraceEvent::TransferStarted {
                job: ghost_core::transfer::TransferJob::new(
                    ChunkId::from_data(b"a"),
                    TierId::Ram,
                    TierId::Disk,
                    100,
                    ghost_core::transfer::TransferPriority::Normal,
                ),
                timestamp: 0,
            },
            TraceEvent::TransferCompleted {
                chunk_id: ChunkId::from_data(b"a"),
                from: TierId::Ram,
                to: TierId::Disk,
                size: 100,
                duration_ms: 10,
                timestamp: 0,
            },
            TraceEvent::TransferFailed {
                chunk_id: ChunkId::from_data(b"a"),
                from: TierId::Ram,
                to: TierId::Disk,
                error: "err".to_string(),
                attempt: 1,
                timestamp: 0,
            },
            TraceEvent::TransferRetry {
                chunk_id: ChunkId::from_data(b"a"),
                from: TierId::Ram,
                to: TierId::Disk,
                attempt: 1,
                timestamp: 0,
            },
            TraceEvent::TransferCancelled {
                chunk_id: ChunkId::from_data(b"a"),
                from: TierId::Ram,
                to: TierId::Disk,
                timestamp: 0,
            },
            TraceEvent::PressureSample {
                state: ghost_core::state::PressureState::new(),
                timestamp: 0,
            },
            TraceEvent::PressureAlert {
                memory_pressure: 0.5,
                vram_pressure: 0.5,
                io_pressure: 0.5,
                timestamp: 0,
            },
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"a"),
                tier: TierId::Ram,
                reason: ghost_core::trace::EvictionReason::Capacity,
                timestamp: 0,
            },
            TraceEvent::PolicyDecision {
                chunk_id: ChunkId::from_data(b"a"),
                from: TierId::Ram,
                to: TierId::Disk,
                reason: "test".to_string(),
                timestamp: 0,
            },
            TraceEvent::DaemonStarted { timestamp: 0 },
            TraceEvent::DaemonStopping { timestamp: 0 },
            TraceEvent::BackendRegistered {
                tier: TierId::Ram,
                timestamp: 0,
            },
            TraceEvent::WorkerSpawned {
                worker_id: 0,
                timestamp: 0,
            },
            TraceEvent::WorkerStopped {
                worker_id: 0,
                timestamp: 0,
            },
            TraceEvent::IpcRequestReceived {
                request_type: "store".to_string(),
                timestamp: 0,
            },
            TraceEvent::IpcResponseSent {
                request_type: "store".to_string(),
                success: true,
                timestamp: 0,
            },
            TraceEvent::IpcConnectionAccepted { timestamp: 0 },
            TraceEvent::IpcConnectionClosed { timestamp: 0 },
            TraceEvent::CompressionStarted {
                chunk_id: ChunkId::from_data(b"a"),
                original_size: 100,
                timestamp: 0,
            },
            TraceEvent::CompressionCompleted {
                chunk_id: ChunkId::from_data(b"a"),
                original_size: 100,
                compressed_size: 50,
                timestamp: 0,
            },
            TraceEvent::DecompressionStarted {
                chunk_id: ChunkId::from_data(b"a"),
                compressed_size: 50,
                timestamp: 0,
            },
            TraceEvent::DecompressionCompleted {
                chunk_id: ChunkId::from_data(b"a"),
                compressed_size: 50,
                decompressed_size: 100,
                timestamp: 0,
            },
        ];

        let mut indices = std::collections::HashSet::new();
        for event in &events {
            let idx = event_type_index(event);
            assert!(indices.insert(idx), "duplicate event type index: {}", idx);
        }
        assert_eq!(indices.len(), 26);
    }
}
