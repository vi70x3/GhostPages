//! Trace file reader for GhostPages replay system.
//!
//! Reads trace events from a binary file with CRC32 validation.

use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::Path;

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::trace::TraceEvent;

use crate::format::{TraceFileHeader, TraceMetadata, TraceRecord};

/// Reads trace events from a binary trace file.
///
/// Usage:
/// ```ignore
/// use ghost_replay::reader::TraceReader;
/// use std::path::Path;
/// let mut reader = TraceReader::open(Path::new("trace.ghost"))?;
/// let _events = reader.read_all()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct TraceReader {
    reader: BufReader<File>,
    header: TraceFileHeader,
    current_index: u64,
}

impl TraceReader {
    /// Open a trace file for reading.
    ///
    /// Validates the magic bytes and reads the header.
    pub fn open(path: &Path) -> GhostResult<Self> {
        let file = File::open(path)
            .map_err(|e| GhostError::ReplayError(format!("failed to open trace file: {}", e)))?;

        let mut reader = BufReader::new(file);
        let header = TraceFileHeader::read_from(&mut reader)?;

        Ok(Self {
            reader,
            header,
            current_index: 0,
        })
    }

    /// Read the next event record from the file.
    ///
    /// Returns `Ok(None)` when all records have been read.
    pub fn read_next(&mut self) -> GhostResult<Option<TraceEvent>> {
        if self.current_index >= self.header.chunk_count {
            return Ok(None);
        }

        let record = TraceRecord::read_from(&mut self.reader)?.ok_or_else(|| {
            GhostError::ReplayError(format!(
                "unexpected EOF at record {} of {}",
                self.current_index, self.header.chunk_count
            ))
        })?;

        // Validate CRC32 if the file has checksums
        if self.header.has_checksum() {
            let computed = crc32fast::hash(&record.payload);
            if computed != record.crc32 {
                return Err(GhostError::ReplayError(format!(
                    "CRC32 mismatch at record {}: expected {:08X}, got {:08X}",
                    self.current_index, record.crc32, computed
                )));
            }
        }

        let event: TraceEvent = bincode::deserialize(&record.payload).map_err(|e| {
            GhostError::ReplayError(format!(
                "failed to deserialize event at record {}: {}",
                self.current_index, e
            ))
        })?;

        self.current_index += 1;
        Ok(Some(event))
    }

    /// Read all remaining events from the file.
    pub fn read_all(&mut self) -> GhostResult<Vec<TraceEvent>> {
        let mut events = Vec::with_capacity(self.header.chunk_count as usize);
        while let Some(event) = self.read_next()? {
            events.push(event);
        }
        Ok(events)
    }

    /// Seek to the first event with a timestamp >= the given value.
    ///
    /// This performs a linear scan from the current position. For large files,
    /// a future version could use an index for binary search.
    pub fn seek_to_timestamp(&mut self, timestamp: u64) -> GhostResult<()> {
        // If we need to seek backwards, restart from beginning
        if self.current_index > 0 {
            self.reader
                .seek(std::io::SeekFrom::Start(TraceFileHeader::SIZE as u64))
                .map_err(|e| GhostError::ReplayError(format!("seek failed: {}", e)))?;
            self.current_index = 0;
        }

        // Scan forward to find the first event at or after the target timestamp
        while self.current_index < self.header.chunk_count {
            let record = TraceRecord::read_from(&mut self.reader)?
                .ok_or_else(|| GhostError::ReplayError("unexpected EOF during seek".to_string()))?;

            if record.timestamp >= timestamp {
                // We found it — seek back to the start of this record so read_next
                // will return it
                let record_size = 8 + 2 + 4 + record.payload.len() + 4;
                self.reader
                    .seek(std::io::SeekFrom::Current(-(record_size as i64)))
                    .map_err(|e| GhostError::ReplayError(format!("seek back failed: {}", e)))?;
                return Ok(());
            }

            self.current_index += 1;
        }

        // Reached end without finding a matching timestamp — that's fine,
        // subsequent read_next calls will return None
        Ok(())
    }

    /// Read the metadata section from the end of the file.
    ///
    /// This seeks to the metadata offset stored in the header.
    pub fn metadata(&self) -> GhostResult<TraceMetadata> {
        let file_ref = self.reader.get_ref();
        let mut metadata_reader = BufReader::new(file_ref);
        metadata_reader
            .seek(std::io::SeekFrom::Start(self.header.metadata_offset))
            .map_err(|e| GhostError::ReplayError(format!("failed to seek to metadata: {}", e)))?;

        TraceMetadata::read_from(&mut metadata_reader)
    }

    /// Get the total number of records in the file.
    pub fn record_count(&self) -> u64 {
        self.header.chunk_count
    }

    /// Check if the file has compressed payloads.
    pub fn is_compressed(&self) -> bool {
        self.header.is_compressed()
    }

    /// Get the file format version.
    pub fn version(&self) -> u16 {
        self.header.version
    }

    /// Get the creation timestamp.
    pub fn created_at(&self) -> u64 {
        self.header.created_at
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::TraceMetadata;
    use crate::writer::TraceWriter;
    use ghost_core::state::ChunkState;
    use ghost_core::types::{ChunkId, TierId};

    #[test]
    fn test_reader_read_all() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path();

        // Write some events
        let mut writer = TraceWriter::create(path, 0).unwrap();
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
        ];
        writer.write_events(&events).unwrap();

        let metadata = TraceMetadata {
            total_events: 2,
            total_chunks: 1,
            tier_ids: vec![TierId::Ram],
            time_range: (1000, 1001),
            policy_name: "test".to_string(),
            config_summary: "test".to_string(),
        };
        writer.close(metadata).unwrap();

        // Read them back
        let mut reader = TraceReader::open(path).unwrap();
        assert_eq!(reader.record_count(), 2);
        assert!(!reader.is_compressed());

        let read_events = reader.read_all().unwrap();
        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].timestamp(), events[0].timestamp());
        assert_eq!(read_events[1].timestamp(), events[1].timestamp());
    }

    #[test]
    fn test_reader_read_next() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut writer = TraceWriter::create(path, 0).unwrap();
        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"chunk1"),
            size: 100,
            tier: TierId::Ram,
            timestamp: 1000,
        };
        writer.write_event(&event).unwrap();

        let metadata = TraceMetadata {
            total_events: 1,
            total_chunks: 1,
            tier_ids: vec![TierId::Ram],
            time_range: (1000, 1000),
            policy_name: "test".to_string(),
            config_summary: "test".to_string(),
        };
        writer.close(metadata).unwrap();

        let mut reader = TraceReader::open(path).unwrap();
        let first = reader.read_next().unwrap();
        assert!(first.is_some());
        assert_eq!(first.unwrap().timestamp(), event.timestamp());

        let second = reader.read_next().unwrap();
        assert!(second.is_none());
    }

    #[test]
    fn test_reader_invalid_magic() {
        use std::io::Write;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut file = std::fs::File::create(tmp.path()).unwrap();
        file.write_all(b"NOTGHOST").unwrap();
        file.write_all(&[0u8; 100]).unwrap();
        drop(file);

        let result = TraceReader::open(tmp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_reader_seek_to_timestamp() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut writer = TraceWriter::create(path, 0).unwrap();
        let events: Vec<TraceEvent> = (0..10)
            .map(|i| TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(&[i as u8]),
                size: 100,
                tier: TierId::Ram,
                timestamp: i as u64 * 100,
            })
            .collect();
        writer.write_events(&events).unwrap();

        let metadata = TraceMetadata {
            total_events: 10,
            total_chunks: 10,
            tier_ids: vec![TierId::Ram],
            time_range: (0, 900),
            policy_name: "test".to_string(),
            config_summary: "test".to_string(),
        };
        writer.close(metadata).unwrap();

        let mut reader = TraceReader::open(path).unwrap();
        reader.seek_to_timestamp(500).unwrap();

        // Should get events starting from timestamp 500
        let event = reader.read_next().unwrap().unwrap();
        assert_eq!(event.timestamp(), 500);
    }
}
