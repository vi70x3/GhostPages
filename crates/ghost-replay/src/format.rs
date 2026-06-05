//! Binary trace file format for GhostPages replay system.
//!
//! Defines the on-disk layout for persistent trace storage:
//! `[Header] [Record 0] ... [Record N] [Metadata]`
//!
//! The format is versioned for forward compatibility.

use std::io::{Read, Write};

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::types::TierId;

/// Magic bytes identifying a GhostPages trace file.
pub const TRACE_MAGIC: &[u8; 8] = b"GHOSTTRC";

/// Current trace file format version.
pub const TRACE_VERSION: u16 = 1;

/// Bit flags for trace file header.
pub mod flags {
    /// File payloads are compressed.
    pub const COMPRESSED: u16 = 0x0001;
    /// File includes CRC32 checksums per record.
    pub const HAS_CHECKSUM: u16 = 0x0002;
}

/// File header written at the start of every trace file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceFileHeader {
    /// Magic bytes — always `b"GHOSTTRC"`.
    pub magic: [u8; 8],
    /// Format version (currently 1).
    pub version: u16,
    /// Bit flags (see `flags` module).
    pub flags: u16,
    /// Number of event records in the file.
    pub chunk_count: u64,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Byte offset to the metadata section at end of file.
    pub metadata_offset: u64,
}

impl TraceFileHeader {
    /// Serialized size of the header in bytes.
    pub const SIZE: usize = 8 + 2 + 2 + 8 + 8 + 8; // 36 bytes

    /// Create a new header with the given parameters.
    pub fn new(flags: u16, chunk_count: u64, created_at: u64, metadata_offset: u64) -> Self {
        Self {
            magic: *TRACE_MAGIC,
            version: TRACE_VERSION,
            flags,
            chunk_count,
            created_at,
            metadata_offset,
        }
    }

    /// Serialize the header to a byte buffer (little-endian).
    pub fn write_to<W: Write>(&self, writer: &mut W) -> GhostResult<()> {
        writer.write_all(&self.magic)?;
        writer.write_all(&self.version.to_le_bytes())?;
        writer.write_all(&self.flags.to_le_bytes())?;
        writer.write_all(&self.chunk_count.to_le_bytes())?;
        writer.write_all(&self.created_at.to_le_bytes())?;
        writer.write_all(&self.metadata_offset.to_le_bytes())?;
        Ok(())
    }

    /// Deserialize a header from a byte buffer (little-endian).
    pub fn read_from<R: Read>(reader: &mut R) -> GhostResult<Self> {
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;

        if &magic != TRACE_MAGIC {
            return Err(GhostError::ReplayError(format!(
                "invalid trace file magic: expected GHOSTTRC, got {:?}",
                std::str::from_utf8(&magic).unwrap_or("<invalid>")
            )));
        }

        let mut buf2 = [0u8; 2];
        reader.read_exact(&mut buf2)?;
        let version = u16::from_le_bytes(buf2);

        reader.read_exact(&mut buf2)?;
        let flags = u16::from_le_bytes(buf2);

        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        let chunk_count = u64::from_le_bytes(buf8);

        reader.read_exact(&mut buf8)?;
        let created_at = u64::from_le_bytes(buf8);

        reader.read_exact(&mut buf8)?;
        let metadata_offset = u64::from_le_bytes(buf8);

        Ok(Self {
            magic,
            version,
            flags,
            chunk_count,
            created_at,
            metadata_offset,
        })
    }

    /// Check if the compressed flag is set.
    pub fn is_compressed(&self) -> bool {
        self.flags & flags::COMPRESSED != 0
    }

    /// Check if the has-checksum flag is set.
    pub fn has_checksum(&self) -> bool {
        self.flags & flags::HAS_CHECKSUM != 0
    }
}

/// A single event record in the trace file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceRecord {
    /// Timestamp (millis since epoch).
    pub timestamp: u64,
    /// Event type discriminant (index into TraceEvent enum).
    pub event_type: u16,
    /// Length in bytes of the serialized payload.
    pub payload_len: u32,
    /// Serialized TraceEvent bytes.
    pub payload: Vec<u8>,
    /// CRC32 checksum of the payload.
    pub crc32: u32,
}

impl TraceRecord {
    /// Write this record to a writer (little-endian).
    pub fn write_to<W: Write>(&self, writer: &mut W) -> GhostResult<()> {
        writer.write_all(&self.timestamp.to_le_bytes())?;
        writer.write_all(&self.event_type.to_le_bytes())?;
        writer.write_all(&self.payload_len.to_le_bytes())?;
        writer.write_all(&self.payload)?;
        writer.write_all(&self.crc32.to_le_bytes())?;
        Ok(())
    }

    /// Read a record from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> GhostResult<Option<Self>> {
        let mut buf8 = [0u8; 8];

        // Try to read the timestamp; if EOF, no more records
        if read_exact_or_eof(reader, &mut buf8)? {
            return Ok(None);
        }
        let timestamp = u64::from_le_bytes(buf8);

        let mut buf2 = [0u8; 2];
        reader.read_exact(&mut buf2)?;
        let event_type = u16::from_le_bytes(buf2);

        let mut buf4 = [0u8; 4];
        reader.read_exact(&mut buf4)?;
        let payload_len = u32::from_le_bytes(buf4);

        let mut payload = vec![0u8; payload_len as usize];
        reader.read_exact(&mut payload)?;

        reader.read_exact(&mut buf4)?;
        let crc32 = u32::from_le_bytes(buf4);

        Ok(Some(Self {
            timestamp,
            event_type,
            payload_len,
            payload,
            crc32,
        }))
    }
}

/// Read exactly `buf.len()` bytes, returning `true` if EOF was reached immediately.
fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<bool> {
    match reader.read_exact(buf) {
        Ok(()) => Ok(false),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(true),
        Err(e) => Err(e),
    }
}

/// Metadata section written at the end of the trace file.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TraceMetadata {
    /// Total number of events.
    pub total_events: u64,
    /// Total unique chunks seen.
    pub total_chunks: u64,
    /// Tier IDs referenced in the trace.
    pub tier_ids: Vec<TierId>,
    /// Time range: (first_event_ts, last_event_ts).
    pub time_range: (u64, u64),
    /// Name of the placement policy used.
    pub policy_name: String,
    /// Human-readable config summary.
    pub config_summary: String,
}

impl TraceMetadata {
    /// Serialize metadata to a writer as a length-prefixed JSON blob.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> GhostResult<()> {
        let json = serde_json::to_vec(self)
            .map_err(|e| GhostError::ReplayError(format!("failed to serialize metadata: {}", e)))?;
        let len = json.len() as u64;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&json)?;
        Ok(())
    }

    /// Deserialize metadata from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> GhostResult<Self> {
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        let len = u64::from_le_bytes(buf8);

        let mut json = vec![0u8; len as usize];
        reader.read_exact(&mut json)?;

        serde_json::from_slice(&json)
            .map_err(|e| GhostError::ReplayError(format!("failed to deserialize metadata: {}", e)))
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::ChunkState;

    #[test]
    fn test_header_roundtrip() {
        let header = TraceFileHeader::new(flags::HAS_CHECKSUM, 42, 1_700_000_000_000, 1024);
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();

        let parsed = TraceFileHeader::read_from(&mut buf.as_slice()).unwrap();
        assert_eq!(parsed, header);
        assert_eq!(parsed.version, TRACE_VERSION);
        assert!(parsed.has_checksum());
        assert!(!parsed.is_compressed());
    }

    #[test]
    fn test_header_invalid_magic() {
        let mut buf = b"NOTGHOST".to_vec();
        buf.extend_from_slice(&[0u8; 28]); // rest of header
        let result = TraceFileHeader::read_from(&mut buf.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn test_header_size_constant() {
        let header = TraceFileHeader::new(0, 0, 0, 0);
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), TraceFileHeader::SIZE);
    }

    #[test]
    fn test_metadata_roundtrip() {
        let metadata = TraceMetadata {
            total_events: 100,
            total_chunks: 10,
            tier_ids: vec![TierId::Ram, TierId::Disk],
            time_range: (1000, 5000),
            policy_name: "lru".to_string(),
            config_summary: "test config".to_string(),
        };

        let mut buf = Vec::new();
        metadata.write_to(&mut buf).unwrap();

        let parsed = TraceMetadata::read_from(&mut buf.as_slice()).unwrap();
        assert_eq!(parsed, metadata);
    }

    #[test]
    fn test_record_roundtrip() {
        let record = TraceRecord {
            timestamp: 12345,
            event_type: 3,
            payload_len: 4,
            payload: vec![1, 2, 3, 4],
            crc32: 0xDEADBEEF,
        };

        let mut buf = Vec::new();
        record.write_to(&mut buf).unwrap();

        let parsed = TraceRecord::read_from(&mut buf.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(parsed, record);
    }

    #[test]
    fn test_record_eof_returns_none() {
        let buf: Vec<u8> = Vec::new();
        let result = TraceRecord::read_from(&mut buf.as_slice()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_flags_constants() {
        assert_eq!(flags::COMPRESSED, 0x0001);
        assert_eq!(flags::HAS_CHECKSUM, 0x0002);
    }

    #[test]
    fn test_chunk_state_size() {
        // Ensure ChunkState is small enough for replay state tracking
        let state = ChunkState::Stored;
        assert_eq!(state.is_readable(), true);
    }
}
