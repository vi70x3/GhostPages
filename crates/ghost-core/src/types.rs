//! Core types for GhostPages.
//!
//! This module defines the fundamental data structures used throughout the system.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Content-addressed chunk identifier.
///
/// Each chunk is identified by its blake3 hash, providing:
/// - **Uniqueness**: Different data produces different IDs (with overwhelming probability)
/// - **Integrity**: ID verification detects data corruption
/// - **Deduplication**: Identical data produces identical IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChunkId(pub [u8; 32]);

impl ChunkId {
    /// Compute a ChunkId from raw data using blake3 hashing.
    ///
    /// This is a content-addressed identifier: the same data always produces
    /// the same ChunkId, and different data (almost certainly) produces different IDs.
    ///
    /// # Examples
    ///
    /// ```
    /// use ghost_core::ChunkId;
    ///
    /// let data = b"Hello, GhostPages!";
    /// let id = ChunkId::from_data(data);
    ///
    /// // Same data produces same ID
    /// let id2 = ChunkId::from_data(data);
    /// assert_eq!(id, id2);
    /// ```
    pub fn from_data(data: &[u8]) -> Self {
        ChunkId(*blake3::hash(data).as_bytes())
    }

    /// Verify that data matches this ChunkId.
    ///
    /// Returns `true` if the blake3 hash of the data equals this ChunkId.
    ///
    /// # Examples
    ///
    /// ```
    /// use ghost_core::ChunkId;
    ///
    /// let data = b"Hello, GhostPages!";
    /// let id = ChunkId::from_data(data);
    ///
    /// assert!(id.verify(data));
    /// assert!(!id.verify(b"Different data"));
    /// ```
    pub fn verify(&self, data: &[u8]) -> bool {
        let computed = blake3::hash(data);
        computed.as_bytes() == &self.0
    }

    /// Get the first 8 bytes as a hex string for display purposes.
    pub fn short_hex(&self) -> String {
        hex::encode(&self.0[..8])
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.short_hex())
    }
}

/// Memory tier identifier.
///
/// Represents the different storage tiers available in the system,
/// ordered from hottest (fastest) to coldest (slowest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TierId {
    /// System RAM (hot tier, fastest access).
    Ram,

    /// GPU VRAM (warm tier, high bandwidth).
    GpuVram,

    /// NVMe/SSD storage (cold tier, persistent).
    Disk,

    /// Simulation backend (for testing and development).
    Simulation,
}

impl TierId {
    /// Get the priority of this tier (lower = hotter).
    ///
    /// Used by placement policies to determine tier ordering.
    pub fn priority(&self) -> u8 {
        match self {
            TierId::Ram => 0,
            TierId::GpuVram => 1,
            TierId::Disk => 2,
            TierId::Simulation => 3,
        }
    }

    /// Check if this tier is a GPU-based tier.
    pub fn is_gpu(&self) -> bool {
        matches!(self, TierId::GpuVram)
    }

    /// Check if this tier is persistent (survives reboot).
    pub fn is_persistent(&self) -> bool {
        matches!(self, TierId::Disk)
    }
}

impl fmt::Display for TierId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TierId::Ram => write!(f, "RAM"),
            TierId::GpuVram => write!(f, "GPU VRAM"),
            TierId::Disk => write!(f, "Disk"),
            TierId::Simulation => write!(f, "Simulation"),
        }
    }
}

/// Compression algorithm identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression (passthrough).
    None,

    /// zstd compression.
    Zstd,
}

impl fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionAlgorithm::None => write!(f, "none"),
            CompressionAlgorithm::Zstd => write!(f, "zstd"),
        }
    }
}

/// Metadata for a stored chunk.
///
/// Contains all information about a chunk except the actual data,
/// including its location, size, and access patterns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    /// Content-addressed chunk identifier.
    pub id: ChunkId,

    /// Original (uncompressed) size in bytes.
    pub size: usize,

    /// Compressed size in bytes.
    pub compressed_size: usize,

    /// Current storage tier.
    pub tier: TierId,

    /// Current lifecycle state.
    pub state: crate::state::ChunkState,

    /// Creation timestamp (Unix timestamp in seconds).
    pub created_at: u64,

    /// Last accessed timestamp (Unix timestamp in seconds).
    pub last_accessed: u64,

    /// Number of times this chunk has been accessed.
    pub access_count: u64,

    /// Compression algorithm used.
    pub compression: CompressionAlgorithm,

    /// blake3 checksum of compressed data for integrity verification.
    pub checksum: [u8; 32],
}

impl ChunkMeta {
    /// Create a new ChunkMeta with current timestamps.
    pub fn new(
        id: ChunkId,
        size: usize,
        compressed_size: usize,
        tier: TierId,
        compression: CompressionAlgorithm,
        checksum: [u8; 32],
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            id,
            size,
            compressed_size,
            tier,
            state: crate::state::ChunkState::Allocated,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            compression,
            checksum,
        }
    }

    /// Record an access to this chunk, updating the access count and timestamp.
    pub fn record_access(&mut self) {
        self.access_count += 1;
        self.last_accessed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Get the compression ratio (original / compressed).
    /// Returns 1.0 if compression did not reduce size.
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_size == 0 {
            1.0
        } else {
            self.size as f64 / self.compressed_size as f64
        }
    }
}

/// System memory pressure levels.
///
/// Used by the placement policy to determine when to migrate data between tiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PressureLevel {
    /// Normal operation, all tiers healthy.
    Normal,

    /// Soft pressure (>80% RAM usage).
    Soft,

    /// Medium pressure (>90% RAM usage).
    Medium,

    /// Hard pressure (>95% RAM usage).
    Hard,

    /// Critical pressure (OOM imminent).
    Critical,
}

impl PressureLevel {
    /// Get the numeric level (0 = normal, 4 = critical).
    pub fn level(&self) -> u8 {
        match self {
            PressureLevel::Normal => 0,
            PressureLevel::Soft => 1,
            PressureLevel::Medium => 2,
            PressureLevel::Hard => 3,
            PressureLevel::Critical => 4,
        }
    }
}

impl fmt::Display for PressureLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PressureLevel::Normal => write!(f, "normal"),
            PressureLevel::Soft => write!(f, "soft"),
            PressureLevel::Medium => write!(f, "medium"),
            PressureLevel::Hard => write!(f, "hard"),
            PressureLevel::Critical => write!(f, "critical"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_id_from_data() {
        let data = b"Hello, GhostPages!";
        let id = ChunkId::from_data(data);

        // Same data produces same ID
        let id2 = ChunkId::from_data(data);
        assert_eq!(id, id2);

        // ID verifies against original data
        assert!(id.verify(data));
    }

    #[test]
    fn test_chunk_id_different_data() {
        let data1 = b"Hello, GhostPages!";
        let data2 = b"Different data";

        let id1 = ChunkId::from_data(data1);
        let id2 = ChunkId::from_data(data2);

        // Different data produces different IDs
        assert_ne!(id1, id2);

        // Each ID verifies only against its own data
        assert!(id1.verify(data1));
        assert!(!id1.verify(data2));
        assert!(id2.verify(data2));
        assert!(!id2.verify(data1));
    }

    #[test]
    fn test_chunk_id_empty_data() {
        let id = ChunkId::from_data(b"");
        assert!(id.verify(b""));
        assert!(!id.verify(b"not empty"));
    }

    #[test]
    fn test_tier_id_priority() {
        assert_eq!(TierId::Ram.priority(), 0);
        assert_eq!(TierId::GpuVram.priority(), 1);
        assert_eq!(TierId::Disk.priority(), 2);
        assert_eq!(TierId::Simulation.priority(), 3);
    }

    #[test]
    fn test_chunk_meta_compression_ratio() {
        let id = ChunkId::from_data(b"test");
        let meta = ChunkMeta::new(
            id,
            1000,
            500,
            TierId::Ram,
            CompressionAlgorithm::Zstd,
            [0u8; 32],
        );
        assert!((meta.compression_ratio() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_chunk_meta_record_access() {
        let id = ChunkId::from_data(b"test");
        let mut meta = ChunkMeta::new(
            id,
            100,
            50,
            TierId::Ram,
            CompressionAlgorithm::None,
            [0u8; 32],
        );

        assert_eq!(meta.access_count, 0);
        meta.record_access();
        assert_eq!(meta.access_count, 1);
        meta.record_access();
        assert_eq!(meta.access_count, 2);
    }

    #[test]
    fn test_chunk_meta_initial_state() {
        let id = ChunkId::from_data(b"test");
        let meta = ChunkMeta::new(
            id,
            100,
            50,
            TierId::Ram,
            CompressionAlgorithm::None,
            [0u8; 32],
        );

        // New chunks should start in Allocated state
        assert_eq!(meta.state, crate::state::ChunkState::Allocated);
    }

    #[test]
    fn test_pressure_level_ordering() {
        assert!(PressureLevel::Normal.level() < PressureLevel::Soft.level());
        assert!(PressureLevel::Soft.level() < PressureLevel::Medium.level());
        assert!(PressureLevel::Medium.level() < PressureLevel::Hard.level());
        assert!(PressureLevel::Hard.level() < PressureLevel::Critical.level());
    }
}
