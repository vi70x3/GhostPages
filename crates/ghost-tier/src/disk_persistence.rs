//! Pure persistence layer for disk storage.
//!
//! This module extracts the file I/O logic (atomic writes, corruption detection,
//! directory layout) from `DiskBackend` into a separate struct with no simulation
//! state. It is the "persistence" half of "SimBackend + persistence".
//!
//! `DiskPersistence` handles:
//! - File I/O (read, write, delete)
//! - Atomic writes (temp file + rename)
//! - Corruption detection (magic bytes, version, blake3 hash verification)
//! - Directory layout (two-level hex prefix structure)
//!
//! It does **not** handle:
//! - Latency simulation
//! - Bandwidth throttling
//! - Failure injection
//! - Pressure calculation
//! - Health tracking
//!
//! Those concerns are handled by the simulation layer (`SimBackend`).

use blake3::Hasher;
use ghost_core::types::{ChunkId, CompressionAlgorithm};
use ghost_compress::{compress, decompress, CompressionConfig};

use std::fs;
use std::path::{Path, PathBuf};

use crate::backend::BackendError;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Magic bytes for chunk file identification.
const CHUNK_MAGIC: &[u8; 8] = b"GHOSTBLK";

/// Current chunk file format version.
const CHUNK_VERSION: u16 = 1;

/// Header size: magic (8) + version (2) + hash (32) + orig_size (4) + comp_size (4) + algo (1) = 51
const HEADER_SIZE: usize = 51;

// ─── Disk Persistence ─────────────────────────────────────────────────────────

/// Pure file-I/O persistence layer with no simulation state.
///
/// Handles actual data storage on disk: reading, writing (atomically), deleting,
/// and verifying chunk files. All operations are synchronous (blocking) and
/// intended to be called from `spawn_blocking` or similar.
///
/// # Directory Layout
///
/// ```text
/// <base_path>/<first_byte_hex>/<chunk_id_hex>.blk
/// ```
///
/// # Example
///
/// ```
/// use ghost_tier::disk_persistence::DiskPersistence;
/// use ghost_core::types::{ChunkId, CompressionAlgorithm};
/// use std::path::PathBuf;
///
/// let persistence = DiskPersistence::new(PathBuf::from("/tmp/ghostpages-test"));
/// let chunk_id = ChunkId::from_data(b"test data");
/// let data = b"Hello, persistence layer!";
/// let hash = *blake3::hash(data).as_bytes();
///
/// // Write
/// persistence.write_chunk(&chunk_id, data, hash, CompressionAlgorithm::None).unwrap();
///
/// // Read
/// let read_data = persistence.read_chunk(&chunk_id, &hash).unwrap();
/// assert_eq!(read_data, data);
///
/// // Delete
/// persistence.delete_chunk(&chunk_id).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct DiskPersistence {
    base_path: PathBuf,
}

impl DiskPersistence {
    /// Create a new disk persistence layer with the given base path.
    ///
    /// Creates the base directory and all 256 hex prefix subdirectories
    /// if they do not exist.
    pub fn new(base_path: PathBuf) -> Self {
        // Create base directory
        let _ = fs::create_dir_all(&base_path);

        // Create subdirectories for all 256 hex prefixes
        for i in 0u8..=255 {
            let prefix_dir = base_path.join(format!("{:02x}", i));
            let _ = fs::create_dir_all(&prefix_dir);
        }

        Self { base_path }
    }

    /// Compute the file path for a chunk ID.
    ///
    /// Uses a two-level directory structure: `<base_path>/<first_byte_hex>/<chunk_id_hex>.blk`
    pub fn chunk_path(&self, chunk_id: &ChunkId) -> PathBuf {
        let hex = hex::encode(chunk_id.0);
        let prefix = &hex[..2];
        self.base_path
            .join(prefix)
            .join(format!("{}.blk", hex))
    }

    /// Write a chunk file atomically using temp file + rename.
    ///
    /// The file format is:
    /// | Offset | Size | Field |
    /// |--------|------|-------|
    /// | 0 | 8 | Magic bytes (`GHOSTBLK`) |
    /// | 8 | 2 | Version (u16 LE) |
    /// | 10 | 32 | blake3 hash of the stored data |
    /// | 42 | 4 | Original data size (u32 LE) |
    /// | 46 | 4 | Compressed data size (u32 LE) |
    /// | 50 | 1 | Compression algorithm (0=None, 1=Zstd) |
    /// | 51 | N | Compressed data |
    ///
    /// Returns the total disk space consumed (header + compressed data).
    pub fn write_chunk(
        &self,
        chunk_id: &ChunkId,
        data: &[u8],
        content_hash: [u8; 32],
        compression: CompressionAlgorithm,
    ) -> Result<usize, BackendError> {
        let file_path = self.chunk_path(chunk_id);

        // Compress data
        let comp_config = CompressionConfig::default();
        let compressed = compress(data, compression, &comp_config).map_err(|e| {
            BackendError::WriteFailed(format!("compression failed: {}", e))
        })?;

        // Build header
        let mut header = Vec::with_capacity(HEADER_SIZE);
        header.extend_from_slice(CHUNK_MAGIC);
        header.extend_from_slice(&CHUNK_VERSION.to_le_bytes());
        header.extend_from_slice(&content_hash);
        header.extend_from_slice(&(data.len() as u32).to_le_bytes());
        header.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        header.push(match compression {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Zstd => 1,
        });

        // Write atomically: temp file then rename
        let temp_path = file_path.with_extension("blk.tmp");

        // Write header + compressed data to temp file
        fs::write(&temp_path, &header).map_err(|e| {
            BackendError::WriteFailed(format!(
                "failed to write header to {}: {}",
                temp_path.display(),
                e
            ))
        })?;

        // Append compressed data
        {
            use std::io::Write;
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&temp_path)
                .map_err(|e| {
                    BackendError::WriteFailed(format!(
                        "failed to open temp file {}: {}",
                        temp_path.display(),
                        e
                    ))
                })?;
            file.write_all(&compressed).map_err(|e| {
                BackendError::WriteFailed(format!(
                    "failed to write data to {}: {}",
                    temp_path.display(),
                    e
                ))
            })?;
        }

        // Atomic rename
        fs::rename(&temp_path, &file_path).map_err(|e| {
            BackendError::WriteFailed(format!(
                "failed to rename {} -> {}: {}",
                temp_path.display(),
                file_path.display(),
                e
            ))
        })?;

        let disk_size = header.len() + compressed.len();
        Ok(disk_size)
    }

    /// Read a chunk file and return the decompressed data.
    ///
    /// Verifies:
    /// - File is at least `HEADER_SIZE` bytes
    /// - Magic bytes match `GHOSTBLK`
    /// - Version is supported
    /// - Content hash matches `expected_hash`
    pub fn read_chunk(
        &self,
        chunk_id: &ChunkId,
        expected_hash: &[u8; 32],
    ) -> Result<Vec<u8>, BackendError> {
        let file_path = self.chunk_path(chunk_id);
        let bytes = fs::read(&file_path).map_err(|e| {
            BackendError::ReadFailed(format!(
                "failed to read chunk file {}: {}",
                file_path.display(),
                e
            ))
        })?;

        if bytes.len() < HEADER_SIZE {
            return Err(BackendError::ReadFailed(format!(
                "chunk file {} is too small ({} bytes, expected at least {})",
                file_path.display(),
                bytes.len(),
                HEADER_SIZE
            )));
        }

        // Verify magic
        if &bytes[..8] != CHUNK_MAGIC {
            return Err(BackendError::ReadFailed(format!(
                "chunk file {} has invalid magic bytes",
                file_path.display()
            )));
        }

        // Parse version
        let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        if version != CHUNK_VERSION {
            return Err(BackendError::ReadFailed(format!(
                "chunk file {} has unsupported version {} (expected {})",
                file_path.display(),
                version,
                CHUNK_VERSION
            )));
        }

        // Parse stored hash
        let stored_hash: [u8; 32] = bytes[10..42].try_into().unwrap();

        // Parse sizes
        let original_size = u32::from_le_bytes(bytes[42..46].try_into().unwrap()) as usize;
        let compressed_size = u32::from_le_bytes(bytes[46..50].try_into().unwrap()) as usize;

        // Parse compression algorithm
        let compression = match bytes[50] {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Zstd,
            other => {
                return Err(BackendError::ReadFailed(format!(
                    "chunk file {} has unknown compression algorithm {}",
                    file_path.display(),
                    other
                )));
            }
        };

        // Extract compressed data
        let compressed_data = &bytes[HEADER_SIZE..HEADER_SIZE + compressed_size];

        // Decompress
        let decompressed = decompress(compressed_data, compression, Some(original_size))
            .map_err(|e| {
                BackendError::ReadFailed(format!(
                    "decompression failed for {}: {}",
                    file_path.display(),
                    e
                ))
            })?;

        // Verify content hash
        let actual_hash = *blake3::hash(&decompressed).as_bytes();
        if &actual_hash != expected_hash {
            return Err(BackendError::IntegrityFailed(format!(
                "hash mismatch for {}: expected {}, got {}",
                file_path.display(),
                hex::encode(expected_hash),
                hex::encode(actual_hash)
            )));
        }

        Ok(decompressed)
    }

    /// Delete a chunk file from disk.
    pub fn delete_chunk(&self, chunk_id: &ChunkId) -> Result<(), BackendError> {
        let file_path = self.chunk_path(chunk_id);
        if file_path.exists() {
            fs::remove_file(&file_path).map_err(|e| {
                BackendError::Internal(format!(
                    "failed to delete chunk file {}: {}",
                    file_path.display(),
                    e
                ))
            })?;
        }
        Ok(())
    }

    /// Check if a chunk file exists on disk.
    pub fn chunk_exists(&self, chunk_id: &ChunkId) -> bool {
        self.chunk_path(chunk_id).exists()
    }

    /// Compute the blake3 hash of data (utility function).
    pub fn compute_hash(data: &[u8]) -> [u8; 32] {
        *blake3::hash(data).as_bytes()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_disk_persistence_write_read_delete() {
        let dir = TempDir::new().unwrap();
        let persistence = DiskPersistence::new(dir.path().to_path_buf());
        let chunk_id = ChunkId::from_data(b"test data");
        let data = b"Hello, persistence layer!";
        let hash = *blake3::hash(data).as_bytes();

        // Write
        let disk_size = persistence
            .write_chunk(&chunk_id, data, hash, CompressionAlgorithm::None)
            .unwrap();
        assert!(disk_size > HEADER_SIZE);
        assert!(persistence.chunk_exists(&chunk_id));

        // Read
        let read_data = persistence.read_chunk(&chunk_id, &hash).unwrap();
        assert_eq!(read_data, data);

        // Delete
        persistence.delete_chunk(&chunk_id).unwrap();
        assert!(!persistence.chunk_exists(&chunk_id));
    }

    #[test]
    fn test_disk_persistence_write_read_compressed() {
        let dir = TempDir::new().unwrap();
        let persistence = DiskPersistence::new(dir.path().to_path_buf());
        let chunk_id = ChunkId::from_data(b"compressed test");
        let data = vec![b'A'; 4096];
        let hash = *blake3::hash(&data).as_bytes();

        let disk_size = persistence
            .write_chunk(&chunk_id, &data, hash, CompressionAlgorithm::Zstd)
            .unwrap();

        // Compressed data should be smaller than original
        assert!(disk_size < HEADER_SIZE + data.len());

        let read_data = persistence.read_chunk(&chunk_id, &hash).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_disk_persistence_chunk_path_deterministic() {
        let dir = TempDir::new().unwrap();
        let persistence = DiskPersistence::new(dir.path().to_path_buf());
        let chunk_id = ChunkId::from_data(b"deterministic test");

        let path1 = persistence.chunk_path(&chunk_id);
        let path2 = persistence.chunk_path(&chunk_id);
        assert_eq!(path1, path2);
        assert!(path1.starts_with(dir.path()));
        assert!(path1.to_string_lossy().ends_with(".blk"));
    }

    #[test]
    fn test_disk_persistence_read_invalid_magic() {
        let dir = TempDir::new().unwrap();
        let persistence = DiskPersistence::new(dir.path().to_path_buf());
        let chunk_id = ChunkId::from_data(b"invalid magic test");

        // Write garbage data directly
        let file_path = persistence.chunk_path(&chunk_id);
        fs::write(&file_path, b"not a valid chunk file at all").unwrap();

        let result = persistence.read_chunk(&chunk_id, &[0u8; 32]);
        assert!(matches!(result, Err(BackendError::ReadFailed(_))));
    }

    #[test]
    fn test_disk_persistence_read_too_small() {
        let dir = TempDir::new().unwrap();
        let persistence = DiskPersistence::new(dir.path().to_path_buf());
        let chunk_id = ChunkId::from_data(b"too small test");

        let file_path = persistence.chunk_path(&chunk_id);
        fs::write(&file_path, b"tiny").unwrap();

        let result = persistence.read_chunk(&chunk_id, &[0u8; 32]);
        assert!(matches!(result, Err(BackendError::ReadFailed(_))));
    }

    #[test]
    fn test_disk_persistence_read_hash_mismatch() {
        let dir = TempDir::new().unwrap();
        let persistence = DiskPersistence::new(dir.path().to_path_buf());
        let chunk_id = ChunkId::from_data(b"hash mismatch test");
        let data = b"original data";
        let hash = *blake3::hash(data).as_bytes();

        persistence
            .write_chunk(&chunk_id, data, hash, CompressionAlgorithm::None)
            .unwrap();

        // Try to read with wrong hash
        let wrong_hash = [0xFFu8; 32];
        let result = persistence.read_chunk(&chunk_id, &wrong_hash);
        assert!(matches!(result, Err(BackendError::IntegrityFailed(_))));
    }

    #[test]
    fn test_disk_persistence_delete_nonexistent() {
        let dir = TempDir::new().unwrap();
        let persistence = DiskPersistence::new(dir.path().to_path_buf());
        let chunk_id = ChunkId::from_data(b"nonexistent");

        // Deleting a non-existent chunk should succeed (no-op)
        let result = persistence.delete_chunk(&chunk_id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_disk_persistence_compute_hash() {
        let data = b"hash test";
        let hash = DiskPersistence::compute_hash(data);
        assert_eq!(hash, *blake3::hash(data).as_bytes());
    }
}
