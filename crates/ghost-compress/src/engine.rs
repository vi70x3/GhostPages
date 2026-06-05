//! Compression engine implementation.

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::types::CompressionAlgorithm;

/// Configuration for compression operations.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Compression level (1-22 for zstd).
    /// Higher levels provide better compression but are slower.
    /// Default: 3 (good balance of speed and ratio).
    pub level: i32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self { level: 3 }
    }
}

/// Trait for compression engine implementations.
pub trait CompressionEngine: Send + Sync + 'static {
    /// Compress data using the configured algorithm.
    fn compress(&self, data: &[u8], config: &CompressionConfig) -> GhostResult<Vec<u8>>;

    /// Decompress data using the configured algorithm.
    fn decompress(&self, data: &[u8], expected_size: Option<usize>) -> GhostResult<Vec<u8>>;
}

/// zstd compression engine implementation.
#[derive(Debug, Clone, Copy, Default)]
pub struct ZstdEngine;

impl CompressionEngine for ZstdEngine {
    fn compress(&self, data: &[u8], config: &CompressionConfig) -> GhostResult<Vec<u8>> {
        zstd::bulk::compress(data, config.level)
            .map_err(|e| GhostError::CompressionError(format!("zstd compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8], expected_size: Option<usize>) -> GhostResult<Vec<u8>> {
        // zstd 0.13 bulk API: decompress(data, max_output_size)
        // If expected_size is provided, use it as the max output buffer size.
        // Otherwise, use a generous default (256 MB) to handle most cases.
        let max_size = expected_size.unwrap_or(256 * 1024 * 1024);
        zstd::bulk::decompress(data, max_size)
            .map_err(|e| GhostError::CompressionError(format!("zstd decompression failed: {}", e)))
    }
}

/// No-compression passthrough engine.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopEngine;

impl CompressionEngine for NoopEngine {
    fn compress(&self, data: &[u8], _config: &CompressionConfig) -> GhostResult<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8], _expected_size: Option<usize>) -> GhostResult<Vec<u8>> {
        Ok(data.to_vec())
    }
}

/// Compress data using the specified algorithm.
///
/// # Examples
///
/// ```
/// use ghost_core::types::CompressionAlgorithm;
/// use ghost_compress::{compress, CompressionConfig};
///
/// let data = b"Hello, GhostPages! This is test data for compression.";
/// let config = CompressionConfig::default();
///
/// let compressed = compress(data, CompressionAlgorithm::None, &config).unwrap();
/// assert_eq!(compressed.len(), data.len());
/// ```
pub fn compress(
    data: &[u8],
    algorithm: CompressionAlgorithm,
    config: &CompressionConfig,
) -> GhostResult<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::None => {
            let engine = NoopEngine;
            engine.compress(data, config)
        }
        CompressionAlgorithm::Zstd => {
            let engine = ZstdEngine;
            engine.compress(data, config)
        }
    }
}

/// Decompress data using the specified algorithm.
///
/// # Examples
///
/// ```
/// use ghost_core::types::CompressionAlgorithm;
/// use ghost_compress::{compress, decompress, CompressionConfig};
///
/// let data = b"Hello, GhostPages! This is test data for compression.";
/// let config = CompressionConfig::default();
///
/// let compressed = compress(data, CompressionAlgorithm::None, &config).unwrap();
/// let decompressed = decompress(&compressed, CompressionAlgorithm::None, None).unwrap();
///
/// assert_eq!(data, decompressed.as_slice());
/// ```
pub fn decompress(
    data: &[u8],
    algorithm: CompressionAlgorithm,
    expected_size: Option<usize>,
) -> GhostResult<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::None => {
            let engine = NoopEngine;
            engine.decompress(data, expected_size)
        }
        CompressionAlgorithm::Zstd => {
            let engine = ZstdEngine;
            engine.decompress(data, expected_size)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::CompressionAlgorithm;

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"Hello, GhostPages! This is test data for compression.";
        let config = CompressionConfig::default();

        let compressed = compress(data, CompressionAlgorithm::Zstd, &config).unwrap();
        let decompressed =
            decompress(&compressed, CompressionAlgorithm::Zstd, Some(data.len())).unwrap();

        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_noop_roundtrip() {
        let data = b"Hello, GhostPages!";
        let config = CompressionConfig::default();

        let compressed = compress(data, CompressionAlgorithm::None, &config).unwrap();
        let decompressed = decompress(&compressed, CompressionAlgorithm::None, None).unwrap();

        assert_eq!(data, decompressed.as_slice());
        // Noop should produce identical output
        assert_eq!(data, compressed.as_slice());
    }

    #[test]
    fn test_zstd_empty_data() {
        let data = b"";
        let config = CompressionConfig::default();

        let compressed = compress(data, CompressionAlgorithm::Zstd, &config).unwrap();
        let decompressed = decompress(&compressed, CompressionAlgorithm::Zstd, Some(0)).unwrap();

        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_zstd_large_data() {
        let data = vec![0u8; 1024 * 1024]; // 1MB of zeros
        let config = CompressionConfig::default();

        let compressed = compress(&data, CompressionAlgorithm::Zstd, &config).unwrap();
        // Highly compressible data should compress well
        assert!(compressed.len() < data.len() / 10);

        let decompressed =
            decompress(&compressed, CompressionAlgorithm::Zstd, Some(data.len())).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_zstd_corrupted_data() {
        let data = b"Hello, GhostPages!";
        let config = CompressionConfig::default();

        let mut compressed = compress(data, CompressionAlgorithm::Zstd, &config).unwrap();
        // Corrupt the data
        if !compressed.is_empty() {
            compressed[0] ^= 0xFF;
        }

        let result = decompress(&compressed, CompressionAlgorithm::Zstd, Some(data.len()));
        assert!(result.is_err());
    }

    #[test]
    fn test_compression_levels() {
        let data = b"Hello, GhostPages! ".repeat(100);

        let low_config = CompressionConfig { level: 1 };
        let high_config = CompressionConfig { level: 19 };

        let low_compressed = compress(&data, CompressionAlgorithm::Zstd, &low_config).unwrap();
        let high_compressed = compress(&data, CompressionAlgorithm::Zstd, &high_config).unwrap();

        // Higher level should produce same or smaller output
        assert!(high_compressed.len() <= low_compressed.len());

        // Both should decompress correctly
        let low_decompressed = decompress(
            &low_compressed,
            CompressionAlgorithm::Zstd,
            Some(data.len()),
        )
        .unwrap();
        let high_decompressed = decompress(
            &high_compressed,
            CompressionAlgorithm::Zstd,
            Some(data.len()),
        )
        .unwrap();

        assert_eq!(data, low_decompressed.as_slice());
        assert_eq!(data, high_decompressed.as_slice());
    }
}
