//! Fuzz target: Compression round-trip.
//!
//! Verifies that compress → decompress produces the original data.

#![no_main]
use libfuzzer_sys::fuzz_target;
use ghost_core::CompressionAlgorithm;
use ghost_compress::{compress, decompress, CompressionConfig};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let config = CompressionConfig::default();

    // Test with no compression
    let compressed = compress(data, CompressionAlgorithm::None, &config).unwrap();
    let decompressed = decompress(&compressed, CompressionAlgorithm::None, None).unwrap();
    assert_eq!(data, decompressed.as_slice());

    // Test with zstd compression
    let compressed = compress(data, CompressionAlgorithm::Zstd, &config).unwrap();
    let decompressed = decompress(&compressed, CompressionAlgorithm::Zstd, Some(data.len())).unwrap();
    assert_eq!(data, decompressed.as_slice());
});
