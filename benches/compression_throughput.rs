//! Benchmark: Compression throughput.
//!
//! Measures compression and decompression speed for various payload sizes
//! and data patterns.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ghost_core::CompressionAlgorithm;
use ghost_compress::{compress, decompress};

fn bench_compress_zstd_1kb(c: &mut Criterion) {
    let data = vec![0u8; 1024];
    c.bench_function("compress_zstd_1kb", |b| {
        b.iter(|| {
            let compressed = compress(black_box(&data), CompressionAlgorithm::Zstd);
            black_box(compressed);
        })
    });
}

fn bench_decompress_zstd_1kb(c: &mut Criterion) {
    let data = vec![0u8; 1024];
    let compressed = compress(&data, CompressionAlgorithm::Zstd);
    c.bench_function("decompress_zstd_1kb", |b| {
        b.iter(|| {
            let decompressed = decompress(black_box(&compressed), CompressionAlgorithm::Zstd);
            black_box(decompressed);
        })
    });
}

fn bench_compress_zstd_64kb(c: &mut Criterion) {
    let data = b"GhostPages compression test data. ".repeat(2048);
    c.bench_function("compress_zstd_64kb_text", |b| {
        b.iter(|| {
            let compressed = compress(black_box(&data), CompressionAlgorithm::Zstd);
            black_box(compressed);
        })
    });
}

fn bench_decompress_zstd_64kb(c: &mut Criterion) {
    let data = b"GhostPages compression test data. ".repeat(2048);
    let compressed = compress(&data, CompressionAlgorithm::Zstd);
    c.bench_function("decompress_zstd_64kb_text", |b| {
        b.iter(|| {
            let decompressed = decompress(black_box(&compressed), CompressionAlgorithm::Zstd);
            black_box(decompressed);
        })
    });
}

fn bench_roundtrip_zstd_1kb(c: &mut Criterion) {
    let data = vec![0xABu8; 1024];
    c.bench_function("roundtrip_zstd_1kb", |b| {
        b.iter(|| {
            let compressed = compress(black_box(&data), CompressionAlgorithm::Zstd);
            let decompressed = decompress(&compressed, CompressionAlgorithm::Zstd);
            black_box(decompressed);
        })
    });
}

criterion_group!(
    compression_benches,
    bench_compress_zstd_1kb,
    bench_decompress_zstd_1kb,
    bench_compress_zstd_64kb,
    bench_decompress_zstd_64kb,
    bench_roundtrip_zstd_1kb
);
criterion_main!(compression_benches);
