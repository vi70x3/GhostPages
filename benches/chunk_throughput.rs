//! Benchmark: Chunk ID computation throughput.
//!
//! Measures the throughput of ChunkId::from_data for various payload sizes.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ghost_core::ChunkId;

fn bench_chunk_id_64b(c: &mut Criterion) {
    let data = vec![0u8; 64];
    c.bench_function("chunk_id_64b", |b| {
        b.iter(|| {
            let id = ChunkId::from_data(black_box(&data));
            black_box(id);
        })
    });
}

fn bench_chunk_id_1kb(c: &mut Criterion) {
    let data = vec![0u8; 1024];
    c.bench_function("chunk_id_1kb", |b| {
        b.iter(|| {
            let id = ChunkId::from_data(black_box(&data));
            black_box(id);
        })
    });
}

fn bench_chunk_id_64kb(c: &mut Criterion) {
    let data = vec![0u8; 64 * 1024];
    c.bench_function("chunk_id_64kb", |b| {
        b.iter(|| {
            let id = ChunkId::from_data(black_box(&data));
            black_box(id);
        })
    });
}

fn bench_chunk_id_1mb(c: &mut Criterion) {
    let data = vec![0u8; 1024 * 1024];
    c.bench_function("chunk_id_1mb", |b| {
        b.iter(|| {
            let id = ChunkId::from_data(black_box(&data));
            black_box(id);
        })
    });
}

criterion_group!(
    chunk_id_benches,
    bench_chunk_id_64b,
    bench_chunk_id_1kb,
    bench_chunk_id_64kb,
    bench_chunk_id_1mb
);
criterion_main!(chunk_id_benches);
