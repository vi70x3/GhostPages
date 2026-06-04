//! Benchmark: RAM backend throughput.
//!
//! Measures store and retrieve throughput for the RAM backend.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ghost_tier::RamBackend;
use ghost_tier::StorageBackend;

fn bench_ram_store_64b(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = RamBackend::new(1024 * 1024 * 64); // 64 MB
    let data = vec![0u8; 64];

    c.bench_function("ram_store_64b", |b| {
        b.iter(|| {
            rt.block_on(async {
                let alloc = backend.allocate(64).await.unwrap();
                backend.write(&alloc, black_box(&data)).await.unwrap();
                black_box(alloc);
            })
        })
    });
}

fn bench_ram_store_1kb(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = RamBackend::new(1024 * 1024 * 64);
    let data = vec![0u8; 1024];

    c.bench_function("ram_store_1kb", |b| {
        b.iter(|| {
            rt.block_on(async {
                let alloc = backend.allocate(1024).await.unwrap();
                backend.write(&alloc, black_box(&data)).await.unwrap();
                black_box(alloc);
            })
        })
    });
}

fn bench_ram_store_64kb(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = RamBackend::new(1024 * 1024 * 64);
    let data = vec![0u8; 64 * 1024];

    c.bench_function("ram_store_64kb", |b| {
        b.iter(|| {
            rt.block_on(async {
                let alloc = backend.allocate(64 * 1024).await.unwrap();
                backend.write(&alloc, black_box(&data)).await.unwrap();
                black_box(alloc);
            })
        })
    });
}

criterion_group!(
    ram_backend_benches,
    bench_ram_store_64b,
    bench_ram_store_1kb,
    bench_ram_store_64kb
);
criterion_main!(ram_backend_benches);
