//! Allocator stress benchmarks.
//!
//! Measures throughput and latency of allocation patterns that stress
//! the backend: rapid alloc/free cycles, fragmentation patterns, and
//! concurrent allocation churn.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ghost_tier::backend::{Allocation, BackendData};
use ghost_tier::RamBackend;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;
use tokio::runtime::Runtime;

/// Capacity for benchmarks.
const CAPACITY: usize = 1024 * 1024; // 1 MB

/// Fixed seed for deterministic benchmarks.
const SEED: u64 = 0xBENCH_MARK_SEED_001;

/// Benchmark: rapid allocation/free cycles.
///
/// Measures the throughput of 1000 random alloc/free operations.
fn bench_alloc_free_storm(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build tokio runtime");

    c.bench_function("alloc_free_storm_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                let backend = RamBackend::new(CAPACITY);
                let mut rng = ChaCha8Rng::seed_from_u64(SEED);
                let mut live: HashMap<usize, usize> = HashMap::new();
                let mut total: usize = 0;

                for _ in 0..1000 {
                    let should_alloc = live.is_empty() || rand::Rng::gen_bool(&mut rng, 0.7);

                    if should_alloc {
                        let size = rand::Rng::gen_range(&mut rng, 64..=8192);
                        if total + size <= CAPACITY {
                            if let Ok(alloc) = backend.allocate(size).await {
                                live.insert(alloc.offset, size);
                                total += size;
                            }
                        }
                    } else if !live.is_empty() {
                        let offsets: Vec<usize> = live.keys().copied().collect();
                        let idx = rand::Rng::gen_range(&mut rng, 0..offsets.len());
                        let offset = offsets[idx];
                        let size = live.remove(&offset).unwrap();
                        let alloc = Allocation::new(offset, size, BackendData::new(size));
                        let _ = backend.deallocate(alloc).await;
                        total -= size;
                    }
                }

                // Clean up
                for (offset, size) in live {
                    let alloc = Allocation::new(offset, size, BackendData::new(size));
                    let _ = backend.deallocate(alloc).await;
                }

                black_box(backend.available());
            });
        });
    });
}

/// Benchmark: fragmentation pattern.
///
/// Measures the cost of allocating varying sizes, freeing every other,
/// and then reallocating in the fragmented space.
fn bench_fragmentation_pattern(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build tokio runtime");

    c.bench_function("fragmentation_pattern", |b| {
        b.iter(|| {
            rt.block_on(async {
                let backend = RamBackend::new(CAPACITY);
                let mut rng = ChaCha8Rng::seed_from_u64(SEED.wrapping_add(1));
                let mut allocs: Vec<Allocation> = Vec::new();

                // Phase 1: Allocate varying sizes
                for _ in 0..100 {
                    let exp = rand::Rng::gen_range(&mut rng, 6..=20);
                    let size = 1usize << exp;
                    if size > CAPACITY / 2 {
                        continue;
                    }
                    if let Ok(alloc) = backend.allocate(size).await {
                        allocs.push(alloc);
                    }
                }

                // Phase 2: Free every other
                let mut kept: Vec<Allocation> = Vec::new();
                for (i, alloc) in allocs.into_iter().enumerate() {
                    if i % 2 == 0 {
                        let _ = backend.deallocate(alloc).await;
                    } else {
                        kept.push(alloc);
                    }
                }

                // Phase 3: Reallocate in fragmented space
                let freed_space = backend.available();
                let realloc_size = freed_space.min(64 * 1024);
                let _ = backend.allocate(realloc_size).await;

                // Clean up
                for alloc in kept {
                    let _ = backend.deallocate(alloc).await;
                }

                black_box(backend.available());
            });
        });
    });
}

/// Benchmark: concurrent allocation churn.
///
/// Measures the cost of 10 concurrent tasks each doing 100 alloc/free cycles.
fn bench_concurrent_churn(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build tokio runtime");

    c.bench_function("concurrent_churn_10x100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let backend = std::sync::Arc::new(RamBackend::new(CAPACITY));
                let mut handles = Vec::new();

                for task_id in 0..10 {
                    let be = backend.clone();
                    let handle = tokio::spawn(async move {
                        let mut rng = ChaCha8Rng::seed_from_u64(SEED.wrapping_add(task_id as u64 * 1000));
                        let mut local_allocs: Vec<Allocation> = Vec::new();

                        for _ in 0..100 {
                            let size = rand::Rng::gen_range(&mut rng, 32..=4096);
                            if let Ok(alloc) = be.allocate(size).await {
                                let pattern = vec![(task_id & 0xFF) as u8; size];
                                let _ = be.write(&alloc, &pattern).await;
                                local_allocs.push(alloc);
                            }

                            if !local_allocs.is_empty() && rand::Rng::gen_bool(&mut rng, 0.3) {
                                let idx = rand::Rng::gen_range(&mut rng, 0..local_allocs.len());
                                let alloc = local_allocs.swap_remove(idx);
                                let _ = be.deallocate(alloc).await;
                            }
                        }

                        for alloc in local_allocs {
                            let _ = be.deallocate(alloc).await;
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    let _ = handle.await;
                }

                black_box(backend.available());
            });
        });
    });
}

/// Benchmark: capacity exhaustion and reclamation.
///
/// Measures the cost of filling to capacity, freeing, and reallocating.
fn bench_capacity_exhaustion(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build tokio runtime");

    c.bench_function("capacity_exhaustion", |b| {
        b.iter(|| {
            rt.block_on(async {
                let backend = RamBackend::new(CAPACITY);
                let mut allocs: Vec<Allocation> = Vec::new();

                // Fill to capacity
                loop {
                    match backend.allocate(1024).await {
                        Ok(alloc) => allocs.push(alloc),
                        Err(_) => break,
                    }
                }

                // Free 10%
                let free_count = allocs.len() / 10;
                for _ in 0..free_count {
                    if let Some(alloc) = allocs.pop() {
                        let _ = backend.deallocate(alloc).await;
                    }
                }

                // Reallocate
                for _ in 0..free_count {
                    if let Ok(alloc) = backend.allocate(1024).await {
                        allocs.push(alloc);
                    }
                }

                // Clean up
                for alloc in allocs {
                    let _ = backend.deallocate(alloc).await;
                }

                black_box(backend.available());
            });
        });
    });
}

criterion_group!(
    benches,
    bench_alloc_free_storm,
    bench_fragmentation_pattern,
    bench_concurrent_churn,
    bench_capacity_exhaustion
);
criterion_main!(benches);
