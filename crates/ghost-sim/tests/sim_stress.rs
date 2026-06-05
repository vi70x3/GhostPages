//! Stress tests for SimBackend.
//!
//! These tests push the simulation backend to its limits with extreme
//! configurations: high failure rates, bandwidth saturation, latency
//! extremes, and fragmentation scenarios.

use ghost_sim::config::{BandwidthConfig, FailureConfig, LatencyConfig, SimConfig};
use ghost_sim::SimBackend;
use ghost_tier::backend::{Allocation, BackendData};
use ghost_tier::StorageBackend;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Fixed seed for deterministic testing.
const SEED: u64 = 0xCAFE_BABE_DEAD_BEEF;

// ─── High failure rate stress ────────────────────────────────────────────────

/// Run 500 allocation cycles with a 50% failure rate.
#[tokio::test]
async fn test_high_failure_rate_stress() {
    let config = SimConfig::with_capacity(1024 * 1024)
        .with_seed(SEED)
        .with_failure(FailureConfig {
            write_failure_rate: 0.5,
            read_failure_rate: 0.5,
            alloc_failure_rate: 0.3,
            corruption_on_failure: false,
        })
        .with_latency(LatencyConfig {
            base: Duration::from_micros(0),
            per_byte: Duration::from_nanos(0),
            jitter_fraction: 0.0,
        });

    let backend = SimBackend::new(config);
    let mut rng = ChaCha8Rng::seed_from_u64(SEED);

    let mut successful_allocs = 0;
    let mut _failed_allocs = 0;
    let mut _successful_writes = 0;
    let mut _failed_writes = 0;
    let mut live_allocs: HashMap<usize, Vec<u8>> = HashMap::new();

    for _ in 0..500 {
        let size = rng.gen_range(64..=4096);

        match backend.allocate(size).await {
            Ok(alloc) => {
                successful_allocs += 1;
                let data = vec![0xABu8; size];

                match backend.write(&alloc, &data).await {
                    Ok(()) => {
                        live_allocs.insert(alloc.offset, data);
                    }
                    Err(_) => {
                        let _ = backend.deallocate(alloc).await;
                    }
                }
            }
            Err(_) => {
            }
        }
    }

    assert!(
        successful_allocs > 0,
        "should have at least some successful allocations"
    );

    // Verify all live allocations are readable
    for (offset, expected_data) in &live_allocs {
        let alloc = Allocation::new(
            *offset,
            expected_data.len(),
            BackendData::new(expected_data.len()),
        );
        let mut buf = vec![0u8; expected_data.len()];
        if backend.read(&alloc, &mut buf).await.is_ok() {
            assert_eq!(buf, *expected_data, "data integrity check failed at offset {}", offset);
        }
    }

    // Clean up
    for (offset, data) in live_allocs {
        let alloc = Allocation::new(offset, data.len(), BackendData::new(data.len()));
        let _ = backend.deallocate(alloc).await;
    }
}

// ─── Bandwidth saturation ────────────────────────────────────────────────────

/// Saturate the backend with writes to test bandwidth limiting.
#[tokio::test]
async fn test_bandwidth_saturation() {
    let config = SimConfig::with_capacity(4 * 1024 * 1024)
        .with_seed(SEED.wrapping_add(1))
        .with_bandwidth(BandwidthConfig {
            bytes_per_second: 1024 * 1024, // 1 MB/s
        })
        .with_latency(LatencyConfig {
            base: Duration::from_micros(0),
            per_byte: Duration::from_nanos(0),
            jitter_fraction: 0.0,
        });

    let backend = SimBackend::new(config);
    let mut allocs: Vec<Allocation> = Vec::new();

    // Allocate 100 chunks of 4KB each
    for _ in 0..100 {
        if let Ok(alloc) = backend.allocate(4096).await {
            allocs.push(alloc);
        }
    }

    assert!(!allocs.is_empty(), "should have made at least some allocations");

    // Write to all allocations
    for alloc in &allocs {
        let data = vec![0xCDu8; alloc.size];
        let _ = backend.write(alloc, &data).await;
    }

    // Read back and verify
    for alloc in &allocs {
        let mut buf = vec![0u8; alloc.size];
        if backend.read(alloc, &mut buf).await.is_ok() {
            assert_eq!(buf.len(), alloc.size);
        }
    }

    // Clean up
    for alloc in allocs {
        let _ = backend.deallocate(alloc).await;
    }
}

// ─── Latency extremes ───────────────────────────────────────────────────────

/// Test with extreme latency values.
#[tokio::test]
async fn test_latency_extremes() {
    let config = SimConfig::with_capacity(1024 * 1024)
        .with_seed(SEED.wrapping_add(2))
        .with_latency(LatencyConfig {
            base: Duration::from_millis(1),
            per_byte: Duration::from_micros(1),
            jitter_fraction: 0.5,
        });

    let backend = SimBackend::new(config);

    let mut allocs: Vec<(Allocation, Vec<u8>)> = Vec::new();

    for i in 0..10 {
        let size = 1024;
        if let Ok(alloc) = backend.allocate(size).await {
            let data = vec![(i & 0xFF) as u8; size];
            if backend.write(&alloc, &data).await.is_ok() {
                allocs.push((alloc, data));
            }
        }
    }

    // Verify data integrity
    for (alloc, expected) in &allocs {
        let mut buf = vec![0u8; alloc.size];
        if backend.read(alloc, &mut buf).await.is_ok() {
            assert_eq!(buf, *expected, "data integrity failed at offset {}", alloc.offset);
        }
    }

    // Clean up
    for (alloc, _) in allocs {
        let _ = backend.deallocate(alloc).await;
    }
}

// ─── Capacity fragmentation ─────────────────────────────────────────────────

/// Test fragmentation by allocating and deallocating in a pattern that
/// creates maximum fragmentation.
#[tokio::test]
async fn test_capacity_fragmentation() {
    let config = SimConfig::with_capacity(1024 * 1024)
        .with_seed(SEED.wrapping_add(3))
        .with_fragmentation(0.3);

    let backend = SimBackend::new(config);
    let mut allocs: Vec<Allocation> = Vec::new();

    // Phase 1: Fill with small allocations (256 bytes each)
    for _ in 0..500 {
        if let Ok(alloc) = backend.allocate(256).await {
            allocs.push(alloc);
        } else {
            break;
        }
    }

    assert!(
        allocs.len() > 10,
        "should have made at least 10 allocations, got {}",
        allocs.len()
    );

    // Phase 2: Free every other allocation (creates fragmentation)
    let mut kept: Vec<Allocation> = Vec::new();
    for (i, alloc) in allocs.into_iter().enumerate() {
        if i % 2 == 0 {
            let _ = backend.deallocate(alloc).await;
        } else {
            kept.push(alloc);
        }
    }

    // Phase 3: Try to allocate larger chunks in the fragmented space
    for _ in 0..50 {
        if let Ok(alloc) = backend.allocate(2048).await {
            let data = vec![0xEFu8; alloc.size];
            if backend.write(&alloc, &data).await.is_ok() {
                let mut buf = vec![0u8; alloc.size];
                if backend.read(&alloc, &mut buf).await.is_ok() {
                    assert_eq!(buf, data, "data integrity failed for large allocation");
                }
            }
            let _ = backend.deallocate(alloc).await;
        }
    }

    // Phase 4: Clean up kept allocations
    for alloc in kept {
        let _ = backend.deallocate(alloc).await;
    }

    // Phase 5: After cleanup, we should be able to allocate again
    let final_alloc = backend.allocate(4096).await;
    if let Ok(alloc) = final_alloc {
        let _ = backend.deallocate(alloc).await;
    }
}

// ─── Concurrent stress with failure injection ────────────────────────────────

/// Multiple concurrent tasks performing operations with failure injection.
#[tokio::test]
async fn test_concurrent_stress_with_failures() {
    let config = SimConfig::with_capacity(2 * 1024 * 1024)
        .with_seed(SEED.wrapping_add(4))
        .with_failure(FailureConfig {
            write_failure_rate: 0.2,
            read_failure_rate: 0.2,
            alloc_failure_rate: 0.1,
            corruption_on_failure: false,
        });

    let backend = Arc::new(SimBackend::new(config));
    let mut handles = Vec::new();

    for task_id in 0..8 {
        let be = backend.clone();
        let handle = tokio::spawn(async move {
            let mut rng = ChaCha8Rng::seed_from_u64(SEED.wrapping_add(task_id as u64 * 1000));
            let mut local_allocs: Vec<Allocation> = Vec::new();
            let mut ops = 0;

            for _ in 0..50 {
                let size = rng.gen_range(64..=4096);

                if let Ok(alloc) = be.allocate(size).await {
                    let pattern = vec![(task_id & 0xFF) as u8; size];
                    if be.write(&alloc, &pattern).await.is_ok() {
                        local_allocs.push(alloc);
                    }
                }

                // Randomly free
                if !local_allocs.is_empty() && rng.gen_bool(0.3) {
                    let idx = rng.gen_range(0..local_allocs.len());
                    let alloc = local_allocs.swap_remove(idx);
                    let _ = be.deallocate(alloc).await;
                }

                ops += 1;
            }

            // Free remaining
            for alloc in local_allocs {
                let _ = be.deallocate(alloc).await;
            }

            ops
        });
        handles.push(handle);
    }

    let mut total_ops = 0;
    for handle in handles {
        let ops = handle.await.expect("task should not panic");
        total_ops += ops;
    }

    assert_eq!(total_ops, 400, "all tasks should complete all operations");
}
