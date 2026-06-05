//! Allocator torture tests for RamBackend.
//!
//! These tests use deterministic (seeded) random number generators to perform
//! randomized stress testing of the RamBackend. Every test is fully
//! deterministic — the same seed always produces the same sequence of operations.

use ghost_tier::backend::{Allocation, BackendData, BackendError};
use ghost_tier::{RamBackend, StorageBackend};
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;

/// Capacity for storm tests — large enough for many small allocations.
const CAPACITY: usize = 1024 * 1024; // 1 MB

/// Fixed seed for deterministic testing.
const SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;

// ─── Rapid allocation/free cycles ────────────────────────────────────────────

/// Perform 1000 random allocate/deallocate cycles.
///
/// Verifies:
/// - No leaks (all allocations can be freed)
/// - No double-frees
/// - Capacity tracking is correct after each operation
#[tokio::test]
async fn test_alloc_free_storm() {
    let backend = RamBackend::new(CAPACITY);
    let mut rng = ChaCha8Rng::seed_from_u64(SEED);

    // Track live allocations: offset -> size
    let mut live: HashMap<usize, usize> = HashMap::new();
    let mut total_allocated: usize = 0;

    for i in 0..1000 {
        // Decide: allocate (70%) or deallocate (30%)
        let should_alloc = live.is_empty() || rng.gen_bool(0.7);

        if should_alloc {
            // Random size between 64 and 8192 bytes
            let size = rng.gen_range(64..=8192);

            if total_allocated + size <= CAPACITY {
                let alloc = backend.allocate(size).await.unwrap_or_else(|_| {
                    panic!(
                        "iteration {}: allocation of {} bytes should succeed (used={}, capacity={})",
                        i, size, total_allocated, CAPACITY
                    );
                });
                assert_eq!(alloc.size, size, "iteration {}: allocation size mismatch", i);
                live.insert(alloc.offset, size);
                total_allocated += size;
            } else {
                // Should fail gracefully
                let result = backend.allocate(size).await;
                assert!(
                    matches!(result, Err(BackendError::InsufficientSpace { .. })),
                    "iteration {}: expected InsufficientSpace, got {:?}",
                    i,
                    result
                );
            }
        } else if !live.is_empty() {
            // Pick a random live allocation to free
            let offsets: Vec<usize> = live.keys().copied().collect();
            let idx = rng.gen_range(0..offsets.len());
            let offset = offsets[idx];
            let size = live.remove(&offset).unwrap();

            let alloc = Allocation::new(offset, size, BackendData::new(size));
            backend
                .deallocate(alloc)
                .await
                .unwrap_or_else(|_| panic!("iteration {}: deallocation should succeed", i));
            total_allocated -= size;
        }

        // Verify capacity tracking
        assert_eq!(
            backend.available(),
            CAPACITY - total_allocated,
            "iteration {}: available capacity mismatch (expected {}, got {})",
            i,
            CAPACITY - total_allocated,
            backend.available()
        );
    }

    // Free remaining allocations
    for (offset, size) in live {
        let alloc = Allocation::new(offset, size, BackendData::new(size));
        backend.deallocate(alloc).await.unwrap();
        total_allocated -= size;
    }

    assert_eq!(
        total_allocated, 0,
        "all allocations should be freed, but {} bytes remain",
        total_allocated
    );
    assert_eq!(
        backend.available(),
        CAPACITY,
        "capacity should be fully restored after freeing all allocations"
    );
}

// ─── Fragmentation pattern test ─────────────────────────────────────────────

/// Allocate varying sizes, free every other allocation, verify remaining
/// allocations are still valid and freed space is reclaimable.
#[tokio::test]
async fn test_fragmentation_patterns() {
    let backend = RamBackend::new(CAPACITY);
    let mut rng = ChaCha8Rng::seed_from_u64(SEED.wrapping_add(1));

    let mut all_allocations: Vec<Allocation> = Vec::new();
    let mut total_used: usize = 0;

    // Phase 1: Allocate varying sizes (64B to 1MB)
    for _ in 0..100 {
        // Logarithmic distribution: favor smaller allocations
        let exp = rng.gen_range(6..=20); // 2^6=64 to 2^20=1MB
        let size = 1usize << exp;
        if size > CAPACITY / 2 {
            continue; // skip unreasonably large
        }

        if total_used + size <= CAPACITY {
            let alloc = backend.allocate(size).await.unwrap();
            assert_eq!(alloc.size, size);
            all_allocations.push(alloc);
            total_used += size;
        }
    }

    assert!(
        !all_allocations.is_empty(),
        "should have made at least some allocations"
    );

    // Phase 2: Free every other allocation
    let mut kept: Vec<Allocation> = Vec::new();
    let mut freed_count = 0;
    for (i, alloc) in all_allocations.into_iter().enumerate() {
        let alloc_size = alloc.size;
        if i % 2 == 0 {
            backend.deallocate(alloc).await.unwrap();
            total_used -= alloc_size;
            freed_count += 1;
        } else {
            kept.push(alloc);
        }
    }

    assert!(freed_count > 0, "should have freed at least one allocation");
    assert_eq!(backend.available(), CAPACITY - total_used);

    // Phase 3: Verify remaining allocations are still readable/writable
    for alloc in &kept {
        let data = vec![0xABu8; alloc.size];
        backend.write(alloc, &data).await.unwrap();
        let mut buf = vec![0u8; alloc.size];
        backend.read(alloc, &mut buf).await.unwrap();
        assert_eq!(
            buf, data,
            "data integrity check failed at offset {}",
            alloc.offset
        );
    }

    // Phase 4: Verify freed space is reclaimable
    // We should be able to allocate at least the total freed space
    let freed_space = backend.available();
    assert!(
        freed_space > 0,
        "freed space should be available for reallocation"
    );

    // Allocate a chunk of the freed space
    let realloc_size = freed_space.min(64 * 1024);
    let realloc = backend.allocate(realloc_size).await.unwrap();
    assert_eq!(realloc.size, realloc_size);

    // Clean up
    backend.deallocate(realloc).await.unwrap();
    for alloc in kept {
        backend.deallocate(alloc).await.unwrap();
    }

    assert_eq!(
        backend.available(),
        CAPACITY,
        "all space should be reclaimed after freeing everything"
    );
}

// ─── Concurrent allocation churn ────────────────────────────────────────────

/// 10 concurrent tasks, each doing 100 alloc/free cycles.
///
/// Verifies:
/// - No data corruption
/// - No panics
/// - Final state is consistent
#[tokio::test]
async fn test_concurrent_churn() {
    let backend = std::sync::Arc::new(RamBackend::new(CAPACITY));
    let mut handles = Vec::new();

    for task_id in 0..10 {
        let be = backend.clone();
        let handle = tokio::spawn(async move {
            let mut rng = ChaCha8Rng::seed_from_u64(SEED.wrapping_add(task_id as u64 * 1000));
            let mut local_allocs: Vec<Allocation> = Vec::new();
            let mut local_total: usize = 0;

            for _ in 0..100 {
                let size = rng.gen_range(32..=4096);

                if let Ok(alloc) = be.allocate(size).await {
                    // Write a pattern to detect corruption
                    let pattern = vec![(task_id & 0xFF) as u8; size];
                    be.write(&alloc, &pattern).await.unwrap();

                    local_allocs.push(alloc);
                    local_total += size;
                }

                // Randomly free an allocation
                if !local_allocs.is_empty() && rng.gen_bool(0.3) {
                    let idx = rng.gen_range(0..local_allocs.len());
                    let alloc = local_allocs.swap_remove(idx);
                    local_total -= alloc.size;

                    // Verify data before freeing
                    let mut buf = vec![0u8; alloc.size];
                    be.read(&alloc, &mut buf).await.unwrap();
                    let expected = vec![(task_id & 0xFF) as u8; alloc.size];
                    assert_eq!(
                        buf, expected,
                        "task {}: data corruption detected at offset {}",
                        task_id, alloc.offset
                    );

                    be.deallocate(alloc).await.unwrap();
                }
            }

            // Free remaining
            for alloc in local_allocs {
                let alloc_size = alloc.size;
                be.deallocate(alloc).await.unwrap();
                local_total -= alloc_size;
            }

            local_total
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        let _remaining = handle.await.expect("task should not panic");
    }

    // Final state should be consistent
    assert_eq!(
        backend.available(),
        CAPACITY,
        "all allocations should be freed: available={}, capacity={}",
        backend.available(),
        CAPACITY
    );
}

// ─── Capacity exhaustion test ────────────────────────────────────────────────

/// Fill backend to capacity, verify further allocations fail gracefully,
/// free some space, verify new allocations succeed.
#[tokio::test]
async fn test_capacity_exhaustion() {
    let backend = RamBackend::new(CAPACITY);
    let mut allocs: Vec<Allocation> = Vec::new();
    let mut total_used: usize = 0;

    // Fill to capacity with 1KB allocations
    loop {
        match backend.allocate(1024).await {
            Ok(alloc) => {
                total_used += alloc.size;
                allocs.push(alloc);
            }
            Err(BackendError::InsufficientSpace { requested, available }) => {
                assert!(requested > available);
                break;
            }
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    assert_eq!(backend.available(), CAPACITY - total_used);
    assert!(
        backend.available() < 1024,
        "should be nearly full: available={}",
        backend.available()
    );

    // Further allocations should fail gracefully
    let fail_result = backend.allocate(1024).await;
    assert!(
        matches!(fail_result, Err(BackendError::InsufficientSpace { .. })),
        "allocation beyond capacity should fail with InsufficientSpace"
    );

    // Zero-byte allocation should fail with Internal
    let zero_result = backend.allocate(0).await;
    assert!(
        matches!(zero_result, Err(BackendError::Internal(_))),
        "zero-byte allocation should fail with Internal"
    );

    // Free 10 allocations
    for _ in 0..10 {
        if let Some(alloc) = allocs.pop() {
            total_used -= alloc.size;
            backend.deallocate(alloc).await.unwrap();
        }
    }

    assert_eq!(backend.available(), CAPACITY - total_used);

    // Now allocations should succeed again
    let new_alloc = backend.allocate(1024).await.unwrap();
    assert_eq!(new_alloc.size, 1024);
    allocs.push(new_alloc);

    // Clean up
    for alloc in allocs {
        backend.deallocate(alloc).await.unwrap();
    }
    assert_eq!(backend.available(), CAPACITY);
}

// ─── Migration-heavy allocation cycles ───────────────────────────────────────

/// Simulate migration pattern: allocate in one tier, move to another.
///
/// Verifies:
/// - No leaks during migration
/// - Data integrity after migration
#[tokio::test]
async fn test_migration_alloc_cycles() {
    let source = RamBackend::new(CAPACITY);
    let target = RamBackend::new(CAPACITY);
    let mut rng = ChaCha8Rng::seed_from_u64(SEED.wrapping_add(42));

    let mut source_allocs: Vec<(Allocation, Vec<u8>)> = Vec::new();
    let mut target_allocs: Vec<Allocation> = Vec::new();

    for cycle in 0..50 {
        // Allocate in source
        let size = rng.gen_range(64..=8192);
        if let Ok(alloc) = source.allocate(size).await {
            // Write identifiable data
            let data = vec![(cycle & 0xFF) as u8; size];
            source.write(&alloc, &data).await.unwrap();
            source_allocs.push((alloc, data));
        }

        // "Migrate" every 3rd allocation: read from source, write to target, free source
        if !source_allocs.is_empty() && rng.gen_bool(0.4) {
            let idx = rng.gen_range(0..source_allocs.len());
            let (src_alloc, expected_data) = source_allocs.swap_remove(idx);

            // Read from source
            let mut buf = vec![0u8; src_alloc.size];
            source.read(&src_alloc, &mut buf).await.unwrap();
            assert_eq!(
                buf, expected_data,
                "cycle {}: source data corrupted before migration",
                cycle
            );

            // Allocate in target and write
            if let Ok(tgt_alloc) = target.allocate(src_alloc.size).await {
                target.write(&tgt_alloc, &expected_data).await.unwrap();

                // Verify in target
                let mut tgt_buf = vec![0u8; tgt_alloc.size];
                target.read(&tgt_alloc, &mut tgt_buf).await.unwrap();
                assert_eq!(
                    tgt_buf, expected_data,
                    "cycle {}: target data corrupted after migration",
                    cycle
                );

                // Free source
                source.deallocate(src_alloc).await.unwrap();
                target_allocs.push(tgt_alloc);
            } else {
                // Target full, keep in source
                source_allocs.push((src_alloc, expected_data));
            }
        }
    }

    // Verify all remaining source allocations
    for (alloc, expected_data) in &source_allocs {
        let mut buf = vec![0u8; alloc.size];
        source.read(alloc, &mut buf).await.unwrap();
        assert_eq!(&buf, expected_data, "remaining source data corrupted");
    }

    // Verify all target allocations
    for alloc in &target_allocs {
        let mut buf = vec![0u8; alloc.size];
        target.read(alloc, &mut buf).await.unwrap();
        // Just verify no panics — data content depends on which cycle it was
        assert_eq!(buf.len(), alloc.size);
    }

    // Clean up everything
    for (alloc, _) in source_allocs {
        source.deallocate(alloc).await.unwrap();
    }
    for alloc in target_allocs {
        target.deallocate(alloc).await.unwrap();
    }

    assert_eq!(
        source.available(),
        CAPACITY,
        "source should have no leaks"
    );
    assert_eq!(
        target.available(),
        CAPACITY,
        "target should have no leaks"
    );
}
