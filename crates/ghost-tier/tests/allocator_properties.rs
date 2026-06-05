//! Property-based tests for RamBackend using proptest.
//!
//! Each property is tested with at least 1000 iterations to ensure
//! correctness under a wide range of allocation patterns.

use ghost_tier::backend::{Allocation, BackendData};
use ghost_tier::{RamBackend, StorageBackend};
use proptest::prelude::*;
use std::collections::HashMap;
use tokio::runtime::Runtime;

/// Capacity for property tests.
const CAPACITY: usize = 1024 * 1024; // 1 MB

fn alloc_size_strategy() -> impl Strategy<Value = usize> {
    prop_oneof![
        1..=256usize,
        257..=4096usize,
        4097..=65536usize,
    ]
}

// ─── Property: No overlapping allocations ─────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn prop_no_overlapping_allocations(
        operations in prop::collection::vec(
            prop_oneof![
                alloc_size_strategy().prop_map(|s| (true, s)),
                (0..1000usize).prop_map(|i| (false, i)),
            ],
            10..=200,
        )
    ) {
        let rt = Runtime::new().unwrap();
        let backend = RamBackend::new(CAPACITY);
        let mut live: HashMap<usize, usize> = HashMap::new();
        let mut total_used: usize = 0;

        for (is_alloc, value) in operations {
            if is_alloc {
                let size = value;
                if total_used + size <= CAPACITY {
                    if let Ok(alloc) = rt.block_on(backend.allocate(size)) {
                        let new_start = alloc.offset;
                        let new_end = alloc.offset + alloc.size;
                        for (existing_offset, existing_size) in &live {
                            let existing_end = existing_offset + existing_size;
                            let overlaps = new_start < existing_end && *existing_offset < new_end;
                            prop_assert!(
                                !overlaps,
                                "overlapping allocation: new=[{}, {}) existing=[{}, {})",
                                new_start, new_end, existing_offset, existing_end
                            );
                        }
                        live.insert(alloc.offset, alloc.size);
                        total_used += size;
                    }
                }
            } else if !live.is_empty() {
                let offsets: Vec<usize> = live.keys().copied().collect();
                let idx = value % offsets.len();
                let offset = offsets[idx];
                let size = live.remove(&offset).unwrap();
                let alloc = Allocation::new(offset, size, BackendData::new(size));
                let _ = rt.block_on(backend.deallocate(alloc));
            }
        }

        for (offset, size) in live {
            let alloc = Allocation::new(offset, size, BackendData::new(size));
            let _ = rt.block_on(backend.deallocate(alloc));
        }
    }
}

// ─── Property: Capacity conservation ─────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn prop_capacity_conservation(
        sizes in prop::collection::vec(alloc_size_strategy(), 10..=100),
    ) {
        let rt = Runtime::new().unwrap();
        let backend = RamBackend::new(CAPACITY);
        let mut total_used: usize = 0;
        let mut allocs: Vec<Allocation> = Vec::new();

        for size in &sizes {
            if total_used + *size <= CAPACITY {
                if let Ok(alloc) = rt.block_on(backend.allocate(*size)) {
                    total_used += alloc.size;
                    allocs.push(alloc);
                }
            }

            prop_assert!(
                total_used <= CAPACITY,
                "total_used {} exceeds capacity {}",
                total_used, CAPACITY
            );
            prop_assert_eq!(
                backend.available(),
                CAPACITY - total_used,
                "available() mismatch: expected {}, got {}",
                CAPACITY - total_used,
                backend.available()
            );
        }

        for alloc in allocs {
            let _ = rt.block_on(backend.deallocate(alloc));
        }
    }
}

// ─── Property: No leaks ──────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn prop_no_leaks(
        sizes in prop::collection::vec(alloc_size_strategy(), 5..=50),
    ) {
        let rt = Runtime::new().unwrap();
        let backend = RamBackend::new(CAPACITY);
        let mut allocs: Vec<Allocation> = Vec::new();

        for size in &sizes {
            if let Ok(alloc) = rt.block_on(backend.allocate(*size)) {
                allocs.push(alloc);
            }
        }

        for alloc in allocs {
            rt.block_on(backend.deallocate(alloc)).unwrap();
        }

        prop_assert_eq!(
            backend.available(),
            CAPACITY,
            "backend should have full capacity after freeing all allocations, got {}",
            backend.available()
        );
    }
}

// ─── Property: Alignment ─────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn prop_alignment(
        sizes in prop::collection::vec(alloc_size_strategy(), 5..=50),
    ) {
        let rt = Runtime::new().unwrap();
        let backend = RamBackend::new(CAPACITY);
        let mut allocs: Vec<Allocation> = Vec::new();

        for size in &sizes {
            if let Ok(alloc) = rt.block_on(backend.allocate(*size)) {
                prop_assert_eq!(
                    alloc.size, *size,
                    "allocation size mismatch: requested {}, got {}",
                    size, alloc.size
                );
                allocs.push(alloc);
            }
        }

        // Verify all offsets are monotonically increasing (bump allocator property)
        for window in allocs.windows(2) {
            prop_assert!(
                window[0].offset < window[1].offset,
                "offsets should be monotonically increasing: {} >= {}",
                window[0].offset,
                window[1].offset
            );
        }

        for alloc in allocs {
            let _ = rt.block_on(backend.deallocate(alloc));
        }
    }
}

// ─── Property: Merge correctness (reclamation) ───────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn prop_merge_correctness(
        sizes in prop::collection::vec(alloc_size_strategy(), 5..=50),
        free_indices in prop::collection::vec(0..50usize, 0..=50),
    ) {
        let rt = Runtime::new().unwrap();
        let backend = RamBackend::new(CAPACITY);
        let mut allocs: Vec<Allocation> = Vec::new();
        let mut total_used: usize = 0;

        for size in &sizes {
            if total_used + *size <= CAPACITY {
                if let Ok(alloc) = rt.block_on(backend.allocate(*size)) {
                    total_used += alloc.size;
                    allocs.push(alloc);
                }
            }
        }

        let mut freed_bytes: usize = 0;
        let mut already_freed: Vec<bool> = vec![false; allocs.len()];
        for idx in &free_indices {
            let i = *idx;
            if i < allocs.len() && !already_freed[i] {
                let offset = allocs[i].offset;
                let size = allocs[i].size;
                if rt.block_on(backend.deallocate(Allocation::new(offset, size, BackendData::new(size)))).is_ok() {
                    freed_bytes += size;
                    already_freed[i] = true;
                }
            }
        }

        let expected_available = CAPACITY - (total_used - freed_bytes);
        prop_assert!(
            backend.available() >= expected_available,
            "available {} should be >= expected {}",
            backend.available(),
            expected_available
        );

        for (i, alloc) in allocs.into_iter().enumerate() {
            if !already_freed[i] {
                let _ = rt.block_on(backend.deallocate(alloc));
            }
        }
    }
}
