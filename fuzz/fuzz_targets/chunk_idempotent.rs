//! Fuzz target: ChunkId idempotency.
//!
//! Verifies that hashing the same data multiple times always
//! produces the same ChunkId.

#![no_main]
use libfuzzer_sys::fuzz_target;
use ghost_core::ChunkId;

fuzz_target!(|data: &[u8]| {
    // Hash the same data 10 times — must always get the same result
    let first = ChunkId::from_data(data);
    for _ in 0..10 {
        let next = ChunkId::from_data(data);
        assert_eq!(first, next);
    }

    // Verify against original data
    assert!(first.verify(data));
});
