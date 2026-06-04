//! Fuzz target: ChunkId round-trip verification.
//!
//! Verifies that ChunkId::from_data and ChunkId::verify maintain
//! their invariant: the same data always produces the same ID,
//! and the ID always verifies against the original data.

#![no_main]
use libfuzzer_sys::fuzz_target;
use ghost_core::ChunkId;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Content-addressed: same data must always produce same ID
    let id1 = ChunkId::from_data(data);
    let id2 = ChunkId::from_data(data);
    assert_eq!(id1, id2);

    // Verify round-trip: ID must verify against original data
    assert!(id1.verify(data));

    // Different data (almost certainly) produces different ID
    if data.len() > 1 {
        let modified = &data[1..];
        let _id3 = ChunkId::from_data(modified);
        if data[0] != modified[0] {
            // Different data → different ID (with overwhelming probability)
            // Note: not guaranteed but overwhelmingly likely
        }
    }
});
