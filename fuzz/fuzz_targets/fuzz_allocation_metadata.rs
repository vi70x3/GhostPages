//! Fuzz target: RamBackend allocation metadata.
//!
//! Feeds random allocation sizes and sequences to the RamBackend to find
//! panics, assertion violations, or memory corruption in the allocation
//! metadata tracking.

#![no_main]

use ghost_tier::backend::{Allocation, BackendData};
use ghost_tier::RamBackend;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }

    // First 4 bytes determine capacity (up to 16 MB)
    let capacity_bytes = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let capacity = (capacity_bytes % (16 * 1024 * 1024)).max(1024);

    let backend = RamBackend::new(capacity);
    let mut allocs: Vec<Allocation> = Vec::new();

    // Process remaining bytes as a sequence of commands:
    // - Byte value 0..127: allocate (size = value * 64 + 64)
    // - Byte value 128..255: deallocate (index = (value - 128) % max(live_allocs, 1))
    for &cmd in &data[4..] {
        if cmd < 128 {
            // Allocate
            let size = (cmd as usize) * 64 + 64; // 64 to 8192 bytes
            if let Ok(alloc) = backend.allocate(size) {
                // Write a pattern to detect corruption
                let pattern = vec![cmd.wrapping_add(0xA0); alloc.size];
                let _ = backend.write(&alloc, &pattern);
                allocs.push(alloc);
            }
        } else if !allocs.is_empty() {
            // Deallocate
            let idx = (cmd as usize - 128) % allocs.len();
            let alloc = allocs.swap_remove(idx);
            let _ = backend.deallocate(alloc);
        }
    }

    // Clean up remaining allocations
    for alloc in allocs {
        let _ = backend.deallocate(alloc);
    }
});
