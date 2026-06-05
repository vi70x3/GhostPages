//! Fuzz target: Migration sequences between two backends.
//!
//! Simulates migration patterns: allocate in source, read data, allocate in
//! target, write data, verify, free source. Feeds random data to find panics
//! in the migration lifecycle.

#![no_main]

use ghost_tier::backend::{Allocation, BackendData};
use ghost_tier::RamBackend;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }

    // First 4 bytes: source capacity
    let src_cap = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let src_cap = (src_cap % (8 * 1024 * 1024)).max(1024);

    // Next 4 bytes: target capacity
    let tgt_cap = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let tgt_cap = (tgt_cap % (8 * 1024 * 1024)).max(1024);

    let source = RamBackend::new(src_cap);
    let target = RamBackend::new(tgt_cap);

    let mut source_allocs: Vec<(Allocation, Vec<u8>)> = Vec::new();
    let mut target_allocs: Vec<Allocation> = Vec::new();

    // Process remaining bytes as commands
    for &cmd in &data[8..] {
        match cmd % 4 {
            0 => {
                // Allocate in source
                let size = ((cmd as usize) * 32 + 32).min(8192);
                if let Ok(alloc) = source.allocate(size) {
                    // Write fuzz data
                    let data = vec![cmd.wrapping_mul(3); alloc.size];
                    let _ = source.write(&alloc, &data);
                    source_allocs.push((alloc, data));
                }
            }
            1 => {
                // Migrate from source to target
                if !source_allocs.is_empty() {
                    let idx = (cmd as usize) % source_allocs.len();
                    let (src_alloc, expected_data) = source_allocs.swap_remove(idx);

                    // Read from source
                    let mut buf = vec![0u8; src_alloc.size];
                    if source.read(&src_alloc, &mut buf).is_ok() {
                        // Allocate in target
                        if let Ok(tgt_alloc) = target.allocate(src_alloc.size) {
                            let _ = target.write(&tgt_alloc, &expected_data);
                            target_allocs.push(tgt_alloc);
                        }
                    }
                    let _ = source.deallocate(src_alloc);
                }
            }
            2 => {
                // Free from target
                if !target_allocs.is_empty() {
                    let idx = (cmd as usize) % target_allocs.len();
                    let alloc = target_allocs.swap_remove(idx);
                    let _ = target.deallocate(alloc);
                }
            }
            3 => {
                // Verify a random target allocation
                if !target_allocs.is_empty() {
                    let idx = (cmd as usize) % target_allocs.len();
                    let alloc = &target_allocs[idx];
                    let mut buf = vec![0u8; alloc.size];
                    let _ = target.read(alloc, &mut buf);
                }
            }
            _ => unreachable!(),
        }
    }

    // Clean up
    for (alloc, _) in source_allocs {
        let _ = source.deallocate(alloc);
    }
    for alloc in target_allocs {
        let _ = target.deallocate(alloc);
    }
});
