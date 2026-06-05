//! Fuzz target: Replay trace record parsing.
//!
//! Feeds random bytes to the trace file format reader to find panics,
//! buffer overflows, or other issues in the binary deserialization.

#![no_main]

use ghost_replay::format::TraceFileHeader;
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    // Try to parse as a trace file header
    let mut cursor = Cursor::new(data.to_vec());
    let _result = TraceFileHeader::read_from(&mut cursor);

    // If we have enough data, also try to parse the remainder as records
    if data.len() > 32 {
        let mut cursor = Cursor::new(data[32..].to_vec());
        // Try reading records until EOF or error
        for _ in 0..100 {
            match ghost_replay::format::TraceRecord::read_from(&mut cursor) {
                Ok(Some(_)) => continue,
                Ok(None) => break, // EOF
                Err(_) => break,   // Parse error
            }
        }
    }
});
