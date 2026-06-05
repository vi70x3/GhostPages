//! Fuzz target: IPC frame parsing.
//!
//! Feeds random bytes to the IPC frame reader to find panics, buffer overflows,
//! or other issues in the length-prefixed framing protocol.

#![no_main]

use ghost_ipc::frame::read_frame;
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    // Create an async runtime for the async read_frame function
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async {
        let mut cursor = Cursor::new(data.to_vec());
        // We expect this to either succeed with valid data or return an error.
        // The key property is: no panics, no buffer overflows, no infinite loops.
        let _result = read_frame(&mut cursor).await;
    });
});
