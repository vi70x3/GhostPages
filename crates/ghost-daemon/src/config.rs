//! Daemon configuration for GhostPages.
//!
//! Skeleton implementation for Phase 0.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for the GhostPages daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    /// Path to the Unix socket for IPC.
    pub socket_path: PathBuf,

    /// RAM tier capacity in bytes.
    pub ram_capacity: usize,

    /// Whether to enable trace recording.
    pub trace_recording: bool,

    /// Path for trace output.
    pub trace_path: Option<PathBuf>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from("/tmp/ghostpages.sock"),
            ram_capacity: 512 * 1024 * 1024, // 512 MB
            trace_recording: false,
            trace_path: None,
        }
    }
}
