//! Trace recorder for GhostPages.
//!
//! Skeleton implementation for Phase 0. Full trace recording
//! will be implemented in Phase 5.

use crate::event::TraceEvent;
use std::path::Path;

/// Records trace events to a file.
#[derive(Debug)]
pub struct TraceRecorder {
    _path: std::path::PathBuf,
}

impl TraceRecorder {
    /// Create a new trace recorder that writes to the given path.
    pub fn new<P: AsRef<Path>>(_path: P) -> Result<Self, std::io::Error> {
        Ok(Self {
            _path: _path.as_ref().to_path_buf(),
        })
    }

    /// Record a trace event.
    pub async fn record(&self, _event: &TraceEvent) -> Result<(), std::io::Error> {
        // TODO: Implement in Phase 5
        Ok(())
    }
}
