//! Trace replayer for GhostPages.
//!
//! Skeleton implementation for Phase 0. Full trace replay
//! will be implemented in Phase 5.

use crate::event::TraceEvent;
use std::path::Path;

/// Replays recorded trace events.
#[derive(Debug)]
pub struct TraceReplayer {
    _path: std::path::PathBuf,
}

impl TraceReplayer {
    /// Create a new trace replayer that reads from the given path.
    pub fn new<P: AsRef<Path>>(_path: P) -> Result<Self, std::io::Error> {
        Ok(Self {
            _path: _path.as_ref().to_path_buf(),
        })
    }

    /// Load all events from the trace file.
    pub async fn load_events(&self) -> Result<Vec<TraceEvent>, std::io::Error> {
        // TODO: Implement in Phase 5
        Ok(Vec::new())
    }

    /// Replay events at the given speed multiplier.
    pub async fn replay(
        &self,
        _speed: f64,
    ) -> Result<(), std::io::Error> {
        // TODO: Implement in Phase 5
        Ok(())
    }
}
