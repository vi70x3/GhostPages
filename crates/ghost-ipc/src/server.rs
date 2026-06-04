//! IPC server for GhostPages daemon.
//!
//! Skeleton implementation for Phase 0. Full Unix socket server
//! will be implemented in Phase 3.

use std::path::Path;

/// IPC server that listens for client connections.
#[derive(Debug)]
pub struct IpcServer {
    _socket_path: std::path::PathBuf,
}

impl IpcServer {
    /// Create a new IPC server bound to the given socket path.
    pub fn new<P: AsRef<Path>>(_socket_path: P) -> Self {
        Self {
            _socket_path: _socket_path.as_ref().to_path_buf(),
        }
    }

    /// Start listening for client connections.
    pub async fn listen(&self) -> Result<(), std::io::Error> {
        // TODO: Implement in Phase 3
        Ok(())
    }

    /// Shutdown the server gracefully.
    pub async fn shutdown(&self) -> Result<(), std::io::Error> {
        // TODO: Implement in Phase 3
        Ok(())
    }
}
