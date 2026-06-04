//! IPC client for GhostPages.
//!
//! Skeleton implementation for Phase 0. Full IPC client
//! will be implemented in Phase 3.

use crate::protocol::Message;
use std::path::Path;

/// IPC client for connecting to the GhostPages daemon.
#[derive(Debug)]
pub struct IpcClient {
    _socket_path: std::path::PathBuf,
}

impl IpcClient {
    /// Connect to the daemon at the given socket path.
    pub async fn connect<P: AsRef<Path>>(_socket_path: P) -> Result<Self, std::io::Error> {
        Ok(Self {
            _socket_path: _socket_path.as_ref().to_path_buf(),
        })
    }

    /// Send a message to the daemon.
    pub async fn send(&self, _message: &Message) -> Result<(), std::io::Error> {
        // TODO: Implement in Phase 3
        Ok(())
    }

    /// Send a message and wait for a response.
    pub async fn request(&self, _message: &Message) -> Result<Message, std::io::Error> {
        // TODO: Implement in Phase 3
        Ok(Message::Error {
            message: "not implemented".to_string(),
        })
    }
}
