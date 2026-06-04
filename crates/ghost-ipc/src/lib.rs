//! IPC communication layer for GhostPages daemon.
//!
//! This crate provides the Unix socket-based IPC mechanism for
//! communication between the GhostPages daemon and CLI tools.
//!
//! # Phase 0 Status
//!
//! This is a skeleton implementation. Full IPC protocol, shared memory
//! transport, and message types will be implemented in Phase 3.

#![warn(missing_docs)]

/// IPC protocol message types.
pub mod protocol;

/// IPC server.
pub mod server;

/// IPC client.
pub mod client;

pub use protocol::Message;
pub use server::IpcServer;
pub use client::IpcClient;
