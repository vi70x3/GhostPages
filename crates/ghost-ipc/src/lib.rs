//! IPC communication layer for GhostPages daemon.
//!
//! This crate provides the Unix socket-based IPC mechanism for
//! communication between the GhostPages daemon and CLI tools.
//!
//! # Architecture
//!
//! The IPC layer uses a simple binary protocol:
//! - 4 bytes: big-endian u32 = payload length
//! - N bytes: JSON-serialized request/response
//!
//! # Modules
//!
//! - [`protocol`]: IPC request/response message types
//! - [`frame`]: Length-prefixed framing for the wire protocol
//! - [`client`]: Async IPC client for connecting to the daemon

#![warn(missing_docs)]

/// IPC protocol message types.
pub mod protocol;

/// Length-prefixed framing.
pub mod frame;

/// IPC client.
pub mod client;

pub use client::{IpcClient, StatusResponse};
pub use frame::{read_frame, write_frame, MAX_FRAME_SIZE};
pub use protocol::{IpcErrorCode, IpcRequest, IpcResponse, TierInfo};
