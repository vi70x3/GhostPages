//! CLI tools for interacting with the GhostPages daemon.
//!
//! This crate provides command-line tools for:
//! - Starting/stopping the daemon
//! - Storing, retrieving, and deleting data
//! - Querying status and metrics
//! - Running trace replays
//! - Configuring simulation parameters
//!
//! # Phase 0 Status
//!
//! This is a skeleton implementation. Full CLI functionality
//! will be implemented in Phase 6.

#![warn(missing_docs)]

/// CLI commands.
pub mod commands;

pub use commands::Cli;
