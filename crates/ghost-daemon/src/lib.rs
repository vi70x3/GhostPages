//! Main daemon process for GhostPages.
//!
//! This crate provides the daemon that manages memory tiers, serves
//! client requests, orchestrates the async transfer pipeline, and
//! enforces placement policies.
//!
//! # Phase 0 Status
//!
//! This is a skeleton implementation. Full daemon functionality
//! will be implemented in Phase 1.

#![warn(missing_docs)]

/// Daemon configuration.
pub mod config;

/// Core engine.
pub mod engine;

/// Async transfer pipeline.
pub mod pipeline;

pub use config::DaemonConfig;
pub use engine::Engine;
pub use pipeline::Pipeline;
