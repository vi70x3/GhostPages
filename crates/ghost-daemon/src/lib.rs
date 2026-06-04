//! Main daemon process for GhostPages.
//!
//! This crate provides the daemon that manages memory tiers, serves
//! client requests, orchestrates the async transfer pipeline, and
//! enforces placement policies.

#![warn(missing_docs)]

/// Daemon configuration.
pub mod config;

/// Core engine.
pub mod engine;

/// Async transfer pipeline.
pub mod pipeline;

/// Transfer metrics.
pub mod metrics;

/// Transfer queue.
pub mod queue;

/// Transfer scheduler.
pub mod scheduler;

/// Append-only trace log.
pub mod trace_log;

/// Worker pool.
pub mod worker;

/// Transfer orchestrator.
pub mod orchestrator;

/// Pressure monitoring and history.
pub mod pressure;

pub use config::OrchestratorConfig;
pub use engine::Engine;
pub use orchestrator::TransferOrchestrator;
pub use pipeline::Pipeline;
