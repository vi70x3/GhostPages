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

/// Backend health tracking.
pub mod health;

/// Retry configuration with bounded backoff.
pub mod retry;

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

/// IPC server.
pub mod ipc_server;

/// Hotness tracking for access pattern analysis.
pub mod hotness_tracker;

/// Migration engine for pressure-driven chunk migration.
pub mod migration;

/// Backpressure controller for overload management.
pub mod backpressure;

/// I/O metrics tracking for disk performance and congestion.
pub mod io_metrics;

/// Diagnostic snapshot and builder.
pub mod diagnostics;

/// Prometheus metrics HTTP exporter.
pub mod exporter;

/// Transfer worker pool for dedicated transfer operations.
pub mod transfer_worker;

pub use config::{BackpressureConfig, HealthConfig, MetricsExporterConfig, MigrationConfig, OrchestratorConfig, RetryConfig, TransferWorkerPoolConfig};
pub use engine::Engine;
pub use health::{BackendHealth, HealthTracker};
pub use io_metrics::IoMetrics;
pub use ipc_server::{IpcServer, IpcServerConfig};
pub use migration::{MigrationEngine, MigrationStats, PendingMigration};
pub use orchestrator::TransferOrchestrator;
pub use pipeline::Pipeline;
pub use retry::RetryConfig as RetryConfigType;
pub use transfer_worker::{TransferCompletion, TransferTask, TransferWorkerPool};
