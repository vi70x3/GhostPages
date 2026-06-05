//! Trace recording and replay system for GhostPages.
//!
//! This crate provides functionality to record migration events and replay
//! them for tuning, A/B testing, regression testing, and offline
//! experimentation.
//!
//! # Modules
//!
//! - [`format`]: Binary trace file format definitions
//! - [`writer`]: Trace file writer with CRC32 checksums
//! - [`reader`]: Trace file reader with validation
//! - [`engine`]: Replay engine with state machine validation
//! - [`metrics`]: Replay metrics collection and policy comparison
//! - [`export`]: Trace export to JSON, CSV, and JSON Lines

#![warn(missing_docs)]

/// Binary trace file format definitions.
pub mod format;

/// Trace file writer.
pub mod writer;

/// Trace file reader.
pub mod reader;

/// Replay engine.
pub mod engine;

/// Replay metrics and policy comparison.
pub mod metrics;

/// Trace export formats.
pub mod export;

// Re-exports for convenience
pub use engine::{ReplayConfig, ReplayEngine, ReplaySummary, ReplayValidationError};
pub use export::{export_trace, ExportFormat};
pub use format::{TraceFileHeader, TraceMetadata, TraceRecord};
pub use metrics::{compare_traces, ComparisonWinner, PolicyComparison, ReplayMetrics};
pub use reader::TraceReader;
pub use writer::TraceWriter;
