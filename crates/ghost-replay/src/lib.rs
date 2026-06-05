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
//! - [`checksum`]: Deterministic blake3-based checksums over event streams
//! - [`divergence`]: Divergence detection between baseline and candidate streams
//! - [`invariants`]: Trait-based invariant validation system
//! - [`verifier`]: Replay verification harness for determinism testing

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

/// Deterministic checksum engine for replay validation.
pub mod checksum;

/// Divergence detection for replay validation.
pub mod divergence;

/// Replay invariant validation system.
pub mod invariants;

/// Replay verification harness.
pub mod verifier;

// Re-exports for convenience
pub use checksum::{from_events, from_file, hash_event, hash_events, EventHash, HashCategory, ReplayChecksum};
pub use divergence::{detect_divergence, DivergenceReport, DivergenceType};
pub use engine::{ReplayConfig, ReplayEngine, ReplaySummary, ReplayValidationError};
pub use export::{export_trace, ExportFormat};
pub use format::{TraceFileHeader, TraceMetadata, TraceRecord};
pub use invariants::{InvariantValidator, InvariantViolation, ReplayInvariant, ViolationSeverity};
pub use metrics::{compare_traces, ComparisonWinner, PolicyComparison, ReplayMetrics};
pub use reader::TraceReader;
pub use verifier::{VerifierConfig, VerificationResult, ReplayVerifier};
pub use writer::TraceWriter;
