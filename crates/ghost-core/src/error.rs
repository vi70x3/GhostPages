//! Error types for GhostPages.
//!
//! This module defines the unified error type used throughout the system.

use thiserror::Error;

/// Unified error type for GhostPages operations.
///
/// Each variant corresponds to a specific subsystem or failure mode,
/// enabling precise error handling and reporting.
#[derive(Debug, Error)]
pub enum GhostError {
    /// Chunk not found in any tier.
    #[error("chunk not found: {0}")]
    ChunkNotFound(String),

    /// Specified memory tier is at capacity.
    #[error("tier {0:?} is full")]
    TierFull(TierId),

    /// Specified memory tier is unavailable.
    #[error("tier {0:?} unavailable")]
    TierUnavailable(TierId),

    /// Data integrity check failed (checksum mismatch).
    #[error("checksum mismatch for chunk {0}")]
    ChecksumMismatch(String),

    /// Data corruption detected during read or write.
    #[error("corruption detected in {0}")]
    CorruptionDetected(String),

    /// Compression or decompression failed.
    #[error("compression error: {0}")]
    CompressionError(String),

    /// Storage backend operation failed.
    #[error("backend error: {0}")]
    BackendError(String),

    /// IPC communication error.
    #[error("IPC error: {0}")]
    IpcError(String),

    /// System ran out of memory.
    #[error("out of memory")]
    OutOfMemory,

    /// Operation exceeded time limit.
    #[error("operation timed out")]
    Timeout,

    /// Async pipeline error.
    #[error("pipeline error: {0}")]
    PipelineError(String),

    /// Operation was cancelled.
    #[error("operation cancelled")]
    Cancelled,

    /// Trace replay error.
    #[error("trace replay error: {0}")]
    ReplayError(String),

    /// Invalid configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Invalid state transition attempted.
    #[error("invalid state transition from {from:?} to {to:?}")]
    InvalidStateTransition { from: String, to: String },

    /// Generic internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type alias for GhostPages operations.
pub type GhostResult<T> = std::result::Result<T, GhostError>;

// Import TierId for use in error variants
use crate::types::TierId;
