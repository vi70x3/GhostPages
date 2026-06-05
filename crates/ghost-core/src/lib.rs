//! # ghost-core
//!
//! Core types, errors, and utilities for the GhostPages memory-tiering system.
//!
//! This crate provides the foundational types used throughout the system:
//! - [`ChunkId`]: Content-addressed identifier using blake3 hashing
//! - [`ChunkMeta`]: Metadata for stored chunks
//! - [`TierId`]: Memory tier identifiers
//! - [`GhostError`]: Unified error type
//! - [`ChunkState`]: Chunk lifecycle state machine
//! - [`StateMachine`]: State transition enforcement
//! - [`PressureState`]: System pressure model
//! - [`TransferJob`]: Transfer job tracking
//! - [`TraceEvent`]: Trace log events

pub mod error;
pub mod hotness;
pub mod state;
pub mod trace;
pub mod transfer;
pub mod types;

// Re-export commonly used types
pub use error::{GhostError, GhostResult};
pub use state::{ChunkState, PressureState, StateMachine};
pub use trace::{EvictionReason, TraceEvent};
pub use transfer::{TransferJob, TransferPriority, TransferState};
pub use types::{ChunkId, ChunkMeta, CompressionAlgorithm, TierId};
