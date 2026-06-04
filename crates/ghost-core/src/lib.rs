//! # ghost-core
//!
//! Core types, errors, and utilities for the GhostPages memory-tiering system.
//!
//! This crate provides the foundational types used throughout the system:
//! - [`ChunkId`]: Content-addressed identifier using blake3 hashing
//! - [`ChunkMeta`]: Metadata for stored chunks
//! - [`TierId`]: Memory tier identifiers
//! - [`GhostError`]: Unified error type

pub mod error;
pub mod types;

// Re-export commonly used types
pub use error::{GhostError, GhostResult};
pub use types::{ChunkId, ChunkMeta, CompressionAlgorithm, TierId};
