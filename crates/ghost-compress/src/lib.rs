//! # ghost-compress
//!
//! Compression abstraction for GhostPages.
//!
//! Provides a simple API for compressing and decompressing data using zstd,
//! with configurable compression levels.

pub mod engine;

pub use engine::{compress, decompress, CompressionConfig, CompressionEngine};
