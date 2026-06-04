//! Simulation backend for GhostPages.
//!
//! This crate provides a RAM-based simulation backend that mimics GPU VRAM
//! behavior with configurable latency, bandwidth limits, fragmentation,
//! and failure injection. It is the primary development and CI backend.
//!
//! # Phase 0 Status
//!
//! This is a skeleton implementation. Full simulation features will be
//! implemented in Phase 4.

#![warn(missing_docs)]

/// Simulation backend module.
pub mod simulation;

pub use simulation::{SimulationBackend, SimulationConfig};
