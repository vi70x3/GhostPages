//! Simulation backend for GhostPages.
//!
//! This crate provides a RAM-based simulation backend that mimics memory tier
//! behavior with configurable latency, bandwidth limits, fragmentation, and
//! failure injection. It is the primary development and CI backend.
//!
//! The backend is deterministic: given the same seed and the same sequence
//! of operations, it produces the same results every time.
//!
//! # Architecture
//!
//! This crate re-exports the simulation types from `ghost-tier`:
//! - [`SimBackend`] — the simulation backend implementation
//! - [`SimConfig`] — configuration for latency, bandwidth, failure injection
//! - [`SimMetrics`] — metrics tracked by the simulation backend
//!
//! The simulation types live in `ghost-tier` so they can be reused by
//! `DiskBackend` as its simulation layer (the "SimBackend + persistence"
//! architecture).

#![warn(missing_docs)]

// Re-export simulation types from ghost-tier
pub use ghost_tier::SimBackend;
pub use ghost_tier::SimConfig;
pub use ghost_tier::SimMetrics;

// Re-export config sub-types for convenience
pub use ghost_tier::sim_config;
pub use ghost_tier::sim_metrics;

// Module aliases for backward compatibility
pub mod config {
    pub use ghost_tier::sim_config::*;
}

pub mod metrics {
    pub use ghost_tier::sim_metrics::*;
}
