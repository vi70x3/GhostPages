//! Storage backend trait and tier implementations for GhostPages.
//!
//! This module defines the [`StorageBackend`] trait that all storage tiers
//! must implement, and provides RAM-based and disk-based implementations for
//! development, testing, and production use.

pub mod backend;
pub mod disk;
pub mod disk_config;
pub mod disk_persistence;
pub mod ram;
pub mod sim_backend;
pub mod sim_config;
pub mod sim_metrics;
pub mod tracker;

pub use backend::{Allocation, BackendData, BackendError, StorageBackend};
pub use disk::DiskBackend;
pub use disk_config::{DiskConfig, DiskConfigBuilder, DiskType};
pub use disk_persistence::DiskPersistence;
pub use ram::RamBackend;
pub use sim_backend::SimBackend;
pub use sim_config::{
    BandwidthConfig, FailureConfig, FailurePattern, LatencyConfig, SimConfig,
};
pub use sim_metrics::SimMetrics;
pub use tracker::AllocationTracker;
