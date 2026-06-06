//! Storage backend trait and tier implementations for GhostPages.
//!
//! This module defines the [`StorageBackend`] trait that all storage tiers
//! must implement, and provides RAM-based and disk-based implementations for
//! development, testing, and production use.

pub mod backend;
pub mod disk;
pub mod disk_config;
pub mod ram;
pub mod tracker;

pub use backend::{Allocation, BackendData, BackendError, StorageBackend};
pub use disk::DiskBackend;
pub use disk_config::{DiskConfig, DiskConfigBuilder, DiskType};
pub use ram::RamBackend;
pub use tracker::AllocationTracker;
