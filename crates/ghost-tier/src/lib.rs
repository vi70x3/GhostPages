//! Storage backend trait and tier implementations for GhostPages.
//!
//! This module defines the [`StorageBackend`] trait that all storage tiers
//! must implement, and provides a RAM-based implementation for development
//! and testing.

pub mod backend;
pub mod ram;

pub use backend::{Allocation, BackendError, BackendData, StorageBackend};
pub use ram::RamBackend;
