//! Core engine for GhostPages daemon.
//!
//! Skeleton implementation for Phase 0. Full engine functionality
//! will be implemented in Phase 1.

use crate::config::DaemonConfig;
use ghost_tier::RamBackend;
use std::sync::Arc;

/// Core engine managing tiers and policies.
#[derive(Debug)]
pub struct Engine {
    config: DaemonConfig,
    ram_backend: Arc<RamBackend>,
}

impl Engine {
    /// Create a new engine with the given configuration.
    pub fn new(config: DaemonConfig) -> Self {
        let ram_backend = Arc::new(RamBackend::new(config.ram_capacity));
        Self {
            config,
            ram_backend,
        }
    }

    /// Get the RAM backend.
    pub fn ram_backend(&self) -> Arc<RamBackend> {
        self.ram_backend.clone()
    }

    /// Get the configuration.
    pub fn config(&self) -> &DaemonConfig {
        &self.config
    }
}
