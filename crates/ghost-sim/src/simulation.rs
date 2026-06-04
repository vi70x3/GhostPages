//! Simulation backend implementation.
//!
//! Skeleton implementation for Phase 0. Full simulation features
//! (configurable latency, bandwidth limits, fragmentation, failure injection)
//! will be implemented in Phase 4.

use async_trait::async_trait;
use ghost_core::types::TierId;
use ghost_tier::backend::{Allocation, BackendError, StorageBackend};
use std::time::Duration;

/// Simulation configuration.
///
/// Controls the behavior of the simulation backend for testing.
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    /// Total simulated VRAM capacity.
    pub capacity: usize,

    /// Simulated transfer latency per chunk.
    pub transfer_latency: Duration,

    /// Bandwidth ceiling in bytes/second.
    pub bandwidth_limit: usize,

    /// Fragmentation level (0.0 = none, 1.0 = fully fragmented).
    pub fragmentation: f64,

    /// Allocation failure rate (0.0 = never, 1.0 = always).
    pub allocation_failure_rate: f64,

    /// Corruption injection rate (0.0 = never, 1.0 = always).
    pub corruption_rate: f64,

    /// Enable eviction pressure simulation.
    pub eviction_pressure: bool,

    /// Random seed for deterministic testing.
    pub seed: Option<u64>,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            capacity: 2 * 1024 * 1024 * 1024, // 2 GB
            transfer_latency: Duration::from_millis(10),
            bandwidth_limit: 8 * 1024 * 1024 * 1024, // 8 GB/s
            fragmentation: 0.1,
            allocation_failure_rate: 0.01,
            corruption_rate: 0.0,
            eviction_pressure: true,
            seed: None,
        }
    }
}

/// Simulation storage backend.
///
/// Skeleton implementation for Phase 0. This will be expanded in Phase 4
/// with configurable latency, bandwidth limits, fragmentation simulation,
/// and failure injection.
pub struct SimulationBackend {
    id: TierId,
    config: SimulationConfig,
}

impl SimulationBackend {
    /// Create a new simulation backend with the given configuration.
    pub fn new(config: SimulationConfig) -> Self {
        Self {
            id: TierId::Simulation,
            config,
        }
    }
}

#[async_trait]
impl StorageBackend for SimulationBackend {
    fn id(&self) -> TierId {
        self.id
    }

    fn capacity(&self) -> usize {
        self.config.capacity
    }

    fn available(&self) -> usize {
        0
    }

    async fn allocate(&self, _size: usize) -> Result<Allocation, BackendError> {
        Err(BackendError::NotSupported(
            "Simulation backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn deallocate(&self, _allocation: Allocation) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Simulation backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn write(&self, _allocation: &Allocation, _data: &[u8]) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Simulation backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn read(
        &self,
        _allocation: &Allocation,
        _buf: &mut [u8],
    ) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Simulation backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn verify_integrity(
        &self,
        _allocation: &Allocation,
        _expected: &[u8; 32],
    ) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Simulation backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn health_check(&self) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Simulation backend is not yet implemented (Phase 4)".to_string(),
        ))
    }
}
