//! Storage backend trait definition.
//!
//! This module defines the [`StorageBackend`] trait — the core abstraction
//! for all storage tiers in GhostPages. Each tier (RAM, GPU VRAM, Disk,
//! Simulation) implements this trait.

use async_trait::async_trait;
use ghost_core::state::{PhysicalCost, PressureState};
use ghost_core::types::TierId;
use std::fmt;

/// Opaque backend-specific allocation data.
///
/// Each backend can attach backend-specific metadata to an allocation.
/// This is stored as a type-erased `Box<dyn Any + Send + Sync>`.
#[derive(Debug)]
pub struct BackendData {
    inner: Box<dyn std::any::Any + Send + Sync>,
}

impl BackendData {
    /// Create a new BackendData wrapping a value.
    pub fn new<T: Send + Sync + 'static>(value: T) -> Self {
        Self {
            inner: Box::new(value),
        }
    }

    /// Attempt to downcast to a concrete type.
    pub fn downcast_ref<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.inner.downcast_ref::<T>()
    }
}

/// An allocation within a storage backend.
///
/// Represents reserved space in a tier where data can be written.
#[derive(Debug)]
pub struct Allocation {
    /// Offset within the backend's address space.
    pub offset: usize,

    /// Size of the allocation in bytes.
    pub size: usize,

    /// Opaque backend-specific data.
    pub backend_data: BackendData,
}

impl Allocation {
    /// Create a new allocation.
    pub fn new(offset: usize, size: usize, backend_data: BackendData) -> Self {
        Self {
            offset,
            size,
            backend_data,
        }
    }
}

/// Errors from storage backend operations.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// Insufficient space for allocation.
    #[error("insufficient space: requested {requested}, available {available}")]
    InsufficientSpace {
        /// Requested size in bytes.
        requested: usize,
        /// Available space in bytes.
        available: usize,
    },

    /// Allocation not found.
    #[error("allocation not found at offset {0}")]
    AllocationNotFound(usize),

    /// Write operation failed.
    #[error("write failed: {0}")]
    WriteFailed(String),

    /// Read operation failed.
    #[error("read failed: {0}")]
    ReadFailed(String),

    /// Integrity verification failed.
    #[error("integrity check failed: {0}")]
    IntegrityFailed(String),

    /// Backend is unhealthy.
    #[error("backend unhealthy: {0}")]
    Unhealthy(String),

    /// Operation not supported by this backend.
    #[error("operation not supported: {0}")]
    NotSupported(String),

    /// Internal backend error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Storage backend trait — the core abstraction for all tiers.
///
/// This trait defines the interface that every storage tier must implement.
/// It is policy-agnostic: it handles only allocation, retrieval, integrity,
/// and transfer of data.
///
/// # Concurrency
///
/// Implementations must be `Send + Sync + 'static`. The trait uses
/// `async-trait` so all methods are async. Implementations should
/// minimize lock holding across `.await` points.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Backend identifier.
    fn id(&self) -> TierId;

    /// Total capacity in bytes.
    fn capacity(&self) -> usize;

    /// Available space in bytes.
    fn available(&self) -> usize;

    /// Allocate space for `size` bytes of data.
    ///
    /// Returns an [`Allocation`] that can be used with [`write`] and [`read`].
    async fn allocate(&self, size: usize) -> Result<Allocation, BackendError>;

    /// Deallocate a previously allocated region.
    async fn deallocate(&self, allocation: Allocation) -> Result<(), BackendError>;

    /// Write data to an allocation.
    ///
    /// The data length must not exceed the allocation size.
    async fn write(&self, allocation: &Allocation, data: &[u8]) -> Result<(), BackendError>;

    /// Read data from an allocation into a buffer.
    ///
    /// The buffer length must not exceed the allocation size.
    async fn read(&self, allocation: &Allocation, buf: &mut [u8]) -> Result<(), BackendError>;

    /// Verify the integrity of data at an allocation.
    ///
    /// Compares the blake3 hash of the stored data against `expected`.
    async fn verify_integrity(
        &self,
        allocation: &Allocation,
        expected: &[u8; 32],
    ) -> Result<(), BackendError>;

    /// Check if the backend is healthy and operational.
    async fn health_check(&self) -> Result<(), BackendError>;

    /// Return the current pressure state of this backend.
    ///
    /// Pressure is a live signal reflecting resource contention. Higher values
    /// indicate more pressure (0.0 = idle, 1.0 = saturated). Implementations
    /// should return a [`PressureState`] with relevant fields populated.
    fn pressure(&self) -> PressureState;

    /// Return the physical cost model for this backend.
    ///
    /// The cost model captures I/O characteristics (latency, bandwidth,
    /// reliability, I/O pressure) that affect migration decisions. The default
    /// implementation returns a zero-cost model; backends should override this
    /// to provide accurate cost information.
    fn cost_model(&self) -> PhysicalCost {
        PhysicalCost::new()
    }
}

impl fmt::Debug for dyn StorageBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageBackend({:?})", self.id())
    }
}
