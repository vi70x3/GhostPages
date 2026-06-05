//! Vulkan VRAM backend implementation.
//!
//! Skeleton implementation for Phase 0. Full Vulkan integration
//! will be implemented in Phase 4.

use async_trait::async_trait;
use ghost_core::state::PressureState;
use ghost_core::types::TierId;
use ghost_tier::backend::{Allocation, BackendError, StorageBackend};

/// Vulkan VRAM storage backend.
///
/// Skeleton implementation for Phase 0. This will be expanded in Phase 4
/// with actual Vulkan device enumeration, memory allocation, and DMA transfers.
pub struct VulkanBackend {
    id: TierId,
    capacity: usize,
}

impl VulkanBackend {
    /// Create a new Vulkan backend skeleton.
    ///
    /// # Errors
    ///
    /// Currently always returns an error indicating that Vulkan
    /// initialization is not yet implemented.
    pub fn new(_capacity: usize) -> Result<Self, BackendError> {
        Err(BackendError::NotSupported(
            "Vulkan backend is not yet implemented (Phase 4)".to_string(),
        ))
    }
}

#[async_trait]
impl StorageBackend for VulkanBackend {
    fn id(&self) -> TierId {
        self.id
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn available(&self) -> usize {
        0
    }

    async fn allocate(&self, _size: usize) -> Result<Allocation, BackendError> {
        Err(BackendError::NotSupported(
            "Vulkan backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn deallocate(&self, _allocation: Allocation) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Vulkan backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn write(&self, _allocation: &Allocation, _data: &[u8]) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Vulkan backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn read(&self, _allocation: &Allocation, _buf: &mut [u8]) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Vulkan backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn verify_integrity(
        &self,
        _allocation: &Allocation,
        _expected: &[u8; 32],
    ) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Vulkan backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    async fn health_check(&self) -> Result<(), BackendError> {
        Err(BackendError::NotSupported(
            "Vulkan backend is not yet implemented (Phase 4)".to_string(),
        ))
    }

    fn pressure(&self) -> PressureState {
        PressureState::new()
    }
}
