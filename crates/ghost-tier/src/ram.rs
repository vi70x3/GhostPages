//! RAM storage backend implementation.
//!
//! A simple in-memory HashMap-backed storage tier for development and testing.
//! Prioritizes correctness and clarity over lock-free optimization.

use async_trait::async_trait;
use bytes::Bytes;
use ghost_core::state::PressureState;
use ghost_core::types::TierId;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::backend::{
    Allocation, BackendData, BackendError, StorageBackend,
};

/// RAM-based storage backend.
///
/// Uses an in-memory `HashMap<usize, Bytes>` to store data, keyed by
/// allocation offset. A `parking_lot::Mutex` protects the map — this is
/// preferred for Phase 0 because it is simple, correct, and does not hold
/// locks across `.await` points (all operations complete synchronously).
///
/// # Concurrency
///
/// The internal map is protected by a `parking_lot::Mutex`. Lock contention
/// is minimal because all operations are O(1) HashMap lookups that complete
/// without yielding. The lock is never held across an `.await` point.
#[derive(Debug)]
pub struct RamBackend {
    id: TierId,
    capacity: usize,
    storage: Arc<Mutex<HashMap<usize, Bytes>>>,
    next_offset: Arc<Mutex<usize>>,
    used: Arc<Mutex<usize>>,
}

impl RamBackend {
    /// Create a new RAM backend with the specified capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use ghost_tier::RamBackend;
    /// use ghost_tier::StorageBackend;
    ///
    /// let backend = RamBackend::new(1024 * 1024); // 1 MB
    /// assert_eq!(backend.capacity(), 1024 * 1024);
    /// ```
    pub fn new(capacity: usize) -> Self {
        Self {
            id: TierId::Ram,
            capacity,
            storage: Arc::new(Mutex::new(HashMap::new())),
            next_offset: Arc::new(Mutex::new(0)),
            used: Arc::new(Mutex::new(0)),
        }
    }

    /// Create a new RAM backend with a specific TierId.
    ///
    /// Useful for testing when you need a RAM backend that reports as a
    /// different tier.
    pub fn with_id(id: TierId, capacity: usize) -> Self {
        Self {
            id,
            capacity,
            storage: Arc::new(Mutex::new(HashMap::new())),
            next_offset: Arc::new(Mutex::new(0)),
            used: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl StorageBackend for RamBackend {
    fn id(&self) -> TierId {
        self.id
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn available(&self) -> usize {
        let used = self.used.lock();
        self.capacity - *used
    }

    async fn allocate(&self, size: usize) -> Result<Allocation, BackendError> {
        if size == 0 {
            return Err(BackendError::Internal(
                "cannot allocate zero bytes".to_string(),
            ));
        }

        let mut used = self.used.lock();
        if *used + size > self.capacity {
            return Err(BackendError::InsufficientSpace {
                requested: size,
                available: self.capacity - *used,
            });
        }

        let mut next_offset = self.next_offset.lock();
        let offset = *next_offset;
        *next_offset += size;
        *used += size;

        Ok(Allocation::new(
            offset,
            size,
            BackendData::new(size),
        ))
    }

    async fn deallocate(&self, allocation: Allocation) -> Result<(), BackendError> {
        let mut storage = self.storage.lock();
        storage.remove(&allocation.offset);

        let mut used = self.used.lock();
        *used -= allocation.size;

        Ok(())
    }

    async fn write(&self, allocation: &Allocation, data: &[u8]) -> Result<(), BackendError> {
        if data.len() > allocation.size {
            return Err(BackendError::WriteFailed(format!(
                "data size {} exceeds allocation size {}",
                data.len(),
                allocation.size
            )));
        }

        let mut storage = self.storage.lock();
        storage.insert(allocation.offset, Bytes::copy_from_slice(data));

        Ok(())
    }

    async fn read(
        &self,
        allocation: &Allocation,
        buf: &mut [u8],
    ) -> Result<(), BackendError> {
        if buf.len() > allocation.size {
            return Err(BackendError::ReadFailed(format!(
                "buffer size {} exceeds allocation size {}",
                buf.len(),
                allocation.size
            )));
        }

        let storage = self.storage.lock();
        match storage.get(&allocation.offset) {
            Some(data) => {
                let len = buf.len().min(data.len());
                buf[..len].copy_from_slice(&data[..len]);
                Ok(())
            }
            None => Err(BackendError::AllocationNotFound(allocation.offset)),
        }
    }

    async fn verify_integrity(
        &self,
        allocation: &Allocation,
        expected: &[u8; 32],
    ) -> Result<(), BackendError> {
        let storage = self.storage.lock();
        match storage.get(&allocation.offset) {
            Some(data) => {
                let hash = *blake3::hash(data).as_bytes();
                if &hash == expected {
                    Ok(())
                } else {
                    Err(BackendError::IntegrityFailed(format!(
                        "hash mismatch at offset {}",
                        allocation.offset
                    )))
                }
            }
            None => Err(BackendError::AllocationNotFound(allocation.offset)),
        }
    }

    async fn health_check(&self) -> Result<(), BackendError> {
        // RAM backend is always healthy
        Ok(())
    }

    fn pressure(&self) -> PressureState {
        let used = self.used.lock();
        let memory_pressure = if self.capacity > 0 {
            (*used as f32) / (self.capacity as f32)
        } else {
            0.0
        };

        PressureState {
            memory_pressure,
            vram_pressure: 0.0,
            io_pressure: 0.0,
            queue_depth: 0,
            throughput_bps: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::ChunkId;

    #[tokio::test]
    async fn test_ram_backend_basic_store_and_retrieve() {
        let backend = RamBackend::new(1024);

        // Allocate space
        let alloc = backend.allocate(128).await.unwrap();
        assert_eq!(alloc.size, 128);

        // Write data
        let data = b"Hello, GhostPages!";
        backend.write(&alloc, data).await.unwrap();

        // Read data back
        let mut buf = vec![0u8; data.len()];
        backend.read(&alloc, &mut buf).await.unwrap();
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_ram_backend_capacity_tracking() {
        let backend = RamBackend::new(256);

        assert_eq!(backend.capacity(), 256);
        assert_eq!(backend.available(), 256);

        let alloc1 = backend.allocate(100).await.unwrap();
        assert_eq!(backend.available(), 156);

        let alloc2 = backend.allocate(100).await.unwrap();
        assert_eq!(backend.available(), 56);

        // Should fail: only 56 bytes left
        let result = backend.allocate(100).await;
        assert!(matches!(
            result,
            Err(BackendError::InsufficientSpace { .. })
        ));

        // Deallocate first allocation
        backend.deallocate(alloc1).await.unwrap();
        assert_eq!(backend.available(), 156);

        // Now allocation should succeed
        let _alloc3 = backend.allocate(100).await.unwrap();
    }

    #[tokio::test]
    async fn test_ram_backend_integrity_verification() {
        let backend = RamBackend::new(1024);

        let data = b"integrity test data";
        let alloc = backend.allocate(data.len()).await.unwrap();
        backend.write(&alloc, data).await.unwrap();

        // Compute expected hash
        let expected_hash = *blake3::hash(data).as_bytes();

        // Should pass integrity check
        backend
            .verify_integrity(&alloc, &expected_hash)
            .await
            .unwrap();

        // Wrong hash should fail
        let wrong_hash = [0u8; 32];
        let result = backend.verify_integrity(&alloc, &wrong_hash).await;
        assert!(matches!(
            result,
            Err(BackendError::IntegrityFailed { .. })
        ));
    }

    #[tokio::test]
    async fn test_ram_backend_health_check() {
        let backend = RamBackend::new(1024);
        backend.health_check().await.unwrap();
    }

    #[tokio::test]
    async fn test_ram_backend_id() {
        let backend = RamBackend::new(1024);
        assert_eq!(backend.id(), TierId::Ram);

        let sim_backend = RamBackend::with_id(TierId::Simulation, 1024);
        assert_eq!(sim_backend.id(), TierId::Simulation);
    }

    #[tokio::test]
    async fn test_ram_backend_zero_allocation_fails() {
        let backend = RamBackend::new(1024);
        let result = backend.allocate(0).await;
        assert!(matches!(result, Err(BackendError::Internal(_))));
    }

    #[tokio::test]
    async fn test_ram_backend_read_nonexistent_allocation() {
        let backend = RamBackend::new(1024);
        let alloc = Allocation::new(9999, 100, BackendData::new(100));
        let mut buf = vec![0u8; 10];
        let result = backend.read(&alloc, &mut buf).await;
        assert!(matches!(
            result,
            Err(BackendError::AllocationNotFound(9999))
        ));
    }

    #[tokio::test]
    async fn test_ram_backend_write_exceeds_allocation() {
        let backend = RamBackend::new(1024);
        let alloc = backend.allocate(10).await.unwrap();
        let data = vec![0u8; 20];
        let result = backend.write(&alloc, &data).await;
        assert!(matches!(
            result,
            Err(BackendError::WriteFailed(_))
        ));
    }
}
