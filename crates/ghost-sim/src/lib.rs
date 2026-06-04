//! Simulation backend for GhostPages.
//!
//! This crate provides a RAM-based simulation backend that mimics memory tier
//! behavior with configurable latency, bandwidth limits, fragmentation, and
//! failure injection. It is the primary development and CI backend.
//!
//! The backend is deterministic: given the same seed and the same sequence
//! of operations, it produces the same results every time.

#![warn(missing_docs)]

pub mod config;
pub mod metrics;

use async_trait::async_trait;
use blake3;
use bytes::Bytes;
use config::SimConfig;
use ghost_core::error::GhostError;
use ghost_core::state::{ChunkState, PressureState, StateMachine};
use ghost_core::types::TierId;
use ghost_core::types::ChunkId;
use ghost_tier::backend::{Allocation, BackendData, BackendError, StorageBackend};
use metrics::SimMetrics;
use parking_lot::Mutex;
use rand::SeedableRng;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::sleep;

/// Simulation backend that mimics memory tier behavior.
///
/// Implements [`StorageBackend`] with configurable latency, bandwidth,
/// fragmentation, and failure injection. All operations are deterministic
/// given the same seed.
#[derive(Debug)]
pub struct SimBackend {
    config: SimConfig,
    /// Storage map: offset -> data bytes.
    storage: Mutex<HashMap<usize, Bytes>>,
    /// Next allocation offset.
    next_offset: AtomicU64,
    /// Current used bytes.
    used: AtomicU64,
    /// RNG for deterministic jitter and failure injection.
    rng: Mutex<ChaCha8Rng>,
    /// Metrics.
    metrics: Arc<SimMetrics>,
    /// State machine for tracking chunk states.
    state_machine: Mutex<StateMachine>,
    /// Set of allocated offsets (tracks all allocations, even before write).
    allocated_offsets: Mutex<HashSet<usize>>,
    /// Time the backend was created.
    created_at: Instant,
}

impl SimBackend {
    /// Create a new simulation backend with the given configuration.
    pub fn new(config: SimConfig) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(config.seed);
        Self {
            config,
            storage: Mutex::new(HashMap::new()),
            next_offset: AtomicU64::new(0),
            used: AtomicU64::new(0),
            rng: Mutex::new(rng),
            metrics: Arc::new(SimMetrics::new()),
            state_machine: Mutex::new(StateMachine::new()),
            allocated_offsets: Mutex::new(HashSet::new()),
            created_at: Instant::now(),
        }
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &SimMetrics {
        &self.metrics
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &SimConfig {
        &self.config
    }

    /// Simulate latency for an operation with the given number of bytes.
    async fn simulate_latency(&self, bytes: usize) {
        let latency = &self.config.latency;
        let base_us = latency.base.as_micros() as u64;
        let per_byte_us = latency.per_byte.as_micros() as u64;
        let total_us = base_us.saturating_add(per_byte_us.saturating_mul(bytes as u64));

        // Add deterministic jitter
        let jitter = if latency.jitter_fraction > 0.0 && total_us > 0 {
            let mut rng = self.rng.lock();
            let jitter_range = (total_us as f64 * latency.jitter_fraction) as u64;
            if jitter_range > 0 {
                rng.gen_range(0..=jitter_range)
            } else {
                0
            }
        } else {
            0
        };

        let total_delay_us = total_us.saturating_add(jitter);
        if total_delay_us > 0 {
            sleep(std::time::Duration::from_micros(total_delay_us)).await;
        }

        self.metrics.record_latency(total_delay_us);
    }

    /// Check if a failure should be injected for the given operation type.
    fn should_fail(&self, operation: &str) -> bool {
        let mut rng = self.rng.lock();
        let rate = match operation {
            "write" => self.config.failure.write_failure_rate,
            "read" => self.config.failure.read_failure_rate,
            "alloc" => self.config.failure.alloc_failure_rate,
            _ => 0.0,
        };
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }
        rng.gen::<f64>() < rate
    }

    /// Calculate effective available space considering fragmentation.
    fn effective_available(&self) -> usize {
        let raw_available = self.config.capacity.saturating_sub(self.used.load(Ordering::Relaxed) as usize);
        if !self.config.simulate_fragmentation || self.config.fragmentation_factor <= 0.0 {
            return raw_available;
        }
        let usable = (raw_available as f64 * (1.0 - self.config.fragmentation_factor)).floor() as usize;
        usable
    }

    /// Get the current memory pressure as a ratio (0.0 to 1.0).
    pub fn memory_pressure(&self) -> f64 {
        let used = self.used.load(Ordering::Relaxed) as f64;
        let capacity = self.config.capacity as f64;
        if capacity <= 0.0 {
            return 1.0;
        }
        (used / capacity).clamp(0.0, 1.0)
    }

    /// Get the current IO pressure as a ratio (0.0 to 1.0).
    pub fn io_pressure(&self) -> f64 {
        // IO pressure is based on operation rate relative to bandwidth capacity.
        // For simplicity, we use a snapshot-based approach: ratio of recent
        // throughput to bandwidth cap.
        let bytes_per_sec = self.config.bandwidth.bytes_per_second as f64;
        if bytes_per_sec <= 0.0 {
            return 1.0;
        }
        // Estimate throughput from recent operations
        let elapsed = self.created_at.elapsed().as_secs_f64();
        if elapsed <= 0.0 {
            return 0.0;
        }
        let total_bytes = self.metrics.bytes_written() + self.metrics.bytes_read();
        let throughput = total_bytes as f64 / elapsed;
        (throughput / bytes_per_sec).clamp(0.0, 1.0)
    }
}

#[async_trait]
impl StorageBackend for SimBackend {
    fn id(&self) -> TierId {
        TierId::Simulation
    }

    fn capacity(&self) -> usize {
        self.config.capacity
    }

    fn available(&self) -> usize {
        self.effective_available()
    }

    async fn allocate(&self, size: usize) -> Result<Allocation, BackendError> {
        if size == 0 {
            return Err(BackendError::Internal(
                "allocation size must be greater than zero".to_string(),
            ));
        }

        // Simulate latency for allocation
        self.simulate_latency(0).await;

        // Check for failure injection
        if self.should_fail("alloc") {
            self.metrics.record_failure();
            return Err(BackendError::Internal(
                "simulated allocation failure".to_string(),
            ));
        }

        let effective_avail = self.effective_available();
        if size > effective_avail {
            return Err(BackendError::InsufficientSpace {
                requested: size,
                available: effective_avail,
            });
        }

        let offset = self.next_offset.fetch_add(size as u64, Ordering::Relaxed) as usize;
        self.used.fetch_add(size as u64, Ordering::Relaxed);

        // Track this offset as allocated
        self.allocated_offsets.lock().insert(offset);

        self.metrics.record_alloc(size);

        tracing::debug!(
            offset = offset,
            size = size,
            used = self.used.load(Ordering::Relaxed),
            "SimBackend: allocated {} bytes at offset {}",
            size,
            offset
        );

        Ok(Allocation::new(offset, size, BackendData::new(())))
    }

    async fn deallocate(&self, allocation: Allocation) -> Result<(), BackendError> {
        // Simulate latency
        self.simulate_latency(0).await;

        // Check if this offset was actually allocated
        {
            let mut allocated = self.allocated_offsets.lock();
            if !allocated.remove(&allocation.offset) {
                return Err(BackendError::AllocationNotFound(allocation.offset));
            }
        }

        // Remove from storage if data was written
        let mut storage = self.storage.lock();
        storage.remove(&allocation.offset);
        drop(storage);

        self.used.fetch_sub(allocation.size as u64, Ordering::Relaxed);
        self.metrics.record_dealloc(allocation.size);

        tracing::debug!(
            offset = allocation.offset,
            size = allocation.size,
            "SimBackend: deallocated {} bytes at offset {}",
            allocation.size,
            allocation.offset
        );

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

        // Simulate latency proportional to data size
        self.simulate_latency(data.len()).await;

        // Check for failure injection
        if self.should_fail("write") {
            self.metrics.record_failure();
            if self.config.failure.corruption_on_failure {
                // Write corrupted data
                let mut storage = self.storage.lock();
                let corrupted: Bytes = vec![0xFFu8; data.len()].into();
                storage.insert(allocation.offset, corrupted);
            }
            return Err(BackendError::WriteFailed(
                "simulated write failure".to_string(),
            ));
        }

        let mut storage = self.storage.lock();
        storage.insert(allocation.offset, Bytes::copy_from_slice(data));

        self.metrics.record_write(data.len());

        tracing::debug!(
            offset = allocation.offset,
            size = data.len(),
            "SimBackend: wrote {} bytes at offset {}",
            data.len(),
            allocation.offset
        );

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

        // Simulate latency proportional to read size
        self.simulate_latency(buf.len()).await;

        // Check for failure injection
        if self.should_fail("read") {
            self.metrics.record_failure();
            return Err(BackendError::ReadFailed(
                "simulated read failure".to_string(),
            ));
        }

        let storage = self.storage.lock();
        match storage.get(&allocation.offset) {
            Some(data) => {
                let to_copy = buf.len().min(data.len());
                buf[..to_copy].copy_from_slice(&data[..to_copy]);
                self.metrics.record_read(to_copy);

                tracing::debug!(
                    offset = allocation.offset,
                    size = to_copy,
                    "SimBackend: read {} bytes from offset {}",
                    to_copy,
                    allocation.offset
                );

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
        self.simulate_latency(0).await;

        let storage = self.storage.lock();
        match storage.get(&allocation.offset) {
            Some(data) => {
                let actual = blake3::hash(data);
                if actual.as_bytes() == expected {
                    Ok(())
                } else {
                    Err(BackendError::IntegrityFailed(format!(
                        "checksum mismatch at offset {}: expected {}, got {}",
                        allocation.offset,
                        blake3::Hash::from(*expected).to_hex(),
                        actual.to_hex()
                    )))
                }
            }
            None => Err(BackendError::AllocationNotFound(allocation.offset)),
        }
    }

    async fn health_check(&self) -> Result<(), BackendError> {
        // Simulate a quick health check with minimal latency
        self.simulate_latency(0).await;

        // Check if we're critically low on space
        let pressure = self.memory_pressure();
        if pressure > 0.99 {
            return Err(BackendError::Unhealthy(format!(
                "memory pressure critical: {:.1}%",
                pressure * 100.0
            )));
        }

        Ok(())
    }

    fn pressure(&self) -> PressureState {
        PressureState {
            memory_pressure: self.memory_pressure() as f32,
            vram_pressure: 0.0,
            io_pressure: self.io_pressure() as f32,
            queue_depth: 0,
            throughput_bps: 0,
        }
    }
}

impl SimBackend {
    /// Register a chunk with the state machine.
    pub fn register_chunk(&self, chunk_id: ChunkId) -> Result<(), GhostError> {
        self.state_machine.lock().register(chunk_id)
    }

    /// Transition a chunk to a new state.
    pub fn transition_chunk(&self, chunk_id: &ChunkId, next: ChunkState) -> Result<ChunkState, GhostError> {
        self.state_machine.lock().transition(chunk_id, next)
    }

    /// Get the current state of a chunk.
    pub fn chunk_state(&self, chunk_id: &ChunkId) -> Option<ChunkState> {
        self.state_machine.lock().get_state(chunk_id)
    }

    /// Get all chunks in a given state.
    pub fn chunks_in_state(&self, state: ChunkState) -> Vec<ChunkId> {
        self.state_machine.lock().chunks_in_state(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::ChunkState;
    use ghost_core::types::ChunkId;

    fn test_config() -> SimConfig {
        SimConfig::default()
            .with_seed(42)
    }

    fn test_config_with_capacity(capacity: usize) -> SimConfig {
        SimConfig::with_capacity(capacity)
            .with_seed(42)
    }

    #[tokio::test]
    async fn test_sim_backend_basic_store_and_retrieve() {
        let backend = SimBackend::new(test_config_with_capacity(1024 * 1024));
        let data = b"hello, ghostpages!";

        // Allocate
        let alloc = backend.allocate(data.len()).await.unwrap();
        assert_eq!(alloc.size, data.len());

        // Write
        backend.write(&alloc, data).await.unwrap();

        // Read
        let mut buf = vec![0u8; data.len()];
        backend.read(&alloc, &mut buf).await.unwrap();
        assert_eq!(&buf, data);

        // Verify metrics
        assert_eq!(backend.metrics().alloc_count(), 1);
        assert_eq!(backend.metrics().write_count(), 1);
        assert_eq!(backend.metrics().read_count(), 1);
    }

    #[tokio::test]
    async fn test_sim_backend_capacity_enforcement() {
        let config = test_config_with_capacity(100);
        let backend = SimBackend::new(config);

        // First allocation should succeed
        let alloc1 = backend.allocate(60).await.unwrap();
        assert_eq!(alloc1.size, 60);

        // Second allocation should succeed (total 90 < 100)
        let alloc2 = backend.allocate(30).await.unwrap();
        assert_eq!(alloc2.size, 30);

        // Third allocation should fail (total would be 120 > 100)
        let result = backend.allocate(50).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            BackendError::InsufficientSpace { requested, available } => {
                assert_eq!(requested, 50);
                assert!(available < 50);
            }
            other => panic!("expected InsufficientSpace, got {:?}", other),
        }

        // Deallocate first, then try again
        backend.deallocate(alloc1).await.unwrap();
        let alloc3 = backend.allocate(50).await.unwrap();
        assert_eq!(alloc3.size, 50);

        backend.deallocate(alloc2).await.unwrap();
        backend.deallocate(alloc3).await.unwrap();
    }

    #[tokio::test]
    async fn test_sim_backend_zero_allocation_fails() {
        let backend = SimBackend::new(test_config_with_capacity(1024));
        let result = backend.allocate(0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sim_backend_read_nonexistent_allocation() {
        let backend = SimBackend::new(test_config_with_capacity(1024));
        let fake_alloc = Allocation::new(9999, 100, BackendData::new(()));
        let mut buf = vec![0u8; 100];
        let result = backend.read(&fake_alloc, &mut buf).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            BackendError::AllocationNotFound(offset) => assert_eq!(offset, 9999),
            other => panic!("expected AllocationNotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_sim_backend_write_exceeds_allocation() {
        let backend = SimBackend::new(test_config_with_capacity(1024));
        let alloc = backend.allocate(10).await.unwrap();
        let data = vec![0u8; 20];
        let result = backend.write(&alloc, &data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sim_backend_integrity_verification() {
        let backend = SimBackend::new(test_config_with_capacity(1024 * 1024));
        let data = b"integrity test data";

        let alloc = backend.allocate(data.len()).await.unwrap();
        backend.write(&alloc, data).await.unwrap();

        // Compute expected hash
        let expected_hash = *blake3::hash(data).as_bytes();

        // Verify integrity should pass
        backend.verify_integrity(&alloc, &expected_hash).await.unwrap();

        // Verify with wrong hash should fail
        let wrong_hash = [0u8; 32];
        let result = backend.verify_integrity(&alloc, &wrong_hash).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sim_backend_health_check() {
        let backend = SimBackend::new(test_config_with_capacity(1024));
        backend.health_check().await.unwrap();
    }

    #[tokio::test]
    async fn test_sim_backend_id() {
        let backend = SimBackend::new(test_config());
        assert_eq!(backend.id(), TierId::Simulation);
    }

    #[tokio::test]
    async fn test_sim_backend_determinism() {
        // Two backends with the same seed should produce the same behavior
        let config1 = SimConfig::with_capacity(1024 * 1024).with_seed(12345);
        let config2 = SimConfig::with_capacity(1024 * 1024).with_seed(12345);

        let backend1 = SimBackend::new(config1);
        let backend2 = SimBackend::new(config2);

        let data1 = b"determinism test A";
        let _data2 = b"determinism test B";

        // Allocate and write on both
        let alloc1_a = backend1.allocate(data1.len()).await.unwrap();
        backend1.write(&alloc1_a, data1).await.unwrap();

        let alloc2_a = backend2.allocate(data1.len()).await.unwrap();
        backend2.write(&alloc2_a, data1).await.unwrap();

        // Both should have the same metrics
        assert_eq!(backend1.metrics().alloc_count(), backend2.metrics().alloc_count());
        assert_eq!(backend1.metrics().write_count(), backend2.metrics().write_count());
        assert_eq!(backend1.metrics().bytes_written(), backend2.metrics().bytes_written());

        // Read back and verify
        let mut buf1 = vec![0u8; data1.len()];
        backend1.read(&alloc1_a, &mut buf1).await.unwrap();

        let mut buf2 = vec![0u8; data1.len()];
        backend2.read(&alloc2_a, &mut buf2).await.unwrap();

        assert_eq!(buf1, buf2);
    }

    #[tokio::test]
    async fn test_sim_backend_failure_injection() {
        use config::FailureConfig;

        let failure_config = FailureConfig {
            write_failure_rate: 1.0, // Always fail
            read_failure_rate: 0.0,
            alloc_failure_rate: 0.0,
            corruption_on_failure: false,
        };

        let config = SimConfig::with_capacity(1024 * 1024)
            .with_seed(42)
            .with_failure(failure_config);

        let backend = SimBackend::new(config);
        let data = b"failure test";

        let alloc = backend.allocate(data.len()).await.unwrap();

        // Write should always fail
        let result = backend.write(&alloc, data).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BackendError::WriteFailed(_)
        ));

        // Failure should be recorded
        assert!(backend.metrics().failure_count() > 0);

        backend.deallocate(alloc).await.unwrap();
    }

    #[tokio::test]
    async fn test_sim_backend_read_failure_injection() {
        use config::FailureConfig;

        let failure_config = FailureConfig {
            write_failure_rate: 0.0,
            read_failure_rate: 1.0, // Always fail
            alloc_failure_rate: 0.0,
            corruption_on_failure: false,
        };

        let config = SimConfig::with_capacity(1024 * 1024)
            .with_seed(42)
            .with_failure(failure_config);

        let backend = SimBackend::new(config);
        let data = b"read failure test";

        let alloc = backend.allocate(data.len()).await.unwrap();
        backend.write(&alloc, data).await.unwrap();

        // Read should always fail
        let mut buf = vec![0u8; data.len()];
        let result = backend.read(&alloc, &mut buf).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BackendError::ReadFailed(_)
        ));

        backend.deallocate(alloc).await.unwrap();
    }

    #[tokio::test]
    async fn test_sim_backend_state_machine_integration() {
        let backend = SimBackend::new(test_config_with_capacity(1024 * 1024));
        let data = b"state machine test";
        let chunk_id = ChunkId::from_data(data);

        // Register chunk
        backend.register_chunk(chunk_id).unwrap();

        // Initial state should be Allocated
        let state = backend.chunk_state(&chunk_id);
        assert_eq!(state, Some(ChunkState::Allocated));

        // Allocate and write
        let alloc = backend.allocate(data.len()).await.unwrap();
        backend.write(&alloc, data).await.unwrap();

        // Transition to Stored
        backend.transition_chunk(&chunk_id, ChunkState::Stored).unwrap();
        assert_eq!(backend.chunk_state(&chunk_id), Some(ChunkState::Stored));

        // Transition to Cached
        backend.transition_chunk(&chunk_id, ChunkState::Cached).unwrap();
        assert_eq!(backend.chunk_state(&chunk_id), Some(ChunkState::Cached));

        // Transition back to Stored first (Cached -> Evicted is not valid)
        backend.transition_chunk(&chunk_id, ChunkState::Stored).unwrap();
        assert_eq!(backend.chunk_state(&chunk_id), Some(ChunkState::Stored));

        // Transition to Evicted
        backend.transition_chunk(&chunk_id, ChunkState::Evicted).unwrap();
        assert_eq!(backend.chunk_state(&chunk_id), Some(ChunkState::Evicted));

        backend.deallocate(alloc).await.unwrap();
    }

    #[tokio::test]
    async fn test_sim_backend_chunks_in_state() {
        let backend = SimBackend::new(test_config_with_capacity(1024 * 1024));

        let data1 = b"chunk one";
        let data2 = b"chunk two";
        let id1 = ChunkId::from_data(data1);
        let id2 = ChunkId::from_data(data2);

        backend.register_chunk(id1).unwrap();
        backend.register_chunk(id2).unwrap();

        // Both start as Allocated
        let allocated = backend.chunks_in_state(ChunkState::Allocated);
        assert_eq!(allocated.len(), 2);

        // Transition one to Stored
        backend.transition_chunk(&id1, ChunkState::Stored).unwrap();

        let allocated = backend.chunks_in_state(ChunkState::Allocated);
        assert_eq!(allocated.len(), 1);
        assert_eq!(allocated[0], id2);

        let stored = backend.chunks_in_state(ChunkState::Stored);
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0], id1);
    }

    #[tokio::test]
    async fn test_sim_backend_memory_pressure() {
        let config = test_config_with_capacity(1000);
        let backend = SimBackend::new(config);

        // Initially no pressure
        assert!((backend.memory_pressure() - 0.0).abs() < f64::EPSILON);

        // Allocate half
        let _alloc1 = backend.allocate(500).await.unwrap();
        assert!((backend.memory_pressure() - 0.5).abs() < 0.01);

        // Allocate more
        let _alloc2 = backend.allocate(300).await.unwrap();
        assert!((backend.memory_pressure() - 0.8).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_sim_backend_fragmentation() {
        use config::SimConfig;

        let config = SimConfig::with_capacity(1000)
            .with_seed(42)
            .with_fragmentation(0.5); // 50% fragmentation

        let backend = SimBackend::new(config);

        // Effective available should be ~500 (50% of 1000)
        let avail = backend.available();
        assert!(avail <= 550 && avail >= 450, "available was {}", avail);
    }

    #[tokio::test]
    async fn test_sim_backend_deallocate_nonexistent() {
        let backend = SimBackend::new(test_config_with_capacity(1024));
        let fake_alloc = Allocation::new(9999, 100, BackendData::new(()));
        let result = backend.deallocate(fake_alloc).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            BackendError::AllocationNotFound(offset) => assert_eq!(offset, 9999),
            other => panic!("expected AllocationNotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_sim_backend_multiple_operations() {
        let backend = SimBackend::new(test_config_with_capacity(1024 * 1024));

        // Perform multiple allocate/write/read cycles
        for i in 0..10 {
            let data = format!("operation {}", i);
            let bytes = data.as_bytes();

            let alloc = backend.allocate(bytes.len()).await.unwrap();
            backend.write(&alloc, bytes).await.unwrap();

            let mut buf = vec![0u8; bytes.len()];
            backend.read(&alloc, &mut buf).await.unwrap();
            assert_eq!(&buf, bytes);

            backend.deallocate(alloc).await.unwrap();
        }

        assert_eq!(backend.metrics().alloc_count(), 10);
        assert_eq!(backend.metrics().write_count(), 10);
        assert_eq!(backend.metrics().read_count(), 10);
        assert_eq!(backend.metrics().dealloc_count(), 10);
    }

    #[tokio::test]
    async fn test_sim_backend_large_data() {
        let backend = SimBackend::new(test_config_with_capacity(1024 * 1024));
        let data = vec![0xABu8; 100_000]; // 100 KB

        let alloc = backend.allocate(data.len()).await.unwrap();
        backend.write(&alloc, &data).await.unwrap();

        let mut buf = vec![0u8; data.len()];
        backend.read(&alloc, &mut buf).await.unwrap();
        assert_eq!(buf, data);

        backend.deallocate(alloc).await.unwrap();
    }

    #[tokio::test]
    async fn test_sim_backend_corruption_on_failure() {
        use config::FailureConfig;

        let failure_config = FailureConfig {
            write_failure_rate: 1.0,
            read_failure_rate: 0.0,
            alloc_failure_rate: 0.0,
            corruption_on_failure: true,
        };

        let config = SimConfig::with_capacity(1024 * 1024)
            .with_seed(42)
            .with_failure(failure_config);

        let backend = SimBackend::new(config);
        let data = b"corruption test";

        let alloc = backend.allocate(data.len()).await.unwrap();

        // Write fails but corrupts data
        backend.write(&alloc, data).await.unwrap_err();

        // Read should succeed but return corrupted data
        let mut buf = vec![0u8; data.len()];
        backend.read(&alloc, &mut buf).await.unwrap();
        assert_eq!(buf, vec![0xFFu8; data.len()]);

        // Integrity check should fail
        let expected_hash = *blake3::hash(data).as_bytes();
        let result = backend.verify_integrity(&alloc, &expected_hash).await;
        assert!(result.is_err());

        backend.deallocate(alloc).await.unwrap();
    }

    #[tokio::test]
    async fn test_sim_backend_capacity_and_available() {
        let config = test_config_with_capacity(500);
        let backend = SimBackend::new(config);

        assert_eq!(backend.capacity(), 500);
        assert_eq!(backend.available(), 500);

        let alloc = backend.allocate(200).await.unwrap();
        assert_eq!(backend.available(), 300);

        backend.deallocate(alloc).await.unwrap();
        assert_eq!(backend.available(), 500);
    }

    #[tokio::test]
    async fn test_sim_backend_read_buffer_smaller_than_allocation() {
        let backend = SimBackend::new(test_config_with_capacity(1024));
        let data = b"hello world, this is a longer message";

        let alloc = backend.allocate(data.len()).await.unwrap();
        backend.write(&alloc, data).await.unwrap();

        // Read into a smaller buffer
        let mut buf = vec![0u8; 5];
        backend.read(&alloc, &mut buf).await.unwrap();
        assert_eq!(&buf, &data[..5]);

        backend.deallocate(alloc).await.unwrap();
    }
}
