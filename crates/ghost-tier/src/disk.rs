//! Disk storage backend implementation.
//!
//! A persistent file-system-backed storage tier that implements the
//! [`StorageBackend`] trait. Chunks are stored as individual files on disk,
//! using a deterministic directory layout and atomic write strategy.
//!
//! # Architecture: SimBackend + Persistence
//!
//! `DiskBackend` is explicitly structured as "SimBackend + persistence":
//!
//! - **Simulation layer** (`SimBackend`): Handles latency simulation, bandwidth
//!   throttling, failure injection, pressure calculation, and health tracking.
//!   This is the same simulation logic used by the pure simulation backend.
//!
//! - **Persistence layer** (`DiskPersistence`): Handles actual file I/O, atomic
//!   writes, corruption detection, and directory layout. No simulation state.
//!
//! The `StorageBackend` implementation delegates to the simulation layer for
//! timing/pressure/health decisions, and to the persistence layer for actual
//! data storage.
//!
//! # Directory Layout
//!
//! ```text
//! <base_path>/<tier_prefix>/<chunk_id_hex>.blk
//! ```
//!
//! Where `<tier_prefix>` is a two-character hex prefix derived from the first
//! byte of the chunk ID, providing up to 256 subdirectories for even
//! distribution.
//!
//! # Chunk File Format
//!
//! Each `.blk` file contains:
//!
//! | Offset | Size | Field |
//! |--------|------|-------|
//! | 0 | 8 | Magic bytes (`GHOSTBLK`) |
//! | 8 | 2 | Version (u16 LE) |
//! | 10 | 32 | blake3 hash of the stored data |
//! | 42 | 4 | Original data size (u32 LE) |
//! | 46 | 4 | Compressed data size (u32 LE) |
//! | 50 | 1 | Compression algorithm (0=None, 1=Zstd) |
//! | 51 | N | Compressed data |
//!
//! # Concurrency
//!
//! The internal state is protected by `parking_lot::Mutex`. File I/O is
//! dispatched to `tokio::task::spawn_blocking` to avoid blocking the async
//! runtime. Locks are never held across `.await` points.

use async_trait::async_trait;
use blake3::Hasher;
use bytes::Bytes;
use ghost_core::emitter::EventEmitter;
use ghost_core::io_abstraction::{IoOperation, IoScheduler};
use ghost_core::state::{PhysicalCost, PressureState};
use ghost_core::time::{RealTimeProvider, TimeProvider};
use ghost_core::types::{ChunkId, CompressionAlgorithm, TierId};
use crate::sim_config::{BandwidthConfig, FailureConfig, FailurePattern, SimConfig};
use crate::sim_backend::SimBackend;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::backend::{Allocation, BackendData, BackendError, StorageBackend};
use crate::disk_config::DiskConfig;
use crate::disk_persistence::DiskPersistence;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Magic bytes for chunk file identification.
const CHUNK_MAGIC: &[u8; 8] = b"GHOSTBLK";

/// Current chunk file format version.
const CHUNK_VERSION: u16 = 1;

/// Header size: magic (8) + version (2) + hash (32) + orig_size (4) + comp_size (4) + algo (1) = 51
const HEADER_SIZE: usize = 51;

// ─── Disk Allocation ──────────────────────────────────────────────────────────

/// Metadata for a disk-backed allocation.
#[derive(Debug, Clone)]
pub struct DiskAllocation {
    /// The chunk ID (content-addressed).
    pub chunk_id: ChunkId,

    /// Path to the chunk file on disk.
    pub file_path: PathBuf,

    /// Original (uncompressed) data size.
    pub original_size: usize,

    /// Compressed data size.
    pub compressed_size: usize,

    /// Total space consumed on disk (header + compressed data).
    pub disk_size: usize,

    /// Reserved size in the used counter (set at allocation time).
    pub reserved_size: usize,

    /// Compression algorithm used.
    pub compression: CompressionAlgorithm,

    /// blake3 hash of the original data.
    pub content_hash: [u8; 32],
}

impl DiskAllocation {
    /// Create a new disk allocation metadata entry.
    pub fn new(
        chunk_id: ChunkId,
        file_path: PathBuf,
        original_size: usize,
        compressed_size: usize,
        compression: CompressionAlgorithm,
        content_hash: [u8; 32],
    ) -> Self {
        let disk_size = HEADER_SIZE + compressed_size;
        Self {
            chunk_id,
            file_path,
            original_size,
            compressed_size,
            disk_size,
            reserved_size: original_size,
            compression,
            content_hash,
        }
    }
}

// ─── Disk Backend ─────────────────────────────────────────────────────────────

/// Persistent disk storage backend.
///
/// Stores chunks as individual files on disk with a deterministic layout.
/// Uses atomic writes (temp file + rename) for crash safety and blake3
/// hashing for integrity verification.
///
/// # Architecture
///
/// This backend is explicitly structured as "SimBackend + persistence":
/// - `simulation: SimBackend` handles latency, pressure, health simulation
/// - `persistence: DiskPersistence` handles actual file I/O
///
/// # Example
///
/// ```
/// use ghost_tier::disk::DiskBackend;
/// use ghost_tier::disk_config::DiskConfig;
/// use ghost_tier::StorageBackend;
///
/// let config = DiskConfig::new("/tmp/ghostpages-test".into(), 1024 * 1024);
/// let backend = DiskBackend::new(config).unwrap();
/// assert_eq!(backend.id(), ghost_core::types::TierId::Disk);
/// ```
pub struct DiskBackend {
    /// Backend identifier (always `TierId::Disk`).
    id: TierId,

    /// Configuration.
    config: DiskConfig,

    /// Total capacity in bytes.
    capacity: usize,

    /// Currently used space in bytes.
    used: Arc<AtomicU64>,

    /// Map of chunk ID to allocation metadata.
    allocations: Arc<Mutex<BTreeMap<ChunkId, DiskAllocation>>>,

    /// Event emitter for observability.
    event_emitter: EventEmitter,

    /// I/O scheduler for deterministic I/O ordering.
    io_scheduler: Arc<Mutex<IoScheduler>>,

    /// Time provider for deterministic or real timing.
    time_provider: Arc<dyn TimeProvider>,

    /// Current queue depth (number of in-flight I/O operations).
    queue_depth: Arc<AtomicU32>,

    /// Total bytes written (for throughput tracking).
    bytes_written: Arc<AtomicU64>,

    /// Total bytes read (for throughput tracking).
    bytes_read: Arc<AtomicU64>,

    // ── SimBackend + Persistence architecture ──

    /// Simulation layer: handles latency, pressure, health simulation.
    /// This is the same SimBackend used by the pure simulation backend.
    simulation: SimBackend,

    /// Persistence layer: handles actual file I/O (atomic writes, corruption detection).
    /// No simulation state — just file I/O.
    persistence: DiskPersistence,
}

impl DiskBackend {
    /// Create a new disk backend with the given configuration.
    ///
    /// Creates the base directory if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns `BackendError::Internal` if the base directory cannot be created.
    pub fn new(config: DiskConfig) -> Result<Self, BackendError> {
        let base_path = &config.base_path;

        // Create base directory if it doesn't exist
        fs::create_dir_all(base_path).map_err(|e| {
            BackendError::Internal(format!(
                "failed to create base directory {}: {}",
                base_path.display(),
                e
            ))
        })?;

        // Create subdirectories for all 256 hex prefixes
        for i in 0u8..=255 {
            let prefix_dir = base_path.join(format!("{:02x}", i));
            fs::create_dir_all(&prefix_dir).map_err(|e| {
                BackendError::Internal(format!(
                    "failed to create prefix directory {}: {}",
                    prefix_dir.display(),
                    e
                ))
            })?;
        }

        let (tx, _rx) = mpsc::channel(256);
        let event_emitter = EventEmitter::new(tx);

        // Create a simple time provider — in production this would be configurable
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(RealTimeProvider::default());

        let io_scheduler = IoScheduler::new(time_provider.clone(), event_emitter.clone(), 64);

        let capacity = config.capacity;

        // Build the simulation layer using SimBackend with matching config
        let sim_config = Self::build_sim_config(&config);
        let simulation = SimBackend::new(sim_config);

        // Build the persistence layer (pure file I/O, no simulation)
        let persistence = DiskPersistence::new(config.base_path.clone());

        Ok(Self {
            id: TierId::Disk,
            config,
            capacity,
            used: Arc::new(AtomicU64::new(0)),
            allocations: Arc::new(Mutex::new(BTreeMap::new())),
            event_emitter,
            io_scheduler: Arc::new(Mutex::new(io_scheduler)),
            time_provider,
            queue_depth: Arc::new(AtomicU32::new(0)),
            bytes_written: Arc::new(AtomicU64::new(0)),
            bytes_read: Arc::new(AtomicU64::new(0)),
            simulation,
            persistence,
        })
    }

    /// Create a new disk backend with a custom event emitter and time provider.
    ///
    /// Useful for testing with deterministic time and event capture.
    pub fn with_emitter(
        config: DiskConfig,
        event_emitter: EventEmitter,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Self, BackendError> {
        let base_path = &config.base_path;

        fs::create_dir_all(base_path).map_err(|e| {
            BackendError::Internal(format!(
                "failed to create base directory {}: {}",
                base_path.display(),
                e
            ))
        })?;

        for i in 0u8..=255 {
            let prefix_dir = base_path.join(format!("{:02x}", i));
            fs::create_dir_all(&prefix_dir).map_err(|e| {
                BackendError::Internal(format!(
                    "failed to create prefix directory {}: {}",
                    prefix_dir.display(),
                    e
                ))
            })?;
        }

        let io_scheduler = IoScheduler::new(time_provider.clone(), event_emitter.clone(), 64);

        let capacity = config.capacity;

        let sim_config = Self::build_sim_config(&config);
        let simulation = SimBackend::new(sim_config);
        let persistence = DiskPersistence::new(config.base_path.clone());

        Ok(Self {
            id: TierId::Disk,
            config,
            capacity,
            used: Arc::new(AtomicU64::new(0)),
            allocations: Arc::new(Mutex::new(BTreeMap::new())),
            event_emitter,
            io_scheduler: Arc::new(Mutex::new(io_scheduler)),
            time_provider,
            queue_depth: Arc::new(AtomicU32::new(0)),
            bytes_written: Arc::new(AtomicU64::new(0)),
            bytes_read: Arc::new(AtomicU64::new(0)),
            simulation,
            persistence,
        })
    }

    /// Build a SimConfig from DiskConfig for the simulation layer.
    ///
    /// This ensures the simulation layer uses the same latency, bandwidth,
    /// and failure injection settings as the disk config.
    fn build_sim_config(config: &DiskConfig) -> SimConfig {
        let failure = FailureConfig {
            write_failure_rate: config.failure.write_failure_rate,
            read_failure_rate: config.failure.read_failure_rate,
            alloc_failure_rate: 0.0, // Disk doesn't inject alloc failures by default
            corruption_on_failure: false,
            corruption_rate: config.failure.corruption_rate,
            timeout_rate: 0.0,
            device_loss_rate: 0.0,
            failure_pattern: FailurePattern::Random,
        };

        SimConfig::with_capacity(config.capacity)
            .with_seed(config.seed.unwrap_or(42))
            .with_latency(crate::sim_config::LatencyConfig { base: config.latency.base, per_byte: config.latency.per_byte, jitter_fraction: config.latency.jitter_fraction })
            .with_bandwidth(BandwidthConfig {
                bytes_per_second: config.bandwidth.bytes_per_second as usize,
            })
            .with_failure(failure)
    }

    /// Compute the file path for a chunk ID.
    ///
    /// Uses a two-level directory structure: `<base_path>/<first_byte_hex>/<chunk_id_hex>.blk`
    pub fn chunk_path(&self, chunk_id: &ChunkId) -> PathBuf {
        self.persistence.chunk_path(chunk_id)
    }

    /// Write a chunk file atomically using temp file + rename.
    ///
    /// Delegates to `DiskPersistence::write_chunk` for the actual file I/O.
    fn write_chunk_file(
        file_path: &Path,
        data: &[u8],
        compression: CompressionAlgorithm,
    ) -> Result<(usize, [u8; 32]), BackendError> {
        let content_hash = *blake3::hash(data).as_bytes();
        let persistence = DiskPersistence::new(file_path.parent().unwrap().to_path_buf());
        let disk_size = persistence.write_chunk(
            &ChunkId::from_data(data),
            data,
            content_hash,
            compression,
        )?;
        Ok((disk_size, content_hash))
    }

    /// Read a chunk file and return the decompressed data.
    ///
    /// Delegates to `DiskPersistence::read_chunk` for the actual file I/O.
    fn read_chunk_file(
        file_path: &Path,
        expected_hash: &[u8; 32],
    ) -> Result<Vec<u8>, BackendError> {
        let persistence = DiskPersistence::new(file_path.parent().unwrap().to_path_buf());
        // We need to derive the chunk_id from the file path for the persistence layer
        // For backward compatibility with tests, we use a hash-based approach
        let chunk_id = ChunkId::from_data(file_path.to_string_lossy().as_bytes());
        persistence.read_chunk(&chunk_id, expected_hash)
    }

    /// Delete a chunk file from disk.
    fn delete_chunk_file(file_path: &Path) -> Result<(), BackendError> {
        fs::remove_file(file_path).map_err(|e| {
            BackendError::Internal(format!(
                "failed to delete chunk file {}: {}",
                file_path.display(),
                e
            ))
        })
    }

    /// Get the current I/O pressure based on usage and queue depth.
    fn calculate_io_pressure(&self) -> f32 {
        let used = self.used.load(Ordering::Relaxed) as f32;
        let capacity = self.capacity as f32;
        let capacity_pressure = if capacity > 0.0 {
            (used / capacity).min(1.0)
        } else {
            0.0
        };

        let queue_depth = self.queue_depth.load(Ordering::Relaxed) as f32;
        let max_queue = self.config.max_queue_depth as f32;
        let queue_pressure = if max_queue > 0.0 {
            (queue_depth / max_queue).min(1.0)
        } else {
            0.0
        };

        // Weighted combination: 40% capacity, 30% queue, 30% bandwidth utilization
        let bandwidth_utilization = if self.config.bandwidth.bytes_per_second > 0 {
            let current_throughput =
                (self.bytes_written.load(Ordering::Relaxed) + self.bytes_read.load(Ordering::Relaxed))
                    as f64;
            // Normalize over a 1-second window (simplified)
            (current_throughput / self.config.bandwidth.bytes_per_second as f64).min(1.0) as f32
        } else {
            0.0
        };

        (0.4 * capacity_pressure + 0.3 * queue_pressure + 0.3 * bandwidth_utilization).min(1.0)
    }

    /// Get a reference to the simulation layer.
    pub fn simulation(&self) -> &SimBackend {
        &self.simulation
    }

    /// Get a reference to the persistence layer.
    pub fn persistence(&self) -> &DiskPersistence {
        &self.persistence
    }
}

#[async_trait]
impl StorageBackend for DiskBackend {
    fn id(&self) -> TierId {
        self.id
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn available(&self) -> usize {
        let used = self.used.load(Ordering::Relaxed) as usize;
        self.capacity.saturating_sub(used)
    }

    async fn allocate(&self, size: usize) -> Result<Allocation, BackendError> {
        if size == 0 {
            return Err(BackendError::Internal(
                "cannot allocate zero bytes".to_string(),
            ));
        }

        // Delegate to simulation layer for latency and failure injection
        self.simulation.simulate_latency(0).await;
        if self.simulation.should_fail("alloc") {
            return Err(BackendError::Internal(
                "simulated allocation failure".to_string(),
            ));
        }

        let used = self.used.load(Ordering::Relaxed) as usize;
        if used + size > self.capacity {
            return Err(BackendError::InsufficientSpace {
                requested: size,
                available: self.capacity - used,
            });
        }

        // Reserve space atomically
        let current = self.used.fetch_add(size as u64, Ordering::SeqCst);
        if current as usize + size > self.capacity {
            // Rollback — capacity exceeded between check and fetch
            self.used.fetch_sub(size as u64, Ordering::SeqCst);
            return Err(BackendError::InsufficientSpace {
                requested: size,
                available: self.capacity - current as usize,
            });
        }

        // Generate a placeholder chunk ID for the allocation
        let chunk_id = ChunkId::from_data(&size.to_le_bytes());
        let file_path = self.persistence.chunk_path(&chunk_id);

        let alloc = DiskAllocation::new(
            chunk_id,
            file_path,
            size,
            0, // compressed_size unknown until write
            CompressionAlgorithm::None,
            [0u8; 32],
        );

        let mut allocations = self.allocations.lock();
        allocations.insert(chunk_id, alloc.clone());

        Ok(Allocation::new(
            0,
            size,
            BackendData::new(alloc),
        ))
    }

    async fn deallocate(&self, allocation: Allocation) -> Result<(), BackendError> {
        // Delegate to simulation layer for latency
        self.simulation.simulate_latency(0).await;

        let disk_alloc = allocation
            .backend_data
            .downcast_ref::<DiskAllocation>()
            .ok_or_else(|| {
                BackendError::Internal("allocation is not a DiskAllocation".to_string())
            })?;

        let chunk_id = disk_alloc.chunk_id;

        // Remove from allocation map
        let mut allocations = self.allocations.lock();
        let removed = allocations.remove(&chunk_id);

        let removed = removed.ok_or_else(|| {
            BackendError::AllocationNotFound(allocation.offset)
        })?;

        // Delete the chunk file via persistence layer
        let file_path = removed.file_path.clone();
        if file_path.exists() {
            Self::delete_chunk_file(&file_path)?;
        }

        // Release capacity
        self.used.fetch_sub(removed.reserved_size as u64, Ordering::SeqCst);

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

        let disk_alloc = allocation
            .backend_data
            .downcast_ref::<DiskAllocation>()
            .ok_or_else(|| {
                BackendError::Internal("allocation is not a DiskAllocation".to_string())
            })?;

        let file_path = disk_alloc.file_path.clone();

        // Determine compression algorithm
        let compression = if data.len() > 1024 {
            CompressionAlgorithm::Zstd
        } else {
            CompressionAlgorithm::None
        };

        let data_len = data.len();

        // Issue I/O request
        let chunk_id = disk_alloc.chunk_id;
        {
            let mut scheduler = self.io_scheduler.lock();
            let _req_id = scheduler.issue(IoOperation::Write, chunk_id, TierId::Disk).map_err(|e| BackendError::Internal(e.to_string()))?;
        }

        // Increment queue depth
        self.queue_depth.fetch_add(1, Ordering::SeqCst);

        // Delegate to simulation layer for latency simulation
        self.simulation.simulate_latency(data.len()).await;

        // Check for failure injection via simulation layer
        if self.simulation.should_fail("write") {
            self.queue_depth.fetch_sub(1, Ordering::SeqCst);
            return Err(BackendError::WriteFailed(
                "simulated write failure".to_string(),
            ));
        }

        // Clone data for spawn_blocking
        let data_clone = data.to_vec();

        // Dispatch file I/O to blocking thread via persistence layer
        let persistence = self.persistence.clone();
        let result = tokio::task::spawn_blocking(move || {
            let content_hash = *blake3::hash(&data_clone).as_bytes();
            persistence.write_chunk(&chunk_id, &data_clone, content_hash, compression)
                .map(|disk_size| (disk_size, content_hash))
        })
        .await
        .map_err(|e| BackendError::WriteFailed(format!("spawn_blocking failed: {}", e)))?;

        // Decrement queue depth
        self.queue_depth.fetch_sub(1, Ordering::SeqCst);

        match result {
            Ok((disk_size, content_hash)) => {
                // Update allocation metadata
                let mut allocations = self.allocations.lock();
                if let Some(alloc) = allocations.get_mut(&chunk_id) {
                    // Adjust used space: subtract old disk_size, add new
                    let old_disk_size = alloc.disk_size;
                    self.used
                        .fetch_sub(old_disk_size as u64, Ordering::SeqCst);
                    self.used.fetch_add(disk_size as u64, Ordering::SeqCst);

                    alloc.compressed_size = disk_size - HEADER_SIZE;
                    alloc.disk_size = disk_size;
                    alloc.compression = compression;
                    alloc.content_hash = content_hash;
                    alloc.original_size = data_len;
                }

                // Track bytes written
                self.bytes_written.fetch_add(disk_size as u64, Ordering::SeqCst);

                // Complete I/O request
                {
                    let mut scheduler = self.io_scheduler.lock();
                    if let Some((&id, _)) = scheduler
                        .pending()
                        .iter()
                        .find(|(_, r)| r.chunk_id == chunk_id && r.operation == IoOperation::Write)
                    {
                        scheduler.complete(id, Ok(()));
                    }
                }

                Ok(())
            }
            Err(e) => {
                // Complete I/O request as failed
                {
                    let mut scheduler = self.io_scheduler.lock();
                    if let Some((&id, _)) = scheduler
                        .pending()
                        .iter()
                        .find(|(_, r)| r.chunk_id == chunk_id && r.operation == IoOperation::Write)
                    {
                        scheduler.complete(id, Err(e.to_string()));
                    }
                }

                // Rollback allocation
                self.used.fetch_sub(allocation.size as u64, Ordering::SeqCst);
                let mut allocations = self.allocations.lock();
                allocations.remove(&chunk_id);

                Err(e)
            }
        }
    }

    async fn read(&self, allocation: &Allocation, buf: &mut [u8]) -> Result<(), BackendError> {
        if buf.len() > allocation.size {
            return Err(BackendError::ReadFailed(format!(
                "buffer size {} exceeds allocation size {}",
                buf.len(),
                allocation.size
            )));
        }

        // Look up the latest allocation metadata from our internal map
        let disk_alloc = allocation
            .backend_data
            .downcast_ref::<DiskAllocation>()
            .ok_or_else(|| {
                BackendError::Internal("allocation is not a DiskAllocation".to_string())
            })?;

        let chunk_id = disk_alloc.chunk_id;

        // Get the latest allocation data (may have been updated by write)
        let disk_alloc = {
            let allocations = self.allocations.lock();
            allocations.get(&chunk_id).cloned()
        };

        let disk_alloc = disk_alloc.ok_or_else(|| {
            BackendError::AllocationNotFound(allocation.offset)
        })?;

        let file_path = disk_alloc.file_path;
        let expected_hash = disk_alloc.content_hash;

        // Issue I/O request
        {
            let mut scheduler = self.io_scheduler.lock();
            let _req_id = scheduler.issue(IoOperation::Read, chunk_id, TierId::Disk).map_err(|e| BackendError::Internal(e.to_string()))?;
        }

        self.queue_depth.fetch_add(1, Ordering::SeqCst);

        // Delegate to simulation layer for latency simulation
        self.simulation.simulate_latency(buf.len()).await;

        // Check for failure injection via simulation layer
        if self.simulation.should_fail("read") {
            self.queue_depth.fetch_sub(1, Ordering::SeqCst);
            return Err(BackendError::ReadFailed(
                "simulated read failure".to_string(),
            ));
        }

        // Dispatch file I/O to blocking thread via persistence layer
        let persistence = self.persistence.clone();
        let result = tokio::task::spawn_blocking(move || {
            persistence.read_chunk(&chunk_id, &expected_hash)
        })
        .await
        .map_err(|e| BackendError::ReadFailed(format!("spawn_blocking failed: {}", e)))?;

        self.queue_depth.fetch_sub(1, Ordering::SeqCst);

        match result {
            Ok(data) => {
                let len = buf.len().min(data.len());
                buf[..len].copy_from_slice(&data[..len]);

                self.bytes_read.fetch_add(data.len() as u64, Ordering::SeqCst);

                // Complete I/O request
                {
                    let mut scheduler = self.io_scheduler.lock();
                    if let Some((&id, _)) = scheduler
                        .pending()
                        .iter()
                        .find(|(_, r)| r.chunk_id == chunk_id && r.operation == IoOperation::Read)
                    {
                        scheduler.complete(id, Ok(()));
                    }
                }

                Ok(())
            }
            Err(e) => {
                // Complete I/O request as failed
                {
                    let mut scheduler = self.io_scheduler.lock();
                    if let Some((&id, _)) = scheduler
                        .pending()
                        .iter()
                        .find(|(_, r)| r.chunk_id == chunk_id && r.operation == IoOperation::Read)
                    {
                        scheduler.complete(id, Err(e.to_string()));
                    }
                }
                Err(e)
            }
        }
    }

    async fn verify_integrity(
        &self,
        allocation: &Allocation,
        expected: &[u8; 32],
    ) -> Result<(), BackendError> {
        // Delegate to simulation layer for latency
        self.simulation.simulate_latency(0).await;

        // Look up the latest allocation metadata from our internal map
        let disk_alloc = allocation
            .backend_data
            .downcast_ref::<DiskAllocation>()
            .ok_or_else(|| {
                BackendError::Internal("allocation is not a DiskAllocation".to_string())
            })?;

        let chunk_id = disk_alloc.chunk_id;

        let disk_alloc = {
            let allocations = self.allocations.lock();
            allocations.get(&chunk_id).cloned()
        };

        let disk_alloc = disk_alloc.ok_or_else(|| {
            BackendError::AllocationNotFound(allocation.offset)
        })?;

        // Check stored hash matches expected
        if &disk_alloc.content_hash != expected {
            return Err(BackendError::IntegrityFailed(format!(
                "stored hash {} does not match expected {}",
                hex::encode(disk_alloc.content_hash),
                hex::encode(expected)
            )));
        }

        // Verify file exists and can be read
        let file_path = &disk_alloc.file_path;
        if !file_path.exists() {
            return Err(BackendError::IntegrityFailed(format!(
                "chunk file {} does not exist",
                file_path.display()
            )));
        }

        // Read and verify the file content via persistence layer
        let stored_hash = disk_alloc.content_hash;
        let persistence = self.persistence.clone();
        let result = tokio::task::spawn_blocking(move || {
            persistence.read_chunk(&chunk_id, &stored_hash)
        })
        .await
        .map_err(|e| {
            BackendError::IntegrityFailed(format!("spawn_blocking failed: {}", e))
        })?;

        match result {
            Ok(data) => {
                let actual_hash = *blake3::hash(&data).as_bytes();
                if &actual_hash != expected {
                    Err(BackendError::IntegrityFailed(format!(
                        "content hash mismatch: expected {}, got {}",
                        hex::encode(expected),
                        hex::encode(actual_hash)
                    )))
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(BackendError::IntegrityFailed(format!(
                "read failed during integrity check: {}",
                e
            ))),
        }
    }

    async fn health_check(&self) -> Result<(), BackendError> {
        // Delegate to simulation layer for latency
        self.simulation.simulate_latency(0).await;

        // Check base directory exists and is accessible
        let base_path = &self.config.base_path;
        if !base_path.exists() {
            return Err(BackendError::Unhealthy(format!(
                "base directory {} does not exist",
                base_path.display()
            )));
        }

        if !base_path.is_dir() {
            return Err(BackendError::Unhealthy(format!(
                "base path {} is not a directory",
                base_path.display()
            )));
        }

        // Try a small write/read to verify I/O works
        let test_file = base_path.join(".ghostpages_health_check");
        fs::write(&test_file, b"health_check").map_err(|e| {
            BackendError::Unhealthy(format!(
                "cannot write to base directory {}: {}",
                base_path.display(),
                e
            ))
        })?;

        fs::read(&test_file).map_err(|e| {
            BackendError::Unhealthy(format!(
                "cannot read from base directory {}: {}",
                base_path.display(),
                e
            ))
        })?;

        fs::remove_file(&test_file).map_err(|e| {
            BackendError::Unhealthy(format!(
                "cannot delete from base directory {}: {}",
                base_path.display(),
                e
            ))
        })?;

        Ok(())
    }

    fn pressure(&self) -> PressureState {
        // Delegate memory pressure calculation to simulation layer
        let memory_pressure = self.simulation.memory_pressure() as f32;
        let io_pressure = self.calculate_io_pressure();
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);

        // Estimate throughput
        let throughput = (self.bytes_written.load(Ordering::Relaxed)
            + self.bytes_read.load(Ordering::Relaxed)) as u64;

        PressureState {
            memory_pressure,
            vram_pressure: 0.0,
            io_pressure,
            queue_depth,
            throughput_bps: throughput,
        }
    }

    fn cost_model(&self) -> PhysicalCost {
        // Delegate to simulation layer's cost model as the base,
        // then overlay disk-specific values
        let io_pressure = self.calculate_io_pressure();
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);

        PhysicalCost {
            latency_ms: self.config.latency.base.as_secs_f64() * 1000.0,
            bandwidth_bps: self.config.bandwidth.bytes_per_second as f64,
            reliability: 1.0 - self.config.failure.write_failure_rate,
            io_pressure,
            queue_depth,
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> DiskConfig {
        DiskConfig::new(dir.path().to_path_buf(), 10 * 1024 * 1024) // 10 MB
    }

    #[tokio::test]
    async fn test_disk_backend_basic_store_and_retrieve() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();

        // Allocate space
        let alloc = backend.allocate(128).await.unwrap();
        assert_eq!(alloc.size, 128);

        // Write data
        let data = b"Hello, GhostPages Disk Backend!";
        backend.write(&alloc, data).await.unwrap();

        // Read data back
        let mut buf = vec![0u8; data.len()];
        backend.read(&alloc, &mut buf).await.unwrap();
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_disk_backend_capacity_tracking() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();

        assert_eq!(backend.capacity(), 10 * 1024 * 1024);
        assert_eq!(backend.available(), 10 * 1024 * 1024);

        let alloc1 = backend.allocate(1000).await.unwrap();
        assert!(backend.available() < 10 * 1024 * 1024);

        let alloc2 = backend.allocate(2000).await.unwrap();
        let available_after_two = backend.available();
        assert!(available_after_two < backend.capacity());

        // Deallocate first
        backend.deallocate(alloc1).await.unwrap();
        assert!(backend.available() > available_after_two);

        // Deallocate second
        backend.deallocate(alloc2).await.unwrap();
        assert_eq!(backend.available(), backend.capacity());
    }

    #[tokio::test]
    async fn test_disk_backend_integrity_verification() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();

        let data = b"integrity test data for disk backend";
        let alloc = backend.allocate(data.len()).await.unwrap();
        backend.write(&alloc, data).await.unwrap();

        // Compute expected hash
        let expected_hash = *blake3::hash(data).as_bytes();

        // Should pass integrity check
        backend
            .verify_integrity(&alloc, &expected_hash)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_disk_backend_health_check() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();
        backend.health_check().await.unwrap();
    }

    #[tokio::test]
    async fn test_disk_backend_zero_allocation_fails() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();
        let result = backend.allocate(0).await;
        assert!(matches!(result, Err(BackendError::Internal(_))));
    }

    #[tokio::test]
    async fn test_disk_backend_write_exceeds_allocation() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();
        let alloc = backend.allocate(10).await.unwrap();
        let data = vec![0u8; 20];
        let result = backend.write(&alloc, &data).await;
        assert!(matches!(result, Err(BackendError::WriteFailed(_))));
    }

    #[tokio::test]
    async fn test_disk_backend_id() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();
        assert_eq!(backend.id(), TierId::Disk);
    }

    #[tokio::test]
    async fn test_disk_backend_chunk_path_deterministic() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();

        let chunk_id = ChunkId::from_data(b"deterministic test");
        let path1 = backend.chunk_path(&chunk_id);
        let path2 = backend.chunk_path(&chunk_id);
        assert_eq!(path1, path2);
        assert!(path1.starts_with(dir.path()));
        assert!(path1.to_string_lossy().ends_with(".blk"));
    }

    #[tokio::test]
    async fn test_disk_backend_pressure_reporting() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();

        let pressure = backend.pressure();
        assert_eq!(pressure.vram_pressure, 0.0);
        assert_eq!(pressure.queue_depth, 0);
    }

    #[tokio::test]
    async fn test_disk_backend_cost_model() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();

        let cost = backend.cost_model();
        assert!(cost.latency_ms > 0.0);
        assert!(cost.bandwidth_bps > 0.0);
        assert!(cost.reliability > 0.0);
    }

    #[test]
    fn test_chunk_file_format_roundtrip() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.blk");
        let data = b"Hello, chunk file format test!";

        let (disk_size, hash) =
            DiskBackend::write_chunk_file(&file_path, data, CompressionAlgorithm::None).unwrap();

        assert!(disk_size > HEADER_SIZE);
        assert!(file_path.exists());

        let read_data =
            DiskBackend::read_chunk_file(&file_path, &blake3::hash(data).as_bytes()).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_chunk_file_format_compressed() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test_compressed.blk");
        let data = vec![b'A'; 4096];

        let (disk_size, hash) =
            DiskBackend::write_chunk_file(&file_path, &data, CompressionAlgorithm::Zstd).unwrap();

        // Compressed data should be smaller than original
        assert!(disk_size < HEADER_SIZE + data.len());

        let read_data =
            DiskBackend::read_chunk_file(&file_path, &blake3::hash(&data).as_bytes()).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_chunk_file_invalid_magic() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("invalid.blk");

        // Write garbage data
        fs::write(&file_path, b"not a valid chunk file at all").unwrap();

        let result =
            DiskBackend::read_chunk_file(&file_path, &[0u8; 32]);
        assert!(matches!(result, Err(BackendError::ReadFailed(_))));
    }

    #[test]
    fn test_chunk_file_too_small() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("small.blk");

        // Write less than header size
        fs::write(&file_path, b"tiny").unwrap();

        let result =
            DiskBackend::read_chunk_file(&file_path, &[0u8; 32]);
        assert!(matches!(result, Err(BackendError::ReadFailed(_))));
    }

    #[tokio::test]
    async fn test_disk_backend_simulation_persistence_access() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = DiskBackend::new(config).unwrap();

        // Verify both layers are accessible
        assert_eq!(backend.simulation().id(), TierId::Simulation);
        assert_eq!(backend.persistence().chunk_path(&ChunkId::from_data(b"test")).extension().unwrap(), "blk");
    }
}
