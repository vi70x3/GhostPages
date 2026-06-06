//! Disk/Sim parity verification tests.
//!
//! These tests verify the "DiskBackend = SimBackend + persistence" architecture:
//! 1. DiskBackend produces identical event sequences to SimBackend for the same workload
//! 2. DiskPersistence works without SimBackend (direct file I/O)
//! 3. SimBackend works without DiskPersistence (pure simulation)

use ghost_tier::backend::{Allocation, BackendError, StorageBackend};
use ghost_tier::disk::DiskBackend;
use ghost_tier::disk_config::DiskConfig;
use ghost_tier::disk_persistence::DiskPersistence;
use ghost_tier::sim_config::SimConfig;
use ghost_tier::SimBackend;

use tempfile::TempDir;

// ─── Test 1: DiskBackend produces identical event sequences to SimBackend ───

/// Verify DiskBackend produces identical event sequences to SimBackend for the same workload.
///
/// This test runs the same sequence of operations against both backends and
/// verifies that the observable results (allocation sizes, read data, error types)
/// are identical.
#[tokio::test]
async fn test_disk_is_sim_plus_persistence() {
    let capacity = 1024 * 1024;
    let sim = SimBackend::new(SimConfig::with_capacity(capacity).with_seed(42));
    let dir = TempDir::new().unwrap();
    let disk = DiskBackend::new(
        DiskConfig::new(dir.path().to_path_buf(), capacity).with_seed(42),
    )
    .unwrap();

    // Phase 1: Allocate in both
    let sim_alloc = sim.allocate(256).await.unwrap();
    let disk_alloc = disk.allocate(256).await.unwrap();
    assert_eq!(sim_alloc.size, disk_alloc.size, "allocation sizes must match");

    // Phase 2: Write same data in both
    let data = b"disk = sim + persistence";
    sim.write(&sim_alloc, data).await.unwrap();
    disk.write(&disk_alloc, data).await.unwrap();

    // Phase 3: Read back from both — data must be identical
    let mut sim_buf = vec![0u8; data.len()];
    let mut disk_buf = vec![0u8; data.len()];
    sim.read(&sim_alloc, &mut sim_buf).await.unwrap();
    disk.read(&disk_alloc, &mut disk_buf).await.unwrap();
    assert_eq!(sim_buf, disk_buf, "read data must be identical");
    assert_eq!(sim_buf, data, "data must match original");

    // Phase 4: Verify integrity in both
    let expected_hash = *blake3::hash(data).as_bytes();
    sim.verify_integrity(&sim_alloc, &expected_hash)
        .await
        .unwrap();
    disk.verify_integrity(&disk_alloc, &expected_hash)
        .await
        .unwrap();

    // Phase 5: Deallocate in both
    sim.deallocate(sim_alloc).await.unwrap();
    disk.deallocate(disk_alloc).await.unwrap();

    // Phase 6: Both should have full capacity available
    assert_eq!(sim.available(), capacity);
    assert_eq!(disk.available(), capacity);
}

/// Verify that DiskBackend's simulation layer and persistence layer are both accessible.
#[tokio::test]
async fn test_disk_backend_exposes_layers() {
    let dir = TempDir::new().unwrap();
    let disk = DiskBackend::new(
        DiskConfig::new(dir.path().to_path_buf(), 1024 * 1024).with_seed(42),
    )
    .unwrap();

    // Simulation layer should be accessible and functional
    let sim = disk.simulation();
    assert_eq!(sim.id(), ghost_core::types::TierId::Simulation);
    assert_eq!(sim.capacity(), 1024 * 1024);

    // Persistence layer should be accessible and functional
    let persistence = disk.persistence();
    let chunk_id = ghost_core::types::ChunkId::from_data(b"test");
    assert!(!persistence.chunk_exists(&chunk_id));
}

/// Verify that DiskBackend delegates pressure calculation to the simulation layer.
#[tokio::test]
async fn test_disk_pressure_delegation() {
    let capacity = 10000;
    let sim = SimBackend::new(SimConfig::with_capacity(capacity).with_seed(42));
    let dir = TempDir::new().unwrap();
    let disk = DiskBackend::new(
        DiskConfig::new(dir.path().to_path_buf(), capacity).with_seed(42),
    )
    .unwrap();

    // Fill both to 50%
    let half = capacity / 2;
    let sim_alloc = sim.allocate(half).await.unwrap();
    let disk_alloc = disk.allocate(half).await.unwrap();

    // Both should report ~50% memory pressure
    let sim_pressure = sim.pressure();
    let disk_pressure = disk.pressure();

    assert!(
        (sim_pressure.memory_pressure - disk_pressure.memory_pressure).abs() < 0.02,
        "memory pressure should be similar: sim={}, disk={}",
        sim_pressure.memory_pressure,
        disk_pressure.memory_pressure
    );

    sim.deallocate(sim_alloc).await.unwrap();
    disk.deallocate(disk_alloc).await.unwrap();
}

// ─── Test 2: Persistence layer isolation ─────────────────────────────────────

/// Verify DiskPersistence works without SimBackend (direct file I/O).
#[tokio::test]
async fn test_persistence_layer_isolation() {
    let dir = TempDir::new().unwrap();
    let persistence = DiskPersistence::new(dir.path().to_path_buf());

    let chunk_id = ghost_core::types::ChunkId::from_data(b"isolation test");
    let data = b"persistence layer works independently";
    let hash = *blake3::hash(data).as_bytes();

    // Write directly via persistence layer
    let disk_size = persistence
        .write_chunk(&chunk_id, data, hash, ghost_core::types::CompressionAlgorithm::None)
        .unwrap();
    assert!(disk_size > 0);
    assert!(persistence.chunk_exists(&chunk_id));

    // Read back
    let read_data = persistence.read_chunk(&chunk_id, &hash).unwrap();
    assert_eq!(read_data, data);

    // Delete
    persistence.delete_chunk(&chunk_id).unwrap();
    assert!(!persistence.chunk_exists(&chunk_id));
}

/// Verify DiskPersistence works with compression.
#[tokio::test]
async fn test_persistence_layer_compression() {
    let dir = TempDir::new().unwrap();
    let persistence = DiskPersistence::new(dir.path().to_path_buf());

    let chunk_id = ghost_core::types::ChunkId::from_data(b"compression test");
    let data = vec![b'X'; 4096]; // Highly compressible
    let hash = *blake3::hash(&data).as_bytes();

    // Write with compression
    let disk_size = persistence
        .write_chunk(&chunk_id, &data, hash, ghost_core::types::CompressionAlgorithm::Zstd)
        .unwrap();

    // Compressed size should be much smaller than original
    assert!(
        disk_size < data.len(),
        "compressed size {} should be less than original {}",
        disk_size,
        data.len()
    );

    // Read back and verify
    let read_data = persistence.read_chunk(&chunk_id, &hash).unwrap();
    assert_eq!(read_data, data);

    persistence.delete_chunk(&chunk_id).unwrap();
}

/// Verify DiskPersistence detects corruption.
#[tokio::test]
async fn test_persistence_layer_corruption_detection() {
    let dir = TempDir::new().unwrap();
    let persistence = DiskPersistence::new(dir.path().to_path_buf());

    let chunk_id = ghost_core::types::ChunkId::from_data(b"corruption test");
    let data = b"original data";
    let hash = *blake3::hash(data).as_bytes();

    persistence
        .write_chunk(&chunk_id, data, hash, ghost_core::types::CompressionAlgorithm::None)
        .unwrap();

    // Try to read with wrong hash — should fail with IntegrityFailed
    let wrong_hash = [0xFFu8; 32];
    let result = persistence.read_chunk(&chunk_id, &wrong_hash);
    assert!(
        matches!(result, Err(BackendError::IntegrityFailed(_))),
        "should detect hash mismatch"
    );

    persistence.delete_chunk(&chunk_id).unwrap();
}

// ─── Test 3: Simulation layer isolation ──────────────────────────────────────

/// Verify SimBackend works without DiskPersistence (pure simulation).
#[tokio::test]
async fn test_simulation_layer_isolation() {
    let sim = SimBackend::new(SimConfig::with_capacity(1024 * 1024).with_seed(42));

    // Allocate
    let alloc = sim.allocate(128).await.unwrap();
    assert_eq!(alloc.size, 128);

    // Write
    let data = b"pure simulation, no persistence";
    sim.write(&alloc, data).await.unwrap();

    // Read
    let mut buf = vec![0u8; data.len()];
    sim.read(&alloc, &mut buf).await.unwrap();
    assert_eq!(&buf, data);

    // Verify integrity
    let expected_hash = *blake3::hash(data).as_bytes();
    sim.verify_integrity(&alloc, &expected_hash).await.unwrap();

    // Health check
    sim.health_check().await.unwrap();

    // Pressure
    let pressure = sim.pressure();
    assert!(pressure.memory_pressure >= 0.0 && pressure.memory_pressure <= 1.0);

    // Cost model
    let cost = sim.cost_model();
    assert!(cost.latency_ms > 0.0);
    assert!(cost.bandwidth_bps > 0.0);

    // Deallocate
    sim.deallocate(alloc).await.unwrap();
    assert_eq!(sim.available(), 1024 * 1024);
}

/// Verify SimBackend is deterministic (same seed = same behavior).
#[tokio::test]
async fn test_simulation_determinism() {
    let config1 = SimConfig::with_capacity(1024 * 1024).with_seed(12345);
    let config2 = SimConfig::with_capacity(1024 * 1024).with_seed(12345);

    let sim1 = SimBackend::new(config1);
    let sim2 = SimBackend::new(config2);

    let data = b"determinism test";

    let alloc1 = sim1.allocate(data.len()).await.unwrap();
    let alloc2 = sim2.allocate(data.len()).await.unwrap();

    sim1.write(&alloc1, data).await.unwrap();
    sim2.write(&alloc2, data).await.unwrap();

    // Both should have the same metrics
    assert_eq!(sim1.metrics().alloc_count(), sim2.metrics().alloc_count());
    assert_eq!(sim1.metrics().write_count(), sim2.metrics().write_count());
    assert_eq!(sim1.metrics().bytes_written(), sim2.metrics().bytes_written());

    // Read back and verify
    let mut buf1 = vec![0u8; data.len()];
    let mut buf2 = vec![0u8; data.len()];
    sim1.read(&alloc1, &mut buf1).await.unwrap();
    sim2.read(&alloc2, &mut buf2).await.unwrap();
    assert_eq!(buf1, buf2);

    sim1.deallocate(alloc1).await.unwrap();
    sim2.deallocate(alloc2).await.unwrap();
}
