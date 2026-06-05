# Disk Tier Contract Specification

**Phase 2.5 — Contract Spec (No Implementation)**

This document defines the complete interface contract for the `DiskBackend` — a future storage backend for GhostPages. It is a **design document only** and contains no code changes.

---

## Table of Contents

1. [Interface Contract](#1-interface-contract)
2. [Latency Model](#2-latency-model)
3. [Failure Modes](#3-failure-modes)
4. [IO Pressure Reporting](#4-io-pressure-reporting)
5. [Migration Boundary Rules](#5-migration-boundary-rules)
6. [Non-Deterministic IO Timing](#6-non-deterministic-io-timing)
7. [Configuration](#7-configuration)
8. [Integration Points](#8-integration-points)
9. [Phase 3 Implementation Checklist](#9-phase-3-implementation-checklist)

---

## 1. Interface Contract

### 1.1 Trait Requirement

`DiskBackend` **MUST** implement the [`StorageBackend`](crates/ghost-tier/src/backend.rs:114) trait. This trait is the core abstraction for all storage tiers in GhostPages and defines the following required methods:

| Method | Signature | Purpose |
|--------|-----------|---------|
| `id()` | `fn id(&self) -> TierId` | Returns `TierId::Disk` |
| `capacity()` | `fn capacity(&self) -> usize` | Total disk space in bytes |
| `available()` | `fn available(&self) -> usize` | Remaining disk space in bytes |
| `allocate()` | `async fn allocate(&self, size: usize) -> Result<Allocation, BackendError>` | Reserve space on disk |
| `deallocate()` | `async fn deallocate(&self, allocation: Allocation) -> Result<(), BackendError>` | Free disk space |
| `write()` | `async fn write(&self, allocation: &Allocation, data: &[u8]) -> Result<(), BackendError>` | Write data to disk |
| `read()` | `async fn read(&self, allocation: &Allocation, buf: &mut [u8]) -> Result<(), BackendError>` | Read data from disk |
| `verify_integrity()` | `async fn verify_integrity(&self, allocation: &Allocation, expected: &[u8; 32]) -> Result<(), BackendError>` | Verify blake3 hash |
| `health_check()` | `async fn health_check(&self) -> Result<(), BackendError>` | Check disk accessibility |
| `pressure()` | `fn pressure(&self) -> PressureState` | Report current IO pressure |

### 1.2 Concurrency Model

Implementations must be `Send + Sync + 'static`. The trait uses `async-trait` so all methods are async. Implementations should minimize lock holding across `.await` points.

**Disk-specific concurrency considerations:**
- File I/O operations are inherently blocking; they **MUST** be dispatched to a blocking thread pool (e.g., `tokio::task::spawn_blocking`) to avoid blocking the async runtime.
- Internal state (allocation map, queue depth counters) should use `parking_lot::Mutex` for synchronous access.
- The `pressure()` method is synchronous and lock-free (reads atomic counters).

### 1.3 Latency Model

The DiskBackend has a configurable latency model that simulates real disk behavior:

- **HDD Profile**: 1–10ms average latency, high variance, rotational delay simulation
- **SSD Profile**: 0.1–1ms average latency, low variance
- **NVMe Profile**: 0.01–0.1ms average latency, minimal variance

Latency is composed of:
```
total_latency = base_latency + (size * per_byte_latency) + jitter + queue_penalty
```

Where `queue_penalty` increases with queue depth (see [Section 2](#2-latency-model)).

### 1.4 Throughput Model

The DiskBackend bounds throughput via:
- **IOPS limit**: Maximum operations per second (e.g., 10K for HDD, 100K for SSD, 500K for NVMe)
- **Bandwidth limit**: Maximum bytes per second (e.g., 200 MB/s for HDD, 500 MB/s for SSD, 3 GB/s for NVMe)

When either limit is reached, operations are queued or rejected based on configuration.

### 1.5 Failure Modes

All failure modes are injectable via [`FailureConfig`](crates/ghost-sim/src/config.rs:64) (existing pattern from SimBackend). See [Section 3](#3-failure-modes) for details.

### 1.6 Pressure Reporting

The `pressure()` method returns a [`PressureState`](crates/ghost-core/src/state.rs:259) with `io_pressure` as the primary dimension. See [Section 4](#4-io-pressure-reporting) for details.

---

## 2. Latency Model

### 2.1 Latency Distribution

The DiskBackend supports configurable latency distributions:

```rust
/// Latency distribution strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyDistribution {
    /// Uniform distribution between min and max.
    Uniform,
    /// Normal (Gaussian) distribution with mean and stddev.
    Normal,
    /// Custom distribution via a user-provided CDF.
    Custom,
}
```

### 2.2 Disk Type Profiles

```rust
/// Disk hardware type profiles with default latency characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskType {
    /// Traditional hard disk drive (high latency, high variance).
    Hdd,
    /// Solid-state drive (low latency, low variance).
    Ssd,
    /// NVMe SSD (very low latency, minimal variance).
    Nvme,
}

impl DiskType {
    /// Default latency range for this disk type.
    pub fn default_latency_range(&self) -> (std::time::Duration, std::time::Duration) {
        match self {
            DiskType::Hdd => (
                std::time::Duration::from_millis(1),
                std::time::Duration::from_millis(10),
            ),
            DiskType::Ssd => (
                std::time::Duration::from_micros(100),
                std::time::Duration::from_millis(1),
            ),
            DiskType::Nvme => (
                std::time::Duration::from_micros(10),
                std::time::Duration::from_micros(100),
            ),
        }
    }

    /// Default IOPS limit for this disk type.
    pub fn default_iops(&self) -> u32 {
        match self {
            DiskType::Hdd => 10_000,
            DiskType::Ssd => 100_000,
            DiskType::Nvme => 500_000,
        }
    }

    /// Default bandwidth limit for this disk type (bytes per second).
    pub fn default_bandwidth(&self) -> usize {
        match self {
            DiskType::Hdd => 200 * 1024 * 1024,      // 200 MB/s
            DiskType::Ssd => 500 * 1024 * 1024,      // 500 MB/s
            DiskType::Nvme => 3 * 1024 * 1024 * 1024, // 3 GB/s
        }
    }
}
```

### 2.3 Latency Under Queue Pressure

Latency increases with queue depth to model real disk behavior:

```
queue_penalty = base_queue_penalty * (queue_depth / max_queue_depth)^exponent
```

Where:
- `base_queue_penalty` is a configurable multiplier (default: 2x base latency)
- `exponent` controls how aggressively latency grows (default: 1.5)
- `max_queue_depth` is the configured maximum concurrent operations

### 2.4 Spike Injection

For testing, the DiskBackend supports latency spike injection:

```rust
/// Configuration for latency spike injection.
#[derive(Debug, Clone)]
pub struct SpikeConfig {
    /// Probability of a spike occurring per operation (0.0 to 1.0).
    pub spike_probability: f64,
    /// Multiplier applied to base latency during a spike.
    pub spike_multiplier: f64,
    /// Duration of each spike (in simulated or real time).
    pub spike_duration: std::time::Duration,
    /// RNG seed for deterministic spike patterns.
    pub seed: Option<u64>,
}
```

---

## 3. Failure Modes

### 3.1 Disk-Specific Error Variants

The existing [`BackendError`](crates/ghost-tier/src/backend.rs:63) enum should be extended with disk-specific variants:

```rust
/// Additional backend error variants for disk-specific failures.
#[derive(Debug, thiserror::Error)]
pub enum DiskBackendError {
    /// Disk capacity has been exceeded.
    #[error("disk full: requested {requested}, available {available}")]
    DiskFull {
        requested: usize,
        available: usize,
    },

    /// IO error during read/write operation.
    #[error("IO error: {0}")]
    IoError(String),

    /// Operation exceeded maximum duration.
    #[error("operation timed out after {duration:?}")]
    Timeout {
        duration: std::time::Duration,
    },

    /// Permission denied for the requested operation.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Data read back differs from what was written (corruption detected).
    #[error("data corruption detected at offset {offset}: expected hash {expected}, got {actual}")]
    Corruption {
        offset: usize,
        expected: String,
        actual: String,
    },

    /// Filesystem-level error (e.g., read-only filesystem, too many open files).
    #[error("filesystem error: {0}")]
    FilesystemError(String),
}
```

**Note:** These variants should be folded into the existing `BackendError` enum or a `DiskBackendError` type that maps to `BackendError`. The preferred approach is to extend `BackendError` with these variants, maintaining backward compatibility.

### 3.2 Failure Injection via FailureConfig

The existing [`FailureConfig`](crates/ghost-sim/src/config.rs:64) struct should be reused with the following disk-specific interpretations:

| Field | Disk Interpretation |
|-------|-------------------|
| `write_failure_rate` | Probability of a write operation failing with `IoError` |
| `read_failure_rate` | Probability of a read operation failing with `IoError` |
| `alloc_failure_rate` | Probability of allocation failing with `DiskFull` |
| `corruption_rate` | Probability of silent data corruption on write |
| `timeout_rate` | Probability of an operation exceeding its deadline |
| `failure_pattern` | Pattern of failure injection (random, burst, degrading, etc.) |

### 3.3 DiskFull Behavior

When `DiskFull` is triggered:
1. The allocation fails with `BackendError::InsufficientSpace` (existing variant) or `DiskBackendError::DiskFull`
2. The `io_pressure` dimension in `PressureState` is set to 1.0
3. The `HealthTracker` records a failure for the `TierId::Disk` tier
4. The `MigrationEngine` is notified that disk is full and should trigger eviction

### 3.4 Corruption Detection

Corruption is detected via:
1. **Write path**: After writing data, compute blake3 hash and store it in the allocation metadata
2. **Read path**: After reading data, compute blake3 hash and compare against stored hash
3. **Background verification**: Periodic integrity checks on stored data

When corruption is detected:
1. The read fails with `DiskBackendError::Corruption`
2. The `HealthTracker` records a failure
3. The affected chunk is marked as `ChunkState::Failed`
4. The system attempts to recover from a higher-tier copy (RAM or GPU VRAM)

---

## 4. IO Pressure Reporting

### 4.1 PressureState Integration

The `pressure()` method returns a [`PressureState`](crates/ghost-core/src/state.rs:259) with the following disk-specific fields:

```rust
PressureState {
    memory_pressure: 0.0,  // Not applicable for disk
    vram_pressure: 0.0,    // Not applicable for disk
    io_pressure: <calculated>,  // Primary dimension for disk
    queue_depth: <current>,    // Current number of queued IO operations
    throughput_bps: <current>,  // Current throughput in bytes/sec
}
```

### 4.2 IO Pressure Calculation

`io_pressure` is calculated as a weighted combination of factors:

```rust
/// Calculate IO pressure for the disk backend.
fn calculate_io_pressure(&self) -> f32 {
    let capacity_pressure = if self.capacity > 0 {
        (self.used as f32) / (self.capacity as f32)
    } else {
        0.0
    };

    let queue_pressure = if self.max_queue_depth > 0 {
        (self.current_queue_depth as f32) / (self.max_queue_depth as f32)
    } else {
        0.0
    };

    let utilization_pressure = self.current_throughput / self.max_throughput;

    // Weighted combination
    0.4 * capacity_pressure + 0.3 * queue_pressure + 0.3 * utilization_pressure
}
```

### 4.3 PressureMonitor EMA Smoothing

The DiskBackend integrates with the existing [`PressureMonitor`](crates/ghost-daemon/src/pressure.rs:205) EMA smoothing:

1. The `PressureMonitor` calls `backend.pressure()` on all backends at the configured `sample_interval_ms`
2. The `PressureMonitor` applies EMA smoothing with the configured `smoothing_factor` (alpha)
3. The smoothed `io_pressure` is stored in the global `PressureState`
4. The `PressureMonitor` records history entries and detects pressure spikes

**No changes are needed** to the `PressureMonitor` itself — it already handles arbitrary backends via the `StorageBackend` trait.

### 4.4 Pressure Thresholds

| Threshold | Value | Action |
|-----------|-------|--------|
| Normal | io_pressure < 0.7 | Normal operation |
| Under Pressure | io_pressure >= 0.7 | Trigger cold eviction from disk |
| Critical | io_pressure >= 0.9 | Aggressive eviction, reject new allocations |

---

## 5. Migration Boundary Rules

### 5.1 RAM → Disk Demotion

**Trigger**: When `memory_pressure` exceeds the configured threshold (default: 0.8) and `io_pressure` is below the critical threshold (default: 0.9).

**Policy**:
1. Identify cold chunks in RAM using `HotnessTracker::find_cold_chunks()`
2. Sort by hotness score (coldest first)
3. For each cold chunk:
   - Allocate space on disk
   - Copy data from RAM to disk
   - Verify integrity via blake3 hash
   - Deallocate from RAM
   - Update `ChunkMeta.tier` to `TierId::Disk`
   - Update `ChunkState` via `StateMachine`

**Constraints**:
- Do not demote chunks currently being accessed (check `access_count` rate)
- Do not demote chunks that are `ChunkState::Migrating`
- Rate-limit demotions to avoid IO saturation

### 5.2 Disk → RAM Promotion

**Trigger**: When a chunk on disk is accessed frequently (hotness score exceeds `hot_threshold`).

**Policy**:
1. Identify hot chunks on disk using `HotnessTracker::find_hot_chunks()`
2. Sort by hotness score (hottest first)
3. For each hot chunk:
   - Allocate space in RAM
   - Copy data from disk to RAM
   - Verify integrity via blake3 hash
   - Deallocate from disk
   - Update `ChunkMeta.tier` to `TierId::Ram`
   - Update `ChunkState` via `StateMachine`

**Constraints**:
- Only promote if `memory_pressure` is below the promotion threshold (default: 0.7)
- Do not promote if RAM is under pressure (would cause cascading evictions)
- Rate-limit promotions to avoid IO saturation

### 5.3 Disk Cold Eviction

**Trigger**: When `io_pressure` exceeds the critical threshold (default: 0.9) or `DiskFull` is reported.

**Policy**:
1. Identify the coldest chunks on disk using `HotnessTracker::find_cold_chunks()`
2. For each cold chunk:
   - If a copy exists in a higher tier (RAM or GPU VRAM), evict from disk (data is safe elsewhere)
   - If no copy exists, attempt to write to a slower tier (if configured) or fail
   - Update `ChunkState` to `ChunkState::Evicted`
   - Deallocate disk space

**Constraints**:
- Never evict the only copy of a chunk unless explicitly configured to do so
- Evict in batches to avoid IO spikes
- Log all evictions via `TraceLog::Eviction`

### 5.4 Disk Is NOT a Migration Source for Vulkan

**Hard rule**: The `PlacementPolicy` **MUST NOT** select `TierId::Disk` as a source tier for migration to `TierId::GpuVram`. Disk → GPU VRAM migration is prohibited because:

1. Disk latency (1–10ms) is orders of magnitude higher than GPU VRAM access (nanoseconds)
2. The transfer bottleneck would stall GPU operations
3. GPU VRAM is a scarce resource that should be populated from RAM only

**Implementation**: The `PlacementPolicy::select_target_tier()` method should filter out `TierId::Disk` when the target is `TierId::GpuVram`.

---

## 6. Non-Deterministic IO Timing

### 6.1 TimeProvider Integration

The DiskBackend uses the [`TimeProvider`](crates/ghost-core/src/time.rs:13) trait for all timestamp operations:

```rust
pub struct DiskBackend {
    // ... other fields ...
    time_provider: Arc<dyn TimeProvider>,
}
```

**Production mode**: Uses `RealTimeProvider` (wall-clock time via `Instant::now()`).

**Testing mode**: Uses `DeterministicTimeProvider` or `DeterministicClock` for reproducible behavior.

### 6.2 DeterministicDiskBackend

For testing, a `DeterministicDiskBackend` mode is available that simulates disk behavior with seeded RNG:

```rust
/// Configuration for deterministic disk simulation.
#[derive(Debug, Clone)]
pub struct DeterministicDiskConfig {
    /// Base configuration.
    pub base: DiskConfig,
    /// RNG seed for deterministic latency jitter.
    pub seed: u64,
    /// Whether to use deterministic time provider.
    pub use_deterministic_time: bool,
}
```

In deterministic mode:
- Latency jitter is derived from a seeded PRNG (e.g., `rand::SeedableRng`)
- Failure injection uses the same seeded PRNG
- Timestamps use `DeterministicClock` instead of wall-clock
- All behavior is fully reproducible given the same seed

### 6.3 Real-Time vs Deterministic Mode

| Aspect | Production (Real-Time) | Testing (Deterministic) |
|--------|----------------------|------------------------|
| Time source | `RealTimeProvider` | `DeterministicClock` |
| Latency jitter | OS + hardware dependent | Seeded PRNG |
| Failure injection | Based on real error rates | Seeded PRNG |
| IO operations | Real file I/O | Simulated with timing |
| Reproducibility | Non-deterministic | Fully reproducible |

---

## 7. Configuration

### 7.1 DiskConfig Struct

```rust
/// Configuration for the DiskBackend.
#[derive(Debug, Clone)]
pub struct DiskConfig {
    /// Base directory path for data storage.
    pub base_path: std::path::PathBuf,

    /// Total capacity in bytes.
    pub capacity: usize,

    /// Latency configuration (reuse from ghost-sim).
    pub latency: LatencyConfig,

    /// Bandwidth configuration (reuse from ghost-sim).
    pub bandwidth: BandwidthConfig,

    /// Failure injection configuration (reuse from ghost-sim).
    pub failure: FailureConfig,

    /// Disk hardware type (HDD, SSD, NVMe).
    pub disk_type: DiskType,

    /// Maximum number of concurrent IO operations.
    pub max_concurrent_ops: usize,

    /// Maximum queue depth before rejecting operations.
    pub max_queue_depth: usize,

    /// Latency spike injection configuration.
    pub spike_config: Option<SpikeConfig>,

    /// RNG seed for deterministic behavior (None for production).
    pub seed: Option<u64>,

    /// Whether to use direct I/O (bypass OS page cache).
    pub direct_io: bool,

    /// Filesystem sync strategy.
    pub sync_strategy: SyncStrategy,
}
```

### 7.2 SyncStrategy

```rust
/// Filesystem sync strategy for data integrity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStrategy {
    /// No explicit syncing (fastest, risk of data loss on crash).
    None,
    /// Sync after each write (slowest, safest).
    Sync,
    /// Sync at interval (balanced).
    Interval(std::time::Duration),
    /// Sync only metadata (fast, protects against corruption).
    MetadataOnly,
}
```

### 7.3 LatencyConfig (Reuse)

Reuse the existing [`LatencyConfig`](crates/ghost-sim/src/config.rs:7) from `ghost-sim`:

```rust
pub struct LatencyConfig {
    pub base: std::time::Duration,
    pub per_byte: std::time::Duration,
    pub jitter_fraction: f64,
}
```

With disk-specific defaults:

```rust
impl LatencyConfig {
    pub fn for_disk_type(disk_type: DiskType) -> Self {
        match disk_type {
            DiskType::Hdd => Self {
                base: std::time::Duration::from_millis(5),
                per_byte: std::time::Duration::from_micros(10),
                jitter_fraction: 0.5,
            },
            DiskType::Ssd => Self {
                base: std::time::Duration::from_micros(500),
                per_byte: std::time::Duration::from_nanos(100),
                jitter_fraction: 0.2,
            },
            DiskType::Nvme => Self {
                base: std::time::Duration::from_micros(50),
                per_byte: std::time::Duration::from_nanos(10),
                jitter_fraction: 0.1,
            },
        }
    }
}
```

### 7.4 BandwidthConfig (Reuse)

Reuse the existing [`BandwidthConfig`](crates/ghost-sim/src/config.rs:27) from `ghost-sim`:

```rust
pub struct BandwidthConfig {
    pub bytes_per_second: usize,
}
```

### 7.5 FailureConfig (Reuse)

Reuse the existing [`FailureConfig`](crates/ghost-sim/src/config.rs:64) from `ghost-sim` (see [Section 3.2](#32-failure-injection-via-failureconfig) for disk-specific interpretations).

### 7.6 DiskConfig Defaults

```rust
impl Default for DiskConfig {
    fn default() -> Self {
        Self {
            base_path: std::path::PathBuf::from("/var/lib/ghostpages/data"),
            capacity: 100 * 1024 * 1024 * 1024, // 100 GB
            latency: LatencyConfig::for_disk_type(DiskType::Ssd),
            bandwidth: BandwidthConfig::default(),
            failure: FailureConfig::default(),
            disk_type: DiskType::Ssd,
            max_concurrent_ops: 64,
            max_queue_depth: 256,
            spike_config: None,
            seed: None,
            direct_io: false,
            sync_strategy: SyncStrategy::Interval(std::time::Duration::from_secs(1)),
        }
    }
}
```

---

## 8. Integration Points

### 8.1 PlacementPolicy Integration

**File**: [`crates/ghost-policy/src/policy.rs`](crates/ghost-policy/src/policy.rs)

The `PlacementPolicy` trait is **backend-agnostic** — it makes decisions based on `ChunkMeta` and `PressureState`, not on `StorageBackend` directly. No changes are needed to the trait itself.

**Changes needed**:
- `select_target_tier()`: Add logic to consider `TierId::Disk` as a valid target when `memory_pressure` is high and `io_pressure` is low.
- `select_viction()`: Add logic to prefer evicting cold chunks from disk when `io_pressure` is critical.
- `should_migrate()`: Add logic to trigger RAM → Disk demotion and Disk → RAM promotion based on pressure thresholds.

**No changes needed** to the trait definition — only to implementations (e.g., `LruPolicy`).

### 8.2 MigrationEngine Integration

**File**: [`crates/ghost-daemon/src/migration.rs`](crates/ghost-daemon/src/migration.rs)

The `MigrationEngine` already handles arbitrary backends via `BTreeMap<TierId, Arc<dyn StorageBackend>>`. No changes are needed to the engine itself.

**Changes needed**:
- Register `DiskBackend` in the backends map: `backends.insert(TierId::Disk, Arc::new(DiskBackend::new(config)))`
- The engine will automatically include disk in migration evaluation cycles
- Add disk-specific migration metrics (see [Section 8.4](#84-metricsregistry-integration))

### 8.3 HealthTracker Integration

**File**: [`crates/ghost-daemon/src/health.rs`](crates/ghost-daemon/src/health.rs)

The `HealthTracker` already handles arbitrary `TierId` values. No changes are needed.

**Changes needed**:
- Register `TierId::Disk` for health tracking: `tracker.register(TierId::Disk)`
- The `DiskBackend::health_check()` method should:
  - Verify the base directory exists and is accessible
  - Check available disk space
  - Attempt a small write/read to verify IO operations work
  - Return `Err(BackendError::Unhealthy(...))` on failure

### 8.4 MetricsRegistry Integration

**File**: [`crates/ghost-metrics/src/registry.rs`](crates/ghost-metrics/src/registry.rs)

**New metrics to add**:

```rust
/// Disk-specific metrics.
#[derive(Debug, Clone)]
pub struct DiskMetrics {
    /// Current disk IO pressure (0.0 to 1.0).
    pub io_pressure: IntGauge,
    /// Current queue depth.
    pub queue_depth: IntGauge,
    /// Current throughput in bytes per second.
    pub throughput_bps: IntGauge,
    /// Total bytes written.
    pub bytes_written_total: IntCounter,
    /// Total bytes read.
    pub bytes_read_total: IntCounter,
    /// Total write operations.
    pub write_ops_total: IntCounter,
    /// Total read operations.
    pub read_ops_total: IntCounter,
    /// Total IO errors.
    pub io_errors_total: IntCounter,
    /// Total timeout errors.
    pub timeout_errors_total: IntCounter,
    /// Total corruption events.
    pub corruption_events_total: IntCounter,
    /// Disk space used in bytes.
    pub space_used_bytes: IntGauge,
    /// Disk space available in bytes.
    pub space_available_bytes: IntGauge,
    /// Histogram of write operation latencies.
    pub write_latency_seconds: Histogram,
    /// Histogram of read operation latencies.
    pub read_latency_seconds: Histogram,
}
```

**Changes needed**:
- Add `DiskMetrics` to `MetricsRegistry`
- Register disk metrics in `MetricsRegistry::new()`
- Update `MetricsRegistry::gather()` to include disk metrics

### 8.5 Required Changes by Crate

| Crate | Changes Needed |
|-------|---------------|
| `ghost-tier` | Add `DiskBackend` struct and `StorageBackend` impl |
| `ghost-core` | Extend `BackendError` with disk-specific variants (or add `DiskBackendError`) |
| `ghost-policy` | Update `LruPolicy` to handle Disk tier in placement decisions |
| `ghost-daemon` | Register `DiskBackend` in `MigrationEngine`, `HealthTracker`, `PressureMonitor` |
| `ghost-metrics` | Add `DiskMetrics` to `MetricsRegistry` |
| `ghost-sim` | No changes (DiskBackend is independent) |
| `ghost-vulkan` | No changes (Disk is not a migration source for Vulkan) |

---

## 9. Phase 3 Implementation Checklist

### 9.1 Core Implementation

- [ ] **Create `disk.rs` module** in `crates/ghost-tier/src/`
  - Define `DiskBackend` struct
  - Implement `StorageBackend` trait
  - Implement file I/O operations using `tokio::task::spawn_blocking`
  - Implement allocation management (offset-based or file-based)

- [ ] **Extend `BackendError`** in `crates/ghost-core/src/error.rs`
  - Add `DiskFull` variant (or reuse `InsufficientSpace`)
  - Add `IoError` variant
  - Add `Timeout` variant
  - Add `PermissionDenied` variant
  - Add `Corruption` variant
  - Add `FilesystemError` variant

- [ ] **Add `DiskType` enum** to `crates/ghost-tier/src/` or `crates/ghost-core/src/`
  - Variants: `Hdd`, `Ssd`, `Nvme`
  - Default latency, IOPS, and bandwidth profiles

- [ ] **Add `DiskConfig` struct** to `crates/ghost-tier/src/`
  - All fields as specified in [Section 7](#7-configuration)
  - Default implementation with SSD profile

- [ ] **Add `SpikeConfig` struct** to `crates/ghost-tier/src/`
  - Spike probability, multiplier, duration, seed

- [ ] **Add `SyncStrategy` enum** to `crates/ghost-tier/src/`
  - Variants: `None`, `Sync`, `Interval(Duration)`, `MetadataOnly`

- [ ] **Add `DeterministicDiskConfig`** to `crates/ghost-tier/src/`
  - For testing with seeded RNG

### 9.2 Latency and Throughput

- [ ] **Implement latency model**
  - Configurable distribution (uniform, normal, custom)
  - Disk-type-specific profiles
  - Queue-depth-dependent latency penalty
  - Spike injection

- [ ] **Implement throughput model**
  - IOPS limiting via token bucket or leaky bucket
  - Bandwidth limiting via rate limiter
  - Queue management with bounded capacity

### 9.3 Failure Injection

- [ ] **Implement failure injection**
  - Wire up `FailureConfig` to disk operations
  - Implement all failure modes: `DiskFull`, `IoError`, `Timeout`, `PermissionDenied`, `Corruption`
  - Support all failure patterns: `Random`, `Burst`, `Degrading`, `Intermittent`, `Cascading`

- [ ] **Implement corruption detection**
  - Store blake3 hash with each allocation
  - Verify on read
  - Background integrity verification task

### 9.4 Integration

- [ ] **Update `LruPolicy`** in `crates/ghost-policy/src/lru.rs`
  - Handle `TierId::Disk` in `select_target_tier()`
  - Handle disk in `select_viction()`
  - Handle disk in `should_migrate()`
  - Enforce "Disk is NOT a migration source for Vulkan" rule

- [ ] **Update `MetricsRegistry`** in `crates/ghost-metrics/src/registry.rs`
  - Add `DiskMetrics` struct
  - Register disk metrics
  - Update `gather()` method

- [ ] **Update daemon orchestrator** in `crates/ghost-daemon/src/orchestrator.rs`
  - Register `DiskBackend` with `MigrationEngine`
  - Register `TierId::Disk` with `HealthTracker`
  - Register `DiskBackend` with `PressureMonitor`

### 9.5 Test Strategy

#### Unit Tests

- [ ] **DiskBackend basic operations**
  - `test_disk_backend_store_and_retrieve` — write then read back
  - `test_disk_backend_capacity_tracking` — allocate until full
  - `test_disk_backend_integrity_verification` — blake3 hash check
  - `test_disk_backend_health_check` — disk accessibility
  - `test_disk_backend_zero_allocation_fails` — edge case
  - `test_disk_backend_read_nonexistent_allocation` — error path
  - `test_disk_backend_write_exceeds_allocation` — bounds check

- [ ] **Latency model tests**
  - `test_latency_within_range` — latency is within configured bounds
  - `test_latency_increases_with_queue_depth` — queue penalty works
  - `test_latency_spike_injection` — spikes occur at configured rate
  - `test_latency_distribution_uniform` — uniform distribution
  - `test_latency_distribution_normal` — normal distribution

- [ ] **Failure injection tests**
  - `test_failure_disk_full` — DiskFull error when capacity exceeded
  - `test_failure_io_error` — IoError injection at configured rate
  - `test_failure_timeout` — Timeout injection at configured rate
  - `test_failure_corruption` — Corruption detection and reporting
  - `test_failure_pattern_burst` — Burst failure pattern
  - `test_failure_pattern_degrading` — Degrading failure pattern

- [ ] **Pressure reporting tests**
  - `test_pressure_increases_with_usage` — io_pressure tracks usage
  - `test_pressure_increases_with_queue` — io_pressure tracks queue depth
  - `test_pressure_critical_threshold` — critical detection works

#### Integration Tests

- [ ] **RAM ↔ Disk migration tests**
  - `test_ram_to_disk_demotion` — cold chunks move from RAM to disk
  - `test_disk_to_ram_promotion` — hot chunks move from disk to RAM
  - `test_disk_eviction_when_full` — cold chunks evicted when disk is full
  - `test_disk_not_source_for_vulkan` — Disk → GPU VRAM migration is blocked

- [ ] **Health tracker integration tests**
  - `test_health_disk_degraded` — disk marked degraded after failures
  - `test_health_disk_unavailable` — disk marked unavailable after threshold
  - `test_health_disk_recovery` — disk recovers after successful probes

- [ ] **Pressure monitor integration tests**
  - `test_pressure_monitor_samples_disk` — PressureMonitor includes disk
  - `test_pressure_ema_smoothing_disk` — EMA smoothing works for io_pressure
  - `test_pressure_alert_disk_critical` — PressureAlert emitted when disk critical

#### Stress Tests

- [ ] **Concurrent workload tests**
  - `test_stress_concurrent_reads` — many concurrent reads saturate IOPS
  - `test_stress_concurrent_writes` — many concurrent writes saturate bandwidth
  - `test_stress_mixed_read_write` — mixed read/write workload
  - `test_stress_allocation_exhaustion` — allocate until disk is full

- [ ] **Migration stress tests**
  - `test_stress_rapid_promotion_demotion` — rapid RAM ↔ Disk migration cycles
  - `test_stress_eviction_under_pressure` — eviction when disk is critical
  - `test_stress_migration_with_failures` — migration during failure injection

### 9.6 Performance Benchmarks

- [ ] **Latency benchmarks**
  - Measure p50, p95, p99 latency for read/write operations
  - Compare against configured latency model
  - Measure latency under varying queue depths

- [ ] **Throughput benchmarks**
  - Measure maximum sustained read/write throughput
  - Measure IOPS under varying operation sizes
  - Compare against configured bandwidth/IOPS limits

- [ ] **Migration benchmarks**
  - Measure RAM → Disk demotion throughput (bytes/sec)
  - Measure Disk → RAM promotion throughput (bytes/sec)
  - Measure migration latency vs. chunk size

- [ ] **Pressure monitoring overhead**
  - Measure overhead of `pressure()` calls
  - Measure overhead of EMA smoothing
  - Verify no measurable impact on throughput

### 9.7 Migration Test Matrix

| Source | Target | Expected Behavior |
|--------|--------|-------------------|
| RAM | Disk | Allowed when memory_pressure > 0.8 |
| Disk | RAM | Allowed when hotness > threshold AND memory_pressure < 0.7 |
| Disk | GPU VRAM | **Prohibited** — too slow |
| GPU VRAM | Disk | Allowed when vram_pressure > 0.8 |
| RAM | GPU VRAM | Allowed (existing behavior) |
| GPU VRAM | RAM | Allowed (existing behavior) |

---

## Appendix A: File Layout

```
crates/ghost-tier/src/
├── backend.rs       # StorageBackend trait (existing)
├── ram.rs           # RamBackend (existing)
├── tracker.rs       # AllocationTracker (existing)
└── disk.rs          # DiskBackend (NEW — Phase 3)

crates/ghost-core/src/
├── types.rs         # TierId enum (Disk variant already exists)
├── state.rs         # PressureState (io_pressure field already exists)
├── time.rs          # TimeProvider trait (existing)
└── error.rs         # BackendError (extend with disk variants)

crates/ghost-sim/src/
└── config.rs        # LatencyConfig, BandwidthConfig, FailureConfig (reuse)

crates/ghost-policy/src/
├── policy.rs        # PlacementPolicy trait (no changes needed)
└── lru.rs           # LruPolicy (update for Disk tier)

crates/ghost-daemon/src/
├── migration.rs     # MigrationEngine (register DiskBackend)
├── health.rs        # HealthTracker (register TierId::Disk)
├── pressure.rs      # PressureMonitor (no changes needed)
└── metrics.rs       # Add DiskMetrics

crates/ghost-metrics/src/
├── registry.rs      # MetricsRegistry (add DiskMetrics)
└── health.rs        # BackendHealthMetrics (extend if needed)
```

## Appendix B: Dependency Additions

The following dependencies should be added to `crates/ghost-tier/Cargo.toml` for Phase 3:

```toml
[dependencies]
# Existing dependencies...
tokio = { version = "1", features = ["fs", "io-util"] }  # Already present
parking_lot = "0.12"                                      # Already present

# New dependencies for DiskBackend
rand = "0.8"              # For deterministic RNG in testing
rand_chacha = "0.3"       # Seedable RNG for deterministic mode
```

---

*This specification is a design document for Phase 3 implementation. No code changes are made as part of this document.*
