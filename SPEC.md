# GhostPages Technical Specification

**Version**: 0.2.0
**Status**: Refined Draft
**Date**: 2025
**Language**: Rust (edition 2021)
**Platform**: Linux (kernel 5.15+)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Repository Structure](#3-repository-structure)
4. [Component Responsibilities](#4-component-responsibilities)
5. [Async Transfer Pipeline](#5-async-transfer-pipeline)
6. [Storage Backend and Placement Policy](#6-storage-backend-and-placement-policy)
7. [Memory Lifecycle](#7-memory-lifecycle)
8. [Chunk Format](#8-chunk-format)
9. [IPC Protocol](#9-ipc-protocol)
10. [Data Flow and Concurrency Model](#10-data-flow-and-concurrency-model)
11. [Trace Replay System](#11-trace-replay-system)
12. [Corruption-Testing Infrastructure](#12-corruption-testing-infrastructure)
13. [Benchmark Plan](#13-benchmark-plan)
14. [Safety Model](#14-safety-model)
15. [Risk Analysis](#15-risk-analysis)
16. [MVP Scope](#16-mvp-scope)
17. [Stretch Goals](#17-stretch-goals)
18. [Implementation Roadmap](#18-implementation-roadmap)

---

## 1. Executive Summary

GhostPages is an experimental heterogeneous memory-tiering system for Linux that enables cold anonymous memory pages to eventually migrate into GPU VRAM as a lower-priority memory tier. The system is designed for safe experimentation, evolving progressively from userspace object storage toward optional kernel integration.

### Core Principles

- **Safety First**: Never risk silent memory corruption; fail safely and recoverably
- **Observability**: Extensive tracing, metrics, and logging at every layer
- **Modularity**: Clean backend abstractions, isolated unsafe code
- **Incrementalism**: Each phase delivers standalone value
- **Pragmatism**: Kernel integration is optional; the system remains useful without it
- **Simulation-First Development**: Most policy work, migration logic testing, and heuristic development happens on the simulation backend before touching real GPU hardware

### Technology Stack

| Layer | Technology |
|-------|------------|
| Language | Rust (2021 edition) |
| GPU Backend | Vulkan 1.3+ (primary), CUDA (future) |
| Simulation Backend | `ghost-sim` (configurable fake VRAM for dev/CI) |
| Compression | zstd |
| IPC | Unix domain sockets + shared memory |
| Async Runtime | tokio |
| Metrics | Prometheus-compatible |
| Tracing | tracing crate with structured logging |
| Content Hashing | blake3 |
| Fuzzing | cargo fuzz |
| Property Testing | proptest |

---

## 2. High-Level Architecture

### 2.1 System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User Applications                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ghost-cli / ghost-ipc (Library)                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ Object API   │  │ Allocator   │  │ Pressure    │  │ Migration Policy    │ │
│  │              │  │ Hooks       │  │ Monitor     │  │ Client              │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                               Unix Socket + SHM
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            ghost-daemon                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                           Core Engine                                   │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐  │ │
│  │  │ Chunk Store  │  │ Tier Manager│  │ Compression │  │ Policy Engine│  │ │
│  │  │              │  │             │  │ Engine      │  │              │  │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └──────────────┘  │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                      Async Transfer Pipeline                            │ │
│  │  ┌──────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │ │
│  │  │ Ingress  │─▶│ Compression  │─▶│ Transfer     │─▶│ Tier          │  │ │
│  │  │ Queue    │  │ Worker Pool  │  │ Worker Pool  │  │ Placement    │  │ │
│  │  └──────────┘  └──────────────┘  └──────────────┘  └───────────────┘  │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                        Backend Abstraction Layer                        │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐  │ │
│  │  │ Vulkan VRAM  │  │ CUDA VRAM   │  │ Simulation  │  │ RAM/Disk     │  │ │
│  │  │ Backend     │  │ Backend     │  │ Backend     │  │ Backends     │  │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └──────────────┘  │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                        Observability Layer                              │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐  │ │
│  │  │ Prometheus   │  │ Tracing     │  │ Metrics     │  │ Health       │  │ │
│  │  │ Exporter     │  │ Subscriber  │  │ Collector   │  │ Monitor      │  │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └──────────────┘  │ │
│  │  ┌─────────────┐  ┌─────────────┐                                      │ │
│  │  │ Trace       │  │ Trace       │                                      │ │
│  │  │ Recorder    │  │ Replayer    │                                      │ │
│  │  └─────────────┘  └─────────────┘                                      │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            System Resources                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ GPU VRAM     │  │ System RAM  │  │ NVMe SSD    │  │ Kernel MM Stats     │ │
│  │              │  │             │  │             │  │ (future)            │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow

#### Upload Path (Hot → Cold)

```
1. Application allocates data via ghost-ipc client
2. Client sends StoreRequest via Unix socket (async)
3. Request enters Ingress Queue (mpsc channel)
4. Compression Worker picks up: compresses data
5. Transfer Worker picks up: transfers compressed data
6. Placement Policy selects target tier (VRAM/RAM/Disk)
7. StorageBackend allocates space in selected tier
8. Data written to tier (GPU via PCIe via Vulkan, or simulated)
9. Metadata updated, completion notification sent
10. Client receives StoreResponse with handle
```

#### Download Path (Cold → Hot)

```
1. Application requests data via handle
2. Client sends RetrieveRequest via Unix socket (async)
3. Request enters Ingress Queue (mpsc channel)
4. Tier Placement locates chunk in tier metadata
5. Transfer Worker picks up: reads from tier
6. If in VRAM: transfer via PCIe to system RAM (or simulated transfer)
7. Compression Worker decompresses
8. Completion notification sent with data
9. Client provides decompressed data to application
10. Placement Policy updates access statistics
```

### 2.3 Component Interaction Diagram

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   Application    │────▶│  ghost-ipc       │────▶│  ghost-daemon    │
│                  │◀────│  (client lib)    │◀────│                  │
└──────────────────┘     └──────────────────┘     └──────────────────┘
                                │                         │
                                │                         ▼
                                │                  ┌──────────────────┐
                                │                  │  Async Pipeline  │
                                │                  │  ┌────────────┐  │
                                │                  │  │ Ingress Q  │  │
                                │                  │  ├────────────┤  │
                                │                  │  │ Compress   │  │
                                │                  │  ├────────────┤  │
                                │                  │  │ Transfer   │  │
                                │                  │  ├────────────┤  │
                                │                  │  │ Placement  │  │
                                │                  │  └────────────┘  │
                                │                  └──────────────────┘
                                │                         │
                                │                         ▼
                                │                  ┌──────────────────┐
                                │                  │  Backend Layer   │
                                │                  │  ┌────────────┐  │
                                │                  │  │ Vulkan     │  │
                                │                  │  ├────────────┤  │
                                │                  │  │ CUDA       │  │
                                │                  │  ├────────────┤  │
                                │                  │  │ Simulation │  │
                                │                  │  ├────────────┤  │
                                │                  │  │ RAM        │  │
                                │                  │  ├────────────┤  │
                                │                  │  │ Disk       │  │
                                │                  │  └────────────┘  │
                                │                  └──────────────────┘
                                ▼
                         ┌──────────────────┐
                         │  Shared Memory   │
                         │  Regions         │
                         └──────────────────┘
```

---

## 3. Repository Structure

### 3.1 Crate Layout

```
ghostpages/
├── Cargo.toml                    # Workspace root
├── Cargo.lock
├── README.md
├── SPEC.md                       # This document
├── LICENSE
├── .github/
│   └── workflows/
│       ├── ci.yml                # Build + test
│       ├── clippy.yml            # Linting
│       └── benchmarks.yml        # Criterion benchmarks
├── crates/
│   ├── ghost-core/               # Core types, errors, ChunkId, ChunkMeta
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── error.rs          # Error types
│   │       ├── types.rs          # Core types (ChunkId, TierId, etc.)
│   │       ├── chunk_id.rs       # Content-addressed ChunkId (blake3)
│   │       ├── config.rs         # Configuration structures
│   │       └── constants.rs      # System constants
│   │
│   ├── ghost-daemon/             # Main daemon binary
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── daemon.rs         # Daemon lifecycle
│   │   │   ├── engine.rs         # Core engine
│   │   │   ├── pipeline/         # Async transfer pipeline
│   │   │   │   ├── mod.rs
│   │   │   │   ├── ingress.rs    # Ingress queue
│   │   │   │   ├── compress.rs   # Compression stage
│   │   │   │   ├── transfer.rs   # Transfer stage
│   │   │   │   └── placement.rs  # Placement stage
│   │   │   ├── ipc/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── server.rs     # Unix socket server
│   │   │   │   ├── protocol.rs   # Message protocol
│   │   │   │   └── shm.rs        # Shared memory management
│   │   │   ├── store/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── chunk_store.rs
│   │   │   │   ├── index.rs      # Chunk index
│   │   │   │   └── metadata.rs   # Metadata management
│   │   │   └── tier/
│   │   │       ├── mod.rs
│   │   │       ├── manager.rs    # Tier orchestration
│   │   │       ├── ram_tier.rs
│   │   │       ├── disk_tier.rs
│   │   │       └── gpu/
│   │   │           ├── mod.rs
│   │   │           ├── backend.rs # StorageBackend trait
│   │   │           ├── vulkan.rs  # Vulkan implementation
│   │   │           └── cuda.rs   # Future CUDA implementation
│   │   └── examples/
│   │       └── basic_daemon.rs
│   │
│   ├── ghost-ipc/                # Client library (IPC protocol types)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── client.rs         # Main client API
│   │       ├── object.rs         # Object handle
│   │       ├── builder.rs        # Client builder
│   │       └── error.rs          # Client errors
│   │
│   ├── ghost-tier/               # StorageBackend trait + RAM/Disk backends
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── trait.rs          # StorageBackend trait definitions
│   │       ├── ram.rs            # RAM tier implementation
│   │       ├── disk.rs           # Disk tier implementation
│   │       └── sim.rs            # Simulation backend (RAM-based with fake latency)
│   │
│   ├── ghost-vulkan/             # Vulkan VRAM backend
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── device.rs         # Vulkan device management
│   │       ├── memory.rs         # VRAM allocation
│   │       ├── transfer.rs       # DMA transfer
│   │       └── buffer.rs         # Buffer management
│   │
│   ├── ghost-sim/                # Simulation / fake VRAM backend
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── config.rs         # Simulation parameters
│   │       ├── backend.rs        # SimulationBackend implementation
│   │       ├── latency.rs        # Artificial delay engine
│   │       ├── bandwidth.rs      # Rate limiter
│   │       ├── fragmentation.rs  # Fragmentation simulator
│   │       └── injection.rs      # Failure/corruption injection
│   │
│   ├── ghost-policy/             # PlacementPolicy trait + implementations
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── trait.rs          # PlacementPolicy trait
│   │       ├── lru.rs            # LRU policy
│   │       ├── lfu.rs            # LFU policy
│   │       ├── pressure.rs       # Pressure-aware policy
│   │       └── eviction.rs       # Eviction order strategies
│   │
│   ├── ghost-compress/           # Compression abstraction
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── engine.rs         # Compression trait
│   │       ├── zstd_impl.rs
│   │       └── none.rs           # No-compression passthrough
│   │
│   ├── ghost-metrics/            # Observability
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── prometheus.rs     # Prometheus exporter
│   │       ├── collector.rs      # Metrics collection
│   │       └── tracing.rs        # Tracing setup
│   │
│   ├── ghost-replay/             # Trace recording/replay
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── recorder.rs       # Trace recording
│   │       ├── replayer.rs       # Trace replay
│   │       ├── format.rs         # Binary trace format
│   │       └── serde.rs          # Serialization/deserialization
│   │
│   └── ghost-cli/                # CLI tools for interacting with daemon
│       ├── Cargo.toml
│       └── src/
│           └── main.rs
│
├── fuzz/                         # cargo fuzz targets
│   ├── Cargo.toml
│   └── fuzz_targets/
│       ├── chunk_serialization.rs
│       ├── chunk_deserialization.rs
│       ├── chunk_roundtrip.rs
│       └── ipc_protocol.rs
│
├── benches/                      # criterion benchmarks
│   ├── throughput.rs
│   ├── latency.rs
│   ├── tier_migration.rs
│   └── pipeline_stages.rs
│
├── tests/
│   ├── integration/
│   │   ├── daemon_lifecycle.rs
│   │   ├── ipc_protocol.rs
│   │   ├── tier_migration.rs
│   │   ├── compression.rs
│   │   ├── pipeline_async.rs
│   │   ├── simulation_backend.rs
│   │   ├── corruption_tests.rs
│   │   └── trace_replay.rs
│   └── fixtures/
│       └── test_data/
│
└── docs/
    ├── architecture.md
    ├── protocol.md
    ├── safety.md
    ├── pipeline.md
    └── roadmap.md
```

### 3.2 Workspace Dependencies

```toml
# Cargo.toml (workspace root)
[workspace]
members = [
    "crates/ghost-core",
    "crates/ghost-daemon",
    "crates/ghost-ipc",
    "crates/ghost-tier",
    "crates/ghost-vulkan",
    "crates/ghost-sim",
    "crates/ghost-policy",
    "crates/ghost-compress",
    "crates/ghost-metrics",
    "crates/ghost-replay",
    "crates/ghost-cli",
    "fuzz",
]

[workspace.dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }

# Serialization
serde = { version = "1", features = ["derive"] }
bincode = "1.3"

# Error handling
thiserror = "1"
anyhow = "1"

# Logging and tracing
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

# Metrics
prometheus = "0.13"

# Compression
zstd = "0.13"

# Content hashing (ChunkId)
blake3 = "1.5"

# Vulkan (primary GPU backend)
ash = "0.38"

# Utilities
bytes = "1"
uuid = { version = "1", features = ["v4"] }
parking_lot = "0.12"
crossbeam = "0.8"

# Testing
criterion = { version = "0.5", features = ["async_tokio"] }
proptest = "1"
tempfile = "3"

# Fuzzing
cargo-fuzz = "0.5"
libfuzzer-sys = "0.14"
```

---

## 4. Component Responsibilities

### 4.1 ghost-core

**Purpose**: Shared types, errors, and utilities used by all crates.

**Responsibilities**:
- Define `ChunkId` (content-addressed via blake3), `TierId`, `ObjectId` newtypes
- Error types with `thiserror`
- Configuration structures
- Common constants (max chunk size, default ports, etc.)
- Feature flags for compile-time configuration

**Key Types**:
```rust
/// Content-addressed chunk identifier (blake3 hash of data)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId(pub [u8; 32]);

impl ChunkId {
    /// Compute ChunkId from raw data (content-addressed)
    pub fn from_data(data: &[u8]) -> Self {
        ChunkId(*blake3::hash(data).as_bytes())
    }

    /// Verify that data matches this ChunkId
    pub fn verify(&self, data: &[u8]) -> bool {
        let computed = blake3::hash(data);
        computed.as_bytes() == &self.0
    }
}

impl std::fmt::Display for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

/// Memory tier identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TierId {
    /// System RAM (hot tier)
    Ram,
    /// GPU VRAM (warm tier)
    GpuVram,
    /// NVMe/SSD storage (cold tier)
    Disk,
    /// Simulation backend (for testing/CI)
    Simulation,
}

/// Memory tier priority (lower = hotter)
impl TierId {
    pub fn priority(&self) -> u8 {
        match self {
            TierId::Ram => 0,
            TierId::GpuVram => 1,
            TierId::Disk => 2,
            TierId::Simulation => 3,
        }
    }
}

/// Chunk metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub id: ChunkId,
    pub size: usize,
    pub compressed_size: usize,
    pub tier: TierId,
    pub created_at: u64,
    pub last_accessed: u64,
    pub access_count: u64,
    pub compression: CompressionAlgorithm,
    pub checksum: [u8; 32],  // blake3 checksum of compressed data
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Zstd,
}
```

### 4.2 ghost-daemon

**Purpose**: Main daemon process managing memory tiers and serving client requests.

**Responsibilities**:
- Lifecycle management (startup, shutdown, signal handling)
- IPC server (Unix socket listener)
- Async transfer pipeline orchestration
- Chunk storage and retrieval
- Tier management and migration
- Policy enforcement
- Metrics collection and export
- Trace recording

**Architecture**:
```rust
pub struct Daemon {
    config: DaemonConfig,
    engine: Arc<Engine>,
    pipeline: Arc<AsyncPipeline>,
    ipc_server: IpcServer,
    metrics: Arc<MetricsCollector>,
    trace_recorder: Arc<TraceRecorder>,
    shutdown: tokio::sync::broadcast::Sender<()>,
}

pub struct Engine {
    chunk_store: Arc<ChunkStore>,
    tier_manager: Arc<TierManager>,
    policy: Arc<dyn PlacementPolicy>,
    compression: Arc<dyn CompressionEngine>,
    trace_recorder: Arc<TraceRecorder>,
}
```

### 4.3 ghost-ipc

**Purpose**: Client library for applications to interact with the daemon.

**Responsibilities**:
- Connection management (connect, reconnect, pool)
- Request/response handling
- Object API (store, retrieve, delete)
- Builder pattern for configuration
- Error handling and retries

**API Design**:
```rust
pub struct GhostPagesClient {
    connection: Connection,
    config: ClientConfig,
}

impl GhostPagesClient {
    /// Connect to daemon
    pub async fn connect(addr: &str) -> Result<Self, ClientError>;

    /// Store data, return handle
    pub async fn store(&self, data: &[u8]) -> Result<ObjectHandle, ClientError>;

    /// Store with options
    pub async fn store_with(
        &self,
        data: &[u8],
        options: StoreOptions,
    ) -> Result<ObjectHandle, ClientError>;

    /// Retrieve data by handle
    pub async fn retrieve(&self, handle: &ObjectHandle) -> Result<Vec<u8>, ClientError>;

    /// Delete object
    pub async fn delete(&self, handle: &ObjectHandle) -> Result<(), ClientError>;

    /// Get object metadata
    pub async fn metadata(&self, handle: &ObjectHandle) -> Result<ChunkMeta, ClientError>;
}

pub struct ObjectHandle {
    chunk_id: ChunkId,
    // Opaque to client
}
```

### 4.4 ghost-tier

**Purpose**: StorageBackend trait definition and RAM/Disk tier implementations.

**Responsibilities**:
- Define `StorageBackend` trait for all tier implementations
- RAM tier: in-memory HashMap-backed storage
- Disk tier: file-based storage
- Simulation backend: RAM-based with configurable latency/bandwidth/failures

**StorageBackend Trait** (policy-agnostic — handles ONLY allocation, retrieval, integrity, transfer):
```rust
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Backend identifier
    fn id(&self) -> TierId;

    /// Total capacity in bytes
    fn capacity(&self) -> usize;

    /// Available space in bytes
    fn available(&self) -> usize;

    /// Allocate space for data
    async fn allocate(&self, size: usize) -> Result<Allocation, BackendError>;

    /// Deallocate space
    async fn deallocate(&self, allocation: Allocation) -> Result<(), BackendError>;

    /// Write data to allocation
    async fn write(&self, allocation: &Allocation, data: &[u8]) -> Result<(), BackendError>;

    /// Read data from allocation
    async fn read(&self, allocation: &Allocation, buf: &mut [u8]) -> Result<(), BackendError>;

    /// Verify integrity of data at allocation (checksum verification)
    async fn verify_integrity(&self, allocation: &Allocation, expected: &[u8; 32]) -> Result<(), BackendError>;

    /// Check if backend is healthy
    async fn health_check(&self) -> Result<(), BackendError>;
}

pub struct Allocation {
    pub offset: usize,
    pub size: usize,
    pub backend_data: BackendData,  // Opaque backend-specific data
}
```

### 4.5 ghost-vulkan

**Purpose**: Vulkan VRAM backend implementation.

**Responsibilities**:
- Vulkan device enumeration and initialization
- VRAM allocation and deallocation
- DMA transfer operations (CPU <-> GPU)
- Buffer management
- Implements `StorageBackend` trait

### 4.6 ghost-sim

**Purpose**: Simulation backend — the primary development and CI backend.

**Responsibilities**:
- Simulates GPU VRAM behavior without touching Vulkan
- Configurable transfer latency (artificial sleep/delay)
- Bandwidth ceilings (rate limiting)
- Fragmentation simulation (random allocation failures under pressure)
- Allocation failure injection (configurable failure rate)
- Corruption injection (configurable corruption rate)
- Eviction pressure simulation
- Implements `StorageBackend` trait

**Design Philosophy**: This is NOT a toy. Most policy work, migration logic testing, and heuristic development should happen here before touching real GPU hardware.

**Simulation Configuration**:
```rust
pub struct SimulationConfig {
    /// Total simulated VRAM capacity
    pub capacity: usize,

    /// Simulated transfer latency per chunk
    pub transfer_latency: Duration,

    /// Bandwidth ceiling in bytes/second
    pub bandwidth_limit: usize,

    /// Fragmentation level (0.0 = none, 1.0 = fully fragmented)
    pub fragmentation: f64,

    /// Allocation failure rate (0.0 = never, 1.0 = always)
    pub allocation_failure_rate: f64,

    /// Corruption injection rate (0.0 = never, 1.0 = always)
    pub corruption_rate: f64,

    /// Enable eviction pressure simulation
    pub eviction_pressure: bool,

    /// Random seed for deterministic testing
    pub seed: Option<u64>,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            capacity: 2 * 1024 * 1024 * 1024,  // 2 GB
            transfer_latency: Duration::from_millis(10),
            bandwidth_limit: 8 * 1024 * 1024 * 1024,  // 8 GB/s
            fragmentation: 0.1,
            allocation_failure_rate: 0.01,
            corruption_rate: 0.0,
            eviction_pressure: true,
            seed: None,
        }
    }
}
```

### 4.7 ghost-policy

**Purpose**: PlacementPolicy trait and implementations — completely independent of storage backends.

**Responsibilities**:
- Define `PlacementPolicy` trait
- What migrates (hotness threshold)
- When (pressure triggers)
- Priority (LRU, LFU, custom)
- Eviction order (which chunk leaves a tier first)
- Backend-agnostic — knows nothing about how/where data is stored

**PlacementPolicy Trait** (backend-agnostic — handles ONLY migration decisions):
```rust
#[async_trait]
pub trait PlacementPolicy: Send + Sync + 'static {
    /// Policy name
    fn name(&self) -> &str;

    /// Given current state, decide which chunks should migrate and where
    async fn decide_migrations(
        &self,
        state: &SystemState,
    ) -> Result<Vec<MigrationDecision>, PolicyError>;

    /// Given a tier under pressure, decide which chunk to evict
    async fn decide_eviction(
        &self,
        tier: TierId,
        candidates: &[ChunkMeta],
    ) -> Result<ChunkId, PolicyError>;

    /// Record an access event (for tracking hotness)
    async fn record_access(&self, chunk_id: &ChunkId) -> Result<(), PolicyError>;

    /// Get the hotness score for a chunk (higher = hotter)
    async fn hotness(&self, chunk_id: &ChunkId) -> Result<f64, PolicyError>;
}

pub struct MigrationDecision {
    pub chunk_id: ChunkId,
    pub source_tier: TierId,
    pub target_tier: TierId,
    pub priority: u8,
}

pub struct SystemState {
    pub tier_usage: HashMap<TierId, TierUsage>,
    pub chunks: HashMap<ChunkId, ChunkMeta>,
    pub pressure_level: PressureLevel,
}
```

### 4.8 ghost-compress

**Purpose**: Compression abstraction with pluggable algorithms.

**Responsibilities**:
- Compression trait definition
- zstd implementation
- No-compression passthrough
- Compression level configuration

### 4.9 ghost-metrics

**Purpose**: Observability and metrics collection.

**Responsibilities**:
- Prometheus metrics export
- Structured tracing setup
- Health monitoring
- Performance counters

### 4.10 ghost-replay

**Purpose**: Trace recording and replay system.

**Responsibilities**:
- Record migration events, timestamps, chunk IDs, source/destination tiers, sizes, durations
- Serialize to compact binary format (or JSON for human-readable)
- Replay recorded traces for tuning, A/B testing, regression testing, offline experimentation

### 4.11 ghost-cli

**Purpose**: CLI tools for interacting with the daemon.

**Responsibilities**:
- Start/stop daemon
- Store/retrieve/delete data
- Query status and metrics
- Run trace replays
- Configure simulation parameters

---

## 5. Async Transfer Pipeline

### 5.1 Pipeline Architecture

The system uses a fully async pipeline from day one. Even if initially single-threaded internally, the architecture is async throughout.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Async Transfer Pipeline                               │
└─────────────────────────────────────────────────────────────────────────────┘

Client API
    │
    ▼
┌──────────────────┐
│  Ingress Queue   │  tokio::sync::mpsc::bounded channel
│  (bounded, 1024) │  Backpressure when full
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Compression     │  Worker Pool (configurable concurrency)
│  Worker Pool     │  Default: num_cpus workers
│  N workers       │  Each: pick from ingress, compress, push to transfer
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Transfer        │  Worker Pool (configurable concurrency)
│  Worker Pool     │  Default: num_cpus workers
│  N workers       │  Each: pick from compress, transfer, push to placement
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Tier Placement  │  Async
│  (async)         │  Policy decides, backend allocates, data written
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Completion      │  Notification via oneshot channel per request
│  Notification    │  Client awaits completion
└──────────────────┘
```

### 5.2 Pipeline Implementation

```rust
/// Async transfer pipeline
pub struct AsyncPipeline {
    /// Ingress channel (client → compression)
    ingress_tx: mpsc::Sender<PipelineRequest>,
    ingress_rx: mpsc::Receiver<PipelineRequest>,

    /// Compression → Transfer channel
    compress_tx: mpsc::Sender<CompressedRequest>,
    compress_rx: mpsc::Receiver<CompressedRequest>,

    /// Transfer → Placement channel
    transfer_tx: mpsc::Sender<TransferRequest>,
    transfer_rx: mpsc::Receiver<TransferRequest>,

    /// Configuration
    config: PipelineConfig,

    /// Shutdown signal
    shutdown: tokio::sync::broadcast::Receiver<()>,
}

pub struct PipelineConfig {
    /// Max pending requests in ingress queue
    pub ingress_capacity: usize,

    /// Number of compression workers
    pub compression_workers: usize,

    /// Number of transfer workers
    pub transfer_workers: usize,

    /// Max time to wait for a stage to complete
    pub stage_timeout: Duration,

    /// Max time for entire pipeline (store/retrieve)
    pub pipeline_timeout: Duration,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            ingress_capacity: 1024,
            compression_workers: num_cpus::get(),
            transfer_workers: num_cpus::get(),
            stage_timeout: Duration::from_secs(30),
            pipeline_timeout: Duration::from_secs(120),
        }
    }
}

/// A request flowing through the pipeline
pub struct PipelineRequest {
    pub chunk_id: ChunkId,
    pub data: Vec<u8>,
    pub operation: PipelineOperation,
    pub completion: oneshot::Sender<Result<PipelineResult, PipelineError>>,
    pub trace_context: TraceContext,
}

pub enum PipelineOperation {
    Store { preferred_tier: Option<TierId> },
    Retrieve,
    Migrate { target_tier: TierId },
}
```

### 5.3 Worker Model

```rust
/// Spawn the pipeline with all worker tasks
impl AsyncPipeline {
    pub fn spawn(config: PipelineConfig, shutdown: tokio::sync::broadcast::Receiver<()>) -> Self {
        let (ingress_tx, ingress_rx) = mpsc::channel(config.ingress_capacity);
        let (compress_tx, compress_rx) = mpsc::channel(config.ingress_capacity);
        let (transfer_tx, transfer_rx) = mpsc::channel(config.ingress_capacity);

        // Spawn compression workers
        for i in 0..config.compression_workers {
            let rx = ingress_rx.resubscribe();  // or use a shared receiver pattern
            let tx = compress_tx.clone();
            let shutdown = shutdown.resubscribe();
            tokio::spawn(compression_worker(i, rx, tx, shutdown));
        }

        // Spawn transfer workers
        for i in 0..config.transfer_workers {
            let rx = compress_rx.resubscribe();
            let tx = transfer_tx.clone();
            let shutdown = shutdown.resubscribe();
            tokio::spawn(transfer_worker(i, rx, tx, shutdown));
        }

        // Spawn placement handler
        let shutdown_clone = shutdown.resubscribe();
        tokio::spawn(placement_handler(transfer_rx, shutdown_clone));

        Self {
            ingress_tx, ingress_rx,
            compress_tx, compress_rx,
            transfer_tx, transfer_rx,
            config,
            shutdown,
        }
    }
}

/// Compression worker: picks up from ingress, compresses, pushes to transfer
async fn compression_worker(
    id: usize,
    mut rx: mpsc::Receiver<PipelineRequest>,
    tx: mpsc::Sender<CompressedRequest>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            Some(request) = rx.recv() => {
                let compressed = compress(&request.data).await;
                let _ = tx.send(CompressedRequest {
                    chunk_id: request.chunk_id,
                    data: compressed,
                    operation: request.operation,
                    completion: request.completion,
                    trace_context: request.trace_context,
                }).await;
            }
            _ = shutdown.recv() => {
                tracing::info!(worker_id = id, "Compression worker shutting down");
                break;
            }
        }
    }
}

/// Transfer worker: picks up from compression, transfers, pushes to placement
async fn transfer_worker(
    id: usize,
    mut rx: mpsc::Receiver<CompressedRequest>,
    tx: mpsc::Sender<TransferRequest>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            Some(request) = rx.recv() => {
                // Transfer logic here
                let _ = tx.send(TransferRequest {
                    chunk_id: request.chunk_id,
                    data: request.data,
                    operation: request.operation,
                    completion: request.completion,
                    trace_context: request.trace_context,
                }).await;
            }
            _ = shutdown.recv() => {
                tracing::info!(worker_id = id, "Transfer worker shutting down");
                break;
            }
        }
    }
}
```

### 5.4 Backpressure

Backpressure propagates naturally through bounded channels:
- If compression workers are slow, ingress queue fills up → client `send().await` blocks
- If transfer workers are slow, compression→transfer channel fills up → compression workers block
- If placement is slow, transfer→placement channel fills up → transfer workers block

### 5.5 Graceful Shutdown

```rust
impl AsyncPipeline {
    /// Graceful shutdown: drain in-flight requests, reject new ones
    pub async fn shutdown(&mut self) {
        // 1. Stop accepting new requests (drop ingress sender)
        // 2. In-flight requests complete naturally (workers drain their channels)
        // 3. Workers exit when channels close and shutdown signal received
        // 4. Timeout for stuck requests

        let timeout = self.config.pipeline_timeout;
        tokio::time::timeout(timeout, async {
            // Wait for all channels to drain
            while !self.is_drained().await {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.ok();

        tracing::info!("Pipeline shutdown complete");
    }
}
```

---

## 6. Storage Backend and Placement Policy

### 6.1 Separation of Concerns

This is a critical architectural separation. Storage backends and placement policies are completely independent:

**StorageBackend** (`ghost-tier`, `ghost-vulkan`, `ghost-sim`):
- HOW data is stored (allocation, retrieval, transfer)
- WHERE data lives (which tier)
- Integrity (checksums)
- Has NO knowledge of policies, hotness, or migration decisions

**PlacementPolicy** (`ghost-policy`):
- WHAT migrates and WHEN
- Eviction order
- Hotness tracking
- Has NO knowledge of how data is stored or transferred

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Separation of Concerns                                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────┐       ┌─────────────────────────────┐
│     StorageBackend          │       │     PlacementPolicy         │
│     (ghost-tier,            │       │     (ghost-policy)          │
│      ghost-vulkan,          │       │                             │
│      ghost-sim)             │       │                             │
│                             │       │                             │
│  • allocate()               │       │  • decide_migrations()      │
│  • deallocate()             │       │  • decide_eviction()        │
│  • write()                  │       │  • record_access()          │
│  • read()                   │       │  • hotness()                │
│  • verify_integrity()       │       │                             │
│  • health_check()           │       │  Knows NOTHING about:       │
│                             │       │  • how data is stored       │
│  Knows NOTHING about:       │       │  • transfer mechanisms     │
│  • hotness                  │       │  • allocation details       │
│  • migration decisions      │       │  • backend capabilities     │
│  • eviction order           │       │                             │
│  • access patterns          │       │  Knows NOTHING about:       │
│                             │       │  • hotness                  │
└──────────┬──────────────────┘       └──────────┬──────────────────┘
           │                                      │
           │         ┌──────────────┐             │
           └────────▶│   Engine     │◀────────────┘
                     │ (orchestrates)│
                     └──────────────┘
```

### 6.2 Backend-Policy Interaction

```rust
/// The Engine orchestrates between StorageBackend and PlacementPolicy
/// without coupling them to each other.
pub struct Engine {
    tier_manager: Arc<TierManager>,    // owns StorageBackends
    policy: Arc<dyn PlacementPolicy>,  // owns migration logic
    trace_recorder: Arc<TraceRecorder>,
}

impl Engine {
    /// Execute a migration decision from the policy
    async fn execute_migration(&self, decision: MigrationDecision) -> Result<(), EngineError> {
        let source = self.tier_manager.backend(decision.source_tier).await?;
        let target = self.tier_manager.backend(decision.target_tier).await?;

        // Read from source backend
        let chunk = source.read(&decision.chunk_id).await?;

        // Write to target backend
        target.write(&decision.chunk_id, &chunk.data).await?;

        // Record the migration event
        self.trace_recorder.record_migration(&decision).await?;

        Ok(())
    }
}
```

---

## 7. Memory Lifecycle

### 7.1 Chunk Lifecycle State Machine

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Chunk Lifecycle                                   │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌─────────┐
     │ Created │
     └────┬────┘
          │
          ▼
     ┌─────────┐      ┌─────────┐
     │ Storing │─────▶│ Stored  │
     └─────────┘      └────┬────┘
          │                 │
          │                 ├──────────────────┐
          │                 │                  │
          │                 ▼                  ▼
          │           ┌─────────┐        ┌─────────┐
          │           │  Hot    │        │  Cold   │
          │           │ (RAM)   │        │ (VRAM)  │
          │           └────┬────┘        └────┬────┘
          │                │                  │
          │                │    ┌─────────┐   │
          │                └───▶│Migrating│◀──┘
          │                     │         │
          │                     └────┬────┘
          │                          │
          │                          ▼
          │                     ┌─────────┐
          │                     │Migrated │
          │                     └────┬────┘
          │                          │
          │                          ▼
          │                     ┌─────────┐
          │                     │Accessing│
          │                     └────┬────┘
          │                          │
          │                          ▼
          │                     ┌─────────┐
          └────────────────────▶│Deleted  │
                                └─────────┘
```

### 7.2 Tier Migration Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Tier Migration Decision                               │
└─────────────────────────────────────────────────────────────────────────────┘

                     ┌─────────────────────┐
                     │  Pressure Monitor   │
                     │  (memory pressure   │
                     │   events)           │
                     └──────────┬──────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │  PlacementPolicy    │
                     │                     │
                     │  - LRU/LFU tracking │
                     │  - Access frequency │
                     │  - Tier capacity    │
                     │  - Hotness scores   │
                     └──────────┬──────────┘
                                │
               ┌────────────────┼────────────────┐
               │                │                │
               ▼                ▼                ▼
         ┌──────────┐    ┌──────────┐    ┌──────────┐
         │   RAM    │    │  GPU     │    │  Disk    │
         │  (Hot)   │    │  VRAM    │    │  (Cold)  │
         │          │    │  (Warm)  │    │          │
         └──────────┘    └──────────┘    └──────────┘
               │                │                │
               │   Promote      │   Demote       │
               │◀───────────────┼───────────────▶│
               │                │                │
               └────────────────┴────────────────┘
```

### 7.3 Memory Pressure Response

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Memory Pressure Levels                                  │
└─────────────────────────────────────────────────────────────────────────────┘

Level 0: Normal
├── All tiers operating normally
├── New allocations go to RAM
└── Background: migrate cold chunks to VRAM/Disk

Level 1: Soft Pressure (memory > 80% of RAM)
├── Begin migrating cold chunks to VRAM
├── Reduce cache sizes
└── Log warnings

Level 2: Medium Pressure (memory > 90% of RAM)
├── Aggressively migrate to VRAM and Disk
├── Reject new large allocations
└── Alert via metrics

Level 3: Hard Pressure (memory > 95% of RAM)
├── Emergency migration of all cold data
├── Synchronous operations only
├── Prepare for OOM
└── Critical alerts

Level 4: Critical (OOM imminent)
├── Reject all new allocations
├── Flush all possible data to disk
└── Graceful degradation mode
```

---

## 8. Chunk Format

### 8.1 On-Disk Format

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          On-Disk Chunk Layout                                │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         Chunk File Header                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ Offset  │ Size    │ Type        │ Description                               │
├─────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ 0       │ 8 bytes │ [u8; 8]     │ Magic: b"GHOST\x01\x02\x03"              │
│ 8       │ 4 bytes │ u32 LE      │ Version: 1                                │
│ 12      │ 32 bytes│ [u8; 32]    │ Chunk ID (blake3 hash)                    │
│ 44      │ 8 bytes │ u64 LE      │ Original size (uncompressed)              │
│ 52      │ 8 bytes │ u64 LE      │ Compressed size                           │
│ 60      │ 1 byte  │ u8          │ Compression algorithm                     │
│ 61      │ 1 byte  │ u8          │ Tier ID                                   │
│ 62      │ 8 bytes │ u64 LE      │ Created timestamp (unix millis)           │
│ 70      │ 8 bytes │ u64 LE      │ Last accessed timestamp                   │
│ 78      │ 8 bytes │ u64 LE      │ Access count                              │
│ 86      │ 32 bytes│ [u8; 32]    │ Checksum (blake3 of compressed data)      │
│ 118     │ 10 bytes│ [u8; 10]    │ Reserved (padding to 128 bytes)           │
├─────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ 128     │ N bytes │ [u8]        │ Compressed data payload                   │
│ 128+N   │ 8 bytes │ u64 LE      │ End marker: 0xDEADBEEFCAFEBABE           │
└─────────────────────────────────────────────────────────────────────────────┘

Total header size: 128 bytes
Total overhead per chunk: 136 bytes (header + end marker)
```

### 8.2 In-Memory Representation

```rust
/// In-memory chunk representation
pub struct Chunk {
    pub meta: ChunkMeta,
    pub data: ChunkData,
    pub state: ChunkState,
}

pub enum ChunkData {
    /// Data is in system RAM
    InMemory(Vec<u8>),
    /// Data is in GPU VRAM (Vulkan buffer)
    InGpu(GpuBuffer),
    /// Data is on disk (file path)
    OnDisk(PathBuf),
    /// Data is being transferred
    InTransfer,
}

pub enum ChunkState {
    /// Being written to
    Writing,
    /// Stable, readable
    Stable,
    /// Being migrated between tiers
    Migrating,
    /// Being read
    Reading,
    /// Marked for deletion
    Deleting,
}

/// GPU buffer wrapper
pub struct GpuBuffer {
    pub buffer: ash::vk::Buffer,
    pub memory: ash::vk::DeviceMemory,
    pub size: usize,
    pub device: Arc<ash::Device>,
}
```

### 8.3 Metadata Database

```rust
/// Chunk index entry (stored in metadata DB)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkIndexEntry {
    pub chunk_id: ChunkId,
    pub meta: ChunkMeta,
    pub location: ChunkLocation,
    pub state: ChunkState,
}

pub enum ChunkLocation {
    Ram { offset: usize },
    Gpu { buffer_handle: u64 },
    Disk { path: PathBuf },
    Sim { offset: usize },  // Simulation backend location
}

/// Metadata store using sled (embedded DB) or SQLite
pub struct MetadataStore {
    db: sled::Db,
    index: RwLock<HashMap<ChunkId, ChunkIndexEntry>>,
}
```

### 8.4 File Naming Convention

```
/var/lib/ghostpages/
├── metadata.db           # Chunk metadata database
├── chunks/
│   ├── ram/              # RAM-backed chunks (tmpfs or memfd)
│   │   ├── {chunk_id_hex}.chunk
│   │   └── ...
│   ├── gpu/              # GPU-backed chunks (staging files)
│   │   ├── {chunk_id_hex}.chunk
│   │   └── ...
│   ├── disk/             # Disk-backed chunks
│   │   ├── {chunk_id_hex}.chunk
│   │   └── ...
│   └── sim/              # Simulation backend chunks
│       ├── {chunk_id_hex}.chunk
│       └── ...
├── sockets/
│   └── ghostpages.sock   # Unix domain socket
├── traces/               # Recorded trace files
│   └── {timestamp}.ghosttrace
└── logs/
    └── daemon.log
```

---

## 9. IPC Protocol

### 9.1 Transport Layer

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          IPC Transport Stack                                 │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│  Application     │
├──────────────────┤
│  ghost-ipc       │
│  (client lib)    │
├──────────────────┤
│  Unix Socket     │  ← Control messages (requests, responses)
│  SOCK_STREAM     │
├──────────────────┤
│  Shared Memory   │  ← Data payload (zero-copy where possible)
│  (memfd + mmap)  │
└──────────────────┘
          │
          ▼
┌──────────────────┐
│  ghost-daemon    │
└──────────────────┘
```

### 9.2 Message Protocol

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Message Frame                                       │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ Offset  │ Size    │ Type        │ Description                               │
├─────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ 0       │ 4 bytes │ [u8; 4]     │ Magic: b"GMSG"                            │
│ 4       │ 2 bytes │ u16 LE      │ Message type                              │
│ 6       │ 2 bytes │ u16 LE      │ Flags                                     │
│ 8       │ 4 bytes │ u32 LE      │ Request ID (correlation)                  │
│ 12      │ 4 bytes │ u32 LE      │ Payload length (bytes)                    │
│ 16      │ 8 bytes │ u64 LE      │ Timestamp (nanos since epoch)            │
│ 24      │ 8 bytes │ u64 LE      │ Reserved                                  │
│ 32      │ N bytes │ [u8]        │ Payload (bincode serialized)              │
│ 32+N    │ 4 bytes │ u32 LE      │ CRC32 of payload                          │
└─────────────────────────────────────────────────────────────────────────────┘

Total frame overhead: 36 bytes + payload
```

### 9.3 Message Types

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum MessageType {
    // Connection management
    Ping = 0x0001,
    Pong = 0x0002,

    // Object operations
    StoreRequest = 0x0100,
    StoreResponse = 0x0101,
    RetrieveRequest = 0x0102,
    RetrieveResponse = 0x0103,
    DeleteRequest = 0x0104,
    DeleteResponse = 0x0105,
    MetadataRequest = 0x0106,
    MetadataResponse = 0x0107,

    // Tier operations
    MigrateRequest = 0x0200,
    MigrateResponse = 0x0201,
    TierStatusRequest = 0x0202,
    TierStatusResponse = 0x0203,

    // System operations
    StatsRequest = 0x0300,
    StatsResponse = 0x0301,
    HealthRequest = 0x0302,
    HealthResponse = 0x0303,

    // Trace replay operations
    ReplayStartRequest = 0x0400,
    ReplayStartResponse = 0x0401,
    ReplayStopRequest = 0x0402,
    ReplayStopResponse = 0x0403,

    // Errors
    ErrorResponse = 0xFF00,
}

bitflags! {
    pub struct MessageFlags: u16 {
        const RESPONSE = 0x0001;
        const ERROR = 0x0002;
        const LARGE_PAYLOAD = 0x0004;  // Use shared memory
        const COMPRESSED = 0x0008;
        const URGENT = 0x0010;
    }
}
```

### 9.4 Request/Response Payloads

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreRequest {
    pub data_size: usize,
    pub compression: CompressionAlgorithm,
    pub preferred_tier: Option<TierId>,
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreResponse {
    pub chunk_id: ChunkId,
    pub stored_size: usize,
    pub tier: TierId,
    pub duration_micros: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrieveRequest {
    pub chunk_id: ChunkId,
    pub decompress: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrieveResponse {
    pub chunk_id: ChunkId,
    pub data_size: usize,
    pub compression: CompressionAlgorithm,
    pub tier: TierId,
    pub shm_region: Option<ShmRegion>,  // If using shared memory
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub chunk_id: ChunkId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub chunk_id: ChunkId,
    pub freed_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateRequest {
    pub chunk_id: ChunkId,
    pub target_tier: TierId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateResponse {
    pub chunk_id: ChunkId,
    pub source_tier: TierId,
    pub target_tier: TierId,
    pub duration_micros: u64,
}
```

### 9.5 Shared Memory Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Shared Memory Region Layout                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         Region Header                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ Offset  │ Size    │ Type        │ Description                               │
├─────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ 0       │ 4 bytes │ [u8; 4]     │ Magic: b"GSHM"                            │
│ 4       │ 4 bytes │ u32 LE      │ Region version                            │
│ 8       │ 8 bytes │ u64 LE      │ Region ID                                 │
│ 16      │ 8 bytes │ u64 LE      │ Total size                                │
│ 24      │ 4 bytes │ u32 LE      │ Slot count                                │
│ 28      │ 4 bytes │ u32 LE      │ Active slots bitmap offset                │
│ 32      │ 32 bytes│ [u8; 32]    │ Reserved                                  │
├─────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ 64      │         │             │ Slot Array                                │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         Slot Layout (each slot)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ Offset  │ Size    │ Type        │ Description                               │
├─────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ 0       │ 4 bytes │ u32 LE      │ Slot state (0=free, 1=writing, 2=ready)   │
│ 4       │ 32 bytes│ [u8; 32]    │ Chunk ID (or zeros if free)               │
│ 36      │ 8 bytes │ u64 LE      │ Data offset (from region start)           │
│ 44      │ 8 bytes │ u64 LE      │ Data size                                 │
│ 52      │ 32 bytes│ [u8; 32]    │ Checksum (blake3)                         │
│ 84      │ 12 bytes│ [u8; 12]    │ Reserved                                  │
├─────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ 96      │ N bytes │ [u8]        │ Data payload                              │
└─────────────────────────────────────────────────────────────────────────────┘

Slot size: 96 bytes header + MAX_PAYLOAD_SIZE
Default MAX_PAYLOAD_SIZE: 64 MB
Default slot count: 16
Default region size: ~1 GB
```

### 9.6 Protocol Flow Examples

#### Store Operation (Small Payload)

```
Client                              Daemon
   │                                    │
   │──── StoreRequest (via socket) ────▶│
   │                                    │
   │                                    │── Enqueue to Ingress Queue
   │                                    │── Compression Worker picks up
   │                                    │── Transfer Worker picks up
   │                                    │── Placement: select tier, allocate
   │                                    │── StorageBackend.write()
   │                                    │── Update metadata
   │                                    │── Record trace event
   │                                    │
   │◀─── StoreResponse (via socket) ───│
   │                                    │
```

#### Store Operation (Large Payload via Shared Memory)

```
Client                              Daemon
   │                                    │
   │── Request shared memory region ───▶│
   │                                    │
   │◀── ShmRegion info ────────────────│
   │                                    │
   │── Write data to shared memory ────│
   │                                    │
   │──── StoreRequest (via socket) ────▶│
   │     (with shm_region info)         │
   │                                    │
   │                                    │── Enqueue to Ingress Queue
   │                                    │── Compression Worker picks up
   │                                    │── Transfer Worker: copy from SHM to tier
   │                                    │── Update metadata
   │                                    │── Record trace event
   │                                    │
   │◀─── StoreResponse (via socket) ───│
   │                                    │
   │── Release shared memory slot ────▶│
```

---

## 10. Data Flow and Concurrency Model

### 10.1 Transfer Lifecycle State Machine

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Transfer Lifecycle (per chunk)                            │
└─────────────────────────────────────────────────────────────────────────────┘

                        ┌──────────────┐
                        │   Pending    │
                        │ (in ingress  │
                        │  queue)      │
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │  Compressing │
                        │              │
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │  Compressed  │
                        │ (waiting for │
                        │  transfer)   │
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │  Transferring│
                        │              │
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │   Placing    │
                        │ (select tier,│
                        │  allocate)   │
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │   Writing    │
                        │ (to backend) │
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │   Verifying  │
                        │ (checksum)   │
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │   Complete   │
                        │ (notify      │
                        │  client)     │
                        └──────────────┘

    At any state ──▶ Cancelled (shutdown or client cancel)
    At any state ──▶ Failed (error, retry or propagate)
```

### 10.2 Worker Model

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Worker Topology                                      │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              ghost-daemon                                     │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        IPC Server Task                                │   │
│  │  (accepts connections, spawns per-connection handlers)               │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     Pipeline Tasks                                    │   │
│  │                                                                       │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │   │
│  │  │ Compression │  │ Compression │  │ Transfer    │  │ Transfer   │  │   │
│  │  │ Worker 0    │  │ Worker 1    │  │ Worker 0    │  │ Worker 1   │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘  │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │   │
│  │  │ Compression │  │ Compression │  │ Transfer    │  │ Transfer   │  │   │
│  │  │ Worker 2    │  │ Worker 3    │  │ Worker 2    │  │ Worker 3   │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘  │   │
│  │                                                                       │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │   │
│  │  │                   Placement Handler Task                        │  │   │
│  │  │  (single task, handles tier selection and backend allocation)  │  │   │
│  │  └─────────────────────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     Background Tasks                                  │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │   │
│  │  │ Policy      │  │ Metrics     │  │ Trace       │  │ Health     │  │   │
│  │  │ Evaluator   │  │ Collector   │  │ Recorder    │  │ Checker    │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 10.3 Channel Topology

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Channel Ownership                                     │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌──────────────────────┐
                    │   IPC Server Task    │
                    │                      │
                    │  Receives requests   │
                    │  from clients via    │
                    │  Unix socket         │
                    └──────────┬───────────┘
                               │
                    ingress_tx │ (mpsc::Sender)
                    (bounded,  │
                     capacity  │
                     = 1024)   │
                               ▼
                    ┌──────────────────────┐
                    │   Ingress Queue      │
                    │   (mpsc channel)     │
                    │                      │
                    │  Owned by: Pipeline  │
                    │  Consumed by:        │
                    │    Compression       │
                    │    Workers           │
                    └──────────┬───────────┘
                               │
                    compress_tx│ (mpsc::Sender)
                    (bounded,  │
                     capacity  │
                     = 1024)   │
                               ▼
                    ┌──────────────────────┐
                    │  Compress→Transfer   │
                    │  (mpsc channel)      │
                    │                      │
                    │  Owned by: Pipeline  │
                    │  Consumed by:        │
                    │    Transfer Workers  │
                    └──────────┬───────────┘
                               │
                    transfer_tx│ (mpsc::Sender)
                    (bounded,  │
                     capacity  │
                     = 1024)   │
                               ▼
                    ┌──────────────────────┐
                    │  Transfer→Placement  │
                    │  (mpsc channel)      │
                    │                      │
                    │  Owned by: Pipeline  │
                    │  Consumed by:        │
                    │    Placement Handler │
                    └──────────────────────┘

  Per-request: oneshot::Sender<Result<...>> (completion notification)
  - Created when request enters pipeline
  - Passed through all stages
  - Fired when request completes or fails
```

### 10.4 Async Boundaries

```rust
/// Key .await points in the pipeline:

// 1. Client sends request (blocks until ingress queue has capacity)
pipeline.ingress_tx.send(request).await?;

// 2. Compression worker awaits work (blocks until request available)
let request = ingress_rx.recv().await;

// 3. Compression itself is CPU-bound — run in spawn_blocking
let compressed = tokio::task::spawn_blocking(move || {
    compress(&request.data)
}).await?;

// 4. Transfer worker awaits work
let request = compress_rx.recv().await;

// 5. Transfer may involve async I/O (e.g., Vulkan DMA)
backend.write(&allocation, &data).await?;

// 6. Client awaits completion
let result = completion_rx.await?;
```

### 10.5 Cancellation Semantics

```rust
/// Cancellation at each stage:

// Stage 1: Ingress Queue
// - Client drops the future → request never sent
// - If already in queue → worker checks cancellation flag before processing

// Stage 2: Compression
// - tokio::select! with shutdown signal
// - In-progress compression cannot be interrupted (CPU-bound)
// - But result is discarded if cancelled

// Stage 3: Transfer
// - Async I/O can be cancelled by dropping the future
// - Partial writes must be handled (write-ahead log or temp allocation)

// Stage 4: Placement
// - Allocation can be undone (deallocate on cancel)
// - Write can be rolled back

/// Cancellation token propagation
pub struct PipelineRequest {
    pub cancellation: tokio_util::sync::CancellationToken,
    // ... other fields
}

// Each stage checks cancellation before starting work
if request.cancellation.is_cancelled() {
    return Err(PipelineError::Cancelled);
}
```

### 10.6 Shutdown Behavior

```rust
/// Shutdown sequence:
///
/// 1. SIGTERM/SIGINT received
/// 2. Broadcast shutdown signal to all tasks
/// 3. IPC server stops accepting new connections
/// 4. Ingress queue stops accepting new requests (sender dropped)
/// 5. In-flight requests drain naturally:
///    - Compression workers finish current item, then exit
///    - Transfer workers finish current item, then exit
///    - Placement handler finishes current item, then exits
/// 6. Timeout: if drain takes > 30s, force-kill remaining
/// 7. Background tasks (policy, metrics, trace) shut down
/// 8. Metadata DB flushed and closed
/// 9. Socket file removed

pub struct ShutdownController {
    /// Broadcast to all pipeline stages
    pipeline_shutdown: tokio::sync::broadcast::Sender<()>,

    /// Broadcast to all background tasks
    background_shutdown: tokio::sync::broadcast::Sender<()>,

    /// Timeout for graceful drain
    drain_timeout: Duration,

    /// Completion signal
    completed: tokio::sync::oneshot::Sender<()>,
}

impl ShutdownController {
    pub async fn shutdown(self) {
        // Signal all stages
        let _ = self.pipeline_shutdown.send(());
        let _ = self.background_shutdown.send(());

        // Wait for drain with timeout
        match tokio::time::timeout(self.drain_timeout, self.completed).await {
            Ok(_) => tracing::info!("Graceful shutdown complete"),
            Err(_) => tracing::warn!("Forced shutdown after timeout"),
        }
    }
}
```

### 10.7 Deadlock Risk Analysis

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Deadlock Risk Analysis                                 │
└─────────────────────────────────────────────────────────────────────────────┘

RISK 1: Circular channel dependency
├── Scenario: Stage A sends to Stage B, Stage B sends to Stage A
├── Mitigation: Pipeline is strictly linear (ingress → compress → transfer → placement)
│   No back-channels between stages. Feedback (e.g., "tier full") goes through
│   the policy engine, not through the pipeline channels.
└── Risk Level: LOW (by design)

RISK 2: Metadata lock ordering
├── Scenario: Two migrations try to update the same chunk metadata
├── Mitigation: All metadata updates go through a single MetadataWriter task.
│   Requests are serialized. No lock ordering needed.
└── Risk Level: LOW (single-writer pattern)

RISK 3: Backend allocation deadlock
├── Scenario: Two transfers allocate from the same backend simultaneously
├── Mitigation: Each StorageBackend uses internal locking (Mutex or lock-free).
│   Allocations are independent. No cross-backend locks.
└── Risk Level: LOW (isolated per-backend)

RISK 4: Client completion channel deadlock
├── Scenario: Client drops the oneshot receiver, sender hangs
├── Mitigation: Use try_send or timeout. Log warnings but don't block pipeline.
└── Risk Level: LOW (defensive coding)

RISK 5: Policy evaluation during migration
├── Scenario: Policy engine holds a read lock on metadata while migration needs write
├── Mitigation: Policy engine works on a snapshot of system state (Arc copy).
│   Migrations update metadata through the single MetadataWriter.
└── Risk Level: MEDIUM (requires careful implementation)

SUMMARY: The linear pipeline architecture with single-writer metadata and
backend-isolated locking makes deadlocks unlikely. The main risk is in
the policy-metadata interaction, which is mitigated by snapshot-based reads.
```

---

## 11. Trace Replay System

### 11.1 Overview

The trace system records all migration and access events for later replay. This is a first-class feature, not an afterthought.

### 11.2 Trace Event Format

```rust
/// A single trace event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    /// Nanosecond timestamp
    pub timestamp: u64,

    /// Event type
    pub kind: TraceEventKind,

    /// Trace ID (groups related events)
    pub trace_id: u64,

    /// Span ID (unique per event)
    pub span_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceEventKind {
    /// Chunk stored
    ChunkStored {
        chunk_id: ChunkId,
        size: usize,
        compressed_size: usize,
        tier: TierId,
        duration_micros: u64,
    },

    /// Chunk retrieved
    ChunkRetrieved {
        chunk_id: ChunkId,
        size: usize,
        tier: TierId,
        duration_micros: u64,
    },

    /// Chunk deleted
    ChunkDeleted {
        chunk_id: ChunkId,
        freed_bytes: usize,
    },

    /// Chunk migrated between tiers
    ChunkMigrated {
        chunk_id: ChunkId,
        source_tier: TierId,
        target_tier: TierId,
        size: usize,
        duration_micros: u64,
    },

    /// Eviction decision
    ChunkEvicted {
        chunk_id: ChunkId,
        tier: TierId,
        reason: EvictionReason,
    },

    /// Policy decision
    PolicyDecision {
        decisions: Vec<MigrationDecision>,
        system_state_hash: [u8; 32],
    },

    /// Pressure level change
    PressureChanged {
        old_level: PressureLevel,
        new_level: PressureLevel,
    },

    /// Error event
    Error {
        message: String,
        component: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionReason {
    Capacity,
    Pressure,
    Policy,
    Manual,
}
```

### 11.3 Binary Trace Format

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Binary Trace File Format (.ghosttrace)                    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         File Header                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ Offset  │ Size    │ Type        │ Description                               │
├─────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ 0       │ 8 bytes │ [u8; 8]     │ Magic: b"GTRACE\x01\x02"                 │
│ 8       │ 4 bytes │ u32 LE      │ Version: 1                                │
│ 12      │ 8 bytes │ u64 LE      │ Creation timestamp (unix nanos)          │
│ 20      │ 4 bytes │ u32 LE      │ Event count (approximate)                │
│ 24      │ 4 bytes │ u32 LE      │ Flags (compression, etc.)                │
│ 28      │ 36 bytes│ [u8; 36]    │ Reserved                                  │
├─────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ 64      │         │             │ Event Records                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         Event Record                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│ Offset  │ Size      │ Type        │ Description                             │
├─────────┼───────────┼─────────────┼─────────────────────────────────────────┤
│ 0       │ 2 bytes   │ u16 LE      │ Event type ID                           │
│ 2       │ 8 bytes   │ u64 LE      │ Timestamp (nanos)                       │
│ 10      │ 8 bytes   │ u64 LE      │ Trace ID                                │
│ 18      │ 8 bytes   │ u64 LE      │ Span ID                                 │
│ 26      │ 4 bytes   │ u32 LE      │ Payload length                          │
│ 30      │ N bytes   │ [u8]        │ Payload (bincode or JSON)               │
│ 30+N    │ 4 bytes   │ u32 LE      │ CRC32 of record                         │
└─────────────────────────────────────────────────────────────────────────────┘

Optional: zstd compression on entire file or per-record group.
```

### 11.4 Trace Recorder

```rust
/// Records trace events to a file
pub struct TraceRecorder {
    config: TraceConfig,
    writer: TraceWriter,
    event_count: AtomicU64,
    flush_interval: Duration,
}

pub struct TraceConfig {
    /// Output file path
    pub output_path: PathBuf,

    /// Maximum file size before rotation
    pub max_file_size: usize,

    /// Maximum number of events before rotation
    pub max_events: u64,

    /// Flush interval
    pub flush_interval: Duration,

    /// Enable compression
    pub compress: bool,

    /// Sampling rate (1.0 = record all, 0.1 = record 10%)
    pub sampling_rate: f64,
}

impl TraceRecorder {
    /// Record a trace event (non-blocking, writes to buffered channel)
    pub fn record(&self, event: TraceEvent) {
        if self.should_sample() {
            let _ = self.writer.try_send(event);
        }
    }
}
```

### 11.5 Trace Replayer

```rust
/// Replays recorded traces through the system
pub struct TraceReplayer {
    reader: TraceReader,
    config: ReplayConfig,
}

pub struct ReplayConfig {
    /// Speed multiplier (1.0 = real-time, 2.0 = 2x speed, 0.0 = as fast as possible)
    pub speed: f64,

    /// Filter: only replay events of these types
    pub event_filter: Option<Vec<EventType>>,

    /// Filter: only replay events for these chunk IDs
    pub chunk_filter: Option<HashSet<ChunkId>>,

    /// Target backend for replay (can replay recorded GPU traces on simulation backend)
    pub target_backend: Option<TierId>,

    /// Compare mode: verify replay produces same decisions as original
    pub compare: bool,
}

impl TraceReplayer {
    /// Replay a trace file
    pub async fn replay(
        &mut self,
        pipeline: &AsyncPipeline,
    ) -> Result<ReplayResult, ReplayError> {
        let mut results = ReplayResult::default();

        for batch in self.reader.read_batches() {
            for event in batch {
                match self.replay_event(event, pipeline).await {
                    Ok(_) => results.events_replayed += 1,
                    Err(e) => results.errors.push(e),
                }
            }
        }

        Ok(results)
    }
}

pub struct ReplayResult {
    pub events_replayed: u64,
    pub events_skipped: u64,
    pub errors: Vec<ReplayError>,
    pub duration: Duration,
    pub comparison: Option<ComparisonResult>,
}
```

### 11.6 Use Cases

| Use Case | Description |
|----------|-------------|
| **Tuning Heuristics** | Replay production traces with different policy parameters to find optimal settings |
| **A/B Testing** | Replay the same trace against two different policies, compare results |
| **Regression Testing** | Replay known-good traces after code changes, verify same behavior |
| **Offline Experimentation** | Develop and test new policies without real workloads |
| **Capacity Planning** | Replay traces with different tier capacities to plan resource allocation |
| **Failure Reproduction** | Replay traces that led to failures to debug issues |

---

## 12. Corruption-Testing Infrastructure

### 12.1 Philosophy

This project is fundamentally "memory movement + unsafe boundaries" — silent corruption is the real enemy, not crashes. Corruption testing is not optional; it is a core part of the development workflow.

### 12.2 Fuzz Targets

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Fuzz Targets                                         │
└─────────────────────────────────────────────────────────────────────────────┘

fuzz_targets/
├── chunk_serialization.rs     # Fuzz chunk serialize/deserialize
├── chunk_deserialization.rs   # Fuzz deserialization of corrupted data
├── chunk_roundtrip.rs         # Fuzz compress → transfer → decompress identity
├── ipc_protocol.rs            # Fuzz IPC message parsing
├── pipeline_messages.rs       # Fuzz pipeline message formats
└── trace_format.rs            # Fuzz trace file parsing
```

### 12.3 Fuzz Target Examples

```rust
// fuzz_targets/chunk_serialization.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use ghost_core::chunk::Chunk;

fuzz_target!(|data: &[u8]| {
    // Try to deserialize arbitrary bytes as a chunk
    let _ = bincode::deserialize::<Chunk>(data);
});

// fuzz_targets/chunk_roundtrip.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use ghost_core::chunk::ChunkId;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() { return; }

    // Content-addressed: same data must always produce same ID
    let id1 = ChunkId::from_data(data);
    let id2 = ChunkId::from_data(data);
    assert_eq!(id1, id2);

    // Different data (almost certainly) produces different ID
    if data.len() > 1 {
        let modified = &data[1..];
        let id3 = ChunkId::from_data(modified);
        // Not guaranteed but overwhelmingly likely
        if data[0] != modified[0] {
            // Different data → different ID (with overwhelming probability)
        }
    }

    // Verify round-trip: ID must verify against original data
    assert!(id1.verify(data));
});

// fuzz_targets/ipc_protocol.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use ghost_ipc::protocol::Message;

fuzz_target!(|data: &[u8]| {
    // Try to parse arbitrary bytes as IPC messages
    let _ = Message::decode(data);
});
```

### 12.4 Randomized Chunk Corruption Tests

```rust
/// Randomized corruption test: flip bits, truncate, reorder
#[cfg(test)]
mod corruption_tests {
    use proptest::prelude::*;
    use ghost_core::chunk::*;

    proptest! {
        /// Round-trip: compress → transfer → decompress == identity
        #[test]
        fn roundtrip_identity(
            data in prop::collection::vec(any::<u8>(), 1..1024*1024),
            compression in prop![Zstd | None],
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let original = data.clone();
                let compressed = compress(&data, compression).await;
                let decompressed = decompress(&compressed, compression).await;
                prop_assert_eq!(original, decompressed);
            });
        }

        /// Corruption detection: any bit flip in compressed data is detected
        #[test]
        fn corruption_detected(
            data in prop::collection::vec(any::<u8>(), 1..1024*1024),
            flip_offset in 0..100usize,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let compressed = compress(&data, CompressionAlgorithm::Zstd).await;
                let mut corrupted = compressed.clone();

                if !corrupted.is_empty() {
                    let idx = flip_offset % corrupted.len();
                    corrupted[idx] ^= 0xFF;  // Flip all bits

                    let result = verify_checksum(&corrupted, &blake3::hash(&compressed).into());
                    prop_assert!(result.is_err());  // Must detect corruption
                }
            });
        }

        /// Truncation detection
        #[test]
        fn truncation_detected(
            data in prop::collection::vec(any::<u8>(), 100..1024*1024),
            truncate_at in 50..100usize,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let compressed = compress(&data, CompressionAlgorithm::Zstd).await;
                let truncated = &compressed[..truncate_at.min(compressed.len())];

                let result = decompress(truncated, CompressionAlgorithm::Zstd).await;
                prop_assert!(result.is_err());  // Must detect truncation
            });
        }
    }
}
```

### 12.5 Checksum Verification on Every Read

```rust
/// All reads verify checksums
impl StorageBackend for SimBackend {
    async fn read(&self, allocation: &Allocation, buf: &mut [u8]) -> Result<(), BackendError> {
        // Read data from backing store
        let data = self.read_raw(allocation).await?;

        // ALWAYS verify checksum
        let expected = self.get_checksum(allocation).await?;
        let actual = *blake3::hash(&data).as_bytes();

        if expected != actual {
            return Err(BackendError::CorruptionDetected {
                allocation: allocation.offset,
                expected,
                actual,
            });
        }

        buf.copy_from_slice(&data);
        Ok(())
    }
}
```

### 12.6 Corruption Injection in Simulation Backend

```rust
/// Simulation backend can inject corruption for testing
pub struct SimBackend {
    config: SimulationConfig,
    storage: RwLock<HashMap<ChunkId, Vec<u8>>>,
    rng: StdRng,
}

impl StorageBackend for SimBackend {
    async fn write(&self, allocation: &Allocation, data: &[u8]) -> Result<(), BackendError> {
        let mut corrupted_data = data.to_vec();

        // Inject corruption at configured rate
        if self.rng.gen::<f64>() < self.config.corruption_rate {
            if !corrupted_data.is_empty() {
                let idx = self.rng.gen_range(0..corrupted_data.len());
                corrupted_data[idx] ^= 1 << self.rng.gen_range(0..8);
                tracing::info!("Injecting corruption at byte {}", idx);
            }
        }

        self.storage.write().await.insert(allocation.chunk_id, corrupted_data);
        Ok(())
    }
}
```

### 12.7 Property-Based Testing for Round-Trip Integrity

```rust
/// Property: compress → transfer → decompress == identity
/// This is the fundamental correctness property of the system.

proptest! {
    #[test]
    fn prop_roundtrip_identity(
        data in prop::collection::vec(any::<u8>(), 0..10*1024*1024),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let chunk_id = ChunkId::from_data(&data);

            // Compress
            let compressed = zstd::compress(&data, 3).unwrap();

            // Transfer (simulated)
            let transferred = compressed.clone();

            // Decompress
            let decompressed = zstd::decompress(&transferred, data.len()).unwrap();

            // Identity property
            prop_assert_eq!(data, decompressed);

            // Content-addressed ID verification
            let id2 = ChunkId::from_data(&decompressed);
            prop_assert_eq!(chunk_id, id2);
        });
    }
}
```

---

## 13. Benchmark Plan

### 13.1 Benchmark Categories

| Category | What to Measure | Tools |
|----------|-----------------|-------|
| Throughput | MB/s for store/retrieve | Criterion + custom harness |
| Latency | P50, P95, P99 for operations | Criterion + tracing |
| Tier Migration | Time to move between tiers | Custom benchmarks |
| Compression | Ratio and speed | zstd benchmarks |
| IPC | Round-trip time, throughput | Unix socket benchmarks |
| GPU Transfer | PCIe bandwidth utilization | Vulkan timestamps |
| Memory Usage | RSS, VRAM usage | procfs + Vulkan queries |
| Pipeline | Per-stage latency, queue depth | Pipeline metrics |
| Simulation | Configurable latency/bandwidth accuracy | Simulation benchmarks |
| Trace Replay | Replay speed vs real-time | Replay benchmarks |

### 13.2 Benchmark Scenarios

```rust
/// Benchmark: Store throughput by payload size
/// Payloads: 64B, 1KB, 64KB, 1MB, 64MB, 1GB
/// Measure: Throughput (MB/s), latency (μs)

/// Benchmark: Tier migration latency
/// Scenarios: RAM→VRAM, VRAM→RAM, RAM→Disk, Disk→RAM
/// Measure: Total migration time, PCIe bandwidth

/// Benchmark: Compression effectiveness
/// Data types: random, text, binary, zero-filled
/// Measure: Compression ratio, speed (MB/s)

/// Benchmark: Concurrent clients
/// Clients: 1, 4, 16, 64
/// Operations: mixed store/retrieve
/// Measure: Aggregate throughput, tail latency

/// Benchmark: Memory pressure response
/// Scenario: Fill RAM, trigger migration
/// Measure: Migration rate, system responsiveness

/// Benchmark: Pipeline stage latency
/// Measure: Time spent in each pipeline stage
/// Config: Vary worker counts, measure throughput impact

/// Benchmark: Simulation backend accuracy
/// Compare: Simulated latency/bandwidth vs configured values
/// Measure: Accuracy of simulation

/// Benchmark: Trace replay speed
/// Replay: Various trace files at different speeds
/// Measure: Events/second, accuracy vs original
```

### 13.3 Metrics to Collect

```rust
/// Core operation metrics
pub struct OperationMetrics {
    pub store_total: Counter,
    pub store_bytes_total: Counter,
    pub store_duration_seconds: Histogram,
    pub store_errors_total: Counter,

    pub retrieve_total: Counter,
    pub retrieve_bytes_total: Counter,
    pub retrieve_duration_seconds: Histogram,
    pub retrieve_errors_total: Counter,

    pub delete_total: Counter,
    pub delete_duration_seconds: Histogram,
}

/// Tier metrics
pub struct TierMetrics {
    pub tier_capacity_bytes: Gauge,
    pub tier_used_bytes: Gauge,
    pub tier_available_bytes: Gauge,
    pub tier_migration_in_total: Counter,
    pub tier_migration_out_total: Counter,
    pub tier_migration_duration_seconds: Histogram,
}

/// IPC metrics
pub struct IpcMetrics {
    pub connections_active: Gauge,
    pub requests_total: Counter,
    pub request_duration_seconds: Histogram,
    pub shm_regions_active: Gauge,
    pub shm_bytes_total: Counter,
}

/// GPU metrics
pub struct GpuMetrics {
    pub vram_total_bytes: Gauge,
    pub vram_used_bytes: Gauge,
    pub transfer_duration_seconds: Histogram,
    pub transfer_bytes_total: Counter,
}

/// Pipeline metrics
pub struct PipelineMetrics {
    pub ingress_queue_depth: Gauge,
    pub compression_queue_depth: Gauge,
    pub transfer_queue_depth: Gauge,
    pub compression_workers_active: Gauge,
    pub transfer_workers_active: Gauge,
    pub pipeline_latency_seconds: Histogram,
    pub stage_latency_seconds: Histogram,  // per-stage
}

/// Simulation metrics
pub struct SimulationMetrics {
    pub simulated_latency_seconds: Histogram,
    pub simulated_bandwidth_bytes_per_second: Histogram,
    pub allocation_failures_injected: Counter,
    pub corruption_events_injected: Counter,
    pub corruption_events_detected: Counter,
}
```

### 13.4 Benchmark Harness

```rust
pub struct BenchmarkHarness {
    daemon: DaemonHandle,
    client: GhostPagesClient,
    config: BenchConfig,
}

impl BenchmarkHarness {
    /// Run throughput benchmark
    pub async fn bench_throughput(
        &self,
        payload_size: usize,
        iterations: usize,
    ) -> BenchResult;

    /// Run latency benchmark
    pub async fn bench_latency(
        &self,
        payload_size: usize,
        iterations: usize,
    ) -> LatencyResult;

    /// Run tier migration benchmark
    pub async fn bench_migration(
        &self,
        source: TierId,
        target: TierId,
        payload_size: usize,
    ) -> MigrationResult;

    /// Run concurrent client benchmark
    pub async fn bench_concurrent(
        &self,
        num_clients: usize,
        ops_per_client: usize,
    ) -> ConcurrentResult;

    /// Run pipeline benchmark
    pub async fn bench_pipeline(
        &self,
        config: PipelineConfig,
        payload_size: usize,
        iterations: usize,
    ) -> PipelineResult;

    /// Run trace replay benchmark
    pub async fn bench_replay(
        &self,
        trace_path: &Path,
        speed: f64,
    ) -> ReplayResult;
}
```

---

## 14. Safety Model

### 14.1 Safety Principles

1. **Unsafe Code Isolation**: All `unsafe` blocks are confined to backend implementations
2. **Fail-Safe Defaults**: Operations fail closed, not open
3. **Checksums Everywhere**: All data has integrity verification (blake3)
4. **Graceful Degradation**: System continues operating with reduced capacity
5. **No Silent Corruption**: Detected corruption triggers immediate alerts
6. **Content-Addressed IDs**: ChunkID = blake3(data) provides natural integrity

### 14.2 Unsafe Code Boundaries

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Unsafe Code Isolation                                │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              Safe Rust                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                         Core Engine                                      │ │
│  │  - Chunk management                                                     │ │
│  │  - Policy decisions                                                     │ │
│  │  - Tier orchestration                                                   │ │
│  │  - Async pipeline                                                       │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                      Backend Abstraction                                 │ │
│  │  - Trait definitions (safe)                                             │ │
│  │  - Buffer management (safe wrappers)                                    │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Unsafe Boundaries                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    Vulkan Backend                                        │ │
│  │  - ash::Device (unsafe FFI)                                            │ │
│  │  - Memory mapping (unsafe)                                              │ │
│  │  - Buffer creation (unsafe)                                            │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    Shared Memory                                         │ │
│  │  - mmap (unsafe)                                                        │ │
│  │  - memfd_create (unsafe FFI)                                            │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    System Calls                                          │ │
│  │  - Unix socket (unsafe FFI)                                             │ │
│  │  - io_uring (if used, unsafe FFI)                                       │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 14.3 Error Handling Strategy

```rust
/// Core error types
#[derive(Debug, thiserror::Error)]
pub enum GhostPagesError {
    #[error("chunk not found: {0}")]
    ChunkNotFound(ChunkId),

    #[error("tier {0:?} is full")]
    TierFull(TierId),

    #[error("tier {0:?} unavailable")]
    TierUnavailable(TierId),

    #[error("checksum mismatch for chunk {0}")]
    ChecksumMismatch(ChunkId),

    #[error("corruption detected in {0}")]
    CorruptionDetected(String),

    #[error("compression error: {0}")]
    CompressionError(String),

    #[error("backend error: {0}")]
    BackendError(String),

    #[error("IPC error: {0}")]
    IpcError(String),

    #[error("out of memory")]
    OutOfMemory,

    #[error("operation timed out")]
    Timeout,

    #[error("pipeline error: {0}")]
    PipelineError(String),

    #[error("operation cancelled")]
    Cancelled,

    #[error("trace replay error: {0}")]
    ReplayError(String),
}

/// Result type
pub type Result<T> = std::result::Result<T, GhostPagesError>;
```

### 14.4 Failure Modes and Recovery

| Failure Mode | Detection | Recovery |
|--------------|-----------|----------|
| GPU device lost | Vulkan error | Migrate all VRAM chunks to RAM/Disk |
| Chunk corruption | blake3 checksum verification | Delete corrupted chunk, log error |
| Daemon crash | Socket disconnect | Client reconnects, daemon restarts |
| Out of memory | Allocation failure | Reject new operations, migrate cold data |
| PCIe error | Transfer timeout | Retry, then fallback to alternative tier |
| Metadata corruption | DB integrity check | Rebuild from chunk files |
| Pipeline stage failure | Worker panic/timeout | Restart worker, retry request |
| Simulation divergence | Metrics comparison | Log warning, adjust simulation parameters |

### 14.5 Data Integrity

```rust
/// All chunks have blake3 checksums (content-addressed)
pub fn compute_checksum(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// Verify chunk integrity
pub fn verify_chunk(data: &[u8], expected_id: &ChunkId) -> Result<()> {
    if !expected_id.verify(data) {
        return Err(GhostPagesError::ChecksumMismatch(*expected_id));
    }
    Ok(())
}

/// Verify compressed data integrity
pub fn verify_compressed(data: &[u8], expected_checksum: &[u8; 32]) -> Result<()> {
    let computed = compute_checksum(data);
    if &computed != expected_checksum {
        return Err(GhostPagesError::CorruptionDetected(
            format!("checksum mismatch: expected {}, got {}", hex::encode(expected_checksum), hex::encode(computed))
        ));
    }
    Ok(())
}
```

### 14.6 Resource Limits

```rust
/// System resource limits
pub struct ResourceLimits {
    /// Maximum total memory usage (RAM)
    pub max_ram_bytes: usize,

    /// Maximum VRAM usage
    pub max_vram_bytes: usize,

    /// Maximum disk usage
    pub max_disk_bytes: usize,

    /// Maximum number of chunks
    pub max_chunks: usize,

    /// Maximum single chunk size
    pub max_chunk_size: usize,

    /// Maximum concurrent connections
    pub max_connections: usize,

    /// Maximum shared memory regions
    pub max_shm_regions: usize,

    /// Maximum pipeline requests in flight
    pub max_pipeline_inflight: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_ram_bytes: 4 * 1024 * 1024 * 1024,  // 4 GB
            max_vram_bytes: 2 * 1024 * 1024 * 1024,  // 2 GB
            max_disk_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            max_chunks: 1_000_000,
            max_chunk_size: 1024 * 1024 * 1024,      // 1 GB
            max_connections: 256,
            max_shm_regions: 64,
            max_pipeline_inflight: 1024,
        }
    }
}
```

---

## 15. Risk Analysis

### 15.1 Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| NVIDIA driver instability | High | High | Graceful fallback to RAM tier; extensive error handling |
| PCIe bandwidth saturation | Medium | High | Batch transfers; async operations; bandwidth monitoring |
| VRAM fragmentation | Medium | Medium | Defragmentation; allocation strategies; monitoring |
| Vulkan validation errors | Medium | Low | Extensive testing; validation layers in dev |
| Memory pressure false positives | Medium | Medium | Hysteresis; configurable thresholds; monitoring |
| zstd compression overhead | Low | Low | Benchmark; optional compression; level tuning |
| Unix socket scalability | Low | Medium | Connection pooling; io_uring for high concurrency |
| Async pipeline complexity | Medium | Medium | Thorough testing; simulation backend for development |
| Silent data corruption | Low | Critical | blake3 checksums on every read; corruption injection testing |
| Deadlock in pipeline | Low | High | Linear pipeline design; deadlock analysis (Section 10.7) |

### 15.2 NVIDIA-Specific Problems

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      NVIDIA-Specific Risks                                   │
└─────────────────────────────────────────────────────────────────────────────┘

1. PROPRIETARY DRIVER ISSUES
   - Vulkan support varies by driver version
   - Memory allocation limits may be opaque
   - GPU reset events can cause VK_ERROR_DEVICE_LOST
   - Mitigation: Extensive error handling, fallback paths

2. VRAM VISIBILITY
   - Some GPUs have limited VRAM visibility via Vulkan
   - Integrated GPUs share system RAM
   - Mitigation: Query actual available VRAM, handle gracefully

3. PCIe TRANSFER OVERHEAD
   - CPU↔GPU transfers limited by PCIe bandwidth
   - PCIe 3.0 x16: ~12 GB/s theoretical
   - PCIe 4.0 x16: ~25 GB/s theoretical
   - Real-world: 60-70% of theoretical
   - Mitigation: Async transfers, batching, compression

4. GPU COMPUTE CONTENTION
   - Other GPU workloads compete for VRAM and bandwidth
   - Display server may reserve VRAM
   - Mitigation: Configurable VRAM limits, monitoring

5. MULTI-GPU COMPLEXITY
   - Different GPUs have different capabilities
   - Peer-to-peer transfers may not be supported
   - Mitigation: Single GPU initially, abstract for multi-GPU
```

### 15.3 Performance Bottlenecks

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Expected Bottlenecks                                    │
└─────────────────────────────────────────────────────────────────────────────┘

1. PCIe TRANSFER (Highest Impact)
   ┌─────────────────────────────────────────────────────────────────────────┐
   │ CPU RAM ◀══════════ PCIe Bus ══════════▶ GPU VRAM                      │
   │         ~12-25 GB/s theoretical                                         │
   │         ~8-17 GB/s realistic                                            │
   └─────────────────────────────────────────────────────────────────────────┘

2. COMPRESSION OVERHEAD
   ┌─────────────────────────────────────────────────────────────────────────┐
   │ zstd level 3: ~500 MB/s compress, ~1500 MB/s decompress                │
   │ zstd level 9: ~100 MB/s compress, ~1500 MB/s decompress                │
   │ Trade-off: CPU time vs. transfer size                                   │
   └─────────────────────────────────────────────────────────────────────────┘

3. CONTEXT SWITCHES
   ┌─────────────────────────────────────────────────────────────────────────┐
   │ User → Kernel → Driver → GPU → Driver → Kernel → User                 │
   │ Each transfer involves multiple context switches                         │
   │ Mitigation: Batch transfers, async operations                           │
   └─────────────────────────────────────────────────────────────────────────┘

4. METADATA OPERATIONS
   ┌─────────────────────────────────────────────────────────────────────────┐
   │ Chunk lookup, index updates, state transitions                          │
   │ Mitigation: In-memory index, batch updates                              │
   └─────────────────────────────────────────────────────────────────────────┘

5. PIPELINE QUEUE DEPTH
   ┌─────────────────────────────────────────────────────────────────────────┐
   │ Bounded channels can fill under load                                    │
   │ Mitigation: Backpressure, configurable capacity, monitoring             │
   └─────────────────────────────────────────────────────────────────────────┘
```

### 15.4 Risk Mitigation Strategies

1. **Simulation-First Development**: Test all policies and migration logic on `ghost-sim` before GPU hardware
2. **Graceful Degradation**: System continues operating with reduced capacity
3. **Circuit Breakers**: Stop using a tier if error rate exceeds threshold
4. **Health Checks**: Periodic verification of all tiers
5. **Metrics-Driven**: Monitor everything, alert on anomalies
6. **Test-Driven**: Comprehensive test suite including fuzzing and corruption testing
7. **Trace Replay**: Reproduce and debug issues offline using recorded traces

---

## 16. MVP Scope

### 16.1 MVP Definition

The MVP is a functional userspace daemon that can:
1. Accept connections via Unix socket
2. Store data in RAM tier with optional compression
3. Retrieve data by chunk ID (content-addressed)
4. Delete data
5. Report basic metrics
6. Handle errors gracefully
7. Run on simulation backend (no GPU required)

### 16.2 MVP Features

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MVP Features                                       │
└─────────────────────────────────────────────────────────────────────────────┘

✅ Core Features:
    - Unix socket IPC
    - RAM tier backend
    - Simulation backend (primary development backend)
    - zstd compression
    - Content-addressed ChunkID (blake3)
    - Chunk metadata store
    - Basic CLI tool
    - Structured logging
    - Prometheus metrics endpoint
    - Async transfer pipeline (single-threaded internally, async architecture)

✅ Operations:
    - Store data
    - Retrieve data
    - Delete data
    - Query metadata
    - Health check

✅ Safety:
    - blake3 checksum verification on every read
    - Resource limits
    - Graceful error handling
    - Signal handling (SIGTERM, SIGINT)
    - Graceful pipeline shutdown

✅ Testing:
    - cargo fuzz targets
    - Corruption injection tests
    - Property-based round-trip tests
    - Trace recording and replay

❌ NOT in MVP:
    - GPU VRAM support
    - Tier migration
    - Disk tier
    - Shared memory transport
    - Policy engine (simple LRU only)
    - Client library (CLI only)
```

### 16.3 MVP Architecture (Simplified)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MVP Architecture                                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│   CLI Tool       │
│   (ghost-cli)    │
├──────────────────┤
│   Unix Socket    │
├──────────────────┤
│   ghost-daemon   │
│  ┌────────────┐  │
│  │ Async      │  │
│  │ Pipeline   │  │
│  │ (bounded)  │  │
│  └────────────┘  │
│  ┌────────────┐  │
│  │ RAM Tier   │  │
│  │ (HashMap)  │  │
│  └────────────┘  │
│  ┌────────────┐  │
│  │ Simulation │  │
│  │ Backend    │  │
│  └────────────┘  │
│  ┌────────────┐  │
│  │ Metadata   │  │
│  │ Store      │  │
│  └────────────┘  │
│  ┌────────────┐  │
│  │ Metrics    │  │
│  │ Exporter   │  │
│  └────────────┘  │
│  ┌────────────┐  │
│  │ Trace      │  │
│  │ Recorder   │  │
│  └────────────┘  │
└──────────────────┘
```

### 16.4 MVP Success Criteria

- [ ] Daemon starts and accepts connections
- [ ] Store 1 GB of data, retrieve and verify (content-addressed)
- [ ] Handle 1000 concurrent chunks
- [ ] Graceful shutdown on SIGTERM
- [ ] Metrics endpoint responds
- [ ] Simulation backend produces accurate latency/bandwidth
- [ ] All fuzz targets run without panics
- [ ] Corruption injection tests pass (detected, not silent)
- [ ] Trace replay works end-to-end
- [ ] All tests pass
- [ ] No unsafe code in MVP (except dependencies)

---

## 17. Stretch Goals

### 17.1 Post-MVP Features

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Stretch Goals                                        │
└─────────────────────────────────────────────────────────────────────────────┘

Phase 2+:
├── GPU VRAM tier (Vulkan)
├── Disk tier (NVMe)
├── Tier migration (RAM ↔ VRAM ↔ Disk)
├── Shared memory transport
├── Client library (Rust)
├── Policy engine (LRU, LFU, pressure-aware)
├── Memory pressure monitoring (/proc/meminfo)
└── Multi-GPU support

Phase 3+:
├── Allocator interception (LD_PRELOAD)
├── Userspace pseudo-paging
├── DAMON integration research
├── CUDA backend
├── Async I/O (io_uring)
└── Performance optimizations

Phase 4+:
├── Kernel HMM experimentation
├── migrate_vma integration
├── Device-private pages
├── NVIDIA-specific optimizations
└── Production hardening
```

### 17.2 Future Integrations

| Integration | Description | Complexity |
|-------------|-------------|------------|
| Linux HMM | Heterogeneous Memory Management | High |
| migrate_vma | VMA migration | High |
| DAMON | Data Access MONitor | Medium |
| UKSMD | Ultra KSM Daemon | Medium |
| io_uring | Async I/O | Medium |
| eBPF | Observability | Low |
| FUSE | Filesystem interface | Medium |

### 17.3 Research Directions

1. **Hotness Tracking**: Implement DAMON-like access pattern tracking
2. **Compression Strategies**: Adaptive compression based on data type
3. **Prefetching**: Predictive chunk loading based on access patterns
4. **Deduplication**: Content-addressable storage for duplicate chunks (trivial with blake3 ChunkId)
5. **Encryption**: At-rest encryption for sensitive data
6. **UKSMD-like Merging**: Merge identical content pages (natural with content-addressed IDs)

---

## 18. Implementation Roadmap

### 18.1 Phase 0: Foundation (Weeks 1-2)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Phase 0: Foundation                                   │
└─────────────────────────────────────────────────────────────────────────────┘

Goals:
  ☐ Repository scaffolding
  ☐ CI/CD pipeline
  ☐ Workspace structure (ghost-* crate naming)
  ☐ Core types and errors (content-addressed ChunkId)
  ☐ Tracing setup
  ☐ Benchmark harness skeleton
  ☐ Simulation backend skeleton
  ☐ Fuzz target scaffolding
  ☐ Trace recording skeleton

Deliverables:
  - Cargo workspace with all ghost-* crates
  - GitHub Actions CI (build, test, clippy, fuzz)
  - Core types (ChunkId with blake3, TierId, ChunkMeta)
  - Tracing subscriber setup
  - Empty benchmark harness
  - Simulation backend with configurable latency
  - Fuzz targets for chunk serialization
  - Trace recorder/replayer skeleton

Dependencies: None
Risk: Low
```

### 18.2 Phase 1: Core Daemon + Pipeline (Weeks 3-5)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Phase 1: Core Daemon + Pipeline                           │
└─────────────────────────────────────────────────────────────────────────────┘

Goals:
  ☐ Unix socket IPC
  ☐ Async transfer pipeline (ingress → compress → transfer → placement)
  ☐ RAM tier backend
  ☐ Simulation backend (full implementation)
  ☐ Chunk store with content-addressed IDs
  ☐ Metadata store
  ☐ Basic CLI tool
  ☐ Prometheus metrics
  ☐ Trace recording/replay (first-class)
  ☐ Corruption-testing infrastructure

Deliverables:
  - Working daemon with socket IPC
  - Async pipeline with bounded channels and worker pools
  - Store/retrieve/delete operations
  - RAM tier with HashMap backend
  - Simulation backend with latency, bandwidth, fragmentation, failure injection
  - sled metadata store
  - CLI tool for basic operations
  - Prometheus metrics endpoint
  - Trace recording to .ghosttrace files
  - Trace replay for tuning and testing
  - Corruption injection tests
  - Property-based round-trip tests
  - cargo fuzz targets running in CI

Dependencies: Phase 0
Risk: Low-Medium
```

### 18.3 Phase 2: Compression & Safety (Weeks 6-7)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Phase 2: Compression & Safety                             │
└─────────────────────────────────────────────────────────────────────────────┘

Goals:
  ☐ zstd compression engine
  ☐ blake3 checksum verification on every read
  ☐ Resource limits
  ☐ Error handling
  ☐ Integration tests
  ☐ Corruption fuzzing in CI

Deliverables:
  - Compression abstraction with zstd
  - blake3 checksums for all chunks (content-addressed)
  - Configurable resource limits
  - Comprehensive error handling
  - Integration test suite
  - Corruption fuzzing running in CI
  - Property-based tests for round-trip integrity

Dependencies: Phase 1
Risk: Low
```

### 18.4 Phase 3: Placement Policy (Weeks 8-9)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Phase 3: Placement Policy                                 │
└─────────────────────────────────────────────────────────────────────────────┘

Goals:
  ☐ PlacementPolicy trait (backend-agnostic)
  ☐ LRU/LFU policy implementations
  ☐ Eviction order strategies
  ☐ Policy-driven migration on simulation backend
  ☐ Trace replay for policy A/B testing

Deliverables:
  - PlacementPolicy trait independent of StorageBackend
  - LRU and LFU policy implementations
  - Eviction strategies
  - Migration working on simulation backend
  - A/B policy comparison via trace replay
  - Policy benchmarks

Dependencies: Phase 2
Risk: Low-Medium
```

### 18.5 Phase 4: GPU Backend (Weeks 10-14)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Phase 4: GPU Backend                                  │
└─────────────────────────────────────────────────────────────────────────────┘

Goals:
  ☐ Vulkan device enumeration
  ☐ VRAM allocation
  ☐ DMA transfer
  ☐ GPU tier backend (StorageBackend trait)
  ☐ GPU metrics

Deliverables:
  - Vulkan backend implementation (ghost-vulkan)
  - VRAM allocation and deallocation
  - CPU↔GPU data transfer
  - GPU tier in tier manager
  - GPU-specific metrics (VRAM usage, transfer speed)
  - Integration tests with real GPU

Dependencies: Phase 3
Risk: Medium (Vulkan complexity, driver issues)
```

### 18.6 Phase 5: Disk Tier & Pressure (Weeks 15-16)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Phase 5: Disk Tier & Pressure                             │
└─────────────────────────────────────────────────────────────────────────────┘

Goals:
  ☐ Disk tier backend
  ☐ Memory pressure monitoring
  ☐ Pressure-aware migration
  ☐ Shared memory transport

Deliverables:
  - Disk tier with file-based storage
  - /proc/meminfo monitoring
  - Pressure-aware migration triggers
  - Shared memory regions for large payloads
  - Performance benchmarks

Dependencies: Phase 4
Risk: Low-Medium
```

### 18.7 Phase 6: Client Library & Polish (Weeks 17-18)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Phase 6: Client Library & Polish                          │
└─────────────────────────────────────────────────────────────────────────────┘

Goals:
  ☐ Client library (ghost-ipc)
  ☐ Documentation
  ☐ Performance tuning
  ☐ Stability testing

Deliverables:
  - ghost-ipc client library
  - API documentation
  - Performance tuning based on benchmarks
  - Stability testing and bug fixes
  - MVP release

Dependencies: Phase 5
Risk: Low
```

### 18.8 Recommended Implementation Order

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Implementation Priority                                    │
└─────────────────────────────────────────────────────────────────────────────┘

1. ghost-core               ← Foundation types (content-addressed ChunkId)
2. ghost-compress           ← Compression engine
3. ghost-metrics            ← Observability
4. ghost-tier               ← StorageBackend trait + RAM/Disk backends
5. ghost-sim                ← Simulation backend (primary dev/CI backend)
6. ghost-policy             ← PlacementPolicy trait + implementations
7. ghost-replay             ← Trace recording/replay
8. ghost-vulkan             ← Vulkan VRAM backend
9. ghost-ipc               ← Client library
10. ghost-daemon            ← Main daemon (RAM + Simulation tier first)
11. ghost-cli               ← CLI tool
12. fuzz/                   ← Fuzz targets
13. benches/                ← Benchmarks
```

### 18.9 Critical Path

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Critical Path                                        │
└─────────────────────────────────────────────────────────────────────────────┘

Phase 0 (Foundation)
    │
    ▼
Phase 1 (Core Daemon + Pipeline + Simulation + Trace + Corruption)
    │
    ▼
Phase 2 (Compression & Safety)
    │
    ▼
Phase 3 (Placement Policy)
    │
    ▼
Phase 4 (GPU Backend) ◀────── HIGHEST RISK
    │
    ▼
Phase 5 (Disk Tier & Pressure)
    │
    ▼
Phase 6 (Client Library & Polish)
    │
    ▼
MVP Release
```

---

## Appendix A: Configuration Format

```toml
# /etc/ghostpages/daemon.toml

[daemon]
socket_path = "/run/ghostpages/ghostpages.sock"
pid_file = "/run/ghostpages/ghostpages.pid"
log_level = "info"
worker_threads = 4

[limits]
max_ram_bytes = 4294967296      # 4 GB
max_vram_bytes = 2147483648     # 2 GB
max_disk_bytes = 107374182400   # 100 GB
max_chunks = 1000000
max_chunk_size = 1073741824     # 1 GB
max_connections = 256
max_pipeline_inflight = 1024

[compression]
algorithm = "zstd"
level = 3

[metrics]
enabled = true
listen = "127.0.0.1:9090"

[pipeline]
ingress_capacity = 1024
compression_workers = 4
transfer_workers = 4
stage_timeout_seconds = 30
pipeline_timeout_seconds = 120

[tier.ram]
enabled = true
priority = 0

[tier.gpu]
enabled = false
priority = 1
device_index = 0

[tier.disk]
enabled = false
priority = 2
path = "/var/lib/ghostpages/chunks"

[tier.simulation]
enabled = true
priority = 3
capacity_bytes = 2147483648     # 2 GB
transfer_latency_ms = 10
bandwidth_limit_bytes = 8589934592  # 8 GB/s
fragmentation = 0.1
allocation_failure_rate = 0.01
corruption_rate = 0.0
eviction_pressure = true
seed = null                     # null = random, number = deterministic

[trace]
enabled = true
output_path = "/var/lib/ghostpages/traces"
max_file_size = 1073741824      # 1 GB
max_events = 10000000
flush_interval_seconds = 5
compress = true
sampling_rate = 1.0             # 1.0 = record all events

[replay]
speed = 1.0                     # 1.0 = real-time
compare = false
```

## Appendix B: CLI Usage Examples

```bash
# Start daemon
ghost-cli --config /etc/ghostpages/daemon.toml

# Store data
echo "Hello, GhostPages!" | ghost-cli store
# Output: chunk_id: a1b2c3d4...

# Retrieve data
ghost-cli retrieve a1b2c3d4...
# Output: Hello, GhostPages!

# Check daemon health
ghost-cli health

# View metrics
curl http://127.0.0.1:9090/metrics

# Record trace
ghost-cli trace start --output /tmp/workload.ghosttrace

# Replay trace
ghost-cli trace replay /tmp/workload.ghosttrace --speed 2.0

# Run corruption test
ghost-cli test corruption --rate 0.01 --iterations 1000

# Configure simulation
ghost-cli sim config --latency 5ms --bandwidth 4GB/s --fragmentation 0.2

# Stop daemon
ghost-cli shutdown
```

## Appendix C: Glossary

| Term | Definition |
|------|------------|
| Chunk | A unit of stored data with metadata |
| ChunkId | Content-addressed identifier (blake3 hash of data) |
| Tier | A memory/storage layer (RAM, VRAM, Disk, Simulation) |
| Migration | Moving a chunk between tiers |
| StorageBackend | Trait for HOW data is stored (allocation, retrieval, transfer) |
| PlacementPolicy | Trait for WHAT migrates and WHEN (eviction, hotness) |
| Pipeline | Async transfer pipeline (ingress → compress → transfer → place) |
| Trace | Recorded sequence of migration/access events for replay |
| HMM | Heterogeneous Memory Management (kernel) |
| VRAM | Video RAM (GPU memory) |
| DAMON | Data Access MONitor (kernel) |
| UKSMD | Ultra KSM Daemon |
| LRU | Least Recently Used |
| LFU | Least Frequently Used |
| IPC | Inter-Process Communication |
| SHM | Shared Memory |
| blake3 | Cryptographic hash function used for content-addressed ChunkId |

---

*End of Specification*
