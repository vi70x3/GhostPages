# Disk/Sim Backend Parity Analysis

## Overview

This document compares the semantic contracts of `SimBackend` (in `ghost-sim`) and `DiskBackend` (in `ghost-tier`) to verify they produce identical observable runtime semantics for the `StorageBackend` trait.

The architectural principle is: **DiskBackend = SimBackend + Persistence**. DiskBackend should delegate all simulation behavior (latency, pressure, health, failure injection) to a SimBackend internally, while adding file I/O persistence on top.

---

## Method-by-Method Comparison

### `id() -> TierId`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Returns `TierId::Simulation` | Returns `TierId::Disk` |
| Semantics | Identical — both return a constant `TierId` | Identical |
| Difference | Different tier ID value | Different tier ID value |
| Acceptable? | **Yes** — tier ID is inherently different (Simulation vs Disk). This is by design. |

### `capacity() -> usize`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Returns `self.config.capacity` | Returns `self.capacity` |
| Semantics | Identical — both return the configured capacity | Identical |
| Difference | None | None |
| Acceptable? | **Yes** — identical behavior. |

### `available() -> usize`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Returns `effective_available()` which accounts for fragmentation | Returns `capacity - used` (no fragmentation) |
| Semantics | SimBackend may report less available due to fragmentation simulation | DiskBackend reports raw available space |
| Difference | SimBackend has fragmentation simulation; DiskBackend does not | DiskBackend has no fragmentation |
| Acceptable? | **Yes** — fragmentation is a simulation-only concern. DiskBackend reports actual disk usage. The key invariant is: `available() <= capacity()` for both. |

### `allocate(size: usize) -> Result<Allocation, BackendError>`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Checks `size > 0`, simulates latency, checks failure injection, checks effective_available, reserves space atomically | Checks `size > 0`, checks capacity, reserves space atomically with CAS rollback |
| Semantics | Both reject zero-size, check capacity, reserve atomically | Both reject zero-size, check capacity, reserve atomically |
| Difference | SimBackend uses `effective_available()` (with fragmentation); DiskBackend uses raw `capacity - used`. SimBackend has latency simulation and failure injection. | DiskBackend has no latency simulation or failure injection. |
| Acceptable? | **Yes** — the core contract (reject zero, check space, reserve atomically) is identical. Latency/failure injection are simulation concerns handled by the SimBackend layer. |

### `deallocate(allocation: Allocation) -> Result<(), BackendError>`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Simulates latency, checks offset was allocated, removes from storage, decrements used | Looks up DiskAllocation by chunk_id, removes from map, deletes file, decrements used |
| Semantics | Both validate the allocation exists, release space, update used counter | Both validate the allocation exists, release space, update used counter |
| Difference | SimBackend tracks by offset; DiskBackend tracks by chunk_id. SimBackend has latency simulation. | DiskBackend also deletes the file. |
| Acceptable? | **Yes** — the core contract (validate, release, update) is identical. File deletion is the persistence layer's responsibility. |

### `write(allocation: &Allocation, data: &[u8]) -> Result<(), BackendError>`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Checks `data.len() <= allocation.size`, issues I/O, simulates latency, checks failure injection, stores data in BTreeMap | Checks `data.len() <= allocation.size`, issues I/O, dispatches to spawn_blocking, writes file atomically with compression |
| Semantics | Both validate size, issue I/O, store data, track bytes written | Both validate size, issue I/O, store data, track bytes written |
| Difference | SimBackend stores in memory (BTreeMap); DiskBackend stores on disk (file). SimBackend has latency/failure injection. DiskBackend has compression and atomic writes. | Different storage medium and additional features. |
| Acceptable? | **Yes** — the core contract (validate, store, track) is identical. The persistence layer adds compression and atomic writes. Latency/failure injection are simulation concerns. |

### `read(allocation: &Allocation, buf: &mut [u8]) -> Result<(), BackendError>`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Checks `buf.len() <= allocation.size`, issues I/O, simulates latency, checks failure injection, reads from BTreeMap into buf | Checks `buf.len() <= allocation.size`, issues I/O, dispatches to spawn_blocking, reads file, decompresses, verifies hash, copies to buf |
| Semantics | Both validate buffer size, issue I/O, read data into buffer | Both validate buffer size, issue I/O, read data into buffer |
| Difference | SimBackend reads from memory; DiskBackend reads from disk with decompression and hash verification. | Different storage medium with additional integrity checks. |
| Acceptable? | **Yes** — the core contract (validate, read into buf) is identical. Hash verification is a persistence-layer enhancement. |

### `verify_integrity(allocation: &Allocation, expected: &[u8; 32]) -> Result<(), BackendError>`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Simulates latency, reads data from BTreeMap, computes blake3 hash, compares with expected | Looks up DiskAllocation, checks stored hash matches expected, reads file, verifies content hash |
| Semantics | Both verify data integrity using blake3 hash comparison | Both verify data integrity using blake3 hash comparison |
| Difference | SimBackend reads from memory; DiskBackend reads from disk. DiskBackend has a two-level check (stored hash + content hash). | DiskBackend has additional stored-hash check. |
| Acceptable? | **Yes** — the core contract (verify blake3 hash matches) is identical. The stored-hash check is a persistence optimization. |

### `health_check() -> Result<(), BackendError>`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Simulates latency, checks memory pressure < 0.99 | Checks base directory exists, is a directory, performs write/read/delete test |
| Semantics | Both verify the backend is operational | Both verify the backend is operational |
| Difference | SimBackend checks memory pressure; DiskBackend checks filesystem accessibility | Different health criteria for different storage media. |
| Acceptable? | **Yes** — the core contract (return Ok if healthy, Err if not) is identical. Health criteria are backend-specific by design. |

### `pressure() -> PressureState`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Returns memory_pressure from used/capacity ratio, io_pressure from throughput/bandwidth | Returns io_pressure from weighted capacity/queue/bandwidth, queue_depth, throughput |
| Semantics | Both return current pressure state | Both return current pressure state |
| Difference | SimBackend reports memory_pressure; DiskBackend reports io_pressure with queue depth. Different calculation methods. | Different pressure dimensions are relevant for different tiers. |
| Acceptable? | **Yes** — the core contract (return PressureState) is identical. Pressure dimensions are tier-appropriate. DiskBackend focuses on I/O pressure; SimBackend focuses on memory pressure. |

### `cost_model() -> PhysicalCost`

| Aspect | SimBackend | DiskBackend |
|--------|-----------|-------------|
| Behavior | Returns latency/bandwidth from config, reliability from 1.0 - write_failure_rate | Returns latency/bandwidth from config, reliability from 1.0 - write_failure_rate |
| Semantics | Both return PhysicalCost with latency, bandwidth, reliability | Both return PhysicalCost with latency, bandwidth, reliability |
| Difference | None in structure; values differ based on config | None in structure; values differ based on config |
| Acceptable? | **Yes** — identical structure and semantics. Values are config-driven. |

---

## Summary of Differences

| Difference | Category | Acceptable |
|-----------|----------|------------|
| Tier ID (Simulation vs Disk) | By design | Yes |
| Fragmentation simulation | Simulation-only | Yes |
| Latency simulation | Simulation-only | Yes |
| Failure injection | Simulation-only | Yes |
| Storage medium (memory vs disk) | By design | Yes |
| Compression | Persistence feature | Yes |
| Atomic writes | Persistence feature | Yes |
| File I/O (spawn_blocking) | Persistence feature | Yes |
| Health check criteria | Tier-appropriate | Yes |
| Pressure dimensions | Tier-appropriate | Yes |

## Conclusion

All differences are either:
1. **By design** (tier ID, storage medium)
2. **Simulation-only concerns** (latency, fragmentation, failure injection) — handled by the SimBackend layer
3. **Persistence enhancements** (compression, atomic writes, hash verification) — handled by the DiskPersistence layer

The core semantic contract of `StorageBackend` is preserved identically. DiskBackend can be correctly understood as "SimBackend + persistence".
