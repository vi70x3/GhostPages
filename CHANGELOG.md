# Changelog

All notable changes to the GhostPages project will be documented in this file.

## [Phase 1] - 2025-01-XX

### Added

#### Core Daemon (`ghost-daemon`)
- **Transfer Orchestrator** (`src/orchestrator.rs`): Central coordinator for chunk lifecycle operations (store, retrieve, migrate, evict). Validates all state machine transitions before submitting jobs to the queue.
- **Transfer Scheduler** (`src/scheduler.rs`): Dequeues jobs from the priority queue and dispatches them to the worker pool. Supports pressure-based throttling.
- **Worker Pool** (`src/worker_pool.rs`): Manages a configurable number of async worker tasks that execute transfer jobs against storage backends. Handles retry logic with exponential backoff.
- **IPC Server** (`src/ipc_server.rs`): Unix domain socket server exposing the daemon's functionality via a JSON-based protocol. Supports store, retrieve, delete, migrate, info, list, status, pressure, trace, pressure-check, and shutdown commands.
- **Pressure Monitor** (`src/pressure.rs`): Periodically samples backend pressure states and emits pressure alerts. Maintains a rolling history with trend analysis (rising, falling, stable).
- **Transfer Queue** (`src/queue.rs`): Priority queue with backpressure support. Emits trace events on submission.
- **Trace Log** (`src/trace_log.rs`): Append-only in-memory event log with configurable capacity and ring buffer overflow behavior.
- **Transfer Metrics** (`src/metrics.rs`): Counters for submissions, completions, failures, cancellations, bytes transferred, and timing histograms.
- **Daemon Main Loop** (`src/main.rs`): `run_daemon` function that wires all components together — orchestrator, scheduler, worker pool, IPC server, pressure monitor — and runs them concurrently with graceful shutdown via signal handling.

#### Integration Tests (`ghost-daemon/tests/`)
- **test_store_retrieve.rs**: Validates chunk creation on RAM and simulation tiers, multi-chunk storage, empty data handling, unregistered chunk retrieval failure, and trace event recording for store operations.
- **test_migration.rs**: Tests tier-to-tier migration (RAM → Simulation, Simulation → RAM), invalid state migration rejection, multiple sequential migrations, and trace event emission during migrations.
- **test_pressure_driven_migration.rs**: Verifies pressure check candidate generation, current pressure reporting, pressure history availability, auto-migration flag behavior, and migration when a tier is full.
- **test_ipc_roundtrip.rs**: End-to-end IPC tests for ping, store-and-retrieve, status, pressure, multiple concurrent connections, and migrate commands via the Unix socket protocol.
- **test_replay.rs**: Tests trace export to binary format, trace replay with metrics validation, direct trace log replay, and empty trace file handling.
- **test_concurrent_workloads.rs**: Stress tests with concurrent stores, concurrent migrations, mixed workloads, rapid store-evict cycles, and concurrent stores across different tiers.
- **test_failure_recovery.rs**: Validates behavior under injected backend failures — partial failure tolerance, daemon recovery after failures, failure trace logging, zero-failure-rate success, and migration with failure recovery.

#### Trace Replay System (`ghost-replay`)
- **Binary Trace Format**: Custom binary format with magic bytes, file header, CRC32 checksums, and metadata section.
- **Trace Writer** (`src/writer.rs`): Writes trace events to binary files with checksum validation.
- **Trace Reader** (`src/reader.rs`): Reads trace events from binary files with CRC32 validation and timestamp-based seeking.
- **Replay Engine** (`src/engine.rs`): Replays trace events, validates state machine transitions, and produces replay summaries with error reporting.
- **Replay Metrics** (`src/metrics.rs`): Computes replay statistics (success rate, transfer counts, evictions by reason) and supports policy comparison between two trace files.
- **Export Module** (`src/export.rs`): Exports trace data in JSON, JSONL, and CSV formats.

#### Core Fixes
- **TransferJob serialization**: Fixed map size mismatch in custom `Serialize` implementation (declared 7 entries but wrote 8, causing `attempts` field to be silently dropped during bincode serialization). This resolved replay test failures where `TransferStarted` events could not be deserialized.

### Architecture

The Phase 1 daemon implements a pipeline architecture:

```
Client → IPC Server → Orchestrator → Queue → Scheduler → Worker Pool → Backends
                                      ↕              ↕
                                  Trace Log ← Pressure Monitor
```

- The **Orchestrator** is the single source of truth for state machine transitions.
- The **Scheduler** dequeues jobs and dispatches them without re-validating transitions.
- The **Worker Pool** executes transfers with retry logic and transitions chunks back to `Stored` after cross-tier migrations.
- The **Pressure Monitor** samples backends independently and can throttle the scheduler.
- The **Trace Log** records every operation for later replay and analysis.

### Test Results
- **211 total tests** across all crates (unit + integration)
- **74 unit tests** in `ghost-daemon` (orchestrator, scheduler, worker pool, queue, pressure, metrics, trace log)
- **37 integration tests** across 7 test files
- All tests pass with `cargo test --workspace`
- Code is clean under `cargo clippy --workspace` and `cargo fmt --all`
