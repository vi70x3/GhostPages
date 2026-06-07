# Architecture Overview

## Global Runtime Clock: EventMultiplexer

The `EventMultiplexer` is the **global runtime clock** for GhostPages. Every state mutation in the system emits a unified `Event` through the `EventEmitter`, which flows into the `EventMultiplexer`. The `EventMultiplexer` then fans out to all registered handlers:

- **`TracingHandler`** — records each event as a structured `tracing::info_span!` entry
- **`MetricsBridge`** — updates Prometheus counters per event category

This design ensures that **every state change is observable** and **no silent retries or hidden side effects** exist outside the event flow. The event stream is the single source of truth for system behavior.

## Ownership Contract
- **`ghost-daemon`** is the sole orchestrator. It owns the **state machine** and is the only component that mutates chunk state. All other crates must treat the state as read-only.
- **`ghost-core`** provides the canonical `StateMachine` implementation and related types (`ChunkState`, `PressureState`). It does **not** perform any state transitions itself.
- **`ghost-core/src/events.rs`** defines the unified `Event` taxonomy (33 variants across 9 categories: Allocation, Orchestration, Scheduler, Migration, Replay, Pressure, Failure, InvariantViolation, IoEvent). All observability events flow through this type.
- **`ghost-core/src/emitter.rs`** provides `EventEmitter` (mpsc-based typed event emission) with both async `emit()`/`typed()` methods and synchronous `try_emit()` for non-async contexts. `EventEmitter` is `Clone`-able so it can be shared across subsystems. An `AtomicU64` counter auto-stamps `sequence_id` at emission time for total ordering.
- **`ghost-core/src/event_multiplexer.rs`** provides `EventMultiplexer` for fan-out event distribution to multiple `EventHandler` implementations.
- **`ghost-core/src/tracing_bridge.rs`** bridges unified `Event`s to structured `tracing::info_span!` entries via `TracingHandler`.
- **`ghost-metrics/src/event_bridge.rs`** bridges unified `Event`s to Prometheus counter updates via `MetricsBridge` and `EventBridgeMetrics`.
- **`ghost-sim`** is a deterministic simulation backend. It no longer contains its own `StateMachine`; it delegates any state-related queries to the orchestrator (currently stubbed to return errors to enforce the ownership rule).
- **`ghost-replay`** replays trace events and validates state transitions using the `StateMachine` from `ghost-core`. It does not mutate state directly.
- **`ghost-ipc`** defines the IPC protocol (`IpcRequest`, `IpcResponse`) and framing utilities (`read_frame`, `write_frame`). All IPC communication is performed through this crate.
- **`ghost-daemon/src/trace_log.rs`** is the single writer for `TraceLog`. No other crate implements a `TraceLog` writer.
- **Event wiring in `ghost-daemon`** -- `EventEmitter` is threaded through `TransferOrchestrator`, `HealthTracker`, `MigrationEngine`, `PressureMonitor`, `TransferScheduler`, `WorkerPool`, and `ReplayEngine`. Each subsystem calls `try_emit(Event::Variant { ... })` at the appropriate site. The `EventMultiplexer` then fans out to `TracingHandler` and `MetricsBridge`.

## Layer Separation
- **Orchestrator Layer (`ghost-daemon`)** -- coordinates storage back-ends, handles IPC, records trace events, and mutates the state machine.
- **Backend Layer (`ghost-sim`, `ghost-replay`, `ghost-tier`, etc.)** -- implements storage semantics, reads state via the orchestrator, and never mutates it.
- **Policy Layer (`ghost-policy`)** -- makes placement decisions based on pressure information; it does not access the state machine.
- **Evaluation Layer (`ghost-evaluator`)** -- scores recommendations, runs policy tournaments, and tracks recommendation stability; it does not access the state machine or perform I/O.
- **IPC Layer (`ghost-ipc`)** -- centralised protocol definitions and framing; used by the daemon and CLI.

## Enforced Invariants
1. **Single `StateMachine`** -- defined in `ghost-core`; all state queries go through the orchestrator.
2. **Single `TraceLog` writer** -- located in `ghost-daemon/src/trace_log.rs`.
3. **IPC definitions live only in `ghost-ipc`**.
4. **Dead code removed** -- `#[expect(dead_code)]` attributes eliminated from `ghost-daemon` and `ghost-replay`.
5. **Unified event taxonomy** -- all observability events use the `Event` enum from `ghost-core/src/events.rs`; no ad-hoc `tracing!` or Prometheus calls for lifecycle events.

These contracts are verified by the test suite (`cargo test --workspace`).

## Physical Awareness (Phase 3 §5)

The migration engine is **physically aware**: migration decisions account for real I/O cost, not just hotness and pressure.

### PhysicalCost Model

`PhysicalCost` (`ghost-core/src/state.rs`) captures the I/O characteristics of a tier:

| Field | Type | Meaning |
|-------|------|---------|
| `latency_ms` | `f64` | Estimated operation latency in milliseconds |
| `bandwidth_bps` | `f64` | Available bandwidth in bytes per second |
| `reliability` | `f64` | Success rate (0.0–1.0) derived from failure injection config |
| `io_pressure` | `f32` | Current I/O subsystem pressure (0.0–1.0) |
| `queue_depth` | `u32` | Number of pending I/O operations |

The `cost_score()` method combines these into a single comparable metric. The `is_too_pressured()` method returns `true` when `io_pressure > 0.85` or `queue_depth > 64`.

### StorageBackend::cost_model()

The `StorageBackend` trait (`ghost-tier/src/backend.rs`) includes a `cost_model(&self) -> PhysicalCost` method with a default implementation returning `PhysicalCost::new()`. Backends override this:

- **RamBackend** — returns near-zero latency (0.01ms), very high bandwidth (10 GB/s), and current memory pressure as `io_pressure`.
- **SimBackend** — derives cost from `SimConfig` latency/bandwidth settings and failure rates, enabling deterministic physical cost in tests.

### I/O-Aware Migration Decisions

`MigrationEngine::decide_migration()` evaluates each candidate migration against:

1. **Backpressure state** — if the current `BackpressureAction` does not allow the migration's priority, the migration is **rejected** (emits `MigrationRejected`).
2. **I/O pressure** — if `io_cost.is_too_pressured()`, the migration is **deferred** (emits `MigrationDeferred`).
3. **I/O cost threshold** — if `io_cost.cost_score() > config.io_cost_threshold`, the migration is **deferred** (emits `MigrationDeferred`).
4. **Capacity** — if the engine is at `max_concurrent_migrations`, the migration is **deferred** (emits `MigrationDeferred`).

Only when all checks pass is the migration **decided** (emits `MigrationDecided`).

`MigrationEngine::estimate_io_cost()` combines the `cost_model()` of source and destination tiers to produce a deterministic I/O cost estimate for a migration.

### I/O-Aware Backpressure

`BackpressureController::evaluate()` now considers I/O-specific pressure alongside overall system pressure:

- **`io_pressure_soft_limit` (default 0.6)** — I/O pressure above this triggers `Throttle` even when overall pressure is low.
- **`io_pressure_hard_limit` (default 0.85)** — I/O pressure above this triggers `Reject`.
- **`queue_depth_threshold` (default 32)** — queue depth above this triggers `Throttle`; above 2× triggers `Reject`.

The controller picks the more restrictive of the I/O-derived action and the overall-pressure-derived action.

### Migration Event Lifecycle

Physical-aware migration emits three event variants:

- **`MigrationDecided`** — migration passed all checks and will proceed.
- **`MigrationDeferred`** — migration postponed due to I/O pressure, cost, or capacity.
- **`MigrationRejected`** — migration blocked by backpressure for its priority level.

These events flow through the `EventEmitter` → `EventMultiplexer` → `TracingHandler`/`MetricsBridge` pipeline, ensuring full observability of physical-aware decisions.

### Determinism

All physical cost calculations are deterministic functions of:
- Backend configuration (latency, bandwidth, failure rates)
- Current pressure state (snapshot at decision time)
- Seeded RNG in `SimBackend` (via `ChaCha8Rng`)

Given the same inputs, `decide_migration()` always produces the same output — verified by replay equivalence tests.

## Recommendation Evaluation & Policy Intelligence (Phase 4.5)

The `ghost-evaluator` crate proves that GhostPages can make **better memory placement decisions than baseline Linux behavior**. It scores recommendations, runs policy tournaments, tracks recommendation stability, and exposes evaluation metrics — all without performing I/O or accessing the state machine.

### Design Principle: Pure Deterministic Scoring

All scoring functions in `ghost-evaluator` are **pure/deterministic** — no I/O, no mutation, no side effects. Given the same inputs, every scoring function always produces the same output. This enables reproducible evaluation and trustworthy policy comparison.

### Modules

| Module | Purpose |
|--------|---------|
| `scoring` | `RecommendationScore` with 6 weighted metrics: fault_reduction, swap_reduction, zram_efficiency, pressure_reduction, tier_balance, stability |
| `baseline` | `LinuxBaselinePolicy` representing default Linux memory placement behavior; serves as the "do nothing" comparison baseline |
| `tournament` | `PolicyArena` tournament framework for head-to-head policy comparison with multiple rounds and leaderboard tracking |
| `stability` | `RecommendationStability` measuring recommendation churn — detects tier oscillation, temperature flips, and confidence variance |
| `adaptive` | `AdaptiveTemperatureModel` for dynamic threshold adjustment based on system pressure and occupancy trends |
| `lifecycle` | `RegionLifecycle` tracking temperature transitions (Hot → Warm → Cold → Frozen) with promotion/demotion counters |
| `replay_analytics` | `ReplayAnalysisReport` for policy disagreement detection — compares what different policies would have recommended from the same signal snapshots |
| `evaluator_metrics` | `EvaluatorMetrics` with 6 Prometheus metrics: recommendation scores, policy wins, stability indices, promotion/demotion counts, policy comparisons |

### Key Types

| Type | Description |
|------|-------------|
| `RecommendationScore` | 6-metric weighted score (0.0–1.0) evaluating a single recommendation |
| `ScoringWeights` | Configurable weights for each metric in `RecommendationScore` |
| `LinuxBaselinePolicy` | Simulates default Linux behavior (no GhostPages intelligence) |
| `PolicyArena` | Tournament runner that scores multiple policies across the same workload |
| `Policy` trait | Interface for pluggable scoring policies |
| `RecommendationStability` | Tracks recommendation churn per region over time |
| `AdaptiveTemperatureModel` | Dynamically adjusts hot/cold thresholds based on system state |
| `RegionLifecycle` | Records temperature state transitions per memory region |
| `ReplayAnalysisReport` | Identifies where policies disagree on placement decisions |
| `EvaluatorMetrics` | Prometheus metric registration and recording |

### Built-in Policies

The evaluator ships with 4 built-in policies:

1. **`ArenaLinuxBaselinePolicy`** — mirrors default Linux behavior; the baseline to beat
2. **`PressurePolicy`** — scores based on memory pressure reduction potential
3. **`HotnessPolicy`** — scores based on data hotness and access frequency
4. **`HybridPolicy`** — combines pressure and hotness signals for balanced scoring

### CLI Integration

The evaluator is accessible through the `ghost-cli` with 4 subcommands:

- `evaluator score` — score a recommendation against all policies
- `evaluator baseline` — run the Linux baseline comparison
- `evaluator tournament` — run a full policy tournament and display the leaderboard
- `evaluator stability` — report recommendation stability metrics

### Strategic Goal

GhostPages' intelligence pipeline follows a clear progression:

```
Signals → State → Recommendations → Evaluation → Policy Learning
```

The `ghost-evaluator` crate completes the bridge from "Signals → State → Recommendations" to the full pipeline. It enables GhostPages to **evaluate whether its recommendations are actually useful** before implementing real migration — a critical safety property for a production memory management system.

## State Ownership

Only `ghost-daemon` may mutate runtime state. This is the highest-priority architectural contract, enforced continuously through both compile-time and runtime mechanisms.

### Core Rule

All state mutations go through `TransferOrchestrator`. No other crate, module, or subsystem may call `StateMachine::transition()` directly.

### Worker → Orchestrator Channel

Workers never touch the state machine. After completing a transfer, a worker sends a [`WorkerCompletion`] report through a dedicated channel. The orchestrator receives these reports and applies the appropriate state transition:

- **Success**: `Migrating → Stored`
- **Failure**: `Migrating → Failed`

This was the only known state ownership violation (documented in `BOUNDARY_AUDIT.md` §1) and has been refactored.

### Enforcement Mechanisms

1. **Type system**: `WorkerPool` does not hold a `StateMachine` reference. It cannot call `transition()` because it doesn't have the type.
2. **Channel architecture**: Workers report via `WorkerCompletion` channel; orchestrator applies changes.
3. **Module boundary**: `StateMachine::transition()` is only called from `ghost-daemon/src/orchestrator.rs`.
4. **Marker token**: `StateMutationToken` (`ghost-core/src/state_ownership.rs`) provides compile-time gating when the `enforce-state-ownership` feature is enabled.
5. **Runtime audit**: `StateOwnershipLog` records every mutation with module, action, timestamp, and chunk ID for post-hoc verification.
6. **Test verification**: `ghost-daemon/tests/state_ownership.rs` contains 8 tests enforcing the contract.

### Reference

See [`STATE_OWNERSHIP.md`](STATE_OWNERSHIP.md) for the full contract, violation table, and architecture diagram.
