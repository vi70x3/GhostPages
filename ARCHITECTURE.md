# Architecture Overview

## Ownership Contract
- **`ghost-daemon`** is the sole orchestrator. It owns the **state machine** and is the only component that mutates chunk state. All other crates must treat the state as read-only.
- **`ghost-core`** provides the canonical `StateMachine` implementation and related types (`ChunkState`, `PressureState`). It does **not** perform any state transitions itself.
- **`ghost-core/src/events.rs`** defines the unified `Event` taxonomy (18 variants across 6 categories: Allocation, Migration, Replay, Pressure, Failure, InvariantViolation). All observability events flow through this type.
- **`ghost-core/src/emitter.rs`** provides `EventEmitter` (mpsc-based typed event emission) with both async `emit()`/`typed()` methods and synchronous `try_emit()` for non-async contexts. `EventEmitter` is `Clone`-able so it can be shared across subsystems.
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
- **IPC Layer (`ghost-ipc`)** -- centralised protocol definitions and framing; used by the daemon and CLI.

## Enforced Invariants
1. **Single `StateMachine`** -- defined in `ghost-core`; all state queries go through the orchestrator.
2. **Single `TraceLog` writer** -- located in `ghost-daemon/src/trace_log.rs`.
3. **IPC definitions live only in `ghost-ipc`**.
4. **Dead code removed** -- `#[expect(dead_code)]` attributes eliminated from `ghost-daemon` and `ghost-replay`.
5. **Unified event taxonomy** -- all observability events use the `Event` enum from `ghost-core/src/events.rs`; no ad-hoc `tracing!` or Prometheus calls for lifecycle events.

These contracts are verified by the test suite (`cargo test --workspace`).
