# Architecture Overview

## Ownership Contract
- **`ghost-daemon`** is the sole orchestrator. It owns the **state machine** and is the only component that mutates chunk state. All other crates must treat the state as read‑only.
- **`ghost-core`** provides the canonical `StateMachine` implementation and related types (`ChunkState`, `PressureState`). It does **not** perform any state transitions itself.
- **`ghost-sim`** is a deterministic simulation backend. It no longer contains its own `StateMachine`; it delegates any state‑related queries to the orchestrator (currently stubbed to return errors to enforce the ownership rule).
- **`ghost-replay`** replays trace events and validates state transitions using the `StateMachine` from `ghost-core`. It does not mutate state directly.
- **`ghost-ipc`** defines the IPC protocol (`IpcRequest`, `IpcResponse`) and framing utilities (`read_frame`, `write_frame`). All IPC communication is performed through this crate.
- **`ghost-daemon/src/trace_log.rs`** is the single writer for `TraceLog`. No other crate implements a `TraceLog` writer.

## Layer Separation
- **Orchestrator Layer (`ghost-daemon`)** – coordinates storage back‑ends, handles IPC, records trace events, and mutates the state machine.
- **Backend Layer (`ghost-sim`, `ghost-replay`, `ghost-tier`, etc.)** – implements storage semantics, reads state via the orchestrator, and never mutates it.
- **Policy Layer (`ghost-policy`)** – makes placement decisions based on pressure information; it does not access the state machine.
- **IPC Layer (`ghost-ipc`)** – centralised protocol definitions and framing; used by the daemon and CLI.

## Enforced Invariants
1. **Single `StateMachine`** – defined in `ghost-core`; all state queries go through the orchestrator.
2. **Single `TraceLog` writer** – located in `ghost-daemon/src/trace_log.rs`.
3. **IPC definitions live only in `ghost-ipc`**.
4. **Dead code removed** – `#[expect(dead_code)]` attributes eliminated from `ghost-daemon` and `ghost-replay`.

These contracts are verified by the test suite (`cargo test --workspace`).