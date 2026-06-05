# Invariant Registry Design Document

## Overview
The **Invariant Registry** provides a single source of truth for runtime invariants that are currently verified only by tests. It enables both the **replay engine** (`crates/ghost-replay`) and the **daemon** (`crates/ghost-daemon`) to invoke the same set of invariant checks, ensuring consistency across debug and release builds.

---

## 1. Core Types

```rust
/// The shared runtime state required by invariants.
/// It aggregates the pieces of state that invariants may need to inspect.
pub struct GhostState {
    /// Mapping of chunk identifiers to their current state.
    pub chunk_map: std::collections::HashMap<ChunkId, ChunkState>,
    /// Queue of pending transfer jobs.
    pub transfer_queue: std::collections::VecDeque<TransferJob>,
    /// Health tracker for the daemon / replay engine.
    pub health: HealthTracker,
    // … any additional fields needed by future invariants.
}

/// Error type returned when an invariant fails.
pub type GhostError = anyhow::Error; // or a custom error enum.
```

*Existing types referenced*: `ChunkState`, `TransferJob`, `HealthTracker` (all defined in their respective crates).

---

## 2. InvariantRegistry Struct

```rust
/// Holds a collection of invariant check functions.
pub struct InvariantRegistry {
    /// Vector of boxed invariant functions.
    invariants: Vec<Box<dyn Fn(&GhostState) -> Result<(), GhostError> + Send + Sync>>, 
}

impl InvariantRegistry {
    /// Creates a new, empty registry.
    pub fn new() -> Self {
        Self { invariants: Vec::new() }
    }

    /// Registers a new invariant at runtime (used by the macro).
    pub fn register<F>(&mut self, f: F)
    where
        F: Fn(&GhostState) -> Result<(), GhostError> + Send + Sync + 'static,
    {
        self.invariants.push(Box::new(f));
    }

    /// Executes all registered invariants against the supplied state.
    pub fn check_all(&self, state: &GhostState) -> Result<(), GhostError> {
        for inv in &self.invariants {
            inv(state)?;
        }
        Ok(())
    }
}
```

---

## 3. Macro `register_invariant!`

The macro simplifies compile‑time registration of invariants. It expands to a call to a **static** `InvariantRegistry` instance that is lazily initialized.

```rust
#[macro_export]
macro_rules! register_invariant {
    ($fn_name:ident) => {
        // Ensure the registry is initialized once per binary.
        static REGISTRY: once_cell::sync::Lazy<std::sync::Mutex<InvariantRegistry>> =
            once_cell::sync::Lazy::new(|| std::sync::Mutex::new(InvariantRegistry::new()));
        // Register the function at program start.
        #[ctor::ctor]
        fn register() {
            REGISTRY.lock().unwrap().register($fn_name);
        }
    };
}
```

*Key points*
- Uses `once_cell::sync::Lazy` for thread‑safe lazy initialization.
- The `ctor` crate ensures registration runs before `main` (or before any use of the registry).
- The macro is invoked in the same module where the invariant function is defined.

---

## 4. Runtime Integration Points

| Integration Point | Description | Code Hook |
|-------------------|-------------|-----------|
| **StateMachine transition** | After each state transition, call `REGISTRY.lock().unwrap().check_all(&state)` to validate invariants. | In `crates/ghost-replay/src/state_machine.rs` (or equivalent) after `self.apply(event)`.
| **Trace event logging** | Immediately after a trace event is emitted, run the checks to catch violations early. | In `crates/ghost-replay/src/tracer.rs` after `self.log(event)`.
| **Daemon background task** | Spawn a periodic async task (e.g., using `tokio::time::interval`) that invokes the checks every second. | In `crates/ghost-daemon/src/background.rs`.

All three locations will import the same `REGISTRY` static, guaranteeing identical invariant sets.

---

## 5. Feature Flag – `runtime-invariants`

To allow disabling the overhead in production builds, wrap the registry and macro in a Cargo feature:

```toml
[features]
# Enabled by default; set to false for minimal runtime overhead.
runtime-invariants = []
```

```rust
#[cfg(feature = "runtime-invariants")]
pub static REGISTRY: once_cell::sync::Lazy<std::sync::Mutex<InvariantRegistry>> =
    once_cell::sync::Lazy::new(|| std::sync::Mutex::new(InvariantRegistry::new()));

#[cfg(not(feature = "runtime-invariants"))]
pub struct InvariantRegistry; // empty stub

#[cfg(not(feature = "runtime-invariants"))]
pub fn check_all(_: &GhostState) -> Result<(), GhostError> { Ok(()) }
```

The macro expands to a no‑op when the feature is disabled, and the `check_all` call becomes a cheap identity function.

---

## 6. Usage Example

```rust
use ghost_core::invariant_registry::{register_invariant, GhostState, GhostError};

fn chunk_map_consistency(state: &GhostState) -> Result<(), GhostError> {
    // Example invariant: every ChunkState must be either `Ready` or `Pending`.
    for (id, cs) in &state.chunk_map {
        match cs {
            ChunkState::Ready | ChunkState::Pending => {}
            _ => return Err(anyhow::anyhow!("Chunk {id:?} in invalid state")),
        }
    }
    Ok(())
}

register_invariant!(chunk_map_consistency);
```

The above function will be automatically registered and executed wherever the registry is consulted.

---

## 7. Documentation & Placement

- The design document lives at `crates/ghost-core/src/invariant_registry.md`.
- The actual implementation (struct, macro, feature gating) will be placed in `crates/ghost-core/src/invariant_registry.rs`.
- Public API is re‑exported from `crates/ghost-core/lib.rs` for easy consumption by both the replay engine and daemon.

---

## 8. Integration Summary

1. **Add the design document** (this file) to `crates/ghost-core/src`.
2. **Implement** `InvariantRegistry`, the `register_invariant!` macro, and feature‑gated stubs in `invariant_registry.rs`.
3. **Replace** existing test‑only invariant checks with calls to the registry’s `check_all`.
4. **Add** the macro invocation to each invariant function in `crates/ghost-replay/src/invariants.rs`.
5. **Hook** the registry into the three runtime points (state machine, tracer, daemon background task).
6. **Toggle** the `runtime-invariants` feature in `Cargo.toml` for release builds where performance is critical.

By following this design, both the replay engine and daemon will share a single, configurable source of runtime invariant checks, improving correctness and maintainability.
