# Determinism in GhostPages

This document describes the determinism guarantees provided by the GhostPages
system and how they are achieved.

## Overview

GhostPages provides **replay equivalence**: given the same inputs, configuration,
and initial state, the system produces byte-identical outputs across runs. This
property is essential for:

- **Testing**: Tests are reproducible and flaky-free
- **Debugging**: Issues can be replayed with identical conditions
- **Audit**: Trace logs can be verified against expected behavior
- **Simulation**: What-if analysis produces consistent results

## Deterministic Components

### 1. Collections

All ordering-dependent data structures use `BTreeMap`/`BTreeSet` instead of
`HashMap`/`HashSet`. This ensures iteration order is deterministic and
reproducible across runs.

| Module | Type | Replacement |
|--------|------|-------------|
| `ghost-daemon` | `TransferOrchestrator.backends` | `BTreeMap` |
| `ghost-daemon` | `MigrationEngine.backends` | `BTreeMap` |
| `ghost-daemon` | `PressureMonitor.per_tier` | `BTreeMap` |
| `ghost-daemon` | `HotnessTracker.hotness` | `BTreeMap` |
| `ghost-daemon` | `WorkerPool.backends` | `BTreeMap` |
| `ghost-daemon` | `HealthTracker.states` | `BTreeMap` |
| `ghost-daemon` | `DiagnosticSnapshot.backends` | `BTreeMap` |
| `ghost-core` | `StateMachine.states` | `BTreeMap` |
| `ghost-sim` | `SimBackend.storage` | `BTreeMap` |
| `ghost-sim` | `SimBackend.state_map` | `BTreeMap` |
| `ghost-replay` | `ReplayEngine.chunk_states` | `BTreeMap` |
| `ghost-replay` | `ReplayMetrics.tier_distribution` | `BTreeMap` |
| `ghost-tier` | `RamBackend.storage` | `BTreeMap` |
| `ghost-tier` | `AllocationTracker.allocations` | `BTreeMap` |

### 2. Random Number Generation

All random number generation uses `ChaCha8Rng` with a configurable seed:

- `OrchestratorConfig.rng_seed: Option<u64>` — master seed for the daemon
- `SimConfig.seed: u64` — seed for the simulation backend

When `rng_seed` is `Some(seed)`, the orchestrator creates a `ChaCha8Rng` seeded
with this value and passes it to components that need randomness.

### 3. Time Source

The `TimeProvider` trait abstracts time measurement:

- `RealTimeProvider` — delegates to `Instant::now()` (production)
- `DeterministicClock` — advances in fixed steps (testing/replay)

Components that need time can accept a `TimeProvider` implementation,
enabling deterministic time progression in tests.

### 4. Async Task Ordering

Transfer jobs are dispatched in priority order. Collections are sorted before
iteration to ensure deterministic processing order. The `select!` macro is
replaced with deterministic priority-based dispatch where order matters.

## Configuration

### Enabling Deterministic Mode

```rust
use ghost_daemon::config::OrchestratorConfig;

let config = OrchestratorConfig {
    deterministic_mode: true,
    rng_seed: Some(42),  // Fixed seed for reproducibility
    ..OrchestratorConfig::default()
};
```

### Simulation Backend Seed

```rust
use ghost_sim::config::SimConfig;

let sim_config = SimConfig::with_capacity(16 * 1024 * 1024)
    .with_seed(0xDEAD_BEEF_CAFE_BABE);
```

## Verification

Run the determinism equivalence tests:

```bash
cargo test --package ghost-daemon --test determinism_equivalence
```

These tests verify:
- State machine transitions are deterministic
- Snapshot ordering is consistent
- Backend failure patterns are reproducible
- Trace event sequences are identical across runs
- Collection iteration order is deterministic

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  OrchestratorConfig                  │
│  ┌──────────────┐  ┌──────────────┐                 │
│  │ rng_seed     │  │ deterministic│                 │
│  │ Option<u64>  │  │ _mode: bool  │                 │
│  └──────┬───────┘  └──────┬───────┘                 │
│         │                 │                          │
│         ▼                 ▼                          │
│  ┌──────────────────────────────────┐               │
│  │    ChaCha8Rng (seeded)           │               │
│  └──────────────────────────────────┘               │
│         │                                            │
│         ▼                                            │
│  ┌──────────────┐  ┌──────────────┐                 │
│  │ SimBackend   │  │ StateMachine │                 │
│  │ (seeded RNG) │  │ (BTreeMap)   │                 │
│  └──────────────┘  └──────────────┘                 │
│         │                                            │
│         ▼                                            │
│  ┌──────────────────────────────────┐               │
│  │    Deterministic Output          │               │
│  └──────────────────────────────────┘               │
└─────────────────────────────────────────────────────┘
```

## Non-Deterministic Components

The following components intentionally use non-deterministic behavior:

- **IPC server**: Network timing is inherently non-deterministic
- **Signal handling**: OS signal delivery timing varies
- **RealTimeProvider**: Wall-clock time varies between runs
- **CLI output**: User-facing output is not part of the deterministic core

## Future Work

- [ ] Wire `DeterministicClock` into `TransferOrchestrator` for replay
- [ ] Add `DeterministicTimeProvider` to `ghost-sim` for simulated time
- [ ] Implement seeded RNG propagation to `BackpressureController`
- [ ] Add cross-run trace comparison tool
- [ ] Property-based testing for determinism invariants
