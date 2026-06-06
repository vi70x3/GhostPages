# State Ownership Contract

## Rule

Only `ghost-daemon` may mutate runtime state.

## What is "runtime state"?

- Chunk locations and metadata
- Transfer queue contents
- Health status
- Pressure state
- Backpressure state
- Hotness scores
- Worker pool state

## Who can mutate?

| Crate | Can Mutate? | Notes |
|-------|-------------|-------|
| ghost-daemon | ✅ Yes | Sole mutator |
| ghost-core | ❌ No | Types and invariants only |
| ghost-tier | ❌ No | Storage I/O only |
| ghost-sim | ❌ No | Simulation I/O only |
| ghost-policy | ❌ No | Pure functions only |
| ghost-replay | ❌ No | Read-only validation |
| ghost-metrics | ❌ No | Observation only |
| ghost-ipc | ❌ No | Protocol types only |
| ghost-cli | ❌ No | User interface only |

## Enforcement

- All state mutations go through `TransferOrchestrator`
- Workers report via channels; orchestrator applies changes
- Backends perform I/O only; they don't know about runtime state
- Policies return decisions; orchestrator applies them
- `StateMachine::transition()` is only called from `ghost-daemon/src/orchestrator.rs`
- `StateMutationToken` (from `ghost-core::state_ownership`) provides compile-time gating
- `StateOwnershipLog` provides runtime audit trail of all mutations

## Known Violations

| Violation | Location | Status |
|-----------|----------|--------|
| `WorkerPool` directly calls `state_machine.transition()` | `ghost-daemon/src/worker.rs` | ✅ **Fixed** — Workers now report via `WorkerCompletion` channel; orchestrator applies transitions |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ghost-daemon                          │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │           TransferOrchestrator                   │    │
│  │   (sole owner of StateMachine mutations)         │    │
│  │                                                  │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │    │
│  │  │  store()  │  │ migrate()│  │   evict()    │   │    │
│  │  └────┬─────┘  └────┬─────┘  └──────┬───────┘   │    │
│  │       │              │               │           │    │
│  │       ▼              ▼               ▼           │    │
│  │  ┌──────────────────────────────────────────┐   │    │
│  │  │           StateMachine                    │   │    │
│  │  │   (transition called only here)           │   │    │
│  │  └──────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────┘    │
│                          ▲                              │
│                          │ completion channel            │
│  ┌───────────────────────┴─────────────────────────┐    │
│  │              WorkerPool                           │    │
│  │   (executes transfers, reports via channel)       │    │
│  │   (NEVER calls state_machine.transition)          │    │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Other crates (ghost-core, ghost-tier, ghost-policy,    │
│  ghost-replay, ghost-metrics, ghost-ipc, ghost-cli)      │
│                                                          │
│  ❌ No state mutation — read-only or pure functions only │
└─────────────────────────────────────────────────────────┘
```

## Verification

Run `cargo test -p ghost-daemon --test state_ownership` to verify the state ownership contract.
