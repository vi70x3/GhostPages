# ghost-daemon Subsystems

This document defines the four subsystems that make up the ghost-daemon crate.
Each subsystem has a clear responsibility and invariants that must be maintained.

## Runtime State Owner

**Modules:** orchestrator, queue, health, pressure, backpressure, hotness_tracker

**Responsibility:** Owns and mutates all runtime state. Only these modules may hold mutable state.

**Invariant:** No other subsystem holds `&mut` access to runtime state.

### Key Types
- `TransferOrchestrator` — top-level coordinator, owns all mutable state
- `TransferQueue` — bounded job queue with priority insertion
- `HealthTracker` — per-backend health state machine
- `PressureMonitor` — live pressure sampling and smoothing
- `BackpressureController` — overload protection via pressure evaluation
- `HotnessTracker` — chunk access pattern analysis

---

## Event Router

**Modules:** trace_log, metrics, io_metrics, diagnostics

**Responsibility:** Routes events to handlers (trace log, metrics, diagnostics). Pure observation — no state mutation.

**Invariant:** Event router modules never call `&mut self` on runtime state.

### Key Types
- `TraceLog` — append-only event log for observability
- `TransferMetrics` — atomic counters for transfer pipeline performance
- `IoMetrics` — atomic, lock-free I/O metrics with rolling latency
- `DiagnosticSnapshot` — comprehensive health snapshot (read-only view)

---

## Migration Engine

**Modules:** migration, scheduler, retry

**Responsibility:** Makes migration decisions and schedules transfers. Uses PlacementPolicy (pure) and PhysicalCost (deterministic).

**Invariant:** Migration engine proposes decisions; Runtime State Owner approves and executes them.

### Key Types
- `MigrationEngine` — evaluates and proposes chunk migrations
- `TransferScheduler` — dequeues jobs and dispatches to workers
- `RetryConfig` — bounded exponential backoff configuration

---

## Worker Runtime

**Modules:** worker, transfer_worker, pipeline

**Responsibility:** Executes transfer tasks. Reports completion back to Runtime State Owner.

**Invariant:** Workers never mutate state directly; they report results via channels.

### Key Types
- `WorkerPool` — pool of worker tasks that process transfer jobs
- `TransferWorkerPool` — dedicated transfer workers with async completion
- `Pipeline` — async transfer pipeline (skeleton)

---

## Cross-Subsystem Dependencies

```
┌─────────────────────────────────────────────────────────────────┐
│                    Runtime State Owner                          │
│  (orchestrator, queue, health, pressure, backpressure,         │
│   hotness_tracker)                                              │
│                                                                 │
│  Owns: TransferQueue, StateMachine, HealthTracker,             │
│        PressureState, BackpressureController, HotnessTracker   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Event Router │  │  Migration   │  │   Worker     │
│              │  │   Engine     │  │   Runtime    │
│ trace_log    │  │              │  │              │
│ metrics      │  │ migration    │  │ worker       │
│ io_metrics   │  │ scheduler    │  │ transfer_wrk │
│ diagnostics  │  │ retry        │  │ pipeline     │
└──────────────┘  └──────────────┘  └──────────────┘
     observes        proposes         executes &
     only            decisions        reports back
```

### Dependency Rules
1. **Event Router → Runtime State Owner:** Read-only access (observes events, records metrics)
2. **Migration Engine → Runtime State Owner:** Proposes migrations via `evaluate()`, orchestrator executes via `submit_job()`
3. **Worker Runtime → Runtime State Owner:** Reports completions via channels, orchestrator updates state
4. **IPC Server → All subsystems:** Thin adapter that delegates to orchestrator methods

### Violations to Watch For
- Event Router modules calling `&mut self` on Runtime State Owner types
- Migration Engine directly mutating queue or state machine without going through orchestrator
- Workers directly modifying queue depth or health state
- IPC server owning any mutable state
