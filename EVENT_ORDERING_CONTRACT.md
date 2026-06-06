# Canonical Event Ordering Contract

## Principle
Events are the runtime truth. Event ordering MUST be deterministic
under identical inputs. If event ordering is nondeterministic, replay
correctness is violated.

## Ordering Rules

### Rule 1: Causal Ordering
If event A causally precedes event B, A MUST appear before B in the trace.
Examples:
- MigrationStarted BEFORE MigrationCompleted
- IoRequestIssued BEFORE IoRequestCompleted
- ChunkCreated BEFORE ChunkStored
- PressureChanged BEFORE BackpressureActivated

### Rule 2: Per-Chunk Ordering
All events for the same chunk_id MUST appear in causal order.
No interleaving that violates causality.

### Rule 3: Per-Tier Ordering
All events for the same tier MUST appear in the order they were emitted.
No reordering within a tier's event stream.

### Rule 4: Cross-Subsystem Ordering
Events from different subsystems (e.g., Migration and Pressure) MAY be
interleaved, but the interleaving MUST be deterministic for identical inputs.

### Rule 5: No Silent Reordering
The EventMultiplexer MUST NOT reorder events. Events are delivered to
handlers in the exact order they were emitted.

## Enforcement
- All event emission goes through EventEmitter (mpsc channel preserves order)
- EventMultiplexer delivers to handlers in channel order
- No async fan-out that could reorder events
- All handlers are called sequentially, not concurrently

## Verification
- `test_event_ordering_determinism` — Run identical workloads, verify identical event sequences
- `test_causal_ordering` — Verify causal ordering rules for all event pairs
- `test_no_reordering` — Verify EventMultiplexer doesn't reorder
