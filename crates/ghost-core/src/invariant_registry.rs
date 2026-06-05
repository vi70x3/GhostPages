use std::collections::BTreeMap;
use crate::error::GhostError;
use crate::events::BackendHealth;
use crate::state::PressureState;
use crate::types::{ChunkId, ChunkMeta};
use crate::io_abstraction::{IoRequest, IoCompletion};

/// Opaque handle for the transfer queue (lives in ghost-daemon).
/// Invariants in ghost-core only reference it; they never inspect its contents.
pub struct TransferQueue;

/// Runtime state required by invariants.
pub struct GhostState<'a> {
    /// Mapping of chunk identifiers to metadata.
    pub chunks: &'a BTreeMap<ChunkId, ChunkMeta>,
    /// Transfer queue reference.
    pub transfer_queue: &'a TransferQueue,
    /// Backend health tracker.
    pub health: &'a BackendHealth,
    /// System pressure state.
    pub pressure: &'a PressureState,
    /// Pending (in-flight) I/O requests, ordered by ID.
    pub io_pending: &'a BTreeMap<u64, IoRequest>,
    /// Completed I/O requests (for replay and auditing).
    pub io_completed: &'a [IoRequest],
    /// Number of currently in-flight I/O requests.
    pub io_in_flight: usize,
}

#[cfg(feature = "runtime-invariants")]
pub struct InvariantRegistry {
    invariants: Vec<Box<dyn Fn(&GhostState) -> Result<(), GhostError> + Send + Sync>>,
}

#[cfg(feature = "runtime-invariants")]
impl InvariantRegistry {
    pub fn new() -> Self {
        Self { invariants: Vec::new() }
    }
    pub fn register<F>(&mut self, f: F)
    where
        F: Fn(&GhostState) -> Result<(), GhostError> + Send + Sync + 'static,
    {
        self.invariants.push(Box::new(f));
    }
    pub fn check_all(&self, state: &GhostState) -> Result<(), GhostError> {
        for inv in &self.invariants {
            inv(state)?;
        }
        Ok(())
    }
}

#[cfg(not(feature = "runtime-invariants"))]
pub struct InvariantRegistry;

#[cfg(not(feature = "runtime-invariants"))]
impl InvariantRegistry {
    pub fn new() -> Self { Self }
    pub fn register<F>(&mut self, _f: F) {}
    pub fn check_all(&self, _state: &GhostState) -> Result<(), GhostError> { Ok(()) }
}

// Six invariant stubs — ghost-daemon registers these at startup.
pub fn no_orphaned_transfers(state: &GhostState) -> Result<(), GhostError> {
    // Simple check: ensure each job's chunk_id exists in chunks map.
    // TransferQueue does not expose jobs publicly; skip detailed check.
    Ok(())
}
pub fn no_illegal_transitions(state: &GhostState) -> Result<(), GhostError> {
    // Placeholder – always ok.
    Ok(())
}
pub fn no_dangling_allocations(state: &GhostState) -> Result<(), GhostError> {
    Ok(())
}
pub fn no_timestamp_regression(state: &GhostState) -> Result<(), GhostError> {
    Ok(())
}
pub fn no_missing_completions(state: &GhostState) -> Result<(), GhostError> {
    Ok(())
}
pub fn state_machine_consistency(state: &GhostState) -> Result<(), GhostError> {
    Ok(())
}

// ─── I/O Invariants ─────────────────────────────────────────────────────────

/// Invariant 1: No double-complete.
///
/// A request that has already been completed (moved to the completed list)
/// must never appear in the pending map. If a completed request's ID is found
/// in the pending map, that means `complete()` was called twice on the same ID.
///
/// This is enforced by `IoScheduler::complete()` which panics on unknown IDs,
/// but this invariant provides a post-hoc check for replay/simulation modes
/// where the scheduler state is reconstructed from events.
pub fn io_no_double_complete(state: &GhostState) -> Result<(), GhostError> {
    // Check that no completed request ID appears in the pending map.
    for completed_req in state.io_completed {
        if state.io_pending.contains_key(&completed_req.id) {
            return Err(GhostError::Internal(format!(
                "I/O invariant violation: request {} is both completed and pending",
                completed_req.id
            )));
        }
    }
    Ok(())
}

/// Invariant 2: Flush completeness.
///
/// After a flush, all previously pending requests must appear in the completed
/// list. We verify this by checking that `io_in_flight` is consistent with
/// `io_pending.len()` — if `io_in_flight == 0` but `io_pending` is non-empty,
/// a flush was missed or incomplete.
pub fn io_flush_completeness(state: &GhostState) -> Result<(), GhostError> {
    // If no requests are in-flight, the pending map should also be empty.
    // A non-empty pending map with io_in_flight == 0 indicates a flush was
    // expected but not performed (or a bookkeeping error).
    if state.io_in_flight == 0 && !state.io_pending.is_empty() {
        return Err(GhostError::Internal(format!(
            "I/O invariant violation: {} pending requests but io_in_flight is 0 — \
             indicates missed flush or bookkeeping error",
            state.io_pending.len()
        )));
    }
    Ok(())
}

/// Invariant 3: Completion bounded.
///
/// The number of completed requests must never exceed the total number of
/// requests ever issued. Since we don't track "total issued" directly, we
/// enforce a simpler bound: completed count + pending count should be
/// consistent with the in-flight count.
///
/// More precisely: `io_in_flight` must equal `io_pending.len()` at all times.
pub fn io_completion_bounded(state: &GhostState) -> Result<(), GhostError> {
    if state.io_in_flight != state.io_pending.len() {
        return Err(GhostError::Internal(format!(
            "I/O invariant violation: io_in_flight ({}) != io_pending.len() ({})",
            state.io_in_flight,
            state.io_pending.len()
        )));
    }
    Ok(())
}

/// Invariant 4: Buffer within capacity.
///
/// The number of pending I/O requests must not exceed a reasonable capacity
/// bound. This prevents unbounded memory growth in the pending map.
/// We use a generous bound of 4096 concurrent I/O requests.
pub fn io_buffer_within_capacity(state: &GhostState) -> Result<(), GhostError> {
    const MAX_CONCURRENT_IO: usize = 4096;
    if state.io_pending.len() > MAX_CONCURRENT_IO {
        return Err(GhostError::Internal(format!(
            "I/O invariant violation: {} pending requests exceeds capacity bound of {}",
            state.io_pending.len(),
            MAX_CONCURRENT_IO
        )));
    }
    Ok(())
}

/// Invariant 5: Request ID monotonic.
///
/// All request IDs in the pending map must be strictly greater than all
/// request IDs in the completed list. This ensures the monotonic ID
/// allocation invariant is maintained even across flush boundaries.
pub fn io_request_id_monotonic(state: &GhostState) -> Result<(), GhostError> {
    if state.io_completed.is_empty() || state.io_pending.is_empty() {
        return Ok(());
    }

    // Find the maximum completed ID
    let max_completed_id = state
        .io_completed
        .iter()
        .map(|r| r.id)
        .max()
        .expect("non-empty checked above");

    // Find the minimum pending ID
    // BTreeMap::keys() returns in sorted order, so first is min
    let min_pending_id = *state
        .io_pending
        .keys()
        .next()
        .expect("non-empty checked above");

    if min_pending_id <= max_completed_id {
        return Err(GhostError::Internal(format!(
            "I/O invariant violation: min pending ID ({}) <= max completed ID ({}) — \
             violates monotonic ID allocation",
            min_pending_id, max_completed_id
        )));
    }
    Ok(())
}

/// Invariant 6: Failure eventual.
///
/// If there are failed I/O requests in the completed list, the system must
/// acknowledge them — i.e., the failed count should be bounded. This is a
/// liveness check: too many consecutive failures indicate a stuck backend.
///
/// We allow up to 256 consecutive failures before flagging the invariant.
pub fn io_failure_eventual(state: &GhostState) -> Result<(), GhostError> {
    const MAX_CONSECUTIVE_FAILURES: usize = 256;

    let failure_count = state
        .io_completed
        .iter()
        .filter(|r| matches!(r.completion, IoCompletion::Failed { .. }))
        .count();

    if failure_count > MAX_CONSECUTIVE_FAILURES {
        return Err(GhostError::Internal(format!(
            "I/O invariant violation: {} completed failures exceeds bound of {} — \
             backend may be stuck",
            failure_count, MAX_CONSECUTIVE_FAILURES
        )));
    }
    Ok(())
}

/// Register all I/O invariants with the given registry.
///
/// This is a convenience function called by ghost-daemon at startup.
#[cfg(feature = "runtime-invariants")]
pub fn register_io_invariants(registry: &mut InvariantRegistry) {
    registry.register(io_no_double_complete);
    registry.register(io_flush_completeness);
    registry.register(io_completion_bounded);
    registry.register(io_buffer_within_capacity);
    registry.register(io_request_id_monotonic);
    registry.register(io_failure_eventual);
}
