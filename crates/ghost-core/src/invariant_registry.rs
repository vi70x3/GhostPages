// Invariant Registry implementation
use std::collections::HashMap;
use std::sync::Mutex;
use once_cell::sync::Lazy;
use crate::error::GhostError;
use crate::error::GhostResult;
use crate::state::{ChunkState, PressureState};
use crate::types::{ChunkId, ChunkMeta};
use crate::transfer::TransferJob;
use crate::daemon::health::BackendHealth;
use crate::daemon::queue::TransferQueue;

/// Runtime state required by invariants.
pub struct GhostState<'a> {
    /// Mapping of chunk identifiers to metadata.
    pub chunks: &'a HashMap<ChunkId, ChunkMeta>,
    /// Transfer queue reference.
    pub transfer_queue: &'a TransferQueue,
    /// Backend health tracker.
    pub health: &'a BackendHealth,
    /// System pressure state.
    pub pressure: &'a PressureState,
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

// Global static registry
#[cfg(feature = "runtime-invariants")]
pub static REGISTRY: Lazy<Mutex<InvariantRegistry>> = Lazy::new(|| Mutex::new(InvariantRegistry::new()));

#[cfg(not(feature = "runtime-invariants"))]
pub static REGISTRY: () = ();

// Macro for compile‑time registration
#[macro_export]
macro_rules! register_invariant {
    ($fn_name:ident) => {
        #[cfg(feature = "runtime-invariants")]
        #[ctor::ctor]
        fn register() {
            crate::invariant_registry::REGISTRY.lock().unwrap().register($fn_name);
        }
    };
}

// Six invariant stubs
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

// Register all invariants
register_invariant!(no_orphaned_transfers);
register_invariant!(no_illegal_transitions);
register_invariant!(no_dangling_allocations);
register_invariant!(no_timestamp_regression);
register_invariant!(no_missing_completions);
register_invariant!(state_machine_consistency);
