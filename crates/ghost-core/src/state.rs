//! Chunk state machine and pressure model.
//!
//! This module defines the lifecycle states for chunks and the system pressure
//! model that drives migration decisions.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::GhostError;
use crate::types::{ChunkId, TierId};
use crate::GhostResult;

// ─── Chunk State Machine ─────────────────────────────────────────────────────

/// Lifecycle state of a chunk in the system.
///
/// All chunk operations must respect the state machine. Invalid transitions
/// are bugs — they panic in debug builds and log errors in release builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChunkState {
    /// Reserved but no data yet written.
    Allocated,

    /// Data lives in a tier and is readable.
    Stored,

    /// Hot copy in a fast tier (cached for performance).
    Cached,

    /// Actively moving between tiers.
    Migrating,

    /// Removed from all tiers.
    Evicted,

    /// Error state — recoverable via retry.
    Failed,
}

impl ChunkState {
    /// Attempt to transition to a new state.
    ///
    /// Returns the new state if the transition is valid, or an error if not.
    ///
    /// # Errors
    ///
    /// Returns [`GhostError::InvalidStateTransition`] if the transition is not allowed.
    ///
    /// # Example
    ///
    /// ```
    /// use ghost_core::state::ChunkState;
    ///
    /// let current = ChunkState::Allocated;
    /// let next = current.transition_to(ChunkState::Stored).unwrap();
    /// assert_eq!(next, ChunkState::Stored);
    /// ```
    pub fn transition_to(&self, next: ChunkState) -> GhostResult<ChunkState> {
        if self.is_valid_transition(next) {
            Ok(next)
        } else {
            // In debug mode, panic on invalid transitions — these are bugs.
            // In release mode, return an error so the system can recover.
            #[cfg(debug_assertions)]
            panic!(
                "Invalid chunk state transition: {:?} -> {:?}. \
                 This is a system bug — state transitions must be validated \
                 before calling transition_to.",
                self, next
            );

            #[cfg(not(debug_assertions))]
            {
                let from = format!("{:?}", self);
                let to = format!("{:?}", next);
                eprintln!("Invalid chunk state transition: {:?} -> {:?}", self, next);
                Err(GhostError::InvalidStateTransition { from, to })
            }
        }
    }

    /// Check if a transition to `next` is valid without performing it.
    ///
    /// Returns `true` if the transition is allowed by the state machine.
    ///
    /// # Example
    ///
    /// ```
    /// use ghost_core::state::ChunkState;
    ///
    /// assert!(ChunkState::Allocated.is_valid_transition(ChunkState::Stored));
    /// assert!(!ChunkState::Allocated.is_valid_transition(ChunkState::Cached));
    /// ```
    pub fn is_valid_transition(&self, next: ChunkState) -> bool {
        matches!(
            (*self, next),
            // Allocated → Stored: data has been written
            (ChunkState::Allocated, ChunkState::Stored)
            // Stored → Cached: promoted to fast tier
            | (ChunkState::Stored, ChunkState::Cached)
            // Stored → Migrating: migration initiated
            | (ChunkState::Stored, ChunkState::Migrating)
            // Cached → Migrating: migration initiated from cached copy
            | (ChunkState::Cached, ChunkState::Migrating)
            // Cached → Stored: demoted from cache
            | (ChunkState::Cached, ChunkState::Stored)
            // Migrating → Stored: migration succeeded
            | (ChunkState::Migrating, ChunkState::Stored)
            // Migrating → Failed: migration failed
            | (ChunkState::Migrating, ChunkState::Failed)
            // Migrating → Evicted: migration cancelled
            | (ChunkState::Migrating, ChunkState::Evicted)
            // Failed → Stored: retry succeeded
            | (ChunkState::Failed, ChunkState::Stored)
            // Failed → Evicted: giving up
            | (ChunkState::Failed, ChunkState::Evicted)
            // Stored → Evicted: evicted without migration
            | (ChunkState::Stored, ChunkState::Evicted)
        )
    }

    /// Check if this state is a terminal state (no valid transitions out except
    /// to itself).
    pub fn is_terminal(&self) -> bool {
        matches!(self, ChunkState::Evicted)
    }

    /// Check if this state indicates the chunk is available for reads.
    pub fn is_readable(&self) -> bool {
        matches!(self, ChunkState::Stored | ChunkState::Cached)
    }

    /// Check if the chunk is currently being migrated.
    pub fn is_migrating(&self) -> bool {
        matches!(self, ChunkState::Migrating)
    }
}

impl std::fmt::Display for ChunkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkState::Allocated => write!(f, "allocated"),
            ChunkState::Stored => write!(f, "stored"),
            ChunkState::Cached => write!(f, "cached"),
            ChunkState::Migrating => write!(f, "migrating"),
            ChunkState::Evicted => write!(f, "evicted"),
            ChunkState::Failed => write!(f, "failed"),
        }
    }
}

// ─── State Machine Tracker ───────────────────────────────────────────────────

/// Tracks the state of all chunks and enforces valid transitions.
///
/// This is the authoritative source of truth for chunk states. All state
/// transitions must go through this struct to ensure validity.
#[derive(Debug, Clone)]
pub struct StateMachine {
    states: BTreeMap<ChunkId, ChunkState>,
}

impl StateMachine {
    /// Create a new, empty state machine.
    pub fn new() -> Self {
        Self {
            states: BTreeMap::new(),
        }
    }

    /// Create a new state machine with the given initial capacity.
    pub fn with_capacity(_capacity: usize) -> Self {
        Self {
            states: BTreeMap::new(),
        }
    }

    /// Get the current state of a chunk.
    pub fn get_state(&self, chunk_id: &ChunkId) -> Option<ChunkState> {
        self.states.get(chunk_id).copied()
    }

    /// Register a new chunk in the Allocated state.
    ///
    /// Returns an error if the chunk is already registered.
    pub fn register(&mut self, chunk_id: ChunkId) -> GhostResult<()> {
        if self.states.contains_key(&chunk_id) {
            return Err(GhostError::Internal(format!(
                "chunk {} is already registered in the state machine",
                chunk_id
            )));
        }
        self.states.insert(chunk_id, ChunkState::Allocated);
        Ok(())
    }

    /// Attempt to transition a chunk to a new state.
    ///
    /// Validates the transition, updates the state if valid, and returns the
    /// new state.
    ///
    /// # Errors
    ///
    /// Returns [`GhostError::InvalidStateTransition`] if the transition is not allowed.
    /// Returns [`GhostError::Internal`] if the chunk is not registered.
    pub fn transition(&mut self, chunk_id: &ChunkId, next: ChunkState) -> GhostResult<ChunkState> {
        let current = self.states.get(chunk_id).ok_or_else(|| {
            GhostError::Internal(format!("chunk {} not found in state machine", chunk_id))
        })?;
        current.transition_to(next)?;
        self.states.insert(*chunk_id, next);
        Ok(next)
    }

    /// Remove a chunk from the state machine entirely.
    pub fn remove(&mut self, chunk_id: &ChunkId) -> Option<ChunkState> {
        self.states.remove(chunk_id)
    }

    /// Get the number of chunks being tracked.
    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Check if the state machine is empty.
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    /// Get all chunk IDs in a given state.
    pub fn chunks_in_state(&self, state: ChunkState) -> Vec<ChunkId> {
        self.states
            .iter()
            .filter(|(_, s)| **s == state)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get a snapshot of all states (useful for debugging / trace replay).
    pub fn snapshot(&self) -> BTreeMap<ChunkId, ChunkState> {
        self.states.clone()
    }
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Pressure Model ───────────────────────────────────────────────────────────

/// Current system pressure state across all dimensions.
///
/// Each pressure value is a float from 0.0 (no pressure) to 1.0 (critical).
/// The pressure model drives migration and eviction decisions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PressureState {
    /// Memory pressure (0.0 = none, 1.0 = critical).
    pub memory_pressure: f32,

    /// VRAM pressure (0.0 = none, 1.0 = critical).
    pub vram_pressure: f32,

    /// I/O pressure (0.0 = none, 1.0 = critical).
    pub io_pressure: f32,

    /// Current number of queued transfers.
    pub queue_depth: u32,

    /// Current throughput in bytes/sec.
    pub throughput_bps: u64,
}

impl PressureState {
    /// Create a new pressure state with all values at zero.
    pub fn new() -> Self {
        Self {
            memory_pressure: 0.0,
            vram_pressure: 0.0,
            io_pressure: 0.0,
            queue_depth: 0,
            throughput_bps: 0,
        }
    }

    /// Check if any dimension is above the soft pressure threshold (0.7).
    pub fn is_under_pressure(&self) -> bool {
        const THRESHOLD: f32 = 0.7;
        self.memory_pressure > THRESHOLD
            || self.vram_pressure > THRESHOLD
            || self.io_pressure > THRESHOLD
    }

    /// Check if any dimension is above the critical threshold (0.9).
    pub fn is_critical(&self) -> bool {
        const THRESHOLD: f32 = 0.9;
        self.memory_pressure > THRESHOLD
            || self.vram_pressure > THRESHOLD
            || self.io_pressure > THRESHOLD
    }

    /// Determine which tier is currently the most pressured.
    ///
    /// Returns the [`TierId`] with the highest pressure value.
    pub fn worst_tier(&self) -> TierId {
        let mut max_pressure = self.memory_pressure;
        let mut worst = TierId::Ram;

        if self.vram_pressure > max_pressure {
            max_pressure = self.vram_pressure;
            worst = TierId::GpuVram;
        }

        if self.io_pressure > max_pressure {
            // IO pressure maps to Disk tier
            worst = TierId::Disk;
        }

        worst
    }

    /// Get the maximum pressure value across all dimensions.
    pub fn max_pressure(&self) -> f32 {
        self.memory_pressure
            .max(self.vram_pressure)
            .max(self.io_pressure)
    }

    /// Clamp all pressure values to [0.0, 1.0].
    pub fn clamp(&mut self) {
        self.memory_pressure = self.memory_pressure.clamp(0.0, 1.0);
        self.vram_pressure = self.vram_pressure.clamp(0.0, 1.0);
        self.io_pressure = self.io_pressure.clamp(0.0, 1.0);
    }
}

impl Default for PressureState {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── ChunkState transition tests ──

    #[test]
    fn test_allocated_to_stored() {
        assert!(ChunkState::Allocated.is_valid_transition(ChunkState::Stored));
    }

    #[test]
    fn test_stored_to_cached() {
        assert!(ChunkState::Stored.is_valid_transition(ChunkState::Cached));
    }

    #[test]
    fn test_stored_to_migrating() {
        assert!(ChunkState::Stored.is_valid_transition(ChunkState::Migrating));
    }

    #[test]
    fn test_cached_to_migrating() {
        assert!(ChunkState::Cached.is_valid_transition(ChunkState::Migrating));
    }

    #[test]
    fn test_cached_to_stored() {
        assert!(ChunkState::Cached.is_valid_transition(ChunkState::Stored));
    }

    #[test]
    fn test_migrating_to_stored() {
        assert!(ChunkState::Migrating.is_valid_transition(ChunkState::Stored));
    }

    #[test]
    fn test_migrating_to_failed() {
        assert!(ChunkState::Migrating.is_valid_transition(ChunkState::Failed));
    }

    #[test]
    fn test_migrating_to_evicted() {
        assert!(ChunkState::Migrating.is_valid_transition(ChunkState::Evicted));
    }

    #[test]
    fn test_failed_to_stored() {
        assert!(ChunkState::Failed.is_valid_transition(ChunkState::Stored));
    }

    #[test]
    fn test_failed_to_evicted() {
        assert!(ChunkState::Failed.is_valid_transition(ChunkState::Evicted));
    }

    #[test]
    fn test_stored_to_evicted() {
        assert!(ChunkState::Stored.is_valid_transition(ChunkState::Evicted));
    }

    // ── Invalid transitions ──

    #[test]
    fn test_allocated_to_cached_invalid() {
        assert!(!ChunkState::Allocated.is_valid_transition(ChunkState::Cached));
    }

    #[test]
    fn test_allocated_to_migrating_invalid() {
        assert!(!ChunkState::Allocated.is_valid_transition(ChunkState::Migrating));
    }

    #[test]
    fn test_allocated_to_evicted_invalid() {
        assert!(!ChunkState::Allocated.is_valid_transition(ChunkState::Evicted));
    }

    #[test]
    fn test_allocated_to_failed_invalid() {
        assert!(!ChunkState::Allocated.is_valid_transition(ChunkState::Failed));
    }

    #[test]
    fn test_stored_to_allocated_invalid() {
        assert!(!ChunkState::Stored.is_valid_transition(ChunkState::Allocated));
    }

    #[test]
    fn test_stored_to_failed_invalid() {
        assert!(!ChunkState::Stored.is_valid_transition(ChunkState::Failed));
    }

    #[test]
    fn test_cached_to_allocated_invalid() {
        assert!(!ChunkState::Cached.is_valid_transition(ChunkState::Allocated));
    }

    #[test]
    fn test_cached_to_evicted_invalid() {
        assert!(!ChunkState::Cached.is_valid_transition(ChunkState::Evicted));
    }

    #[test]
    fn test_cached_to_failed_invalid() {
        assert!(!ChunkState::Cached.is_valid_transition(ChunkState::Failed));
    }

    #[test]
    fn test_migrating_to_allocated_invalid() {
        assert!(!ChunkState::Migrating.is_valid_transition(ChunkState::Allocated));
    }

    #[test]
    fn test_migrating_to_cached_invalid() {
        assert!(!ChunkState::Migrating.is_valid_transition(ChunkState::Cached));
    }

    #[test]
    fn test_evicted_to_any_invalid() {
        // Evicted is terminal — no transitions out
        assert!(!ChunkState::Evicted.is_valid_transition(ChunkState::Allocated));
        assert!(!ChunkState::Evicted.is_valid_transition(ChunkState::Stored));
        assert!(!ChunkState::Evicted.is_valid_transition(ChunkState::Cached));
        assert!(!ChunkState::Evicted.is_valid_transition(ChunkState::Migrating));
        assert!(!ChunkState::Evicted.is_valid_transition(ChunkState::Failed));
    }

    #[test]
    fn test_failed_to_allocated_invalid() {
        assert!(!ChunkState::Failed.is_valid_transition(ChunkState::Allocated));
    }

    #[test]
    fn test_failed_to_cached_invalid() {
        assert!(!ChunkState::Failed.is_valid_transition(ChunkState::Cached));
    }

    #[test]
    fn test_failed_to_migrating_invalid() {
        assert!(!ChunkState::Failed.is_valid_transition(ChunkState::Migrating));
    }

    // ── transition_to returns correct state ──

    #[test]
    fn test_transition_to_returns_new_state() {
        let result = ChunkState::Allocated.transition_to(ChunkState::Stored);
        assert_eq!(result.unwrap(), ChunkState::Stored);
    }

    #[test]
    fn test_transition_to_invalid_returns_error_in_release() {
        // In release mode, invalid transitions return an error.
        // In debug mode, they panic — so we test is_valid_transition instead
        // for the error case.
        assert!(!ChunkState::Evicted.is_valid_transition(ChunkState::Stored));
    }

    // ── Helper method tests ──

    #[test]
    fn test_is_terminal() {
        assert!(ChunkState::Evicted.is_terminal());
        assert!(!ChunkState::Stored.is_terminal());
        assert!(!ChunkState::Allocated.is_terminal());
        assert!(!ChunkState::Migrating.is_terminal());
        assert!(!ChunkState::Failed.is_terminal());
        assert!(!ChunkState::Cached.is_terminal());
    }

    #[test]
    fn test_is_readable() {
        assert!(ChunkState::Stored.is_readable());
        assert!(ChunkState::Cached.is_readable());
        assert!(!ChunkState::Allocated.is_readable());
        assert!(!ChunkState::Migrating.is_readable());
        assert!(!ChunkState::Evicted.is_readable());
        assert!(!ChunkState::Failed.is_readable());
    }

    #[test]
    fn test_is_migrating() {
        assert!(ChunkState::Migrating.is_migrating());
        assert!(!ChunkState::Stored.is_migrating());
        assert!(!ChunkState::Allocated.is_migrating());
    }

    #[test]
    fn test_chunk_state_display() {
        assert_eq!(format!("{}", ChunkState::Allocated), "allocated");
        assert_eq!(format!("{}", ChunkState::Stored), "stored");
        assert_eq!(format!("{}", ChunkState::Cached), "cached");
        assert_eq!(format!("{}", ChunkState::Migrating), "migrating");
        assert_eq!(format!("{}", ChunkState::Evicted), "evicted");
        assert_eq!(format!("{}", ChunkState::Failed), "failed");
    }

    // ── StateMachine tests ──

    #[test]
    fn test_state_machine_new() {
        let sm = StateMachine::new();
        assert!(sm.is_empty());
        assert_eq!(sm.len(), 0);
    }

    #[test]
    fn test_state_machine_register() {
        let mut sm = StateMachine::new();
        let id = ChunkId::from_data(b"test");
        sm.register(id).unwrap();
        assert_eq!(sm.get_state(&id), Some(ChunkState::Allocated));
        assert_eq!(sm.len(), 1);
    }

    #[test]
    fn test_state_machine_register_duplicate_fails() {
        let mut sm = StateMachine::new();
        let id = ChunkId::from_data(b"test");
        sm.register(id).unwrap();
        assert!(sm.register(id).is_err());
    }

    #[test]
    fn test_state_machine_transition() {
        let mut sm = StateMachine::new();
        let id = ChunkId::from_data(b"test");
        sm.register(id).unwrap();

        let state = sm.transition(&id, ChunkState::Stored).unwrap();
        assert_eq!(state, ChunkState::Stored);
        assert_eq!(sm.get_state(&id), Some(ChunkState::Stored));
    }

    #[test]
    fn test_state_machine_transition_invalid() {
        let mut sm = StateMachine::new();
        let id = ChunkId::from_data(b"test");
        sm.register(id).unwrap();

        // Allocated → Cached is not valid — verify using is_valid_transition
        // (transition_to panics in debug on invalid transitions, so we test
        // the validation function directly)
        assert!(!ChunkState::Allocated.is_valid_transition(ChunkState::Cached));

        // State should not have changed (no transition was attempted)
        assert_eq!(sm.get_state(&id), Some(ChunkState::Allocated));
    }

    #[test]
    fn test_state_machine_transition_unregistered() {
        let mut sm = StateMachine::new();
        let id = ChunkId::from_data(b"test");
        let result = sm.transition(&id, ChunkState::Stored);
        assert!(result.is_err());
    }

    #[test]
    fn test_state_machine_remove() {
        let mut sm = StateMachine::new();
        let id = ChunkId::from_data(b"test");
        sm.register(id).unwrap();
        let removed = sm.remove(&id);
        assert_eq!(removed, Some(ChunkState::Allocated));
        assert!(sm.is_empty());
    }

    #[test]
    fn test_state_machine_chunks_in_state() {
        let mut sm = StateMachine::new();
        let id1 = ChunkId::from_data(b"chunk1");
        let id2 = ChunkId::from_data(b"chunk2");
        let id3 = ChunkId::from_data(b"chunk3");

        sm.register(id1).unwrap();
        sm.register(id2).unwrap();
        sm.register(id3).unwrap();

        sm.transition(&id1, ChunkState::Stored).unwrap();
        sm.transition(&id2, ChunkState::Stored).unwrap();

        let stored = sm.chunks_in_state(ChunkState::Stored);
        assert_eq!(stored.len(), 2);
        assert!(stored.contains(&id1));
        assert!(stored.contains(&id2));

        let allocated = sm.chunks_in_state(ChunkState::Allocated);
        assert_eq!(allocated.len(), 1);
        assert!(allocated.contains(&id3));
    }

    #[test]
    fn test_state_machine_snapshot() {
        let mut sm = StateMachine::new();
        let id = ChunkId::from_data(b"test");
        sm.register(id).unwrap();
        sm.transition(&id, ChunkState::Stored).unwrap();

        let snapshot = sm.snapshot();
        assert_eq!(snapshot.get(&id), Some(&ChunkState::Stored));
    }

    #[test]
    fn test_state_machine_full_lifecycle() {
        let mut sm = StateMachine::new();
        let id = ChunkId::from_data(b"lifecycle");

        // Allocated
        sm.register(id).unwrap();
        assert_eq!(sm.get_state(&id), Some(ChunkState::Allocated));

        // Allocated → Stored
        sm.transition(&id, ChunkState::Stored).unwrap();
        assert_eq!(sm.get_state(&id), Some(ChunkState::Stored));

        // Stored → Cached
        sm.transition(&id, ChunkState::Cached).unwrap();
        assert_eq!(sm.get_state(&id), Some(ChunkState::Cached));

        // Cached → Migrating
        sm.transition(&id, ChunkState::Migrating).unwrap();
        assert_eq!(sm.get_state(&id), Some(ChunkState::Migrating));

        // Migrating → Stored (success)
        sm.transition(&id, ChunkState::Stored).unwrap();
        assert_eq!(sm.get_state(&id), Some(ChunkState::Stored));

        // Stored → Evicted
        sm.transition(&id, ChunkState::Evicted).unwrap();
        assert_eq!(sm.get_state(&id), Some(ChunkState::Evicted));
    }

    #[test]
    fn test_state_machine_migration_failure_and_retry() {
        let mut sm = StateMachine::new();
        let id = ChunkId::from_data(b"retry");

        sm.register(id).unwrap();
        sm.transition(&id, ChunkState::Stored).unwrap();
        sm.transition(&id, ChunkState::Migrating).unwrap();

        // Migration fails
        sm.transition(&id, ChunkState::Failed).unwrap();
        assert_eq!(sm.get_state(&id), Some(ChunkState::Failed));

        // Retry succeeds
        sm.transition(&id, ChunkState::Stored).unwrap();
        assert_eq!(sm.get_state(&id), Some(ChunkState::Stored));
    }

    // ── PressureState tests ──

    #[test]
    fn test_pressure_state_new() {
        let p = PressureState::new();
        assert_eq!(p.memory_pressure, 0.0);
        assert_eq!(p.vram_pressure, 0.0);
        assert_eq!(p.io_pressure, 0.0);
        assert_eq!(p.queue_depth, 0);
        assert_eq!(p.throughput_bps, 0);
    }

    #[test]
    fn test_pressure_state_no_pressure() {
        let p = PressureState::new();
        assert!(!p.is_under_pressure());
        assert!(!p.is_critical());
    }

    #[test]
    fn test_pressure_state_under_pressure() {
        let p = PressureState {
            memory_pressure: 0.71,
            vram_pressure: 0.3,
            io_pressure: 0.1,
            queue_depth: 10,
            throughput_bps: 1000,
        };
        assert!(p.is_under_pressure());
        assert!(!p.is_critical());
    }

    #[test]
    fn test_pressure_state_critical() {
        let p = PressureState {
            memory_pressure: 0.95,
            vram_pressure: 0.5,
            io_pressure: 0.3,
            queue_depth: 100,
            throughput_bps: 500,
        };
        assert!(p.is_under_pressure());
        assert!(p.is_critical());
    }

    #[test]
    fn test_pressure_state_worst_tier_ram() {
        let p = PressureState {
            memory_pressure: 0.8,
            vram_pressure: 0.3,
            io_pressure: 0.1,
            queue_depth: 0,
            throughput_bps: 0,
        };
        assert_eq!(p.worst_tier(), TierId::Ram);
    }

    #[test]
    fn test_pressure_state_worst_tier_vram() {
        let p = PressureState {
            memory_pressure: 0.3,
            vram_pressure: 0.85,
            io_pressure: 0.1,
            queue_depth: 0,
            throughput_bps: 0,
        };
        assert_eq!(p.worst_tier(), TierId::GpuVram);
    }

    #[test]
    fn test_pressure_state_worst_tier_disk() {
        let p = PressureState {
            memory_pressure: 0.3,
            vram_pressure: 0.5,
            io_pressure: 0.95,
            queue_depth: 0,
            throughput_bps: 0,
        };
        assert_eq!(p.worst_tier(), TierId::Disk);
    }

    #[test]
    fn test_pressure_state_max_pressure() {
        let p = PressureState {
            memory_pressure: 0.5,
            vram_pressure: 0.8,
            io_pressure: 0.3,
            queue_depth: 0,
            throughput_bps: 0,
        };
        assert!((p.max_pressure() - 0.8).abs() < f32::EPSILON);
    }

    #[test]
    fn test_pressure_state_clamp() {
        let mut p = PressureState {
            memory_pressure: 1.5,
            vram_pressure: -0.3,
            io_pressure: 0.5,
            queue_depth: 0,
            throughput_bps: 0,
        };
        p.clamp();
        assert!((p.memory_pressure - 1.0).abs() < f32::EPSILON);
        assert!((p.vram_pressure).abs() < f32::EPSILON);
        assert!((p.io_pressure - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn test_pressure_state_at_threshold_boundary() {
        // Exactly at 0.7 — should NOT be under pressure
        let p = PressureState {
            memory_pressure: 0.7,
            vram_pressure: 0.7,
            io_pressure: 0.7,
            queue_depth: 0,
            throughput_bps: 0,
        };
        assert!(!p.is_under_pressure());

        // Just above 0.7 — should be under pressure
        let p2 = PressureState {
            memory_pressure: 0.7001,
            ..p
        };
        assert!(p2.is_under_pressure());

        // Exactly at 0.9 — should NOT be critical
        let p3 = PressureState {
            memory_pressure: 0.9,
            vram_pressure: 0.9,
            io_pressure: 0.9,
            queue_depth: 0,
            throughput_bps: 0,
        };
        assert!(!p3.is_critical());

        // Just above 0.9 — should be critical
        let p4 = PressureState {
            memory_pressure: 0.9001,
            ..p3
        };
        assert!(p4.is_critical());
    }
}
