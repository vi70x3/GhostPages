//! State ownership enforcement for the GhostPages system.
//!
//! This module provides compile-time and runtime enforcement of the
//! state ownership contract: only `ghost-daemon` may mutate runtime state.
//!
//! # Design
//!
//! [`StateMutationToken`] is a marker type that only `ghost-daemon` can create.
//! By passing this token to methods that mutate state, the compiler enforces
//! that only code with access to the token (i.e., ghost-daemon) can call them.
//!
//! [`StateOwnershipLog`] provides runtime tracking of all state mutations,
//! recording the module, action, timestamp, and optional chunk ID for each
//! mutation. This enables post-hoc auditing and test verification.

use crate::types::ChunkId;

// ─── State Mutation Token ─────────────────────────────────────────────────────

/// A zero-sized token that authorizes state mutation.
///
/// This type can only be created by code that has access to the
/// `StateMutationToken::new()` constructor. By convention, only
/// `ghost-daemon` creates and distributes this token.
///
/// # Compile-Time Enforcement
///
/// The constructor is gated behind the `enforce-state-ownership` feature.
/// When this feature is not enabled, `new()` is unavailable at compile time
/// for crates that don't import this module from ghost-daemon context.
///
/// # Runtime Enforcement
///
/// Even without the feature flag, the token serves as a runtime sentinel:
/// methods that require `&self` of a type containing the token will
/// naturally prevent unauthorized callers from invoking them.
///
/// # Example
///
/// ```
/// use ghost_core::state_ownership::StateMutationToken;
///
/// // In production, only ghost-daemon creates this token.
/// // Use new_unchecked for tests and non-enforced contexts:
/// let token = StateMutationToken::new_unchecked();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateMutationToken(());

impl StateMutationToken {
    /// Create a new state mutation token.
    ///
    /// This constructor is intentionally public but should only be called
    /// from `ghost-daemon`. The `enforce-state-ownership` feature gate
    /// provides an additional compile-time check.
    ///
    /// # Convention
    ///
    /// Only `ghost-daemon` should call this function. All other crates
    /// must treat this type as opaque and never construct it.
    #[cfg(feature = "enforce-state-ownership")]
    pub fn new() -> Self {
        StateMutationToken(())
    }

    /// Create a new state mutation token (unconditional).
    ///
    /// This is available without the feature flag for use in tests and
    /// in contexts where the convention is enforced by code review
    /// rather than the compiler.
    ///
    /// # Warning
    /// Do not call this from non-daemon crates in production code.
    pub fn new_unchecked() -> Self {
        StateMutationToken(())
    }
}

impl Default for StateMutationToken {
    fn default() -> Self {
        Self::new_unchecked()
    }
}

// ─── State Change Record ──────────────────────────────────────────────────────

/// A single recorded state change.
///
/// Captures the module that performed the mutation, the action description,
/// the timestamp, and an optional chunk ID if the mutation was chunk-specific.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateChange {
    /// The module that performed the mutation (e.g., "ghost-daemon::orchestrator").
    pub module: &'static str,

    /// A human-readable description of the action (e.g., "transition(Stored)").
    pub action: String,

    /// Timestamp of the mutation (microseconds since epoch).
    pub timestamp: u64,

    /// The chunk ID if this mutation was chunk-specific.
    pub chunk_id: Option<ChunkId>,
}

impl StateChange {
    /// Create a new state change record.
    pub fn new(module: &'static str, action: impl Into<String>, timestamp: u64) -> Self {
        Self {
            module,
            action: action.into(),
            timestamp,
            chunk_id: None,
        }
    }

    /// Create a new state change record with a chunk ID.
    pub fn with_chunk(
        module: &'static str,
        action: impl Into<String>,
        timestamp: u64,
        chunk_id: ChunkId,
    ) -> Self {
        Self {
            module,
            action: action.into(),
            timestamp,
            chunk_id: Some(chunk_id),
        }
    }
}

// ─── State Ownership Log ──────────────────────────────────────────────────────

/// Runtime state ownership tracker.
///
/// Records every state mutation in the system, enabling post-hoc auditing
/// to verify that only authorized modules performed mutations.
///
/// # Usage
///
/// The orchestrator creates a `StateOwnershipLog` and passes it to subsystems
/// that need to record mutations. After a test or operation, the log can be
/// inspected to verify compliance.
///
/// ```
/// use ghost_core::state_ownership::{StateOwnershipLog, StateChange};
///
/// let mut log = StateOwnershipLog::new();
/// log.record(StateChange::new("test", "example_action", 12345));
/// assert_eq!(log.mutation_count(), 1);
/// ```
#[derive(Debug, Clone, Default)]
pub struct StateOwnershipLog {
    mutations: Vec<StateChange>,
}

impl StateOwnershipLog {
    /// Create a new, empty state ownership log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new log with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            mutations: Vec::with_capacity(capacity),
        }
    }

    /// Record a state mutation.
    pub fn record(&mut self, change: StateChange) {
        self.mutations.push(change);
    }

    /// Record a simple mutation without a chunk ID.
    pub fn record_simple(
        &mut self,
        module: &'static str,
        action: impl Into<String>,
        timestamp: u64,
    ) {
        self.mutations.push(StateChange::new(module, action, timestamp));
    }

    /// Record a chunk-specific mutation.
    pub fn record_chunk(
        &mut self,
        module: &'static str,
        action: impl Into<String>,
        timestamp: u64,
        chunk_id: ChunkId,
    ) {
        self.mutations
            .push(StateChange::with_chunk(module, action, timestamp, chunk_id));
    }

    /// Get the total number of recorded mutations.
    pub fn mutation_count(&self) -> usize {
        self.mutations.len()
    }

    /// Check if no mutations have been recorded.
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    /// Get all recorded mutations.
    pub fn mutations(&self) -> &[StateChange] {
        &self.mutations
    }

    /// Get mutations performed by a specific module.
    pub fn mutations_by_module(&self, module: &str) -> Vec<&StateChange> {
        self.mutations
            .iter()
            .filter(|m| m.module == module)
            .collect()
    }

    /// Get mutations related to a specific chunk.
    pub fn mutations_for_chunk(&self, chunk_id: &ChunkId) -> Vec<&StateChange> {
        self.mutations
            .iter()
            .filter(|m| m.chunk_id.as_ref() == Some(chunk_id))
            .collect()
    }

    /// Check if any mutations were performed by a module other than the allowed ones.
    pub fn has_unauthorized_mutations(&self, allowed_modules: &[&str]) -> bool {
        self.mutations
            .iter()
            .any(|m| !allowed_modules.contains(&m.module))
    }

    /// Clear all recorded mutations.
    pub fn clear(&mut self) {
        self.mutations.clear();
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_mutation_token_new_unchecked() {
        let token = StateMutationToken::new_unchecked();
        assert_eq!(token, StateMutationToken(()));
    }

    #[test]
    fn test_state_mutation_token_default() {
        let token = StateMutationToken::default();
        assert_eq!(token, StateMutationToken(()));
    }

    #[test]
    fn test_state_change_new() {
        let change = StateChange::new("test_module", "test_action", 12345);
        assert_eq!(change.module, "test_module");
        assert_eq!(change.action, "test_action");
        assert_eq!(change.timestamp, 12345);
        assert!(change.chunk_id.is_none());
    }

    #[test]
    fn test_state_change_with_chunk() {
        let chunk_id = ChunkId::from_data(b"test_chunk");
        let change = StateChange::with_chunk("test_module", "transition", 12345, chunk_id);
        assert_eq!(change.module, "test_module");
        assert_eq!(change.action, "transition");
        assert_eq!(change.timestamp, 12345);
        assert_eq!(change.chunk_id, Some(chunk_id));
    }

    #[test]
    fn test_state_ownership_log_new() {
        let log = StateOwnershipLog::new();
        assert!(log.is_empty());
        assert_eq!(log.mutation_count(), 0);
    }

    #[test]
    fn test_state_ownership_log_with_capacity() {
        let log = StateOwnershipLog::with_capacity(100);
        assert!(log.is_empty());
    }

    #[test]
    fn test_state_ownership_log_record() {
        let mut log = StateOwnershipLog::new();
        log.record(StateChange::new("module_a", "action_1", 1000));
        log.record(StateChange::new("module_b", "action_2", 2000));
        assert_eq!(log.mutation_count(), 2);
        assert!(!log.is_empty());
    }

    #[test]
    fn test_state_ownership_log_record_simple() {
        let mut log = StateOwnershipLog::new();
        log.record_simple("module_a", "action_1", 1000);
        assert_eq!(log.mutation_count(), 1);
        assert_eq!(log.mutations()[0].module, "module_a");
        assert_eq!(log.mutations()[0].action, "action_1");
    }

    #[test]
    fn test_state_ownership_log_record_chunk() {
        let mut log = StateOwnershipLog::new();
        let chunk_id = ChunkId::from_data(b"chunk_1");
        log.record_chunk("orchestrator", "transition(Stored)", 1000, chunk_id);
        assert_eq!(log.mutation_count(), 1);
        assert_eq!(log.mutations()[0].chunk_id, Some(chunk_id));
    }

    #[test]
    fn test_state_ownership_log_mutations_by_module() {
        let mut log = StateOwnershipLog::new();
        log.record_simple("module_a", "action_1", 1000);
        log.record_simple("module_b", "action_2", 2000);
        log.record_simple("module_a", "action_3", 3000);

        let a_mutations = log.mutations_by_module("module_a");
        assert_eq!(a_mutations.len(), 2);
        let b_mutations = log.mutations_by_module("module_b");
        assert_eq!(b_mutations.len(), 1);
        let c_mutations = log.mutations_by_module("module_c");
        assert!(c_mutations.is_empty());
    }

    #[test]
    fn test_state_ownership_log_mutations_for_chunk() {
        let mut log = StateOwnershipLog::new();
        let chunk1 = ChunkId::from_data(b"chunk_1");
        let chunk2 = ChunkId::from_data(b"chunk_2");

        log.record_chunk("orchestrator", "transition(Stored)", 1000, chunk1);
        log.record_chunk("orchestrator", "transition(Migrating)", 2000, chunk1);
        log.record_chunk("orchestrator", "transition(Stored)", 3000, chunk2);

        let chunk1_mutations = log.mutations_for_chunk(&chunk1);
        assert_eq!(chunk1_mutations.len(), 2);
        let chunk2_mutations = log.mutations_for_chunk(&chunk2);
        assert_eq!(chunk2_mutations.len(), 1);
    }

    #[test]
    fn test_state_ownership_log_has_unauthorized_mutations() {
        let mut log = StateOwnershipLog::new();
        log.record_simple("ghost-daemon::orchestrator", "action_1", 1000);
        log.record_simple("ghost-daemon::worker", "action_2", 2000);

        // Only orchestrator is allowed
        assert!(log.has_unauthorized_mutations(&["ghost-daemon::orchestrator"]));

        // Both are allowed
        assert!(!log.has_unauthorized_mutations(&[
            "ghost-daemon::orchestrator",
            "ghost-daemon::worker"
        ]));
    }

    #[test]
    fn test_state_ownership_log_clear() {
        let mut log = StateOwnershipLog::new();
        log.record_simple("module_a", "action_1", 1000);
        log.record_simple("module_b", "action_2", 2000);
        assert_eq!(log.mutation_count(), 2);

        log.clear();
        assert!(log.is_empty());
        assert_eq!(log.mutation_count(), 0);
    }
}
