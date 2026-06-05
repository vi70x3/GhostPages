//! Replay invariant validation system.
//!
//! Provides a trait-based invariant framework with built-in validators
//! for common replay correctness properties.

use std::collections::BTreeMap;
use std::fmt;

use ghost_core::emitter::EventEmitter;
use ghost_core::state::ChunkState;
use ghost_core::trace::TraceEvent;
use ghost_core::types::ChunkId;

/// Severity of an invariant violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ViolationSeverity {
    /// Informational, not necessarily a bug.
    Info,
    /// Potential issue that should be investigated.
    Warning,
    /// Definite correctness bug.
    Error,
    /// Critical invariant failure.
    Critical,
}

impl fmt::Display for ViolationSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViolationSeverity::Info => write!(f, "INFO"),
            ViolationSeverity::Warning => write!(f, "WARN"),
            ViolationSeverity::Error => write!(f, "ERROR"),
            ViolationSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// A single invariant violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantViolation {
    /// Name of the invariant that was violated.
    pub invariant: String,
    /// Severity of the violation.
    pub severity: ViolationSeverity,
    /// Human-readable description.
    pub message: String,
    /// Index of the event that triggered the violation.
    pub event_index: Option<usize>,
    /// Chunk ID involved, if any.
    pub chunk_id: Option<ChunkId>,
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let chunk = self
            .chunk_id
            .map(|id| id.short_hex())
            .unwrap_or_default();
        let idx = self
            .event_index
            .map(|i| i.to_string())
            .unwrap_or_default();
        write!(
            f,
            "[{}] {} (invariant={}, chunk={}, event={})",
            self.severity, self.message, self.invariant, chunk, idx
        )
    }
}

/// Trait for replay invariants.
///
/// Implementors define a validation function that checks a slice of events
/// for a specific correctness property.
pub trait ReplayInvariant: Send + Sync {
    /// Name of the invariant.
    fn name(&self) -> &'static str;

    /// Validates the event stream and returns any violations.
    fn validate(&self, events: &[TraceEvent]) -> Vec<InvariantViolation>;
}

/// Built-in invariant validator that runs multiple invariants.
#[derive(Default)]
pub struct InvariantValidator {
    invariants: Vec<Box<dyn ReplayInvariant>>,
    /// Optional event emitter for unified event taxonomy.
    event_emitter: Option<EventEmitter>,
}

impl InvariantValidator {
    /// Creates a new validator with no invariants.
    pub fn new() -> Self {
        Self {
            invariants: Vec::new(),
            event_emitter: None,
        }
    }

    /// Set the event emitter for unified event taxonomy.
    pub fn set_event_emitter(&mut self, emitter: EventEmitter) {
        self.event_emitter = Some(emitter);
    }

    /// Creates a validator with all built-in invariants registered.
    pub fn with_defaults() -> Self {
        let mut validator = Self::new();
        validator.register(Box::new(NoOrphanedTransfers));
        validator.register(Box::new(NoIllegalTransitions));
        validator.register(Box::new(NoDanglingAllocations));
        validator.register(Box::new(NoTimestampRegression));
        validator.register(Box::new(NoMissingCompletions));
        validator.register(Box::new(StateMachineConsistency));
        validator
    }

    /// Registers a new invariant.
    pub fn register(&mut self, invariant: Box<dyn ReplayInvariant>) {
        self.invariants.push(invariant);
    }

    /// Validates the event stream against all registered invariants.
    pub fn validate(&self, events: &[TraceEvent]) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();
        for invariant in &self.invariants {
            violations.extend(invariant.validate(events));
        }
        violations
    }

    /// Returns the number of registered invariants.
    pub fn len(&self) -> usize {
        self.invariants.len()
    }

    /// Returns true if no invariants are registered.
    pub fn is_empty(&self) -> bool {
        self.invariants.is_empty()
    }
}

// ─── Built-in Invariants ───────────────────────────────────────────────────────

/// Ensures every transfer has a corresponding completion or failure event.
pub struct NoOrphanedTransfers;

impl ReplayInvariant for NoOrphanedTransfers {
    fn name(&self) -> &'static str {
        "NoOrphanedTransfers"
    }

    fn validate(&self, events: &[TraceEvent]) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();
        let mut started: BTreeMap<ChunkId, usize> = BTreeMap::new();
        let mut completed: BTreeMap<ChunkId, bool> = BTreeMap::new();

        for (i, event) in events.iter().enumerate() {
            match event {
                TraceEvent::TransferStarted { job, .. } => {
                    let key = job.chunk_id;
                    started.insert(key, i);
                    completed.entry(key).or_insert(false);
                }
                TraceEvent::TransferCompleted { chunk_id, .. } => {
                    *completed.entry(*chunk_id).or_insert(true) = true;
                }
                TraceEvent::TransferFailed { chunk_id, .. } => {
                    *completed.entry(*chunk_id).or_insert(true) = true;
                }
                _ => {}
            }
        }

        for (chunk_id, &start_idx) in &started {
            if !completed.get(chunk_id).copied().unwrap_or(false) {
                violations.push(InvariantViolation {
                    invariant: self.name().to_string(),
                    severity: ViolationSeverity::Error,
                    message: format!(
                        "Transfer started for chunk {} at event {} but never completed or failed",
                        chunk_id.short_hex(),
                        start_idx
                    ),
                    event_index: Some(start_idx),
                    chunk_id: Some(*chunk_id),
                });
            }
        }

        violations
    }
}

/// Ensures all state transitions are valid according to the state machine.
pub struct NoIllegalTransitions;

impl ReplayInvariant for NoIllegalTransitions {
    fn name(&self) -> &'static str {
        "NoIllegalTransitions"
    }

    fn validate(&self, events: &[TraceEvent]) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();
        let mut last_state: BTreeMap<ChunkId, ChunkState> = BTreeMap::new();

        for (i, event) in events.iter().enumerate() {
            if let TraceEvent::ChunkStateChanged {
                chunk_id,
                from,
                to,
                ..
            } = event
            {
                // Check if the 'from' matches the last known state
                if let Some(&expected_from) = last_state.get(chunk_id) {
                    if expected_from != *from {
                        violations.push(InvariantViolation {
                            invariant: self.name().to_string(),
                            severity: ViolationSeverity::Error,
                            message: format!(
                                "State transition for chunk {} at event {}: expected from={:?} but got from={:?}",
                                chunk_id.short_hex(),
                                i,
                                expected_from,
                                from
                            ),
                            event_index: Some(i),
                            chunk_id: Some(*chunk_id),
                        });
                    }
                }

                // Check if the transition is valid
                if !from.is_valid_transition(*to) {
                    violations.push(InvariantViolation {
                        invariant: self.name().to_string(),
                        severity: ViolationSeverity::Error,
                        message: format!(
                            "Invalid state transition for chunk {} at event {}: {:?} -> {:?}",
                            chunk_id.short_hex(),
                            i,
                            from,
                            to
                        ),
                        event_index: Some(i),
                        chunk_id: Some(*chunk_id),
                    });
                }

                last_state.insert(*chunk_id, *to);
            }
        }

        violations
    }
}

/// Ensures no chunks are deleted without being created first.
pub struct NoDanglingAllocations;

impl ReplayInvariant for NoDanglingAllocations {
    fn name(&self) -> &'static str {
        "NoDanglingAllocations"
    }

    fn validate(&self, events: &[TraceEvent]) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();
        let mut created: BTreeMap<ChunkId, bool> = BTreeMap::new();
        let mut deleted: BTreeMap<ChunkId, bool> = BTreeMap::new();

        for (_i, event) in events.iter().enumerate() {
            match event {
                TraceEvent::ChunkCreated { chunk_id, .. } => {
                    created.insert(*chunk_id, true);
                }
                TraceEvent::ChunkDeleted { chunk_id, .. } => {
                    deleted.insert(*chunk_id, true);
                }
                _ => {}
            }
        }

        // Check for chunks that were deleted without being created
        for (chunk_id, _) in &deleted {
            if !created.get(chunk_id).copied().unwrap_or(false) {
                violations.push(InvariantViolation {
                    invariant: self.name().to_string(),
                    severity: ViolationSeverity::Warning,
                    message: format!(
                        "Chunk {} was deleted but never appeared as created",
                        chunk_id.short_hex()
                    ),
                    event_index: None,
                    chunk_id: Some(*chunk_id),
                });
            }
        }

        violations
    }
}

/// Ensures timestamps are monotonically non-decreasing.
pub struct NoTimestampRegression;

impl ReplayInvariant for NoTimestampRegression {
    fn name(&self) -> &'static str {
        "NoTimestampRegression"
    }

    fn validate(&self, events: &[TraceEvent]) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();
        let mut last_timestamp: Option<u64> = None;

        for (i, event) in events.iter().enumerate() {
            let ts = event.timestamp();
            if let Some(last) = last_timestamp {
                if ts < last {
                    violations.push(InvariantViolation {
                        invariant: self.name().to_string(),
                        severity: ViolationSeverity::Error,
                        message: format!(
                            "Timestamp regression at event {}: {} < {}",
                            i, ts, last
                        ),
                        event_index: Some(i),
                        chunk_id: event.chunk_id(),
                    });
                }
            }
            last_timestamp = Some(ts);
        }

        violations
    }
}

/// Ensures every TransferStarted has a matching TransferCompleted or TransferFailed.
pub struct NoMissingCompletions;

impl ReplayInvariant for NoMissingCompletions {
    fn name(&self) -> &'static str {
        "NoMissingCompletions"
    }

    fn validate(&self, events: &[TraceEvent]) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();
        let mut pending: BTreeMap<ChunkId, usize> = BTreeMap::new();

        for (i, event) in events.iter().enumerate() {
            match event {
                TraceEvent::TransferStarted { job, .. } => {
                    pending.insert(job.chunk_id, i);
                }
                TraceEvent::TransferCompleted { chunk_id, .. } => {
                    pending.remove(chunk_id);
                }
                TraceEvent::TransferFailed { chunk_id, .. } => {
                    pending.remove(chunk_id);
                }
                TraceEvent::TransferCancelled { chunk_id, .. } => {
                    pending.remove(chunk_id);
                }
                _ => {}
            }
        }

        for (chunk_id, &start_idx) in &pending {
            violations.push(InvariantViolation {
                invariant: self.name().to_string(),
                severity: ViolationSeverity::Error,
                message: format!(
                    "Transfer for chunk {} started at event {} but never completed, failed, or was cancelled",
                    chunk_id.short_hex(),
                    start_idx
                ),
                event_index: Some(start_idx),
                chunk_id: Some(*chunk_id),
            });
        }

        violations
    }
}

/// Ensures the state machine is consistent across all events.
pub struct StateMachineConsistency;

impl ReplayInvariant for StateMachineConsistency {
    fn name(&self) -> &'static str {
        "StateMachineConsistency"
    }

    fn validate(&self, events: &[TraceEvent]) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();
        let mut chunk_states: BTreeMap<ChunkId, ChunkState> = BTreeMap::new();

        for (i, event) in events.iter().enumerate() {
            match event {
                TraceEvent::ChunkCreated { chunk_id, .. } => {
                    if let Some(existing) = chunk_states.get(chunk_id) {
                        violations.push(InvariantViolation {
                            invariant: self.name().to_string(),
                            severity: ViolationSeverity::Error,
                            message: format!(
                                "Chunk {} created at event {} but already in state {:?}",
                                chunk_id.short_hex(),
                                i,
                                existing
                            ),
                            event_index: Some(i),
                            chunk_id: Some(*chunk_id),
                        });
                    }
                    chunk_states.insert(*chunk_id, ChunkState::Allocated);
                }
                TraceEvent::ChunkDeleted { chunk_id, .. } => {
                    if !chunk_states.contains_key(chunk_id) {
                        violations.push(InvariantViolation {
                            invariant: self.name().to_string(),
                            severity: ViolationSeverity::Warning,
                            message: format!(
                                "Chunk {} deleted at event {} but was never tracked",
                                chunk_id.short_hex(),
                                i
                            ),
                            event_index: Some(i),
                            chunk_id: Some(*chunk_id),
                        });
                    }
                    chunk_states.remove(chunk_id);
                }
                TraceEvent::ChunkStateChanged {
                    chunk_id,
                    from,
                    to,
                    ..
                } => {
                    if let Some(&current) = chunk_states.get(chunk_id) {
                        if current != *from {
                            violations.push(InvariantViolation {
                                invariant: self.name().to_string(),
                                severity: ViolationSeverity::Error,
                                message: format!(
                                    "State mismatch for chunk {} at event {}: expected {:?}, got {:?}",
                                    chunk_id.short_hex(),
                                    i,
                                    current,
                                    from
                                ),
                                event_index: Some(i),
                                chunk_id: Some(*chunk_id),
                            });
                        }
                    }
                    chunk_states.insert(*chunk_id, *to);
                }
                _ => {}
            }
        }

        violations
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::TierId;

    fn sample_events() -> Vec<TraceEvent> {
        vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1000,
                size: 5,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1001,
                from: ChunkState::Allocated,
                to: ChunkState::Stored,
            },
            TraceEvent::TransferStarted {
                timestamp: 1002,
                job: ghost_core::transfer::TransferJob::new(
                    ChunkId::from_data(b"hello"),
                    TierId::Ram,
                    TierId::Disk,
                    5,
                    ghost_core::transfer::TransferPriority::Normal,
                ),
            },
            TraceEvent::TransferCompleted {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1005,
                from: TierId::Ram,
                to: TierId::Disk,
                size: 5,
                duration_ms: 3,
            },
        ]
    }

    #[test]
    fn test_invariant_validator_new() {
        let validator = InvariantValidator::new();
        assert!(validator.is_empty());
        assert_eq!(validator.len(), 0);
    }

    #[test]
    fn test_invariant_validator_with_defaults() {
        let validator = InvariantValidator::with_defaults();
        assert_eq!(validator.len(), 6);
    }

    #[test]
    fn test_no_illegal_transitions_valid() {
        let events = sample_events();
        let invariant = NoIllegalTransitions;
        let violations = invariant.validate(&events);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_no_illegal_transitions_invalid() {
        let events = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test"),
                timestamp: 1000,
                size: 4,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"test"),
                timestamp: 1001,
                from: ChunkState::Allocated,
                to: ChunkState::Cached,
            },
        ];
        let invariant = NoIllegalTransitions;
        let violations = invariant.validate(&events);
        // Allocated -> Cached is not a valid transition
        assert!(!violations.is_empty());
    }

    #[test]
    fn test_no_timestamp_regression_valid() {
        let events = sample_events();
        let invariant = NoTimestampRegression;
        let violations = invariant.validate(&events);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_no_timestamp_regression_invalid() {
        let events = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test"),
                timestamp: 2000,
                size: 4,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test2"),
                timestamp: 1000, // regression
                size: 4,
                tier: TierId::Ram,
            },
        ];
        let invariant = NoTimestampRegression;
        let violations = invariant.validate(&events);
        assert!(!violations.is_empty());
    }

    #[test]
    fn test_no_orphaned_transfers_valid() {
        let events = sample_events();
        let invariant = NoOrphanedTransfers;
        let violations = invariant.validate(&events);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_no_orphaned_transfers_invalid() {
        let events = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test"),
                timestamp: 1000,
                size: 4,
                tier: TierId::Ram,
            },
            TraceEvent::TransferStarted {
                timestamp: 1002,
                job: ghost_core::transfer::TransferJob::new(
                    ChunkId::from_data(b"test"),
                    TierId::Ram,
                    TierId::Disk,
                    4,
                    ghost_core::transfer::TransferPriority::Normal,
                ),
            },
            // No completion or failure
        ];
        let invariant = NoOrphanedTransfers;
        let violations = invariant.validate(&events);
        assert!(!violations.is_empty());
    }

    #[test]
    fn test_no_missing_completions_valid() {
        let events = sample_events();
        let invariant = NoMissingCompletions;
        let violations = invariant.validate(&events);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_no_missing_completions_invalid() {
        let events = vec![TraceEvent::TransferStarted {
            timestamp: 1002,
            job: ghost_core::transfer::TransferJob::new(
                ChunkId::from_data(b"test"),
                TierId::Ram,
                TierId::Disk,
                4,
                ghost_core::transfer::TransferPriority::Normal,
            ),
        }];
        let invariant = NoMissingCompletions;
        let violations = invariant.validate(&events);
        assert!(!violations.is_empty());
    }

    #[test]
    fn test_state_machine_consistency_valid() {
        let events = sample_events();
        let invariant = StateMachineConsistency;
        let violations = invariant.validate(&events);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_state_machine_consistency_double_create() {
        let events = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test"),
                timestamp: 1000,
                size: 4,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test"),
                timestamp: 1001,
                size: 4,
                tier: TierId::Ram,
            },
        ];
        let invariant = StateMachineConsistency;
        let violations = invariant.validate(&events);
        assert!(!violations.is_empty());
    }

    #[test]
    fn test_no_dangling_allocations_valid() {
        let events = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test"),
                timestamp: 1000,
                size: 4,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkDeleted {
                chunk_id: ChunkId::from_data(b"test"),
                tier: TierId::Ram,
                timestamp: 1001,
            },
        ];
        let invariant = NoDanglingAllocations;
        let violations = invariant.validate(&events);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_no_dangling_allocations_invalid() {
        let events = vec![TraceEvent::ChunkDeleted {
            chunk_id: ChunkId::from_data(b"test"),
            timestamp: 1000,
            tier: TierId::Ram,
        }];
        let invariant = NoDanglingAllocations;
        let violations = invariant.validate(&events);
        assert!(!violations.is_empty());
    }

    #[test]
    fn test_validator_validate_all() {
        let events = sample_events();
        let validator = InvariantValidator::with_defaults();
        let violations = validator.validate(&events);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_validator_validate_with_errors() {
        let events = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test"),
                timestamp: 2000,
                size: 4,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test2"),
                timestamp: 1000, // timestamp regression
                size: 4,
                tier: TierId::Ram,
            },
        ];
        let validator = InvariantValidator::with_defaults();
        let violations = validator.validate(&events);
        assert!(!violations.is_empty());
    }

    #[test]
    fn test_violation_severity_ordering() {
        assert!(ViolationSeverity::Critical > ViolationSeverity::Error);
        assert!(ViolationSeverity::Error > ViolationSeverity::Warning);
        assert!(ViolationSeverity::Warning > ViolationSeverity::Info);
    }

    #[test]
    fn test_violation_display() {
        let violation = InvariantViolation {
            invariant: "TestInvariant".to_string(),
            severity: ViolationSeverity::Error,
            message: "test message".to_string(),
            event_index: Some(5),
            chunk_id: None,
        };
        let display = format!("{}", violation);
        assert!(display.contains("ERROR"));
        assert!(display.contains("TestInvariant"));
    }

    #[test]
    fn test_custom_invariant() {
        struct AlwaysFails;
        impl ReplayInvariant for AlwaysFails {
            fn name(&self) -> &'static str {
                "AlwaysFails"
            }
            fn validate(&self, _events: &[TraceEvent]) -> Vec<InvariantViolation> {
                vec![InvariantViolation {
                    invariant: "AlwaysFails".to_string(),
                    severity: ViolationSeverity::Info,
                    message: "always fails".to_string(),
                    event_index: None,
                    chunk_id: None,
                }]
            }
        }

        let mut validator = InvariantValidator::new();
        validator.register(Box::new(AlwaysFails));
        let violations = validator.validate(&[]);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].invariant, "AlwaysFails");
    }
}
