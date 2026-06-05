//! Replay engine for GhostPages.
//!
//! Replays recorded trace events to validate state machine transitions,
//! measure outcomes, and compare placement policies.

use std::collections::BTreeMap;
use std::path::Path;

use ghost_core::emitter::EventEmitter;
use ghost_core::error::GhostResult;
use ghost_core::state::{ChunkState, StateMachine};
use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, TierId};

use crate::reader::TraceReader;

/// Configuration for the replay engine.
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Whether to validate state machine transitions.
    pub validate_transitions: bool,
    /// Whether to stop on the first validation error.
    pub stop_on_error: bool,
    /// Maximum number of events to replay (0 = all).
    pub max_events: u64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            validate_transitions: true,
            stop_on_error: false,
            max_events: 0,
        }
    }
}

/// Errors that can occur during replay validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayValidationError {
    /// The event index where the error occurred.
    pub event_index: u64,
    /// The event that caused the error.
    pub event: String,
    /// Description of the validation failure.
    pub message: String,
}

impl std::fmt::Display for ReplayValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReplayValidationError at event {}: {} ({})",
            self.event_index, self.message, self.event
        )
    }
}

/// Summary of a replay run.
#[derive(Debug, Clone, Default)]
pub struct ReplaySummary {
    /// Total events replayed.
    pub events_replayed: u64,
    /// Number of validation errors encountered.
    pub validation_errors: u64,
    /// Number of chunks that were created during replay.
    pub chunks_created: u64,
    /// Number of chunks that were deleted during replay.
    pub chunks_deleted: u64,
    /// Number of state transitions observed.
    pub state_transitions: u64,
    /// Number of transfers completed.
    pub transfers_completed: u64,
    /// Number of transfers failed.
    pub transfers_failed: u64,
    /// Number of evictions.
    pub evictions: u64,
    /// Number of pressure alerts.
    pub pressure_alerts: u64,
    /// Number of policy decisions.
    pub policy_decisions: u64,
    /// Unique chunk IDs seen.
    pub unique_chunks: u64,
    /// Time range of the replay (first_ts, last_ts).
    pub time_range: (u64, u64),
    /// Validation errors collected (up to a limit).
    pub errors: Vec<ReplayValidationError>,
}

/// Tracks the state of a single chunk during replay.
#[derive(Debug, Clone)]
pub struct ChunkReplayState {
    current_state: ChunkState,
    current_tier: Option<TierId>,
    created: bool,
}

/// Replay engine that processes trace events and validates state transitions.
pub struct ReplayEngine {
    config: ReplayConfig,
    state_machine: StateMachine,
    chunk_states: BTreeMap<ChunkId, ChunkReplayState>,
    summary: ReplaySummary,
    event_count: u64,
    /// Optional event emitter for unified event taxonomy.
    event_emitter: Option<EventEmitter>,
}

impl ReplayEngine {
    /// Create a new replay engine with the given configuration.
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            config,
            state_machine: StateMachine::new(),
            chunk_states: BTreeMap::new(),
            summary: ReplaySummary::default(),
            event_count: 0,
            event_emitter: None,
        }
    }

    /// Set the event emitter for unified event taxonomy.
    pub fn set_event_emitter(&mut self, emitter: EventEmitter) {
        self.event_emitter = Some(emitter);
    }

    /// Load a trace file and replay all events.
    pub fn load(path: &Path, config: ReplayConfig) -> GhostResult<(Self, ReplaySummary)> {
        let mut engine = Self::new(config);
        let mut reader = TraceReader::open(path)?;
        let events = reader.read_all()?;
        let summary = engine.replay(&events)?;
        Ok((engine, summary))
    }

    /// Replay events from an in-memory list.
    pub fn from_events(
        events: &[TraceEvent],
        config: ReplayConfig,
    ) -> GhostResult<(Self, ReplaySummary)> {
        let mut engine = Self::new(config);
        let summary = engine.replay(events)?;
        Ok((engine, summary))
    }

    /// Replay all events, returning a summary.
    pub fn replay(&mut self, events: &[TraceEvent]) -> GhostResult<ReplaySummary> {
        let max = if self.config.max_events > 0 {
            (self.config.max_events as usize).min(events.len())
        } else {
            events.len()
        };

        for (i, event) in events[..max].iter().enumerate() {
            self.event_count = i as u64;
            self.process_event(event)?;

            if self.config.stop_on_error && self.summary.validation_errors > 0 {
                break;
            }
        }

        self.summary.events_replayed = max as u64;
        self.summary.unique_chunks = self.chunk_states.len() as u64;

        Ok(self.summary.clone())
    }

    /// Replay exactly `n` events.
    pub fn replay_n(&mut self, events: &[TraceEvent], n: usize) -> GhostResult<ReplaySummary> {
        let count = n.min(events.len());
        self.replay(&events[..count])
    }

    /// Replay events until a predicate returns false.
    pub fn replay_until<F>(
        &mut self,
        events: &[TraceEvent],
        mut predicate: F,
    ) -> GhostResult<ReplaySummary>
    where
        F: FnMut(&TraceEvent) -> bool,
    {
        let mut count = 0;
        for event in events {
            if !predicate(event) {
                break;
            }
            self.event_count = count;
            self.process_event(event)?;

            if self.config.stop_on_error && self.summary.validation_errors > 0 {
                break;
            }
            count += 1;
        }

        self.summary.events_replayed = count;
        self.summary.unique_chunks = self.chunk_states.len() as u64;

        Ok(self.summary.clone())
    }

    /// Validate events without modifying state.
    pub fn validate(&self, events: &[TraceEvent]) -> GhostResult<Vec<ReplayValidationError>> {
        let mut errors = Vec::new();
        let mut state_machine = StateMachine::new();
        let mut chunk_states: BTreeMap<ChunkId, ChunkState> = BTreeMap::new();

        for (i, event) in events.iter().enumerate() {
            if let Some(chunk_id) = event.chunk_id() {
                match event {
                    TraceEvent::ChunkCreated { .. } => {
                        state_machine.register(chunk_id)?;
                        chunk_states.insert(chunk_id, ChunkState::Allocated);
                    }
                    TraceEvent::ChunkStateChanged { from, to, .. } => {
                        let current = chunk_states
                            .get(&chunk_id)
                            .copied()
                            .unwrap_or(ChunkState::Allocated);

                        if current != *from {
                            errors.push(ReplayValidationError {
                                event_index: i as u64,
                                event: format!("{:?}", event),
                                message: format!(
                                    "expected from={:?}, but chunk is in state {:?}",
                                    from, current
                                ),
                            });
                        }

                        if !ChunkState::is_valid_transition(&current, *to) {
                            errors.push(ReplayValidationError {
                                event_index: i as u64,
                                event: format!("{:?}", event),
                                message: format!("invalid transition: {:?} -> {:?}", current, to),
                            });
                        }

                        if state_machine.transition(&chunk_id, *to).is_err() {
                            errors.push(ReplayValidationError {
                                event_index: i as u64,
                                event: format!("{:?}", event),
                                message: format!("chunk {} not found in state machine", chunk_id),
                            });
                        }
                        chunk_states.insert(chunk_id, *to);
                    }
                    TraceEvent::ChunkDeleted { .. } => {
                        chunk_states.remove(&chunk_id);
                    }
                    _ => {}
                }
            }
        }

        Ok(errors)
    }

    /// Get the current replay summary.
    pub fn summary(&self) -> &ReplaySummary {
        &self.summary
    }

    /// Get the current state machine snapshot.
    pub fn state_machine(&self) -> &StateMachine {
        &self.state_machine
    }

    /// Get chunk states tracked during replay.
    pub fn chunk_states(&self) -> &BTreeMap<ChunkId, ChunkReplayState> {
        &self.chunk_states
    }

    // ─── Private ──────────────────────────────────────────────────────────────

    fn process_event(&mut self, event: &TraceEvent) -> GhostResult<()> {
        // Track time range
        let ts = event.timestamp();
        if self.summary.time_range == (0, 0) {
            self.summary.time_range = (ts, ts);
        } else {
            self.summary.time_range.0 = self.summary.time_range.0.min(ts);
            self.summary.time_range.1 = self.summary.time_range.1.max(ts);
        }

        match event {
            TraceEvent::ChunkCreated { chunk_id, tier, .. } => {
                self.summary.chunks_created += 1;
                self.state_machine.register(*chunk_id)?;
                self.chunk_states.insert(
                    *chunk_id,
                    ChunkReplayState {
                        current_state: ChunkState::Allocated,
                        current_tier: Some(*tier),
                        created: true,
                    },
                );
            }
            TraceEvent::ChunkDeleted { chunk_id, .. } => {
                self.summary.chunks_deleted += 1;
                self.chunk_states.remove(chunk_id);
            }
            TraceEvent::ChunkStateChanged {
                chunk_id, from, to, ..
            } => {
                self.summary.state_transitions += 1;

                let current = self
                    .chunk_states
                    .get(chunk_id)
                    .map(|s| s.current_state)
                    .unwrap_or(ChunkState::Allocated);

                if self.config.validate_transitions {
                    if current != *from {
                        self.add_error(format!(
                            "expected from={:?}, but chunk is in state {:?}",
                            from, current
                        ));
                    }

                    if !ChunkState::is_valid_transition(&current, *to) {
                        self.add_error(format!("invalid transition: {:?} -> {:?}", from, to));
                    }
                }

                // Only update state machine if the transition is valid
                if ChunkState::is_valid_transition(&current, *to) {
                    self.state_machine.transition(chunk_id, *to)?;
                    // Update tracked state
                    if let Some(state) = self.chunk_states.get_mut(chunk_id) {
                        state.current_state = *to;
                    } else {
                        self.chunk_states.insert(
                            *chunk_id,
                            ChunkReplayState {
                                current_state: *to,
                                current_tier: None,
                                created: false,
                            },
                        );
                    }
                }
            }
            TraceEvent::TransferCompleted { .. } => {
                self.summary.transfers_completed += 1;
            }
            TraceEvent::TransferFailed { .. } => {
                self.summary.transfers_failed += 1;
            }
            TraceEvent::Eviction { .. } => {
                self.summary.evictions += 1;
            }
            TraceEvent::PressureAlert { .. } => {
                self.summary.pressure_alerts += 1;
            }
            TraceEvent::PolicyDecision { .. } => {
                self.summary.policy_decisions += 1;
            }
            _ => {}
        }

        Ok(())
    }

    fn add_error(&mut self, message: String) {
        self.summary.validation_errors += 1;
        if self.summary.errors.len() < 100 {
            self.summary.errors.push(ReplayValidationError {
                event_index: self.event_count,
                event: "state_transition".to_string(),
                message,
            });
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::ChunkState;
    use ghost_core::types::{ChunkId, TierId};

    fn test_events() -> Vec<TraceEvent> {
        vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"chunk1"),
                size: 1024,
                tier: TierId::Ram,
                timestamp: 1000,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"chunk1"),
                from: ChunkState::Allocated,
                to: ChunkState::Stored,
                timestamp: 1001,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"chunk1"),
                from: ChunkState::Stored,
                to: ChunkState::Cached,
                timestamp: 1002,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"chunk1"),
                from: ChunkState::Cached,
                to: ChunkState::Stored,
                timestamp: 1003,
            },
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"chunk1"),
                tier: TierId::Ram,
                reason: ghost_core::trace::EvictionReason::Capacity,
                timestamp: 1004,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"chunk1"),
                from: ChunkState::Stored,
                to: ChunkState::Evicted,
                timestamp: 1005,
            },
        ]
    }

    #[test]
    fn test_replay_basic() {
        let config = ReplayConfig::default();
        let (engine, summary) = ReplayEngine::from_events(&test_events(), config).unwrap();

        assert_eq!(summary.events_replayed, 6);
        assert_eq!(summary.chunks_created, 1);
        assert_eq!(summary.state_transitions, 4);
        assert_eq!(summary.evictions, 1);
        assert_eq!(summary.validation_errors, 0);
        assert_eq!(summary.unique_chunks, 1);
        assert_eq!(summary.time_range, (1000, 1005));

        let sm = engine.state_machine();
        assert_eq!(sm.chunks_in_state(ChunkState::Evicted).len(), 1);
    }

    #[test]
    fn test_replay_invalid_transition() {
        let events = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"chunk1"),
                size: 1024,
                tier: TierId::Ram,
                timestamp: 1000,
            },
            // Skip Stored, go directly from Allocated to Cached — invalid
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"chunk1"),
                from: ChunkState::Allocated,
                to: ChunkState::Cached,
                timestamp: 1001,
            },
        ];

        let config = ReplayConfig::default();
        let (_, summary) = ReplayEngine::from_events(&events, config).unwrap();
        assert!(summary.validation_errors > 0);
    }

    #[test]
    fn test_replay_n() {
        let config = ReplayConfig::default();
        let mut engine = ReplayEngine::new(config);
        let summary = engine.replay_n(&test_events(), 2).unwrap();
        assert_eq!(summary.events_replayed, 2);
        assert_eq!(summary.chunks_created, 1);
        assert_eq!(summary.state_transitions, 1);
    }

    #[test]
    fn test_replay_until() {
        let config = ReplayConfig::default();
        let mut engine = ReplayEngine::new(config);
        let summary = engine
            .replay_until(&test_events(), |e| e.timestamp() < 1002)
            .unwrap();
        assert_eq!(summary.events_replayed, 2); // events at 1000 and 1001
    }

    #[test]
    fn test_validate() {
        let engine = ReplayEngine::new(ReplayConfig::default());
        let errors = engine.validate(&test_events()).unwrap();
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_validate_catches_invalid() {
        let engine = ReplayEngine::new(ReplayConfig::default());
        let events = vec![TraceEvent::ChunkStateChanged {
            chunk_id: ChunkId::from_data(b"chunk1"),
            from: ChunkState::Allocated,
            to: ChunkState::Cached,
            timestamp: 1000,
        }];
        let errors = engine.validate(&events).unwrap();
        assert!(errors.len() > 0);
    }

    #[test]
    fn test_replay_from_file() {
        use crate::format::flags;
        use crate::format::TraceMetadata;
        use crate::writer::TraceWriter;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut writer = TraceWriter::create(path, flags::HAS_CHECKSUM).unwrap();
        let events = test_events();
        writer.write_events(&events).unwrap();

        let metadata = TraceMetadata {
            total_events: 6,
            total_chunks: 1,
            tier_ids: vec![TierId::Ram],
            time_range: (1000, 1005),
            policy_name: "test".to_string(),
            config_summary: "test".to_string(),
        };
        writer.close(metadata).unwrap();

        let config = ReplayConfig::default();
        let (engine, summary) = ReplayEngine::load(path, config).unwrap();
        assert_eq!(summary.events_replayed, 6);
        assert_eq!(summary.validation_errors, 0);
        assert_eq!(engine.chunk_states().len(), 1);
    }
}
