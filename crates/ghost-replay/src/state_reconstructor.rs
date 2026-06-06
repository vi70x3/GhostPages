//! Event-by-event physical state reconstruction for cross-domain replay validation.
//!
//! Reconstructs the full physical state (chunk states, pressure, tier placement)
//! at each event in a trace, enabling comparison of state evolution across
//! different execution domains.

use ghost_core::state::{ChunkState, PressureState, StateMachine};
use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, TierId};

/// A snapshot of the physical state at a particular event index.
///
/// Note: `PartialEq` is not derived because `TraceEvent` and `PressureState`
/// do not implement it. Use [`StateReconstructor::compare`] for diffing.
#[derive(Debug, Clone)]
pub struct StateSnapshot {
    /// Index of the event that produced this state.
    pub event_index: usize,
    /// Timestamp of the event.
    pub timestamp: u64,
    /// The event that produced this state.
    pub event: TraceEvent,
    /// Chunk states at this point.
    pub chunk_states: Vec<(ChunkId, ChunkState)>,
    /// Pressure state at this point.
    pub pressure: PressureState,
    /// Total bytes allocated across all tiers.
    pub total_allocated: u64,
    /// Total bytes stored (excluding evicted/failed).
    pub total_stored: u64,
    /// Number of chunks in each tier.
    pub tier_counts: Vec<(TierId, usize)>,
}

impl StateSnapshot {
    /// Create an empty state snapshot (before any events).
    pub fn empty() -> Self {
        Self {
            event_index: 0,
            timestamp: 0,
            event: TraceEvent::DaemonStarted { timestamp: 0 },
            chunk_states: Vec::new(),
            pressure: PressureState::new(),
            total_allocated: 0,
            total_stored: 0,
            tier_counts: Vec::new(),
        }
    }

    /// Returns true if the state machine is in a consistent state.
    pub fn is_consistent(&self) -> bool {
        for (_, state) in &self.chunk_states {
            if *state == ChunkState::Failed {
                let stored_count = self
                    .chunk_states
                    .iter()
                    .filter(|(_, s)| *s == ChunkState::Stored)
                    .count();
                if stored_count > 0 {
                    return false;
                }
            }
        }
        true
    }

    /// Get the state of a specific chunk, if known.
    pub fn chunk_state(&self, chunk_id: &ChunkId) -> Option<ChunkState> {
        self.chunk_states
            .iter()
            .find(|(id, _)| id == chunk_id)
            .map(|(_, state)| *state)
    }

    /// Get the number of chunks in a specific state.
    pub fn count_in_state(&self, state: ChunkState) -> usize {
        self.chunk_states
            .iter()
            .filter(|(_, s)| *s == state)
            .count()
    }

    /// Returns true if the chunk-state decisions (which chunks are in which
    /// state, total allocated, total stored) are the same as another snapshot.
    /// Timing and pressure are intentionally excluded.
    pub fn same_decisions(&self, other: &Self) -> bool {
        self.chunk_states == other.chunk_states
            && self.total_allocated == other.total_allocated
            && self.total_stored == other.total_stored
    }
}

/// Reconstructs physical state by replaying events sequentially.
///
/// The `StateReconstructor` processes events one at a time, maintaining
/// a `StateMachine` and `PressureState` to produce `StateSnapshot`s
/// at each step.
#[derive(Debug, Clone)]
pub struct StateReconstructor {
    state_machine: StateMachine,
    pressure: PressureState,
    total_allocated: u64,
    total_stored: u64,
    tier_usage: Vec<(TierId, u64)>,
    snapshots: Vec<StateSnapshot>,
}

impl StateReconstructor {
    /// Create a new state reconstructor with empty initial state.
    pub fn new() -> Self {
        Self {
            state_machine: StateMachine::new(),
            pressure: PressureState::new(),
            total_allocated: 0,
            total_stored: 0,
            tier_usage: vec![
                (TierId::Ram, 0),
                (TierId::GpuVram, 0),
                (TierId::Disk, 0),
                (TierId::Simulation, 0),
            ],
            snapshots: Vec::new(),
        }
    }

    /// Process a single event and update internal state.
    pub fn process_event(&mut self, index: usize, event: &TraceEvent) {
        let timestamp = event.timestamp();

        // Update state machine based on event type
        match event {
            TraceEvent::ChunkCreated {
                chunk_id, size, tier, ..
            } => {
                let _ = self.state_machine.register(*chunk_id);
                // register() already sets state to Allocated — no transition needed
                self.total_allocated += *size as u64;
                self.add_tier_usage(*tier, *size as u64);
            }
            TraceEvent::ChunkStateChanged {
                chunk_id, from, to, ..
            } => {
                // Only transition if the current state matches the expected `from` state
                // and the transition is valid. This prevents panics on malformed traces.
                if let Some(current) = self.state_machine.get_state(chunk_id) {
                    if current == *from && current.is_valid_transition(*to) {
                        let _ = self.state_machine.transition(chunk_id, *to);
                    }
                }
            }
            TraceEvent::ChunkDeleted {
                chunk_id, ..
            } => {
                self.state_machine.remove(chunk_id);
            }
            TraceEvent::Eviction {
                chunk_id, ..
            } => {
                if let Some(current) = self.state_machine.get_state(chunk_id) {
                    if current.is_valid_transition(ChunkState::Evicted) {
                        let _ = self.state_machine.transition(chunk_id, ChunkState::Evicted);
                    }
                }
            }
            TraceEvent::PressureSample { state, .. } => {
                self.pressure = *state;
            }
            TraceEvent::PressureAlert {
                memory_pressure,
                vram_pressure,
                io_pressure,
                ..
            } => {
                self.pressure = PressureState::new();
                self.pressure = PressureState {
                    memory_pressure: *memory_pressure,
                    vram_pressure: *vram_pressure,
                    io_pressure: *io_pressure,
                    queue_depth: self.pressure.queue_depth,
                    throughput_bps: self.pressure.throughput_bps,
                };
            }
            _ => {}
        }

        // Compute total stored (chunks that are Allocated, Stored, Cached, or Migrating)
        self.total_stored = self
            .state_machine
            .snapshot()
            .values()
            .filter(|s| {
                matches!(
                    s,
                    ChunkState::Allocated
                        | ChunkState::Stored
                        | ChunkState::Cached
                        | ChunkState::Migrating
                )
            })
            .count() as u64;

        // Build tier counts
        let tier_counts = self.build_tier_counts();

        // Build chunk states snapshot
        let chunk_states = self
            .state_machine
            .snapshot()
            .into_iter()
            .collect::<Vec<(ChunkId, ChunkState)>>();

        let snapshot = StateSnapshot {
            event_index: index,
            timestamp,
            event: event.clone(),
            chunk_states,
            pressure: self.pressure,
            total_allocated: self.total_allocated,
            total_stored: self.total_stored,
            tier_counts,
        };

        self.snapshots.push(snapshot);
    }

    /// Process a slice of events, producing a snapshot for each.
    pub fn process_events(&mut self, events: &[TraceEvent]) {
        for (i, event) in events.iter().enumerate() {
            self.process_event(i, event);
        }
    }

    /// Get all snapshots produced so far.
    pub fn snapshots(&self) -> &[StateSnapshot] {
        &self.snapshots
    }

    /// Get the snapshot at a specific event index.
    pub fn snapshot_at(&self, index: usize) -> Option<&StateSnapshot> {
        self.snapshots.get(index)
    }

    /// Get the final state snapshot.
    pub fn final_snapshot(&self) -> Option<&StateSnapshot> {
        self.snapshots.last()
    }

    /// Compare two reconstructions and return indices where states differ.
    ///
    /// Two snapshots are considered different if their chunk states, pressure
    /// (with floating-point tolerance), or totals differ.
    pub fn compare(a: &[StateSnapshot], b: &[StateSnapshot]) -> Vec<usize> {
        let min_len = a.len().min(b.len());
        let mut diffs = Vec::new();

        for i in 0..min_len {
            let sa = &a[i];
            let sb = &b[i];

            // Compare chunk states
            if sa.chunk_states != sb.chunk_states {
                diffs.push(i);
                continue;
            }

            // Compare pressure (with floating-point tolerance)
            if (sa.pressure.memory_pressure - sb.pressure.memory_pressure).abs() > f32::EPSILON
                || (sa.pressure.vram_pressure - sb.pressure.vram_pressure).abs() > f32::EPSILON
                || (sa.pressure.io_pressure - sb.pressure.io_pressure).abs() > f32::EPSILON
            {
                diffs.push(i);
                continue;
            }

            // Compare totals
            if sa.total_allocated != sb.total_allocated || sa.total_stored != sb.total_stored {
                diffs.push(i);
            }
        }

        diffs
    }

    fn add_tier_usage(&mut self, tier: TierId, size: u64) {
        for (t, usage) in &mut self.tier_usage {
            if *t == tier {
                *usage += size;
                break;
            }
        }
    }

    fn build_tier_counts(&self) -> Vec<(TierId, usize)> {
        let mut counts: std::collections::HashMap<TierId, usize> = std::collections::HashMap::new();
        for (_, state) in self.state_machine.snapshot() {
            if state == ChunkState::Stored || state == ChunkState::Cached {
                *counts.entry(TierId::Ram).or_insert(0) += 1;
            }
        }
        let mut result: Vec<(TierId, usize)> = counts.into_iter().collect();
        result.sort_by_key(|(tier, _)| tier.priority());
        result
    }
}

impl Default for StateReconstructor {
    fn default() -> Self {
        Self::new()
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
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"chunk2"),
                size: 2048,
                tier: TierId::Disk,
                timestamp: 1002,
            },
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"chunk1"),
                tier: TierId::Ram,
                reason: ghost_core::trace::EvictionReason::Capacity,
                timestamp: 1003,
            },
        ]
    }

    #[test]
    fn test_reconstructor_new() {
        let recon = StateReconstructor::new();
        assert!(recon.snapshots().is_empty());
    }

    #[test]
    fn test_reconstructor_process_events() {
        let mut recon = StateReconstructor::new();
        let events = sample_events();
        recon.process_events(&events);

        let snapshots = recon.snapshots();
        assert_eq!(snapshots.len(), 4);

        // First snapshot: chunk1 created
        assert_eq!(snapshots[0].chunk_states.len(), 1);
        assert_eq!(snapshots[0].total_allocated, 1024);

        // Second snapshot: chunk1 transitioned to Stored
        assert_eq!(
            snapshots[1].chunk_state(&ChunkId::from_data(b"chunk1")),
            Some(ChunkState::Stored)
        );

        // Third snapshot: chunk2 created
        assert_eq!(snapshots[2].chunk_states.len(), 2);
        assert_eq!(snapshots[2].total_allocated, 1024 + 2048);

        // Fourth snapshot: chunk1 evicted
        assert_eq!(
            snapshots[3].chunk_state(&ChunkId::from_data(b"chunk1")),
            Some(ChunkState::Evicted)
        );
    }

    #[test]
    fn test_reconstructor_snapshot_at() {
        let mut recon = StateReconstructor::new();
        recon.process_events(&sample_events());

        let snap = recon.snapshot_at(2);
        assert!(snap.is_some());
        assert_eq!(snap.unwrap().timestamp, 1002);

        let snap = recon.snapshot_at(99);
        assert!(snap.is_none());
    }

    #[test]
    fn test_reconstructor_final_snapshot() {
        let mut recon = StateReconstructor::new();
        recon.process_events(&sample_events());

        let final_snap = recon.final_snapshot();
        assert!(final_snap.is_some());
        assert_eq!(final_snap.unwrap().timestamp, 1003);
    }

    #[test]
    fn test_reconstructor_compare_identical() {
        let mut recon1 = StateReconstructor::new();
        let mut recon2 = StateReconstructor::new();
        let events = sample_events();

        recon1.process_events(&events);
        recon2.process_events(&events);

        let diffs = StateReconstructor::compare(recon1.snapshots(), recon2.snapshots());
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_reconstructor_compare_different() {
        let mut recon1 = StateReconstructor::new();
        let mut recon2 = StateReconstructor::new();

        let events1 = sample_events();
        let mut events2 = sample_events();
        // Modify the second event in events2
        events2[1] = TraceEvent::ChunkStateChanged {
            chunk_id: ChunkId::from_data(b"chunk1"),
            from: ChunkState::Allocated,
            to: ChunkState::Cached, // different from Stored
            timestamp: 1001,
        };

        recon1.process_events(&events1);
        recon2.process_events(&events2);

        let diffs = StateReconstructor::compare(recon1.snapshots(), recon2.snapshots());
        assert!(!diffs.is_empty());
        assert_eq!(diffs[0], 1); // First difference at index 1
    }

    #[test]
    fn test_state_snapshot_empty() {
        let snap = StateSnapshot::empty();
        assert!(snap.chunk_states.is_empty());
        assert_eq!(snap.total_allocated, 0);
        assert_eq!(snap.total_stored, 0);
    }

    #[test]
    fn test_state_snapshot_count_in_state() {
        let mut recon = StateReconstructor::new();
        recon.process_events(&sample_events());

        let snap = recon.snapshot_at(2).unwrap();
        assert_eq!(snap.count_in_state(ChunkState::Stored), 1);
        assert_eq!(snap.count_in_state(ChunkState::Evicted), 0);
    }

    #[test]
    fn test_state_snapshot_is_consistent() {
        let mut recon = StateReconstructor::new();
        recon.process_events(&sample_events());

        for snap in recon.snapshots() {
            assert!(snap.is_consistent());
        }
    }

    #[test]
    fn test_state_snapshot_same_decisions() {
        let mut recon1 = StateReconstructor::new();
        let mut recon2 = StateReconstructor::new();
        let events = sample_events();

        recon1.process_events(&events);
        recon2.process_events(&events);

        let s1 = recon1.snapshot_at(2).unwrap();
        let s2 = recon2.snapshot_at(2).unwrap();
        assert!(s1.same_decisions(s2));
    }
}
