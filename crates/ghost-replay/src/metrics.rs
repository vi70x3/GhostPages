//! Replay metrics and policy comparison for GhostPages.
//!
//! Provides metrics collection during replay and comparison between
//! different placement policies.

use std::collections::HashMap;

use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, TierId};

/// Metrics collected during a single replay run.
#[derive(Debug, Clone, Default)]
pub struct ReplayMetrics {
    /// Total events processed.
    pub total_events: u64,
    /// Events per second (if replay was timed).
    pub events_per_second: f64,
    /// Total chunks created.
    pub chunks_created: u64,
    /// Total chunks deleted.
    pub chunks_deleted: u64,
    /// Total state transitions.
    pub state_transitions: u64,
    /// Transitions per chunk (average).
    pub avg_transitions_per_chunk: f64,
    /// Total transfers completed.
    pub transfers_completed: u64,
    /// Total transfers failed.
    pub transfers_failed: u64,
    /// Transfer success rate (0.0 to 1.0).
    pub transfer_success_rate: f64,
    /// Total evictions.
    pub evictions: u64,
    /// Evictions broken down by reason.
    pub evictions_by_reason: HashMap<String, u64>,
    /// Total pressure alerts.
    pub pressure_alerts: u64,
    /// Peak memory pressure observed.
    pub peak_memory_pressure: f32,
    /// Peak VRAM pressure observed.
    pub peak_vram_pressure: f32,
    /// Peak IO pressure observed.
    pub peak_io_pressure: f32,
    /// Number of policy decisions.
    pub policy_decisions: u64,
    /// Policy decisions resulting in migration.
    pub migrations_decided: u64,
    /// Unique chunks touched.
    pub unique_chunks: u64,
    /// Tier distribution: tier_id -> chunk count.
    pub tier_distribution: HashMap<TierId, u64>,
    /// Average chunk lifetime in events.
    pub avg_chunk_lifetime_events: f64,
    /// Time range (first_ts, last_ts).
    pub time_range: (u64, u64),
}

impl ReplayMetrics {
    /// Create a new metrics collector with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Collect metrics from a slice of trace events.
    pub fn from_events(events: &[TraceEvent]) -> Self {
        let mut metrics = Self::new();
        let mut chunk_create_events: HashMap<ChunkId, u64> = HashMap::new();
        let mut chunk_delete_events: HashMap<ChunkId, u64> = HashMap::new();
        let mut chunk_transition_counts: HashMap<ChunkId, u64> = HashMap::new();
        let mut tier_counts: HashMap<TierId, u64> = HashMap::new();

        metrics.total_events = events.len() as u64;

        for (i, event) in events.iter().enumerate() {
            let ts = event.timestamp();
            if metrics.time_range == (0, 0) {
                metrics.time_range = (ts, ts);
            } else {
                metrics.time_range.0 = metrics.time_range.0.min(ts);
                metrics.time_range.1 = metrics.time_range.1.max(ts);
            }

            match event {
                TraceEvent::ChunkCreated { chunk_id, tier, .. } => {
                    metrics.chunks_created += 1;
                    chunk_create_events.insert(*chunk_id, i as u64);
                    *tier_counts.entry(*tier).or_insert(0) += 1;
                }
                TraceEvent::ChunkDeleted { chunk_id, .. } => {
                    metrics.chunks_deleted += 1;
                    chunk_delete_events.insert(*chunk_id, i as u64);
                }
                TraceEvent::ChunkStateChanged { chunk_id, .. } => {
                    metrics.state_transitions += 1;
                    *chunk_transition_counts.entry(*chunk_id).or_insert(0) += 1;
                }
                TraceEvent::TransferCompleted { .. } => {
                    metrics.transfers_completed += 1;
                }
                TraceEvent::TransferFailed { .. } => {
                    metrics.transfers_failed += 1;
                }
                TraceEvent::Eviction { reason, .. } => {
                    metrics.evictions += 1;
                    let reason_str = format!("{:?}", reason);
                    *metrics.evictions_by_reason.entry(reason_str).or_insert(0) += 1;
                }
                TraceEvent::PressureAlert {
                    memory_pressure,
                    vram_pressure,
                    io_pressure,
                    ..
                } => {
                    metrics.pressure_alerts += 1;
                    metrics.peak_memory_pressure =
                        metrics.peak_memory_pressure.max(*memory_pressure);
                    metrics.peak_vram_pressure = metrics.peak_vram_pressure.max(*vram_pressure);
                    metrics.peak_io_pressure = metrics.peak_io_pressure.max(*io_pressure);
                }
                TraceEvent::PolicyDecision { from, to, .. } => {
                    metrics.policy_decisions += 1;
                    if from != to {
                        metrics.migrations_decided += 1;
                    }
                }
                _ => {}
            }
        }

        metrics.unique_chunks = chunk_create_events.len() as u64;
        metrics.tier_distribution = tier_counts;

        // Compute averages
        if metrics.unique_chunks > 0 {
            metrics.avg_transitions_per_chunk =
                metrics.state_transitions as f64 / metrics.unique_chunks as f64;
        }

        let total_transfers = metrics.transfers_completed + metrics.transfers_failed;
        if total_transfers > 0 {
            metrics.transfer_success_rate =
                metrics.transfers_completed as f64 / total_transfers as f64;
        }

        // Average chunk lifetime
        if !chunk_create_events.is_empty() {
            let mut total_lifetime: u64 = 0;
            let mut lifetime_count: u64 = 0;
            for (chunk_id, create_idx) in &chunk_create_events {
                if let Some(delete_idx) = chunk_delete_events.get(chunk_id) {
                    total_lifetime += *delete_idx - *create_idx;
                    lifetime_count += 1;
                }
            }
            if lifetime_count > 0 {
                metrics.avg_chunk_lifetime_events = total_lifetime as f64 / lifetime_count as f64;
            }
        }

        metrics
    }
}

/// Result of comparing two replay runs (e.g., different policies).
#[derive(Debug, Clone, Default)]
pub struct PolicyComparison {
    /// Name of the baseline policy.
    pub baseline_name: String,
    /// Name of the candidate policy.
    pub candidate_name: String,
    /// Baseline metrics.
    pub baseline: ReplayMetrics,
    /// Candidate metrics.
    pub candidate: ReplayMetrics,
    /// Which policy "won" overall.
    pub winner: ComparisonWinner,
    /// Per-metric deltas (candidate - baseline).
    pub deltas: HashMap<String, f64>,
    /// Human-readable summary.
    pub summary: String,
}

/// Which policy performed better.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub enum ComparisonWinner {
    /// Baseline performed better.
    Baseline,
    /// Candidate performed better.
    Candidate,
    /// Neither was clearly better.
    #[default]
    Tie,
}

impl std::fmt::Display for ComparisonWinner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Baseline => write!(f, "baseline"),
            Self::Candidate => write!(f, "candidate"),
            Self::Tie => write!(f, "tie"),
        }
    }
}

/// Compare two trace event sets as if they were produced by different policies.
///
/// Returns a `PolicyComparison` with deltas and a winner determination.
pub fn compare_traces(
    baseline_events: &[TraceEvent],
    candidate_events: &[TraceEvent],
    baseline_name: &str,
    candidate_name: &str,
) -> PolicyComparison {
    let baseline_metrics = ReplayMetrics::from_events(baseline_events);
    let candidate_metrics = ReplayMetrics::from_events(candidate_events);

    let mut deltas = HashMap::new();

    // Compute deltas (positive = candidate is better for "lower is better" metrics)
    deltas.insert(
        "evictions".to_string(),
        baseline_metrics.evictions as f64 - candidate_metrics.evictions as f64,
    );
    deltas.insert(
        "transfer_failures".to_string(),
        baseline_metrics.transfers_failed as f64 - candidate_metrics.transfers_failed as f64,
    );
    deltas.insert(
        "transfer_success_rate".to_string(),
        candidate_metrics.transfer_success_rate - baseline_metrics.transfer_success_rate,
    );
    deltas.insert(
        "pressure_alerts".to_string(),
        baseline_metrics.pressure_alerts as f64 - candidate_metrics.pressure_alerts as f64,
    );
    deltas.insert(
        "peak_memory_pressure".to_string(),
        baseline_metrics.peak_memory_pressure as f64
            - candidate_metrics.peak_memory_pressure as f64,
    );
    deltas.insert(
        "avg_transitions_per_chunk".to_string(),
        baseline_metrics.avg_transitions_per_chunk - candidate_metrics.avg_transitions_per_chunk,
    );

    // Determine winner using a simple scoring system
    let mut baseline_score: i32 = 0;
    let mut candidate_score: i32 = 0;

    // Fewer evictions is better
    if baseline_metrics.evictions < candidate_metrics.evictions {
        baseline_score += 1;
    } else if candidate_metrics.evictions < baseline_metrics.evictions {
        candidate_score += 1;
    }

    // Higher transfer success rate is better
    if baseline_metrics.transfer_success_rate > candidate_metrics.transfer_success_rate {
        baseline_score += 1;
    } else if candidate_metrics.transfer_success_rate > baseline_metrics.transfer_success_rate {
        candidate_score += 1;
    }

    // Fewer pressure alerts is better
    if baseline_metrics.pressure_alerts < candidate_metrics.pressure_alerts {
        baseline_score += 1;
    } else if candidate_metrics.pressure_alerts < baseline_metrics.pressure_alerts {
        candidate_score += 1;
    }

    // Lower peak memory pressure is better
    if baseline_metrics.peak_memory_pressure < candidate_metrics.peak_memory_pressure {
        baseline_score += 1;
    } else if candidate_metrics.peak_memory_pressure < baseline_metrics.peak_memory_pressure {
        candidate_score += 1;
    }

    // Fewer transfer failures is better
    if baseline_metrics.transfers_failed < candidate_metrics.transfers_failed {
        baseline_score += 1;
    } else if candidate_metrics.transfers_failed < baseline_metrics.transfers_failed {
        candidate_score += 1;
    }

    let winner = if baseline_score > candidate_score {
        ComparisonWinner::Baseline
    } else if candidate_score > baseline_score {
        ComparisonWinner::Candidate
    } else {
        ComparisonWinner::Tie
    };

    let summary = format!(
        "Policy comparison: {} (score {}) vs {} (score {}) — winner: {}",
        baseline_name, baseline_score, candidate_name, candidate_score, winner
    );

    PolicyComparison {
        baseline_name: baseline_name.to_string(),
        candidate_name: candidate_name.to_string(),
        baseline: baseline_metrics,
        candidate: candidate_metrics,
        winner,
        deltas,
        summary,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::ChunkState;
    use ghost_core::types::ChunkId;

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
            TraceEvent::TransferCompleted {
                chunk_id: ChunkId::from_data(b"chunk1"),
                from: TierId::Ram,
                to: TierId::Disk,
                size: 1024,
                duration_ms: 50,
                timestamp: 1002,
            },
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"chunk1"),
                tier: TierId::Disk,
                reason: ghost_core::trace::EvictionReason::Capacity,
                timestamp: 1003,
            },
            TraceEvent::PressureAlert {
                memory_pressure: 0.8,
                vram_pressure: 0.3,
                io_pressure: 0.5,
                timestamp: 1004,
            },
        ]
    }

    #[test]
    fn test_metrics_from_events() {
        let events = sample_events();
        let metrics = ReplayMetrics::from_events(&events);

        assert_eq!(metrics.total_events, 5);
        assert_eq!(metrics.chunks_created, 1);
        assert_eq!(metrics.state_transitions, 1);
        assert_eq!(metrics.transfers_completed, 1);
        assert_eq!(metrics.evictions, 1);
        assert_eq!(metrics.pressure_alerts, 1);
        assert_eq!(metrics.unique_chunks, 1);
        assert_eq!(metrics.time_range, (1000, 1004));
        assert!(metrics.peak_memory_pressure - 0.8 < f32::EPSILON);
    }

    #[test]
    fn test_metrics_transfer_success_rate() {
        let events = vec![
            TraceEvent::TransferCompleted {
                chunk_id: ChunkId::from_data(b"a"),
                from: TierId::Ram,
                to: TierId::Disk,
                size: 100,
                duration_ms: 10,
                timestamp: 1000,
            },
            TraceEvent::TransferCompleted {
                chunk_id: ChunkId::from_data(b"b"),
                from: TierId::Ram,
                to: TierId::Disk,
                size: 100,
                duration_ms: 10,
                timestamp: 1001,
            },
            TraceEvent::TransferFailed {
                chunk_id: ChunkId::from_data(b"c"),
                from: TierId::Ram,
                to: TierId::Disk,
                error: "timeout".to_string(),
                attempt: 1,
                timestamp: 1002,
            },
        ];

        let metrics = ReplayMetrics::from_events(&events);
        assert_eq!(metrics.transfers_completed, 2);
        assert_eq!(metrics.transfers_failed, 1);
        assert!((metrics.transfer_success_rate - 2.0 / 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compare_traces() {
        let baseline = sample_events();
        let candidate = vec![
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
            // No eviction, no transfer failure — better!
            TraceEvent::PressureAlert {
                memory_pressure: 0.5,
                vram_pressure: 0.2,
                io_pressure: 0.3,
                timestamp: 1004,
            },
        ];

        let comparison = compare_traces(&baseline, &candidate, "lru", "ml-policy");
        assert_eq!(comparison.winner, ComparisonWinner::Candidate);
        assert!(comparison.deltas.contains_key("evictions"));
    }

    #[test]
    fn test_compare_traces_tie() {
        let events = sample_events();
        let comparison = compare_traces(&events, &events, "policy-a", "policy-b");
        assert_eq!(comparison.winner, ComparisonWinner::Tie);
    }

    #[test]
    fn test_evictions_by_reason() {
        let events = vec![
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"a"),
                tier: TierId::Ram,
                reason: ghost_core::trace::EvictionReason::Capacity,
                timestamp: 1000,
            },
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"b"),
                tier: TierId::Ram,
                reason: ghost_core::trace::EvictionReason::Capacity,
                timestamp: 1001,
            },
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"c"),
                tier: TierId::Disk,
                reason: ghost_core::trace::EvictionReason::Policy,
                timestamp: 1002,
            },
        ];

        let metrics = ReplayMetrics::from_events(&events);
        assert_eq!(metrics.evictions, 3);
        assert_eq!(
            *metrics.evictions_by_reason.get("Capacity").unwrap_or(&0),
            2
        );
        assert_eq!(*metrics.evictions_by_reason.get("Policy").unwrap_or(&0), 1);
    }

    #[test]
    fn test_comparison_winner_display() {
        assert_eq!(format!("{}", ComparisonWinner::Baseline), "baseline");
        assert_eq!(format!("{}", ComparisonWinner::Candidate), "candidate");
        assert_eq!(format!("{}", ComparisonWinner::Tie), "tie");
    }
}
