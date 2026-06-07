//! Policy Tournament Framework for GhostPages.
//!
//! This module provides a deterministic tournament runner that evaluates
//! multiple policies against identical replay streams and compares their
//! scores. All functions are **pure** — same inputs always produce same
//! outputs. No I/O, no mutation, no side effects.

use std::collections::HashMap;

use ghost_core::types::{ChunkId, TierId};
use ghost_linux::policy::Recommendation;
use ghost_linux::policy_rules::{PressureLevel, SystemState};

use crate::baseline::evaluate_baseline;
use crate::scoring::{RecommendationScore, score_policy_evaluation};

// ─── Policy Trait ─────────────────────────────────────────────────────────────

/// A pluggable policy that can evaluate system state and produce recommendations.
///
/// Policies must be `Send + Sync` so they can be used in concurrent contexts,
/// though the tournament runner itself is sequential and deterministic.
pub trait Policy: Send + Sync {
    /// The unique name of this policy.
    fn name(&self) -> &'static str;

    /// Evaluate the system state and return a set of recommendations.
    fn evaluate(&self, state: &SystemState) -> Vec<Recommendation>;
}

// ─── Policy Result ────────────────────────────────────────────────────────────

/// The result of evaluating a single policy in a single round.
#[derive(Debug, Clone)]
pub struct PolicyResult {
    /// The name of the policy that produced this result.
    pub policy_name: &'static str,
    /// The recommendations the policy produced.
    pub recommendations: Vec<Recommendation>,
    /// The score of the recommendations against the state change.
    pub score: RecommendationScore,
    /// The system state before the round.
    pub state_before: SystemState,
    /// The system state after the round.
    pub state_after: SystemState,
}

// ─── Policy Round ─────────────────────────────────────────────────────────────

/// A single evaluation round — one state snapshot evaluated by all policies.
#[derive(Debug, Clone)]
pub struct PolicyRound {
    /// The index of this round in the tournament.
    pub round_index: usize,
    /// The system state before the round.
    pub state_before: SystemState,
    /// The system state after the round.
    pub state_after: SystemState,
    /// Results from each policy in this round.
    pub results: Vec<PolicyResult>,
    /// The name of the winning policy (highest overall score).
    pub round_winner: Option<&'static str>,
}

// ─── Tournament Summary ───────────────────────────────────────────────────────

/// Aggregate statistics for a complete tournament.
#[derive(Debug, Clone)]
pub struct TournamentSummary {
    /// Total number of rounds played.
    pub total_rounds: usize,
    /// Number of rounds won by each policy.
    pub policy_wins: HashMap<&'static str, usize>,
    /// Average overall score for each policy across all rounds.
    pub average_scores: HashMap<&'static str, f32>,
    /// The best overall score achieved by any policy in any round.
    pub best_overall_score: f32,
    /// The worst overall score achieved by any policy in any round.
    pub worst_overall_score: f32,
}

// ─── Tournament Result ────────────────────────────────────────────────────────

/// The complete output of a tournament.
#[derive(Debug, Clone)]
pub struct TournamentResult {
    /// All rounds that were played.
    pub rounds: Vec<PolicyRound>,
    /// The name of the overall winning policy (most round wins).
    pub winner: Option<&'static str>,
    /// Aggregate statistics.
    pub summary: TournamentSummary,
}

// ─── Policy Arena ─────────────────────────────────────────────────────────────

/// Tournament runner that evaluates multiple policies against the same
/// replay stream and compares their scores.
///
/// The arena is **deterministic**: same policies + same state pairs always
/// produce the same tournament result.
pub struct PolicyArena {
    policies: Vec<Box<dyn Policy>>,
    results: Vec<PolicyResult>,
}

impl PolicyArena {
    /// Create a new empty arena.
    pub fn new() -> Self {
        Self {
            policies: Vec::new(),
            results: Vec::new(),
        }
    }

    /// Add a policy to the arena. Returns `&mut Self` for chaining.
    pub fn add_policy(&mut self, policy: Box<dyn Policy>) -> &mut Self {
        self.policies.push(policy);
        self
    }

    /// Run a single round with the given before/after states.
    ///
    /// Each policy is evaluated against `state_before`, and the resulting
    /// recommendations are scored against the state change.
    pub fn run_round(
        &mut self,
        state_before: &SystemState,
        state_after: &SystemState,
    ) -> PolicyRound {
        let round_index = self.results.len() / self.policies.len().max(1);

        let mut round_results: Vec<PolicyResult> = Vec::new();

        for policy in &self.policies {
            let recommendations = policy.evaluate(state_before);
            let score =
                score_policy_evaluation(&recommendations, state_before, state_after);

            let result = PolicyResult {
                policy_name: policy.name(),
                recommendations,
                score,
                state_before: state_before.clone(),
                state_after: state_after.clone(),
            };

            self.results.push(result.clone());
            round_results.push(result);
        }

        // Determine round winner (highest overall score)
        let round_winner = round_results
            .iter()
            .max_by(|a, b| {
                a.score
                    .overall_score
                    .partial_cmp(&b.score.overall_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|r| r.policy_name);

        PolicyRound {
            round_index,
            state_before: state_before.clone(),
            state_after: state_after.clone(),
            results: round_results,
            round_winner,
        }
    }

    /// Run a full tournament across multiple rounds.
    ///
    /// Each tuple in `rounds` is `(state_before, state_after)`.
    pub fn run_tournament(
        &mut self,
        rounds: &[(&SystemState, &SystemState)],
    ) -> TournamentResult {
        let mut policy_rounds: Vec<PolicyRound> = Vec::new();

        for (state_before, state_after) in rounds {
            let round = self.run_round(state_before, state_after);
            policy_rounds.push(round);
        }

        // Build summary
        let total_rounds = policy_rounds.len();

        // Count wins per policy
        let mut policy_wins: HashMap<&'static str, usize> = HashMap::new();
        for round in &policy_rounds {
            if let Some(winner) = round.round_winner {
                *policy_wins.entry(winner).or_insert(0) += 1;
            }
        }

        // Compute average scores per policy
        let mut score_sums: HashMap<&'static str, (f32, usize)> = HashMap::new();
        for result in &self.results {
            let entry = score_sums
                .entry(result.policy_name)
                .or_insert((0.0, 0));
            entry.0 += result.score.overall_score;
            entry.1 += 1;
        }

        let average_scores: HashMap<&'static str, f32> = score_sums
            .iter()
            .map(|(name, (sum, count))| {
                let avg = if *count > 0 {
                    *sum / *count as f32
                } else {
                    0.0
                };
                (*name, avg)
            })
            .collect();

        // Find best and worst overall scores
        let best_overall_score = self
            .results
            .iter()
            .map(|r| r.score.overall_score)
            .fold(0.0_f32, |a, b| a.max(b));

        let worst_overall_score = self
            .results
            .iter()
            .map(|r| r.score.overall_score)
            .fold(1.0_f32, |a, b| a.min(b));

        // If no results, worst should be 0.0
        let worst_overall_score = if self.results.is_empty() {
            0.0
        } else {
            worst_overall_score
        };

        // Determine overall winner (most round wins)
        let winner = policy_wins
            .iter()
            .max_by(|a, b| a.1.cmp(b.1))
            .map(|(name, _)| *name);

        TournamentResult {
            rounds: policy_rounds,
            winner,
            summary: TournamentSummary {
                total_rounds,
                policy_wins,
                average_scores,
                best_overall_score,
                worst_overall_score,
            },
        }
    }

    /// Return a leaderboard of policies sorted by average overall score
    /// (descending).
    pub fn leaderboard(&self) -> Vec<(&'static str, f32)> {
        let mut scores: HashMap<&'static str, (f32, usize)> = HashMap::new();

        for result in &self.results {
            let entry = scores
                .entry(result.policy_name)
                .or_insert((0.0, 0));
            entry.0 += result.score.overall_score;
            entry.1 += 1;
        }

        let mut leaderboard: Vec<(&'static str, f32)> = scores
            .into_iter()
            .map(|(name, (sum, count))| {
                let avg = if count > 0 { sum / count as f32 } else { 0.0 };
                (name, avg)
            })
            .collect();

        // Sort by average score descending
        leaderboard.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        leaderboard
    }
}

impl Default for PolicyArena {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Built-in Policy: LinuxBaselinePolicy (arena wrapper) ────────────────────

/// Wraps the existing `LinuxBaselinePolicy` to implement the `Policy` trait.
///
/// This converts `BaselineRecommendation` outputs into `Recommendation` values
/// for scoring compatibility.
#[derive(Debug, Clone, Copy, Default)]
pub struct ArenaLinuxBaselinePolicy;

impl Policy for ArenaLinuxBaselinePolicy {
    fn name(&self) -> &'static str {
        "LinuxBaseline"
    }

    fn evaluate(&self, state: &SystemState) -> Vec<Recommendation> {
        evaluate_baseline(state)
            .into_iter()
            .map(Recommendation::from)
            .collect()
    }
}

// ─── Built-in Policy: PressurePolicy ─────────────────────────────────────────

/// A pressure-only GhostPages policy.
///
/// Uses `SystemState::pressure_level()` for decisions:
/// - `Low` → `NoAction`
/// - `Medium` → light eviction if `dram_utilization > 0.75`
/// - `High` → `EvictCold` + `MoveToDiskSwap`
/// - `Critical` → aggressive `EvictCold` + `MoveToDiskSwap`
///
/// No hotness awareness, but ZRAM-aware (generates `MoveToZram` when
/// `zram_utilization < 0.7`).
#[derive(Debug, Clone, Copy, Default)]
pub struct PressurePolicy;

impl Policy for PressurePolicy {
    fn name(&self) -> &'static str {
        "Pressure"
    }

    fn evaluate(&self, state: &SystemState) -> Vec<Recommendation> {
        let pressure = state.pressure_level();

        match pressure {
            PressureLevel::Low => vec![Recommendation::NoAction {
                reason: "low pressure — system is idle".to_string(),
                confidence: 0.95,
                factors: vec!["low_pressure".to_string()],
            }],
            PressureLevel::Medium => {
                let mut recs = Vec::new();

                if state.dram_utilization > 0.75 {
                    recs.push(Recommendation::EvictCold {
                        tier: TierId::Ram,
                        count: 4,
                        confidence: 0.6,
                        factors: vec!["medium_pressure".to_string(), "high_dram".to_string()],
                    });
                }

                // ZRAM-aware: move cold data to ZRAM if there's room
                if let Some(zram_util) = state.zram_utilization {
                    if zram_util < 0.7 {
                        recs.push(Recommendation::MoveToZram {
                            chunk_id: ChunkId::from_data(b"pressure_cold"),
                            reason: "medium pressure — moving cold data to ZRAM".to_string(),
                            confidence: 0.5,
                            factors: vec!["medium_pressure".to_string(), "zram_available".to_string()],
                        });
                    }
                }

                if recs.is_empty() {
                    vec![Recommendation::NoAction {
                        reason: "medium pressure but utilization acceptable".to_string(),
                        confidence: 0.8,
                        factors: vec!["medium_pressure".to_string()],
                    }]
                } else {
                    recs
                }
            }
            PressureLevel::High => {
                let mut recs = vec![Recommendation::EvictCold {
                    tier: TierId::Ram,
                    count: 8,
                    confidence: 0.8,
                    factors: vec!["high_pressure".to_string()],
                }];

                recs.push(Recommendation::MoveToDiskSwap {
                    chunk_id: ChunkId::from_data(b"pressure_swap"),
                    reason: "high pressure — swapping out cold pages".to_string(),
                    confidence: 0.7,
                    factors: vec!["high_pressure".to_string()],
                });

                // ZRAM-aware under high pressure
                if let Some(zram_util) = state.zram_utilization {
                    if zram_util < 0.7 {
                        recs.push(Recommendation::MoveToZram {
                            chunk_id: ChunkId::from_data(b"pressure_to_zram"),
                            reason: "high pressure — compressing cold data to ZRAM".to_string(),
                            confidence: 0.6,
                            factors: vec!["high_pressure".to_string(), "zram_available".to_string()],
                        });
                    }
                }

                recs
            }
            PressureLevel::Critical => {
                let mut recs = vec![Recommendation::EvictCold {
                    tier: TierId::Ram,
                    count: 16,
                    confidence: 0.95,
                    factors: vec!["critical_pressure".to_string()],
                }];

                recs.push(Recommendation::MoveToDiskSwap {
                    chunk_id: ChunkId::from_data(b"critical_swap"),
                    reason: "critical pressure — emergency swap-out".to_string(),
                    confidence: 0.85,
                    factors: vec!["critical_pressure".to_string()],
                });

                // ZRAM-aware under critical pressure
                if let Some(zram_util) = state.zram_utilization {
                    if zram_util < 0.7 {
                        recs.push(Recommendation::MoveToZram {
                            chunk_id: ChunkId::from_data(b"critical_to_zram"),
                            reason: "critical pressure — emergency ZRAM compression".to_string(),
                            confidence: 0.75,
                            factors: vec!["critical_pressure".to_string(), "zram_available".to_string()],
                        });
                    }
                }

                recs
            }
        }
    }
}

// ─── Built-in Policy: HotnessPolicy ──────────────────────────────────────────

/// A hotness-aware GhostPages policy.
///
/// Uses `hotness_summary` when available:
/// - Hot chunks → `PromoteToDram`
/// - Cold chunks → `MoveToZram` or `MoveToDiskSwap`
///
/// Falls back to pressure-only when hotness is `None`.
#[derive(Debug, Clone, Copy, Default)]
pub struct HotnessPolicy;

impl Policy for HotnessPolicy {
    fn name(&self) -> &'static str {
        "Hotness"
    }

    fn evaluate(&self, state: &SystemState) -> Vec<Recommendation> {
        let pressure = state.pressure_level();

        // If we have hotness data, use it
        if let Some(ref hotness) = state.hotness_summary {
            let confidence = state
                .hotness_confidence
                .as_ref()
                .map(|c| c.score)
                .unwrap_or(0.5);

            let mut recs = Vec::new();

            // Promote hot chunks to DRAM (but only if pressure allows)
            if pressure == PressureLevel::Low || pressure == PressureLevel::Medium {
                if hotness.hot_count > 0 {
                    let promote_count = hotness.hot_count.min(8);
                    for i in 0..promote_count {
                        recs.push(Recommendation::PromoteToDram {
                            chunk_id: ChunkId::from_data(
                                format!("hot_chunk_{}", i).as_bytes(),
                            ),
                            reason: format!(
                                "hot chunk detected — promoting to DRAM (hot_count={})",
                                hotness.hot_count
                            ),
                            confidence,
                            factors: vec![
                                "hotness_hot".to_string(),
                                format!("hot_percentage={}", hotness.hot_percentage),
                            ],
                        });
                    }
                }
            }

            // Move cold chunks to ZRAM or swap
            if hotness.cold_count > 0 {
                let cold_action_count = hotness.cold_count.min(8);
                for i in 0..cold_action_count {
                    // Prefer ZRAM if available and not full
                    let use_zram = state
                        .zram_utilization
                        .map(|u| u < 0.7)
                        .unwrap_or(false);

                    if use_zram {
                        recs.push(Recommendation::MoveToZram {
                            chunk_id: ChunkId::from_data(
                                format!("cold_chunk_{}", i).as_bytes(),
                            ),
                            reason: format!(
                                "cold chunk — moving to ZRAM (cold_count={})",
                                hotness.cold_count
                            ),
                            confidence,
                            factors: vec![
                                "hotness_cold".to_string(),
                                "zram_preferred".to_string(),
                            ],
                        });
                    } else {
                        recs.push(Recommendation::MoveToDiskSwap {
                            chunk_id: ChunkId::from_data(
                                format!("cold_chunk_{}", i).as_bytes(),
                            ),
                            reason: format!(
                                "cold chunk — moving to swap (cold_count={})",
                                hotness.cold_count
                            ),
                            confidence,
                            factors: vec!["hotness_cold".to_string()],
                        });
                    }
                }
            }

            // Handle frozen chunks — always move to swap
            if hotness.frozen_count > 0 {
                let frozen_count = hotness.frozen_count.min(4);
                for i in 0..frozen_count {
                    recs.push(Recommendation::MoveToDiskSwap {
                        chunk_id: ChunkId::from_data(
                            format!("frozen_chunk_{}", i).as_bytes(),
                        ),
                        reason: format!(
                            "frozen chunk — moving to swap (frozen_count={})",
                            hotness.frozen_count
                        ),
                        confidence,
                        factors: vec!["hotness_frozen".to_string()],
                    });
                }
            }

            if recs.is_empty() {
                vec![Recommendation::NoAction {
                    reason: "hotness data available but no actionable chunks".to_string(),
                    confidence: 0.7,
                    factors: vec!["hotness_stable".to_string()],
                }]
            } else {
                recs
            }
        } else {
            // No hotness data — fall back to pressure-only
            fallback_pressure_only(state, pressure)
        }
    }
}

/// Fallback pressure-only logic for when hotness data is unavailable.
fn fallback_pressure_only(state: &SystemState, pressure: PressureLevel) -> Vec<Recommendation> {
    match pressure {
        PressureLevel::Low => vec![Recommendation::NoAction {
            reason: "low pressure, no hotness data — no action".to_string(),
            confidence: 0.9,
            factors: vec!["low_pressure".to_string(), "no_hotness".to_string()],
        }],
        PressureLevel::Medium => {
            if state.dram_utilization > 0.8 {
                vec![Recommendation::EvictCold {
                    tier: TierId::Ram,
                    count: 4,
                    confidence: 0.6,
                    factors: vec!["medium_pressure".to_string(), "no_hotness".to_string()],
                }]
            } else {
                vec![Recommendation::NoAction {
                    reason: "medium pressure, no hotness data — monitoring".to_string(),
                    confidence: 0.7,
                    factors: vec!["medium_pressure".to_string(), "no_hotness".to_string()],
                }]
            }
        }
        PressureLevel::High => vec![
            Recommendation::EvictCold {
                tier: TierId::Ram,
                count: 8,
                confidence: 0.8,
                factors: vec!["high_pressure".to_string(), "no_hotness".to_string()],
            },
            Recommendation::MoveToDiskSwap {
                chunk_id: ChunkId::from_data(b"fallback_swap"),
                reason: "high pressure fallback — swapping out".to_string(),
                confidence: 0.7,
                factors: vec!["high_pressure".to_string(), "no_hotness".to_string()],
            },
        ],
        PressureLevel::Critical => vec![
            Recommendation::EvictCold {
                tier: TierId::Ram,
                count: 16,
                confidence: 0.95,
                factors: vec!["critical_pressure".to_string(), "no_hotness".to_string()],
            },
            Recommendation::MoveToDiskSwap {
                chunk_id: ChunkId::from_data(b"fallback_critical_swap"),
                reason: "critical pressure fallback — emergency swap".to_string(),
                confidence: 0.85,
                factors: vec!["critical_pressure".to_string(), "no_hotness".to_string()],
            },
        ],
    }
}

// ─── Built-in Policy: HybridPolicy ────────────────────────────────────────────

/// A combined pressure + hotness policy.
///
/// Uses both pressure and hotness signals with weighted decision-making:
/// - 60% pressure weight, 40% hotness weight
///
/// This is the most sophisticated built-in policy.
#[derive(Debug, Clone, Copy, Default)]
pub struct HybridPolicy;

impl Policy for HybridPolicy {
    fn name(&self) -> &'static str {
        "Hybrid"
    }

    fn evaluate(&self, state: &SystemState) -> Vec<Recommendation> {
        let pressure = state.pressure_level();
        let pressure_weight = 0.6_f32;
        let hotness_weight = 0.4_f32;

        let mut recs = Vec::new();

        // ── Pressure component (60%) ──
        match pressure {
            PressureLevel::Low => {
                // Low pressure: only act if hotness shows something interesting
                if let Some(ref hotness) = state.hotness_summary {
                    if hotness.hot_count > 0 && state.dram_utilization < 0.7 {
                        // Promote a few hot chunks
                        let count = hotness.hot_count.min(4);
                        for i in 0..count {
                            recs.push(Recommendation::PromoteToDram {
                                chunk_id: ChunkId::from_data(
                                    format!("hybrid_hot_{}", i).as_bytes(),
                                ),
                                reason: format!(
                                    "hybrid: low pressure but hot chunk detected (hot_count={})",
                                    hotness.hot_count
                                ),
                                confidence: 0.7 * hotness_weight,
                                factors: vec![
                                    "low_pressure".to_string(),
                                    "hotness_hot".to_string(),
                                    "hybrid_decision".to_string(),
                                ],
                            });
                        }
                    }
                }

                if recs.is_empty() {
                    recs.push(Recommendation::NoAction {
                        reason: "hybrid: low pressure, system stable".to_string(),
                        confidence: 0.95,
                        factors: vec!["low_pressure".to_string(), "hybrid_stable".to_string()],
                    });
                }
            }
            PressureLevel::Medium => {
                // Medium pressure: balanced approach
                if state.dram_utilization > 0.75 {
                    recs.push(Recommendation::EvictCold {
                        tier: TierId::Ram,
                        count: 4,
                        confidence: 0.6 * pressure_weight,
                        factors: vec![
                            "medium_pressure".to_string(),
                            "high_dram".to_string(),
                            "hybrid_decision".to_string(),
                        ],
                    });
                }

                // Hotness-informed decisions
                if let Some(ref hotness) = state.hotness_summary {
                    if hotness.hot_count > 0 && state.dram_utilization < 0.6 {
                        recs.push(Recommendation::PromoteToDram {
                            chunk_id: ChunkId::from_data(b"hybrid_promote"),
                            reason: "hybrid: promoting hot chunk under medium pressure"
                                .to_string(),
                            confidence: 0.6 * hotness_weight,
                            factors: vec![
                                "medium_pressure".to_string(),
                                "hotness_hot".to_string(),
                                "hybrid_decision".to_string(),
                            ],
                        });
                    }

                    if hotness.cold_count > 0 {
                        let use_zram = state
                            .zram_utilization
                            .map(|u| u < 0.7)
                            .unwrap_or(false);
                        if use_zram {
                            recs.push(Recommendation::MoveToZram {
                                chunk_id: ChunkId::from_data(b"hybrid_cold_zram"),
                                reason: "hybrid: moving cold chunk to ZRAM".to_string(),
                                confidence: 0.5 * hotness_weight,
                                factors: vec![
                                    "medium_pressure".to_string(),
                                    "hotness_cold".to_string(),
                                    "hybrid_decision".to_string(),
                                ],
                            });
                        }
                    }
                }

                if recs.is_empty() {
                    recs.push(Recommendation::NoAction {
                        reason: "hybrid: medium pressure, no actionable signals".to_string(),
                        confidence: 0.75,
                        factors: vec!["medium_pressure".to_string(), "hybrid_monitoring".to_string()],
                    });
                }
            }
            PressureLevel::High => {
                // High pressure: aggressive action
                recs.push(Recommendation::EvictCold {
                    tier: TierId::Ram,
                    count: 8,
                    confidence: 0.8 * pressure_weight,
                    factors: vec![
                        "high_pressure".to_string(),
                        "hybrid_decision".to_string(),
                    ],
                });

                recs.push(Recommendation::MoveToDiskSwap {
                    chunk_id: ChunkId::from_data(b"hybrid_swap"),
                    reason: "hybrid: high pressure — swapping out cold pages".to_string(),
                    confidence: 0.7 * pressure_weight,
                    factors: vec![
                        "high_pressure".to_string(),
                        "hybrid_decision".to_string(),
                    ],
                });

                // Hotness can modulate: if lots of hot data, also promote
                if let Some(ref hotness) = state.hotness_summary {
                    if hotness.hot_percentage > 10.0 && state.dram_utilization < 0.85 {
                        recs.push(Recommendation::PromoteToDram {
                            chunk_id: ChunkId::from_data(b"hybrid_promote_hot"),
                            reason: format!(
                                "hybrid: high pressure but {:.1}% hot — selective promotion",
                                hotness.hot_percentage
                            ),
                            confidence: 0.5 * hotness_weight,
                            factors: vec![
                                "high_pressure".to_string(),
                                "hotness_hot".to_string(),
                                "hybrid_selective".to_string(),
                            ],
                        });
                    }
                }

                // ZRAM-aware
                if let Some(zram_util) = state.zram_utilization {
                    if zram_util < 0.7 {
                        recs.push(Recommendation::MoveToZram {
                            chunk_id: ChunkId::from_data(b"hybrid_zram"),
                            reason: "hybrid: high pressure — compressing to ZRAM".to_string(),
                            confidence: 0.6 * pressure_weight,
                            factors: vec![
                                "high_pressure".to_string(),
                                "zram_available".to_string(),
                                "hybrid_decision".to_string(),
                            ],
                        });
                    }
                }
            }
            PressureLevel::Critical => {
                // Critical pressure: maximum aggression
                recs.push(Recommendation::EvictCold {
                    tier: TierId::Ram,
                    count: 16,
                    confidence: 0.95,
                    factors: vec![
                        "critical_pressure".to_string(),
                        "hybrid_decision".to_string(),
                    ],
                });

                recs.push(Recommendation::MoveToDiskSwap {
                    chunk_id: ChunkId::from_data(b"hybrid_critical_swap"),
                    reason: "hybrid: critical pressure — emergency swap".to_string(),
                    confidence: 0.85,
                    factors: vec![
                        "critical_pressure".to_string(),
                        "hybrid_decision".to_string(),
                    ],
                });

                // Even under critical pressure, ZRAM can help
                if let Some(zram_util) = state.zram_utilization {
                    if zram_util < 0.7 {
                        recs.push(Recommendation::MoveToZram {
                            chunk_id: ChunkId::from_data(b"hybrid_critical_zram"),
                            reason: "hybrid: critical pressure — emergency ZRAM".to_string(),
                            confidence: 0.75,
                            factors: vec![
                                "critical_pressure".to_string(),
                                "zram_available".to_string(),
                                "hybrid_decision".to_string(),
                            ],
                        });
                    }
                }

                // Hotness: demote warm chunks to make room
                if let Some(ref hotness) = state.hotness_summary {
                    if hotness.warm_count > 0 {
                        recs.push(Recommendation::DemoteHot {
                            tier: TierId::Ram,
                            target: TierId::Disk,
                            confidence: 0.6 * hotness_weight,
                            factors: vec![
                                "critical_pressure".to_string(),
                                "hotness_warm".to_string(),
                                "hybrid_decision".to_string(),
                            ],
                        });
                    }
                }
            }
        }

        recs
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::hotness_confidence::HotnessConfidence;
    use ghost_core::hotness_summary::HotnessSummary;
    use ghost_core::state::PressureState;

    // ── Helper functions ──

    fn idle_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState::default(),
            dram_utilization: 0.3,
            swap_utilization: 0.1,
            zram_utilization: Some(0.2),
            io_pressure: PressureState::default(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn medium_pressure_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.55,
                ..Default::default()
            },
            dram_utilization: 0.7,
            swap_utilization: 0.2,
            zram_utilization: Some(0.3),
            io_pressure: PressureState::default(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn high_pressure_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.8,
                ..Default::default()
            },
            dram_utilization: 0.85,
            swap_utilization: 0.3,
            zram_utilization: Some(0.4),
            io_pressure: PressureState::default(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn critical_pressure_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.95,
                ..Default::default()
            },
            dram_utilization: 0.97,
            swap_utilization: 0.5,
            zram_utilization: Some(0.6),
            io_pressure: PressureState::default(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn improved_from_high() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.4,
                ..Default::default()
            },
            dram_utilization: 0.5,
            swap_utilization: 0.15,
            zram_utilization: Some(0.5),
            io_pressure: PressureState::default(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn state_with_hotness(base: SystemState) -> SystemState {
        SystemState {
            hotness_summary: Some(HotnessSummary {
                hot_count: 10,
                warm_count: 20,
                cold_count: 30,
                frozen_count: 5,
                total_regions: 65,
                hot_percentage: 15.4,
                warm_percentage: 30.8,
                cold_percentage: 46.2,
                frozen_percentage: 7.7,
                avg_access_count: 100,
                max_access_count: 1000,
                min_access_count: 0,
            }),
            hotness_confidence: Some(HotnessConfidence {
                score: 0.9,
                factors: vec![],
            }),
            ..base
        }
    }

    // ── Tests ──

    #[test]
    fn test_arena_add_policies() {
        let mut arena = PolicyArena::new();
        arena
            .add_policy(Box::new(ArenaLinuxBaselinePolicy))
            .add_policy(Box::new(PressurePolicy))
            .add_policy(Box::new(HotnessPolicy))
            .add_policy(Box::new(HybridPolicy));

        // Run a round to verify all 4 policies are registered
        let before = high_pressure_state();
        let after = improved_from_high();
        let round = arena.run_round(&before, &after);

        assert_eq!(round.results.len(), 4);
        assert_eq!(round.results[0].policy_name, "LinuxBaseline");
        assert_eq!(round.results[1].policy_name, "Pressure");
        assert_eq!(round.results[2].policy_name, "Hotness");
        assert_eq!(round.results[3].policy_name, "Hybrid");
    }

    #[test]
    fn test_arena_round_deterministic() {
        let before = high_pressure_state();
        let after = improved_from_high();

        // Run 1
        let mut arena1 = PolicyArena::new();
        arena1
            .add_policy(Box::new(ArenaLinuxBaselinePolicy))
            .add_policy(Box::new(PressurePolicy));
        let round1 = arena1.run_round(&before, &after);

        // Run 2 with same inputs
        let mut arena2 = PolicyArena::new();
        arena2
            .add_policy(Box::new(ArenaLinuxBaselinePolicy))
            .add_policy(Box::new(PressurePolicy));
        let round2 = arena2.run_round(&before, &after);

        // Results should be identical
        assert_eq!(round1.results.len(), round2.results.len());
        for (r1, r2) in round1.results.iter().zip(round2.results.iter()) {
            assert_eq!(r1.policy_name, r2.policy_name);
            assert_eq!(r1.score.overall_score, r2.score.overall_score);
            assert_eq!(r1.recommendations.len(), r2.recommendations.len());
        }
        assert_eq!(round1.round_winner, round2.round_winner);
    }

    #[test]
    fn test_arena_leaderboard_order() {
        let before = critical_pressure_state();
        let after = improved_from_high();

        let mut arena = PolicyArena::new();
        arena
            .add_policy(Box::new(ArenaLinuxBaselinePolicy))
            .add_policy(Box::new(PressurePolicy))
            .add_policy(Box::new(HybridPolicy));

        // Run multiple rounds to accumulate scores
        arena.run_round(&before, &after);
        arena.run_round(&high_pressure_state(), &idle_state());

        let leaderboard = arena.leaderboard();

        // Leaderboard should be sorted by average score descending
        for i in 1..leaderboard.len() {
            assert!(
                leaderboard[i - 1].1 >= leaderboard[i].1,
                "leaderboard should be sorted descending: {:?}",
                leaderboard
            );
        }

        // All policies should appear
        assert_eq!(leaderboard.len(), 3);
    }

    #[test]
    fn test_tournament_multiple_rounds() {
        let rounds = vec![
            (high_pressure_state(), improved_from_high()),
            (critical_pressure_state(), idle_state()),
            (medium_pressure_state(), idle_state()),
        ];

        let mut arena = PolicyArena::new();
        arena
            .add_policy(Box::new(ArenaLinuxBaselinePolicy))
            .add_policy(Box::new(PressurePolicy))
            .add_policy(Box::new(HybridPolicy));

        let result = arena.run_tournament(
            &rounds
                .iter()
                .map(|(b, a)| (b, a))
                .collect::<Vec<_>>(),
        );

        assert_eq!(result.rounds.len(), 3);
        assert_eq!(result.summary.total_rounds, 3);

        // Total wins should equal number of rounds (each round has a winner)
        let total_wins: usize = result.summary.policy_wins.values().sum();
        assert_eq!(total_wins, 3);

        // Average scores should be present for all policies
        assert_eq!(result.summary.average_scores.len(), 3);
    }

    #[test]
    fn test_linux_baseline_in_arena() {
        let mut arena = PolicyArena::new();
        arena.add_policy(Box::new(ArenaLinuxBaselinePolicy));

        let before = high_pressure_state();
        let after = improved_from_high();

        let round = arena.run_round(&before, &after);

        assert_eq!(round.results.len(), 1);
        assert_eq!(round.results[0].policy_name, "LinuxBaseline");

        // Should have produced recommendations
        assert!(!round.results[0].recommendations.is_empty());

        // Score should be valid
        assert!(round.results[0].score.overall_score >= 0.0);
        assert!(round.results[0].score.overall_score <= 1.0);
    }

    #[test]
    fn test_pressure_policy_in_arena() {
        let mut arena = PolicyArena::new();
        arena.add_policy(Box::new(PressurePolicy));

        let before = high_pressure_state();
        let after = improved_from_high();

        let round = arena.run_round(&before, &after);

        assert_eq!(round.results.len(), 1);
        assert_eq!(round.results[0].policy_name, "Pressure");

        // High pressure should produce multiple recommendations
        assert!(
            round.results[0].recommendations.len() >= 2,
            "high pressure should produce eviction + swap"
        );

        // Score should be valid
        assert!(round.results[0].score.overall_score >= 0.0);
        assert!(round.results[0].score.overall_score <= 1.0);
    }

    #[test]
    fn test_hotness_policy_in_arena() {
        let mut arena = PolicyArena::new();
        arena.add_policy(Box::new(HotnessPolicy));

        // Use state with hotness data
        let before = state_with_hotness(idle_state());
        let after = improved_from_high();

        let round = arena.run_round(&before, &after);

        assert_eq!(round.results.len(), 1);
        assert_eq!(round.results[0].policy_name, "Hotness");

        // Should have produced recommendations based on hotness
        assert!(!round.results[0].recommendations.is_empty());

        // Score should be valid
        assert!(round.results[0].score.overall_score >= 0.0);
        assert!(round.results[0].score.overall_score <= 1.0);
    }

    #[test]
    fn test_hybrid_policy_in_arena() {
        let mut arena = PolicyArena::new();
        arena.add_policy(Box::new(HybridPolicy));

        let before = high_pressure_state();
        let after = improved_from_high();

        let round = arena.run_round(&before, &after);

        assert_eq!(round.results.len(), 1);
        assert_eq!(round.results[0].policy_name, "Hybrid");

        // High pressure should produce recommendations
        assert!(!round.results[0].recommendations.is_empty());

        // Score should be valid
        assert!(round.results[0].score.overall_score >= 0.0);
        assert!(round.results[0].score.overall_score <= 1.0);
    }

    #[test]
    fn test_tournament_winner_exists() {
        // Use varied states so different policies may win different rounds
        let rounds = vec![
            (critical_pressure_state(), idle_state()),
            (high_pressure_state(), improved_from_high()),
            (medium_pressure_state(), idle_state()),
            (idle_state(), idle_state()),
        ];

        let mut arena = PolicyArena::new();
        arena
            .add_policy(Box::new(ArenaLinuxBaselinePolicy))
            .add_policy(Box::new(PressurePolicy))
            .add_policy(Box::new(HotnessPolicy))
            .add_policy(Box::new(HybridPolicy));

        let result = arena.run_tournament(
            &rounds
                .iter()
                .map(|(b, a)| (b, a))
                .collect::<Vec<_>>(),
        );

        // With varied states, there should be a winner
        assert!(
            result.winner.is_some(),
            "tournament with varied states should have a winner"
        );

        // Winner should be one of the registered policies
        let winner = result.winner.unwrap();
        assert!(
            winner == "LinuxBaseline"
                || winner == "Pressure"
                || winner == "Hotness"
                || winner == "Hybrid",
            "winner should be a registered policy, got {}",
            winner
        );
    }

    #[test]
    fn test_empty_tournament() {
        let rounds: Vec<(&SystemState, &SystemState)> = vec![];

        let mut arena = PolicyArena::new();
        arena
            .add_policy(Box::new(ArenaLinuxBaselinePolicy))
            .add_policy(Box::new(PressurePolicy));

        let result = arena.run_tournament(&rounds);

        assert_eq!(result.rounds.len(), 0);
        assert_eq!(result.summary.total_rounds, 0);
        assert!(result.winner.is_none());
        assert!(result.summary.policy_wins.is_empty());
        assert!(result.summary.average_scores.is_empty());
        assert_eq!(result.summary.best_overall_score, 0.0);
        assert_eq!(result.summary.worst_overall_score, 0.0);
    }
}
