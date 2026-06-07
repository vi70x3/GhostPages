//! Recommendation scoring model for GhostPages.
//!
//! All scoring functions are **pure** — no I/O, no mutation, no side effects.
//! Same inputs always produce same outputs. Deterministic by design.

use ghost_core::state::PressureState;
use ghost_linux::policy::Recommendation;
use ghost_linux::policy_rules::SystemState;

// ─── Recommendation Score ─────────────────────────────────────────────────────

/// A multi-dimensional score for a recommendation or policy evaluation.
///
/// Each metric ranges from 0.0 (worst) to 1.0 (best). The `overall_score`
/// is a weighted combination of all metrics.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecommendationScore {
    /// Reduction in page fault pressure (0.0 = no reduction, 1.0 = full reduction).
    pub fault_reduction: f32,
    /// Reduction in swap utilization (0.0 = no reduction, 1.0 = full reduction).
    pub swap_reduction: f32,
    /// Improvement in ZRAM compression ratio (0.0 = no improvement, 1.0 = best).
    pub zram_efficiency: f32,
    /// Reduction in memory pressure (0.0 = no reduction, 1.0 = full reduction).
    pub pressure_reduction: f32,
    /// How well balanced tiers are (0.0 = unbalanced, 1.0 = perfectly balanced).
    pub tier_balance: f32,
    /// Whether the recommendation stabilizes the system (0.0 = unstable, 1.0 = stable).
    pub stability: f32,
    /// Weighted combination of all metrics (0.0 = worst, 1.0 = best).
    pub overall_score: f32,
}

// ─── Scoring Weights ──────────────────────────────────────────────────────────

/// Weights for combining individual metrics into `overall_score`.
///
/// All weights should be non-negative. They are normalized during scoring
/// so they don't need to sum to 1.0, but the default set does.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoringWeights {
    /// Weight for fault reduction.
    pub fault_reduction_weight: f32,
    /// Weight for swap reduction.
    pub swap_reduction_weight: f32,
    /// Weight for ZRAM efficiency.
    pub zram_efficiency_weight: f32,
    /// Weight for pressure reduction.
    pub pressure_reduction_weight: f32,
    /// Weight for tier balance.
    pub tier_balance_weight: f32,
    /// Weight for stability.
    pub stability_weight: f32,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            fault_reduction_weight: 0.25,
            swap_reduction_weight: 0.20,
            zram_efficiency_weight: 0.15,
            pressure_reduction_weight: 0.20,
            tier_balance_weight: 0.10,
            stability_weight: 0.10,
        }
    }
}

// ─── Individual Scoring Functions ─────────────────────────────────────────────

/// Score fault reduction by comparing DRAM pressure before and after.
///
/// Page fault pressure is approximated by `dram_pressure.memory_pressure`
/// and `dram_pressure.io_pressure`. Lower pressure after = higher score.
///
/// Returns 0.0 if pressure increased or stayed the same, up to 1.0 for
/// complete elimination of pressure.
pub fn score_fault_reduction(before: &SystemState, after: &SystemState) -> f32 {
    let before_pressure = combined_pressure(&before.dram_pressure);
    let after_pressure = combined_pressure(&after.dram_pressure);

    if after_pressure >= before_pressure {
        // No improvement — if both are zero, return 1.0 (nothing to improve)
        if before_pressure == 0.0 {
            return 1.0;
        }
        return 0.0;
    }

    let reduction = before_pressure - after_pressure;
    // Normalize: a reduction of 1.0 (full elimination) = score 1.0
    reduction.clamp(0.0, 1.0)
}

/// Score swap reduction by comparing swap utilization before and after.
///
/// Lower swap utilization after = higher score.
pub fn score_swap_reduction(before: &SystemState, after: &SystemState) -> f32 {
    if after.swap_utilization >= before.swap_utilization {
        if before.swap_utilization == 0.0 {
            return 1.0;
        }
        return 0.0;
    }

    let reduction = before.swap_utilization - after.swap_utilization;
    reduction.clamp(0.0, 1.0)
}

/// Score ZRAM efficiency improvement.
///
/// ZRAM efficiency is measured by how well utilized the ZRAM tier is
/// without being overfull. A moderate utilization (0.3-0.7) is ideal.
/// Moving from overfull (>0.8) to ideal range scores highest.
pub fn score_zram_efficiency(before: &SystemState, after: &SystemState) -> f32 {
    let before_util = before.zram_utilization.unwrap_or(0.0);
    let after_util = after.zram_utilization.unwrap_or(0.0);

    // Ideal ZRAM utilization is in the 0.3-0.7 range
    let ideal_low = 0.3_f32;
    let ideal_high = 0.7_f32;

    let before_score = zram_utilization_score(before_util, ideal_low, ideal_high);
    let after_score = zram_utilization_score(after_util, ideal_low, ideal_high);

    if after_score > before_score {
        after_score
    } else if (after_score - before_score).abs() < f32::EPSILON {
        // No change — return current state score
        after_score
    } else {
        // Worse — penalize slightly
        after_score * 0.5
    }
}

/// Score pressure reduction across all dimensions.
///
/// Combines DRAM memory pressure, DRAM I/O pressure, and overall I/O pressure
/// into a single reduction score.
pub fn score_pressure_reduction(before: &SystemState, after: &SystemState) -> f32 {
    let before_max = max_pressure(before);
    let after_max = max_pressure(after);

    if after_max >= before_max {
        if before_max == 0.0 {
            return 1.0;
        }
        return 0.0;
    }

    let reduction = before_max - after_max;
    reduction.clamp(0.0, 1.0)
}

/// Score tier balance by comparing utilization variance across tiers.
///
/// Lower standard deviation of tier utilizations = better balance = higher score.
/// Tiers considered: DRAM, swap, ZRAM (if available).
pub fn score_tier_balance(before: &SystemState, after: &SystemState) -> f32 {
    let before_balance = compute_balance(before);
    let after_balance = compute_balance(after);

    // If balance improved or stayed the same, use after balance
    if after_balance >= before_balance {
        after_balance
    } else {
        // Penalize degradation but don't go below 0
        after_balance * 0.5
    }
}

/// Score stability — whether the recommendation stabilizes the system.
///
/// A stable system has:
/// - Low pressure across all dimensions
/// - No critical pressure levels
/// - Moderate (not extreme) utilization across tiers
pub fn score_stability(before: &SystemState, after: &SystemState) -> f32 {
    let before_stability = system_stability(before);
    let after_stability = system_stability(after);

    if after_stability >= before_stability {
        after_stability
    } else {
        after_stability * 0.5
    }
}

// ─── Composite Scoring ────────────────────────────────────────────────────────

/// Score a single recommendation by comparing before/after system state.
///
/// This is the primary scoring entry point. It computes all individual
/// metrics and combines them using the provided weights.
pub fn score_recommendation(
    recommendation: &Recommendation,
    before: &SystemState,
    after: &SystemState,
    weights: &ScoringWeights,
) -> RecommendationScore {
    let fault_reduction = score_fault_reduction(before, after);
    let swap_reduction = score_swap_reduction(before, after);
    let zram_efficiency = score_zram_efficiency(before, after);
    let pressure_reduction = score_pressure_reduction(before, after);
    let tier_balance = score_tier_balance(before, after);
    let stability = score_stability(before, after);

    // Apply recommendation-type bonus
    let type_bonus = recommendation_type_bonus(recommendation, before, after);

    // Weighted combination
    let total_weight = weights.fault_reduction_weight
        + weights.swap_reduction_weight
        + weights.zram_efficiency_weight
        + weights.pressure_reduction_weight
        + weights.tier_balance_weight
        + weights.stability_weight;

    let overall_score = if total_weight > 0.0 {
        (fault_reduction * weights.fault_reduction_weight
            + swap_reduction * weights.swap_reduction_weight
            + zram_efficiency * weights.zram_efficiency_weight
            + pressure_reduction * weights.pressure_reduction_weight
            + tier_balance * weights.tier_balance_weight
            + stability * weights.stability_weight)
            / total_weight
    } else {
        0.0
    };

    // Apply type bonus (small multiplier, capped at 1.0)
    let overall_score = (overall_score * (1.0 + type_bonus)).min(1.0);

    RecommendationScore {
        fault_reduction,
        swap_reduction,
        zram_efficiency,
        pressure_reduction,
        tier_balance,
        stability,
        overall_score,
    }
}

/// Score a full policy evaluation (set of recommendations) against state change.
///
/// Uses default weights. Computes the aggregate score across all recommendations.
pub fn score_policy_evaluation(
    recommendations: &[Recommendation],
    before: &SystemState,
    after: &SystemState,
) -> RecommendationScore {
    let weights = ScoringWeights::default();

    if recommendations.is_empty() {
        // No recommendations — score based on state change alone
        return score_recommendation(
            &Recommendation::NoAction {
                reason: "no recommendations".to_string(),
                confidence: 1.0,
                factors: vec![],
            },
            before,
            after,
            &weights,
        );
    }

    // Score each recommendation and average
    let mut total_fault_reduction = 0.0_f32;
    let mut total_swap_reduction = 0.0_f32;
    let mut total_zram_efficiency = 0.0_f32;
    let mut total_pressure_reduction = 0.0_f32;
    let mut total_tier_balance = 0.0_f32;
    let mut total_stability = 0.0_f32;
    let mut total_overall = 0.0_f32;

    for rec in recommendations {
        let score = score_recommendation(rec, before, after, &weights);
        total_fault_reduction += score.fault_reduction;
        total_swap_reduction += score.swap_reduction;
        total_zram_efficiency += score.zram_efficiency;
        total_pressure_reduction += score.pressure_reduction;
        total_tier_balance += score.tier_balance;
        total_stability += score.stability;
        total_overall += score.overall_score;
    }

    let n = recommendations.len() as f32;

    RecommendationScore {
        fault_reduction: total_fault_reduction / n,
        swap_reduction: total_swap_reduction / n,
        zram_efficiency: total_zram_efficiency / n,
        pressure_reduction: total_pressure_reduction / n,
        tier_balance: total_tier_balance / n,
        stability: total_stability / n,
        overall_score: total_overall / n,
    }
}

// ─── Helper Functions ─────────────────────────────────────────────────────────

/// Combined pressure from memory and I/O dimensions.
fn combined_pressure(state: &PressureState) -> f32 {
    state.memory_pressure.max(state.io_pressure)
}

/// Maximum pressure across all dimensions and tiers.
fn max_pressure(state: &SystemState) -> f32 {
    let dram_max = state.dram_pressure.memory_pressure.max(state.dram_pressure.io_pressure);
    let io_max = state.io_pressure.memory_pressure.max(state.io_pressure.io_pressure);
    dram_max.max(io_max)
}

/// Score a ZRAM utilization value based on how close it is to the ideal range.
fn zram_utilization_score(util: f32, ideal_low: f32, ideal_high: f32) -> f32 {
    if util >= ideal_low && util <= ideal_high {
        // In ideal range — perfect score
        1.0
    } else if util < ideal_low {
        // Below ideal — linear scale from 0.0 at util=0 to 1.0 at ideal_low
        if ideal_low > 0.0 {
            (util / ideal_low).clamp(0.0, 1.0)
        } else {
            1.0
        }
    } else {
        // Above ideal — penalize overfull
        // At util=1.0, score should be ~0.3
        let overfill = util - ideal_high;
        let max_overfill = 1.0 - ideal_high;
        if max_overfill > 0.0 {
            (1.0 - (overfill / max_overfill) * 0.7).clamp(0.0, 1.0)
        } else {
            1.0
        }
    }
}

/// Compute a balance score from tier utilizations.
/// Returns 1.0 for perfectly balanced, 0.0 for maximally unbalanced.
fn compute_balance(state: &SystemState) -> f32 {
    let mut utilizations = vec![state.dram_utilization, state.swap_utilization];
    if let Some(zram) = state.zram_utilization {
        utilizations.push(zram);
    }

    if utilizations.is_empty() {
        return 1.0;
    }

    let mean = utilizations.iter().sum::<f32>() / utilizations.len() as f32;

    if mean == 0.0 {
        return 1.0;
    }

    let variance = utilizations
        .iter()
        .map(|&u| {
            let diff = u - mean;
            diff * diff
        })
        .sum::<f32>()
        / utilizations.len() as f32;

    let std_dev = variance.sqrt();

    // Normalize: std_dev of 0 = score 1.0, std_dev of 0.5+ = score ~0.0
    (1.0 - (std_dev / 0.5).min(1.0)).max(0.0)
}

/// Compute system stability score based on pressure and utilization.
fn system_stability(state: &SystemState) -> f32 {
    let max_p = max_pressure(state);

    // Penalize high pressure heavily
    let pressure_score = if max_p >= 0.9 {
        0.0
    } else if max_p >= 0.7 {
        0.3
    } else if max_p >= 0.5 {
        0.6
    } else if max_p >= 0.3 {
        0.8
    } else {
        1.0
    };

    // Penalize extreme utilizations
    let util_penalty = if state.dram_utilization > 0.95 || state.swap_utilization > 0.95 {
        0.3
    } else if state.dram_utilization > 0.85 || state.swap_utilization > 0.85 {
        0.6
    } else {
        1.0
    };

    pressure_score * util_penalty
}

/// Compute a small bonus multiplier based on recommendation type and context.
///
/// Recommendations that are appropriate for the current system state
/// receive a small bonus. This is a pure heuristic.
fn recommendation_type_bonus(
    recommendation: &Recommendation,
    before: &SystemState,
    _after: &SystemState,
) -> f32 {
    match recommendation {
        Recommendation::PromoteToDram { .. } => {
            // Promoting to DRAM is good when DRAM pressure is low-moderate
            let pressure = before.dram_pressure.memory_pressure;
            if pressure < 0.5 {
                0.05
            } else if pressure < 0.7 {
                0.02
            } else {
                // Promoting under high pressure is risky
                -0.02
            }
        }
        Recommendation::MoveToZram { .. } => {
            // Good when ZRAM is available and not full
            if let Some(zram_util) = before.zram_utilization {
                if zram_util < 0.7 {
                    0.05
                } else {
                    -0.02
                }
            } else {
                -0.05
            }
        }
        Recommendation::MoveToDiskSwap { .. } => {
            // Good when swap is available and not full
            if before.swap_utilization < 0.7 {
                0.04
            } else {
                -0.02
            }
        }
        Recommendation::EvictCold { .. } => {
            // Good under high pressure
            let pressure = before.dram_pressure.memory_pressure;
            if pressure >= 0.8 {
                0.08
            } else if pressure >= 0.6 {
                0.03
            } else {
                // Evicting when pressure is low is unnecessary
                -0.03
            }
        }
        Recommendation::DemoteHot { .. } => {
            // Good under medium pressure
            let pressure = before.dram_pressure.memory_pressure;
            if pressure >= 0.5 && pressure < 0.8 {
                0.04
            } else {
                0.01
            }
        }
        Recommendation::NoAction { .. } => {
            // Good when system is stable
            let pressure = before.dram_pressure.memory_pressure;
            if pressure < 0.5 && before.swap_utilization < 0.5 {
                0.05
            } else {
                // No action under high pressure is bad
                -0.05
            }
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::ChunkId;

    // ── Helper functions ──

    fn idle_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState::new(),
            dram_utilization: 0.3,
            swap_utilization: 0.1,
            zram_utilization: Some(0.2),
            io_pressure: PressureState::new(),
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
            io_pressure: PressureState::new(),
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
            io_pressure: PressureState::new(),
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
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    // ── Required tests ──

    #[test]
    fn test_scoring_weights_default() {
        let weights = ScoringWeights::default();
        let sum = weights.fault_reduction_weight
            + weights.swap_reduction_weight
            + weights.zram_efficiency_weight
            + weights.pressure_reduction_weight
            + weights.tier_balance_weight
            + weights.stability_weight;
        assert!(
            (sum - 1.0).abs() < 1e-6,
            "weights should sum to 1.0, got {}",
            sum
        );
    }

    #[test]
    fn test_score_recommendation_deterministic() {
        let rec = Recommendation::NoAction {
            reason: "test".to_string(),
            confidence: 1.0,
            factors: vec![],
        };
        let before = high_pressure_state();
        let after = improved_from_high();
        let weights = ScoringWeights::default();

        let score1 = score_recommendation(&rec, &before, &after, &weights);
        let score2 = score_recommendation(&rec, &before, &after, &weights);

        assert_eq!(score1.fault_reduction, score2.fault_reduction);
        assert_eq!(score1.swap_reduction, score2.swap_reduction);
        assert_eq!(score1.zram_efficiency, score2.zram_efficiency);
        assert_eq!(score1.pressure_reduction, score2.pressure_reduction);
        assert_eq!(score1.tier_balance, score2.tier_balance);
        assert_eq!(score1.stability, score2.stability);
        assert_eq!(score1.overall_score, score2.overall_score);
    }

    #[test]
    fn test_score_fault_reduction_improvement() {
        let before = high_pressure_state();
        let after = improved_from_high();

        let score = score_fault_reduction(&before, &after);
        assert!(
            score > 0.3,
            "lower pressure after should give positive fault reduction score, got {}",
            score
        );

        // No improvement should give 0
        let no_improve = score_fault_reduction(&before, &before);
        assert_eq!(no_improve, 0.0, "no improvement should give 0");
    }

    #[test]
    fn test_score_swap_reduction_improvement() {
        let before = high_pressure_state();
        let after = improved_from_high();

        let score = score_swap_reduction(&before, &after);
        assert!(
            score > 0.0,
            "lower swap after should give positive score, got {}",
            score
        );

        // No improvement
        let no_improve = score_swap_reduction(&before, &before);
        assert_eq!(no_improve, 0.0);
    }

    #[test]
    fn test_score_pressure_reduction_improvement() {
        let before = high_pressure_state();
        let after = improved_from_high();

        let score = score_pressure_reduction(&before, &after);
        assert!(
            score > 0.3,
            "lower pressure after should give positive score, got {}",
            score
        );

        // No improvement
        let no_improve = score_pressure_reduction(&before, &before);
        assert_eq!(no_improve, 0.0);
    }

    #[test]
    fn test_score_tier_balance() {
        // Balanced state
        let balanced = SystemState {
            dram_pressure: PressureState::new(),
            dram_utilization: 0.5,
            swap_utilization: 0.5,
            zram_utilization: Some(0.5),
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        };

        // Unbalanced state
        let unbalanced = SystemState {
            dram_pressure: PressureState::new(),
            dram_utilization: 0.95,
            swap_utilization: 0.05,
            zram_utilization: Some(0.1),
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        };

        let balanced_score = compute_balance(&balanced);
        let unbalanced_score = compute_balance(&unbalanced);

        assert!(
            balanced_score > unbalanced_score,
            "balanced tiers should score higher: {} vs {}",
            balanced_score,
            unbalanced_score
        );
    }

    #[test]
    fn test_score_stability() {
        let stable = idle_state();
        let unstable = critical_pressure_state();

        let stable_score = system_stability(&stable);
        let unstable_score = system_stability(&unstable);

        assert!(
            stable_score > unstable_score,
            "stable system should score higher: {} vs {}",
            stable_score,
            unstable_score
        );
    }

    #[test]
    fn test_overall_score_in_range() {
        let weights = ScoringWeights::default();

        // Test with various state combinations
        let states = vec![
            (idle_state(), idle_state()),
            (high_pressure_state(), improved_from_high()),
            (critical_pressure_state(), high_pressure_state()),
            (idle_state(), critical_pressure_state()),
            (high_pressure_state(), idle_state()),
        ];

        let rec = Recommendation::NoAction {
            reason: "test".to_string(),
            confidence: 1.0,
            factors: vec![],
        };

        for (before, after) in states {
            let score = score_recommendation(&rec, &before, &after, &weights);
            assert!(
                score.overall_score >= 0.0 && score.overall_score <= 1.0,
                "overall_score {} should be in [0.0, 1.0]",
                score.overall_score
            );
        }
    }

    #[test]
    fn test_score_no_action() {
        let before = idle_state();
        let after = idle_state();
        let weights = ScoringWeights::default();

        let rec = Recommendation::NoAction {
            reason: "system stable".to_string(),
            confidence: 1.0,
            factors: vec!["low_pressure".to_string()],
        };

        let score = score_recommendation(&rec, &before, &after, &weights);

        // NoAction on stable system should score reasonably well
        assert!(
            score.overall_score > 0.3,
            "NoAction on stable system should score > 0.3, got {}",
            score.overall_score
        );
    }

    #[test]
    fn test_score_promote_to_dram() {
        let before = high_pressure_state();
        let after = improved_from_high();
        let weights = ScoringWeights::default();

        let rec = Recommendation::PromoteToDram {
            chunk_id: ChunkId::from_data(b"hot_chunk"),
            reason: "hot chunk".to_string(),
            confidence: 0.9,
            factors: vec!["high_access".to_string()],
        };

        let score = score_recommendation(&rec, &before, &after, &weights);

        // PromoteToDram under pressure with improvement should score well
        assert!(
            score.overall_score > 0.3,
            "PromoteToDram under pressure with improvement should score > 0.3, got {}",
            score.overall_score
        );
    }

    #[test]
    fn test_score_evict_cold() {
        let before = critical_pressure_state();
        let after = high_pressure_state();
        let weights = ScoringWeights::default();

        let rec = Recommendation::EvictCold {
            tier: ghost_core::types::TierId::Ram,
            count: 8,
            confidence: 1.0,
            factors: vec!["critical_pressure".to_string()],
        };

        let score = score_recommendation(&rec, &before, &after, &weights);

        // EvictCold under critical pressure with improvement should score well
        assert!(
            score.overall_score > 0.3,
            "EvictCold under critical pressure should score > 0.3, got {}",
            score.overall_score
        );
    }

    #[test]
    fn test_custom_weights() {
        let before = high_pressure_state();
        let after = improved_from_high();

        let rec = Recommendation::NoAction {
            reason: "test".to_string(),
            confidence: 1.0,
            factors: vec![],
        };

        // Default weights
        let default_weights = ScoringWeights::default();
        let default_score = score_recommendation(&rec, &before, &after, &default_weights);

        // Custom weights emphasizing fault reduction
        let custom_weights = ScoringWeights {
            fault_reduction_weight: 1.0,
            swap_reduction_weight: 0.0,
            zram_efficiency_weight: 0.0,
            pressure_reduction_weight: 0.0,
            tier_balance_weight: 0.0,
            stability_weight: 0.0,
        };
        let custom_score = score_recommendation(&rec, &before, &after, &custom_weights);

        // With these specific before/after states, fault_reduction is high
        // so emphasizing it should give a different overall score
        assert!(
            (default_score.overall_score - custom_score.overall_score).abs() > 0.01
                || default_score.overall_score == 1.0,
            "custom weights should affect overall score: default={}, custom={}",
            default_score.overall_score,
            custom_score.overall_score
        );

        // Custom score should be based entirely on fault_reduction
        assert!(
            custom_score.overall_score > 0.0,
            "custom score should be positive when fault reduction occurred"
        );
    }

    // ── Additional edge case tests ──

    #[test]
    fn test_score_functions_pure() {
        // Verify same inputs produce same outputs across multiple calls
        let before = high_pressure_state();
        let after = improved_from_high();

        for _ in 0..10 {
            let s1 = score_fault_reduction(&before, &after);
            let s2 = score_swap_reduction(&before, &after);
            let s3 = score_zram_efficiency(&before, &after);
            let s4 = score_pressure_reduction(&before, &after);
            let s5 = score_tier_balance(&before, &after);
            let s6 = score_stability(&before, &after);

            assert_eq!(s1, score_fault_reduction(&before, &after));
            assert_eq!(s2, score_swap_reduction(&before, &after));
            assert_eq!(s3, score_zram_efficiency(&before, &after));
            assert_eq!(s4, score_pressure_reduction(&before, &after));
            assert_eq!(s5, score_tier_balance(&before, &after));
            assert_eq!(s6, score_stability(&before, &after));
        }
    }

    #[test]
    fn test_score_policy_evaluation_empty() {
        let before = high_pressure_state();
        let after = improved_from_high();

        let score = score_policy_evaluation(&[], &before, &after);
        assert!(
            score.overall_score >= 0.0 && score.overall_score <= 1.0,
            "empty recommendations should still produce valid score"
        );
    }

    #[test]
    fn test_score_policy_evaluation_multiple() {
        let before = critical_pressure_state();
        let after = improved_from_high();

        let recs = vec![
            Recommendation::EvictCold {
                tier: ghost_core::types::TierId::Ram,
                count: 8,
                confidence: 1.0,
                factors: vec!["critical_pressure".to_string()],
            },
            Recommendation::MoveToZram {
                chunk_id: ChunkId::from_data(b"cold"),
                reason: "cold chunk".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
        ];

        let score = score_policy_evaluation(&recs, &before, &after);
        assert!(
            score.overall_score > 0.0,
            "multiple recommendations with improvement should score > 0"
        );
    }

    #[test]
    fn test_individual_metrics_in_range() {
        let before = high_pressure_state();
        let after = improved_from_high();

        assert!((0.0..=1.0).contains(&score_fault_reduction(&before, &after)));
        assert!((0.0..=1.0).contains(&score_swap_reduction(&before, &after)));
        assert!((0.0..=1.0).contains(&score_zram_efficiency(&before, &after)));
        assert!((0.0..=1.0).contains(&score_pressure_reduction(&before, &after)));
        assert!((0.0..=1.0).contains(&score_tier_balance(&before, &after)));
        assert!((0.0..=1.0).contains(&score_stability(&before, &after)));
    }
}
