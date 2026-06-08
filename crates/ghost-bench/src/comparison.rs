//! Policy comparison runs for GhostPages benchmarking.
//!
//! Provides deterministic evaluation of policies against workload scenarios,
//! producing per-run scores and aggregate comparisons.

use ghost_evaluator::quality::{compute_recommendation_quality, QualityWeights, RecommendationQuality};
use ghost_evaluator::scoring::{score_policy_evaluation, RecommendationScore, ScoringWeights};
use ghost_evaluator::stability::{RecommendationStability, StabilityTracker};
use ghost_evaluator::tournament::Policy;
use ghost_linux::policy::Recommendation;

use crate::workload::WorkloadScenario;

// ─── Policy Comparison Run ────────────────────────────────────────────────────

/// A single policy comparison run: one workload evaluated by one policy.
#[derive(Debug, Clone)]
pub struct PolicyComparisonRun {
    /// Name of the workload that was evaluated.
    pub workload_name: String,
    /// Name of the policy that was evaluated.
    pub policy_name: String,
    /// Per-snapshot scores (one per consecutive snapshot pair).
    pub scores: Vec<RecommendationScore>,
    /// Averaged score across all snapshots.
    pub average_score: RecommendationScore,
    /// Total number of recommendations generated across all snapshots.
    pub recommendation_count: usize,
    /// Number of non-NoAction recommendations.
    pub active_recommendation_count: usize,
    /// Stability metrics across the run.
    pub stability: RecommendationStability,
    /// Overall quality assessment.
    pub quality: RecommendationQuality,
}

// ─── Workload Comparison ──────────────────────────────────────────────────────

/// Results from running multiple policies against the same workload.
#[derive(Debug, Clone)]
pub struct WorkloadComparison {
    /// Name of the workload.
    pub workload_name: String,
    /// Individual policy runs.
    pub runs: Vec<PolicyComparisonRun>,
    /// Name of the winning policy (highest average overall_score).
    pub winner: String,
    /// The winner's average overall_score.
    pub winner_score: f32,
}

// ─── Comparison Functions ─────────────────────────────────────────────────────

/// Run a single policy against a workload scenario.
///
/// Iterates through consecutive snapshot pairs, evaluates the policy on each
/// "before" state, scores the recommendations against the state change, and
/// computes aggregate metrics.
pub fn run_policy_comparison(
    scenario: &WorkloadScenario,
    policy: &dyn Policy,
    weights: &ScoringWeights,
) -> PolicyComparisonRun {
    let mut scores = Vec::new();
    let mut all_recommendations = Vec::new();
    let mut active_count = 0usize;
    let mut tracker = StabilityTracker::new(64);
    let quality_weights = QualityWeights::default();

    for window in scenario.snapshots.windows(2) {
        let before = &window[0].state;
        let after = &window[1].state;
        let timestamp_ms = window[0].timestamp_ms;

        let recommendations = policy.evaluate(before);
        let score = score_policy_evaluation(&recommendations, before, after);

        scores.push(score);
        all_recommendations.extend(recommendations.clone());

        let non_noaction: Vec<Recommendation> = recommendations
            .iter()
            .filter(|r| !matches!(r, Recommendation::NoAction { .. }))
            .cloned()
            .collect();
        active_count += non_noaction.len();

        // Record each recommendation for stability tracking
        for rec in &recommendations {
            tracker.record(rec.clone(), before, timestamp_ms);
        }
    }

    let average_score = average_scores(&scores);
    let stability = tracker.evaluate();

    // Compute quality from all accumulated data
    let before_state = scenario
        .snapshots
        .first()
        .map(|s| s.state.clone())
        .unwrap_or_else(|| {
            ghost_linux::policy_rules::SystemState {
                dram_pressure: ghost_core::state::PressureState::new(),
                dram_utilization: 0.0,
                swap_utilization: 0.0,
                zram_utilization: None,
                io_pressure: ghost_core::state::PressureState::new(),
                hotness_summary: None,
                hotness_confidence: None,
            }
        });
    let after_state = scenario
        .snapshots
        .last()
        .map(|s| s.state.clone())
        .unwrap_or_else(|| before_state.clone());

    let quality = compute_recommendation_quality(
        &all_recommendations,
        &scores,
        &stability,
        &before_state,
        &after_state,
        &quality_weights,
    );

    PolicyComparisonRun {
        workload_name: scenario.definition.name.clone(),
        policy_name: policy.name().to_string(),
        scores,
        average_score,
        recommendation_count: all_recommendations.len(),
        active_recommendation_count: active_count,
        stability,
        quality,
    }
}

/// Run multiple policies against the same workload and determine the winner.
pub fn run_workload_comparison(
    scenario: &WorkloadScenario,
    policies: &[Box<dyn Policy>],
    weights: &ScoringWeights,
) -> WorkloadComparison {
    let runs: Vec<PolicyComparisonRun> = policies
        .iter()
        .map(|policy| run_policy_comparison(scenario, policy.as_ref(), weights))
        .collect();

    let winner = runs
        .iter()
        .max_by(|a, b| {
            a.average_score
                .overall_score
                .partial_cmp(&b.average_score.overall_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|r| r.policy_name.clone())
        .unwrap_or_default();

    let winner_score = runs
        .iter()
        .find(|r| r.policy_name == winner)
        .map(|r| r.average_score.overall_score)
        .unwrap_or(0.0);

    WorkloadComparison {
        workload_name: scenario.definition.name.clone(),
        runs,
        winner,
        winner_score,
    }
}

/// Average a slice of RecommendationScore values.
fn average_scores(scores: &[RecommendationScore]) -> RecommendationScore {
    if scores.is_empty() {
        return RecommendationScore {
            fault_reduction: 0.0,
            swap_reduction: 0.0,
            zram_efficiency: 0.0,
            pressure_reduction: 0.0,
            tier_balance: 0.0,
            stability: 0.0,
            overall_score: 0.0,
        };
    }

    let n = scores.len() as f32;
    let mut total_fault = 0.0_f32;
    let mut total_swap = 0.0_f32;
    let mut total_zram = 0.0_f32;
    let mut total_pressure = 0.0_f32;
    let mut total_balance = 0.0_f32;
    let mut total_stability = 0.0_f32;
    let mut total_overall = 0.0_f32;

    for s in scores {
        total_fault += s.fault_reduction;
        total_swap += s.swap_reduction;
        total_zram += s.zram_efficiency;
        total_pressure += s.pressure_reduction;
        total_balance += s.tier_balance;
        total_stability += s.stability;
        total_overall += s.overall_score;
    }

    RecommendationScore {
        fault_reduction: total_fault / n,
        swap_reduction: total_swap / n,
        zram_efficiency: total_zram / n,
        pressure_reduction: total_pressure / n,
        tier_balance: total_balance / n,
        stability: total_stability / n,
        overall_score: total_overall / n,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_evaluator::tournament::PressurePolicy;

    fn test_scenario() -> WorkloadScenario {
        use ghost_core::state::PressureState;
        use ghost_linux::policy_rules::SystemState;

        let def = crate::workload::WorkloadDefinition {
            name: "test_workload".to_string(),
            class: crate::workload::WorkloadClass::MemoryPressure,
            description: "test".to_string(),
            duration_seconds: 10,
            snapshot_interval_ms: 2000,
            seed: 42,
        };

        let snapshots = vec![
            crate::workload::TimedSnapshot {
                timestamp_ms: 0,
                state: SystemState {
                    dram_pressure: PressureState {
                        memory_pressure: 0.8,
                        ..PressureState::new()
                    },
                    dram_utilization: 0.85,
                    swap_utilization: 0.3,
                    zram_utilization: Some(0.4),
                    io_pressure: PressureState::new(),
                    hotness_summary: None,
                    hotness_confidence: None,
                },
            },
            crate::workload::TimedSnapshot {
                timestamp_ms: 2000,
                state: SystemState {
                    dram_pressure: PressureState {
                        memory_pressure: 0.5,
                        ..PressureState::new()
                    },
                    dram_utilization: 0.6,
                    swap_utilization: 0.2,
                    zram_utilization: Some(0.5),
                    io_pressure: PressureState::new(),
                    hotness_summary: None,
                    hotness_confidence: None,
                },
            },
            crate::workload::TimedSnapshot {
                timestamp_ms: 4000,
                state: SystemState {
                    dram_pressure: PressureState {
                        memory_pressure: 0.3,
                        ..PressureState::new()
                    },
                    dram_utilization: 0.4,
                    swap_utilization: 0.1,
                    zram_utilization: Some(0.3),
                    io_pressure: PressureState::new(),
                    hotness_summary: None,
                    hotness_confidence: None,
                },
            },
        ];

        let _metadata = crate::workload::WorkloadGenerator::generate(
            &crate::workload::WorkloadGenerator::new(42),
            &def,
        )
        .metadata;

        // Reconstruct with our custom snapshots
        use crate::workload::{PressureTimeDistribution, ScenarioMetadata};
        WorkloadScenario {
            definition: def,
            snapshots,
            metadata: ScenarioMetadata {
                total_snapshots: 3,
                peak_dram_pressure: 0.8,
                peak_dram_utilization: 0.85,
                avg_dram_utilization: 0.62,
                avg_swap_utilization: 0.2,
                pressure_time_distribution: PressureTimeDistribution {
                    idle_fraction: 0.0,
                    low_fraction: 0.33,
                    medium_fraction: 0.33,
                    high_fraction: 0.34,
                    critical_fraction: 0.0,
                },
            },
        }
    }

    #[test]
    fn test_run_policy_comparison() {
        let scenario = test_scenario();
        let policy = PressurePolicy;
        let weights = ScoringWeights::default();

        let run = run_policy_comparison(&scenario, &policy, &weights);

        assert_eq!(run.workload_name, "test_workload");
        assert_eq!(run.policy_name, "Pressure");
        // 3 snapshots = 2 consecutive pairs
        assert_eq!(run.scores.len(), 2);
        assert!(run.recommendation_count > 0);
    }

    #[test]
    fn test_run_workload_comparison() {
        let scenario = test_scenario();
        let weights = ScoringWeights::default();

        use ghost_evaluator::tournament::HybridPolicy;

        let policies: Vec<Box<dyn Policy>> =
            vec![Box::new(PressurePolicy), Box::new(HybridPolicy)];

        let comparison = run_workload_comparison(&scenario, &policies, &weights);

        assert_eq!(comparison.workload_name, "test_workload");
        assert_eq!(comparison.runs.len(), 2);
        assert!(!comparison.winner.is_empty());
        assert!(comparison.winner_score >= 0.0);
    }

    #[test]
    fn test_comparison_scores_in_range() {
        let scenario = test_scenario();
        let policy = PressurePolicy;
        let weights = ScoringWeights::default();

        let run = run_policy_comparison(&scenario, &policy, &weights);

        for score in &run.scores {
            assert!(score.overall_score >= 0.0 && score.overall_score <= 1.0);
            assert!(score.fault_reduction >= 0.0 && score.fault_reduction <= 1.0);
            assert!(score.swap_reduction >= 0.0 && score.swap_reduction <= 1.0);
            assert!(score.zram_efficiency >= 0.0 && score.zram_efficiency <= 1.0);
            assert!(score.pressure_reduction >= 0.0 && score.pressure_reduction <= 1.0);
            assert!(score.tier_balance >= 0.0 && score.tier_balance <= 1.0);
            assert!(score.stability >= 0.0 && score.stability <= 1.0);
        }

        // Average score should also be in range
        assert!(
            run.average_score.overall_score >= 0.0 && run.average_score.overall_score <= 1.0
        );
    }

    #[test]
    fn test_comparison_deterministic() {
        let scenario = test_scenario();
        let policy = PressurePolicy;
        let weights = ScoringWeights::default();

        let run1 = run_policy_comparison(&scenario, &policy, &weights);
        let run2 = run_policy_comparison(&scenario, &policy, &weights);

        assert_eq!(run1.scores.len(), run2.scores.len());
        assert_eq!(
            run1.average_score.overall_score,
            run2.average_score.overall_score
        );
        assert_eq!(run1.recommendation_count, run2.recommendation_count);
        assert_eq!(
            run1.active_recommendation_count,
            run2.active_recommendation_count
        );
    }
}
