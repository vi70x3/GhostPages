//! Recommendation Quality Measurement for GhostPages.
//!
//! Measures overall quality of a set of recommendations across four dimensions:
//! stability, efficiency, simplicity, and confidence.
//!
//! All functions are **pure** — no I/O, no mutation, no side effects.
//! Same inputs always produce same outputs. Deterministic by design.

use ghost_linux::policy::Recommendation;
use ghost_linux::policy_rules::SystemState;

use crate::scoring::RecommendationScore;
use crate::stability::RecommendationStability;

// ─── Quality Dimension ─────────────────────────────────────────────────────────

/// A single quality dimension with score, label, and supporting evidence.
#[derive(Debug, Clone, PartialEq)]
pub struct QualityDimension {
    /// Score for this dimension (0.0 = worst, 1.0 = best).
    pub score: f32,
    /// Human-readable label describing the dimension.
    pub label: String,
    /// Supporting evidence lines explaining the score.
    pub details: Vec<String>,
}

// ─── Recommendation Quality ────────────────────────────────────────────────────

/// Overall quality of a set of recommendations across four dimensions.
///
/// Each dimension ranges from 0.0 (worst) to 1.0 (best). The `overall_quality`
/// is a weighted average of all dimension scores.
#[derive(Debug, Clone, PartialEq)]
pub struct RecommendationQuality {
    /// Stability dimension — measures recommendation consistency.
    pub stability: QualityDimension,
    /// Efficiency dimension — measures pressure/swap/ZRAM reduction.
    pub efficiency: QualityDimension,
    /// Simplicity dimension — fewer active recommendations = simpler.
    pub simplicity: QualityDimension,
    /// Confidence dimension — average confidence with low variance.
    pub confidence: QualityDimension,
    /// Overall quality score (weighted average of dimensions, 0.0–1.0).
    pub overall_quality: f32,
}

// ─── Quality Weights ──────────────────────────────────────────────────────────

/// Weights for combining individual dimensions into `overall_quality`.
///
/// All weights should be non-negative. They are normalized during computation
/// so they don't need to sum to 1.0, but the default set does.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QualityWeights {
    /// Weight for the stability dimension.
    pub stability_weight: f32,
    /// Weight for the efficiency dimension.
    pub efficiency_weight: f32,
    /// Weight for the simplicity dimension.
    pub simplicity_weight: f32,
    /// Weight for the confidence dimension.
    pub confidence_weight: f32,
}

impl Default for QualityWeights {
    fn default() -> Self {
        Self {
            stability_weight: 0.25,
            efficiency_weight: 0.30,
            simplicity_weight: 0.20,
            confidence_weight: 0.25,
        }
    }
}

// ─── Quality Computation ──────────────────────────────────────────────────────

/// Compute the overall quality of a set of recommendations.
///
/// Evaluates four dimensions:
/// - **Stability**: derived from `RecommendationStability` — high stability_index = good,
///   high oscillation/churn = bad.
/// - **Efficiency**: derived from `RecommendationScore` averages — high pressure/swap
///   reduction and ZRAM efficiency = good.
/// - **Simplicity**: fewer active (non-NoAction) recommendations = better.
/// - **Confidence**: high average confidence with low variance = better.
pub fn compute_recommendation_quality(
    recommendations: &[Recommendation],
    scores: &[RecommendationScore],
    stability: &RecommendationStability,
    before_state: &SystemState,
    after_state: &SystemState,
    weights: &QualityWeights,
) -> RecommendationQuality {
    let stability_dim = compute_stability_dimension(stability);
    let efficiency_dim = compute_efficiency_dimension(scores);
    let simplicity_dim = compute_simplicity_dimension(recommendations);
    let confidence_dim = compute_confidence_dimension(recommendations, before_state, after_state);

    // Weighted combination
    let total_weight = weights.stability_weight
        + weights.efficiency_weight
        + weights.simplicity_weight
        + weights.confidence_weight;

    let overall_quality = if total_weight > 0.0 {
        (stability_dim.score * weights.stability_weight
            + efficiency_dim.score * weights.efficiency_weight
            + simplicity_dim.score * weights.simplicity_weight
            + confidence_dim.score * weights.confidence_weight)
            / total_weight
    } else {
        0.0
    };

    RecommendationQuality {
        stability: stability_dim,
        efficiency: efficiency_dim,
        simplicity: simplicity_dim,
        confidence: confidence_dim,
        overall_quality: overall_quality.clamp(0.0, 1.0),
    }
}

// ─── Dimension Helpers ────────────────────────────────────────────────────────

/// Compute a single quality dimension from a raw metric value.
///
/// The metric should be in 0.0–1.0 range where higher is better.
/// Returns a `QualityDimension` with the score, a label, and details.
pub fn compute_quality_dimension(metric: f32, name: &str) -> QualityDimension {
    let score = metric.clamp(0.0, 1.0);
    let label = format!("{}: {:.2}", name, score);

    let quality_label = if score >= 0.8 {
        "excellent"
    } else if score >= 0.6 {
        "good"
    } else if score >= 0.4 {
        "moderate"
    } else if score >= 0.2 {
        "poor"
    } else {
        "critical"
    };

    let details = vec![
        format!("Raw score: {:.4}", score),
        format!("Quality: {}", quality_label),
    ];

    QualityDimension {
        score,
        label,
        details,
    }
}

/// Compute the stability dimension from `RecommendationStability`.
fn compute_stability_dimension(stability: &RecommendationStability) -> QualityDimension {
    // High stability_index = good.
    // Penalize high oscillation rate (tier_oscillations) and churn (recommendations_per_hour).
    let base_score = stability.stability_index;

    // Additional penalty from oscillations: 0 oscillations = no penalty, 3+ = max penalty.
    let oscillation_penalty = (stability.tier_oscillations as f32 / 3.0).min(1.0) * 0.2;

    // Additional penalty from high recommendation rate: 0/hr = no penalty, 5+/hr = max penalty.
    let churn_penalty = (stability.recommendations_per_hour / 5.0).min(1.0) * 0.1;

    // Additional penalty from temperature flips: 0 = no penalty, 3+ = max penalty.
    let flip_penalty = (stability.temperature_flips as f32 / 3.0).min(1.0) * 0.15;

    let score = (base_score - oscillation_penalty - churn_penalty - flip_penalty).clamp(0.0, 1.0);

    let details = vec![
        format!("Stability index: {:.4}", stability.stability_index),
        format!("Tier oscillations: {}", stability.tier_oscillations),
        format!("Recommendations/hour: {:.2}", stability.recommendations_per_hour),
        format!("Temperature flips: {}", stability.temperature_flips),
        format!("Confidence variance: {:.4}", stability.confidence_variance),
    ];

    QualityDimension {
        score,
        label: format!("Stability: {:.2}", score),
        details,
    }
}

/// Compute the efficiency dimension from `RecommendationScore` averages.
fn compute_efficiency_dimension(scores: &[RecommendationScore]) -> QualityDimension {
    if scores.is_empty() {
        return QualityDimension {
            score: 0.5,
            label: "Efficiency: 0.50 (no scores)".to_string(),
            details: vec!["No recommendation scores available — defaulting to neutral.".to_string()],
        };
    }

    let n = scores.len() as f32;
    let avg_pressure_reduction: f32 =
        scores.iter().map(|s| s.pressure_reduction).sum::<f32>() / n;
    let avg_swap_reduction: f32 =
        scores.iter().map(|s| s.swap_reduction).sum::<f32>() / n;
    let avg_zram_efficiency: f32 =
        scores.iter().map(|s| s.zram_efficiency).sum::<f32>() / n;
    let avg_fault_reduction: f32 =
        scores.iter().map(|s| s.fault_reduction).sum::<f32>() / n;

    // Weighted combination of efficiency metrics
    let score = (avg_pressure_reduction * 0.35
        + avg_swap_reduction * 0.25
        + avg_zram_efficiency * 0.20
        + avg_fault_reduction * 0.20)
        .clamp(0.0, 1.0);

    let details = vec![
        format!("Avg pressure reduction: {:.4}", avg_pressure_reduction),
        format!("Avg swap reduction: {:.4}", avg_swap_reduction),
        format!("Avg ZRAM efficiency: {:.4}", avg_zram_efficiency),
        format!("Avg fault reduction: {:.4}", avg_fault_reduction),
        format!("Number of scores: {}", scores.len()),
    ];

    QualityDimension {
        score,
        label: format!("Efficiency: {:.2}", score),
        details,
    }
}

/// Compute the simplicity dimension — fewer active recommendations = simpler.
fn compute_simplicity_dimension(recommendations: &[Recommendation]) -> QualityDimension {
    if recommendations.is_empty() {
        return QualityDimension {
            score: 1.0,
            label: "Simplicity: 1.00 (no recommendations)".to_string(),
            details: vec!["No recommendations — perfectly simple.".to_string()],
        };
    }

    let total = recommendations.len();
    let active_count = recommendations
        .iter()
        .filter(|r| !matches!(r, Recommendation::NoAction { .. }))
        .count();

    let active_ratio = active_count as f32 / total as f32;

    // Invert: fewer active = higher simplicity. 0 active = 1.0, all active = 0.0
    let score = (1.0 - active_ratio).clamp(0.0, 1.0);

    // Also consider absolute count: even if all are active, a small number is simpler.
    let count_factor = if total <= 2 {
        1.0
    } else if total <= 5 {
        0.8
    } else if total <= 10 {
        0.6
    } else {
        0.4
    };

    // Blend: 70% active ratio, 30% count factor
    let blended = (score * 0.7 + count_factor * 0.3).clamp(0.0, 1.0);

    let details = vec![
        format!("Total recommendations: {}", total),
        format!("Active (non-NoAction): {}", active_count),
        format!("Active ratio: {:.2}", active_ratio),
        format!("Count factor: {:.2}", count_factor),
    ];

    QualityDimension {
        score: blended,
        label: format!("Simplicity: {:.2}", blended),
        details,
    }
}

/// Compute the confidence dimension — high average confidence with low variance.
fn compute_confidence_dimension(
    recommendations: &[Recommendation],
    before_state: &SystemState,
    after_state: &SystemState,
) -> QualityDimension {
    if recommendations.is_empty() {
        // No recommendations — check if system is stable (NoAction would be appropriate)
        let pressure = before_state.dram_pressure.memory_pressure;
        let is_stable = pressure < 0.5 && before_state.swap_utilization < 0.5;
        let score = if is_stable { 0.8 } else { 0.3 };

        return QualityDimension {
            score,
            label: format!("Confidence: {:.2}", score),
            details: vec![
                "No recommendations to evaluate.".to_string(),
                format!(
                    "System pressure level: {}",
                    if is_stable { "stable" } else { "unstable" }
                ),
            ],
        };
    }

    let n = recommendations.len() as f32;
    let avg_confidence: f32 =
        recommendations.iter().map(|r| r.confidence()).sum::<f32>() / n;

    // Compute variance
    let variance = recommendations
        .iter()
        .map(|r| {
            let diff = r.confidence() - avg_confidence;
            diff * diff
        })
        .sum::<f32>()
        / n;

    // Low variance is good. Penalize high variance.
    // Variance of 0 = no penalty, variance of 0.25+ = max penalty.
    let variance_penalty = (variance / 0.25).min(1.0) * 0.2;

    // Check if state change aligns with recommendations
    let pressure_before = before_state.dram_pressure.memory_pressure;
    let pressure_after = after_state.dram_pressure.memory_pressure;
    let pressure_improved = pressure_after < pressure_before;

    // If pressure improved, boost confidence slightly
    let improvement_bonus = if pressure_improved { 0.05 } else { 0.0 };

    let score = (avg_confidence - variance_penalty + improvement_bonus).clamp(0.0, 1.0);

    let details = vec![
        format!("Average confidence: {:.4}", avg_confidence),
        format!("Confidence variance: {:.4}", variance),
        format!("Pressure before: {:.4}", pressure_before),
        format!("Pressure after: {:.4}", pressure_after),
        format!(
            "Pressure improvement: {}",
            if pressure_improved { "yes" } else { "no" }
        ),
    ];

    QualityDimension {
        score,
        label: format!("Confidence: {:.2}", score),
        details,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::PressureState;
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

    fn perfect_stability() -> RecommendationStability {
        RecommendationStability {
            recommendations_per_hour: 0.0,
            temperature_flips: 0,
            tier_oscillations: 0,
            confidence_variance: 0.0,
            stability_index: 1.0,
        }
    }

    fn good_scores() -> Vec<RecommendationScore> {
        vec![RecommendationScore {
            fault_reduction: 0.8,
            swap_reduction: 0.7,
            zram_efficiency: 0.6,
            pressure_reduction: 0.75,
            tier_balance: 0.5,
            stability: 0.6,
            overall_score: 0.68,
        }]
    }

    fn chunk_a() -> ChunkId {
        ChunkId::from_data(b"chunk_a")
    }

    // ── Required tests ──

    #[test]
    fn test_quality_weights_default() {
        let weights = QualityWeights::default();
        let sum = weights.stability_weight
            + weights.efficiency_weight
            + weights.simplicity_weight
            + weights.confidence_weight;
        assert!(
            (sum - 1.0).abs() < 1e-6,
            "default weights should sum to 1.0, got {}",
            sum
        );
    }

    #[test]
    fn test_compute_quality_empty_recommendations() {
        let weights = QualityWeights::default();
        let stability = perfect_stability();
        let before = idle_state();
        let after = idle_state();

        let quality = compute_recommendation_quality(&[], &[], &stability, &before, &after, &weights);

        // All dimension scores should be in range
        assert!((0.0..=1.0).contains(&quality.stability.score));
        assert!((0.0..=1.0).contains(&quality.efficiency.score));
        assert!((0.0..=1.0).contains(&quality.simplicity.score));
        assert!((0.0..=1.0).contains(&quality.confidence.score));
        assert!((0.0..=1.0).contains(&quality.overall_quality));
    }

    #[test]
    fn test_compute_quality_high_stability() {
        let weights = QualityWeights::default();
        let stability = perfect_stability();
        let before = idle_state();
        let after = idle_state();
        let recs = vec![Recommendation::NoAction {
            reason: "stable".to_string(),
            confidence: 1.0,
            factors: vec![],
        }];
        let scores = good_scores();

        let quality =
            compute_recommendation_quality(&recs, &scores, &stability, &before, &after, &weights);

        assert!(
            quality.stability.score >= 0.8,
            "perfect stability should give high stability score, got {}",
            quality.stability.score
        );
    }

    #[test]
    fn test_compute_quality_efficiency_from_scores() {
        let weights = QualityWeights::default();
        let stability = perfect_stability();
        let before = high_pressure_state();
        let after = improved_from_high();
        let recs = vec![Recommendation::NoAction {
            reason: "test".to_string(),
            confidence: 1.0,
            factors: vec![],
        }];
        let scores = good_scores();

        let quality =
            compute_recommendation_quality(&recs, &scores, &stability, &before, &after, &weights);

        assert!(
            quality.efficiency.score > 0.5,
            "good scores should give high efficiency, got {}",
            quality.efficiency.score
        );
    }

    #[test]
    fn test_compute_quality_simplicity_fewer_recs() {
        let weights = QualityWeights::default();
        let stability = perfect_stability();
        let before = idle_state();
        let after = idle_state();

        // All NoAction = very simple
        let recs_simple = vec![
            Recommendation::NoAction {
                reason: "stable".to_string(),
                confidence: 1.0,
                factors: vec![],
            },
            Recommendation::NoAction {
                reason: "stable".to_string(),
                confidence: 1.0,
                factors: vec![],
            },
        ];

        // Some active = less simple
        let recs_active = vec![
            Recommendation::PromoteToDram {
                chunk_id: chunk_a(),
                reason: "hot".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
            Recommendation::NoAction {
                reason: "stable".to_string(),
                confidence: 1.0,
                factors: vec![],
            },
        ];

        let scores = good_scores();
        let q_simple = compute_recommendation_quality(
            &recs_simple,
            &scores,
            &stability,
            &before,
            &after,
            &weights,
        );
        let q_active = compute_recommendation_quality(
            &recs_active,
            &scores,
            &stability,
            &before,
            &after,
            &weights,
        );

        assert!(
            q_simple.simplicity.score >= q_active.simplicity.score,
            "all-NoAction should be simpler: {} vs {}",
            q_simple.simplicity.score,
            q_active.simplicity.score
        );
    }

    #[test]
    fn test_compute_quality_confidence_high() {
        let weights = QualityWeights::default();
        let stability = perfect_stability();
        let before = idle_state();
        let after = idle_state();

        let recs = vec![
            Recommendation::NoAction {
                reason: "stable".to_string(),
                confidence: 0.95,
                factors: vec![],
            },
            Recommendation::NoAction {
                reason: "stable".to_string(),
                confidence: 0.92,
                factors: vec![],
            },
        ];
        let scores = good_scores();

        let quality =
            compute_recommendation_quality(&recs, &scores, &stability, &before, &after, &weights);

        assert!(
            quality.confidence.score >= 0.7,
            "high average confidence should give high confidence dimension, got {}",
            quality.confidence.score
        );
    }

    #[test]
    fn test_compute_quality_overall_in_range() {
        let weights = QualityWeights::default();
        let before = high_pressure_state();
        let after = improved_from_high();

        // Test with various scenarios
        let scenarios: Vec<(Vec<Recommendation>, Vec<RecommendationScore>, RecommendationStability)> = vec![
            // Empty
            (vec![], vec![], perfect_stability()),
            // Single NoAction
            (
                vec![Recommendation::NoAction {
                    reason: "test".to_string(),
                    confidence: 1.0,
                    factors: vec![],
                }],
                good_scores(),
                perfect_stability(),
            ),
            // Multiple active
            (
                vec![
                    Recommendation::PromoteToDram {
                        chunk_id: chunk_a(),
                        reason: "hot".to_string(),
                        confidence: 0.9,
                        factors: vec![],
                    },
                    Recommendation::MoveToZram {
                        chunk_id: chunk_a(),
                        reason: "cold".to_string(),
                        confidence: 0.8,
                        factors: vec![],
                    },
                ],
                vec![good_scores()[0], good_scores()[0]],
                RecommendationStability {
                    recommendations_per_hour: 2.0,
                    temperature_flips: 1,
                    tier_oscillations: 1,
                    confidence_variance: 0.01,
                    stability_index: 0.5,
                },
            ),
        ];

        for (recs, scores, stability) in scenarios {
            let quality =
                compute_recommendation_quality(&recs, &scores, &stability, &before, &after, &weights);
            assert!(
                quality.overall_quality >= 0.0 && quality.overall_quality <= 1.0,
                "overall_quality {} should be in [0.0, 1.0]",
                quality.overall_quality
            );
        }
    }

    #[test]
    fn test_compute_quality_deterministic() {
        let weights = QualityWeights::default();
        let stability = perfect_stability();
        let before = high_pressure_state();
        let after = improved_from_high();
        let recs = vec![Recommendation::NoAction {
            reason: "test".to_string(),
            confidence: 1.0,
            factors: vec![],
        }];
        let scores = good_scores();

        let q1 = compute_recommendation_quality(&recs, &scores, &stability, &before, &after, &weights);
        let q2 = compute_recommendation_quality(&recs, &scores, &stability, &before, &after, &weights);

        assert_eq!(q1.overall_quality, q2.overall_quality);
        assert_eq!(q1.stability.score, q2.stability.score);
        assert_eq!(q1.efficiency.score, q2.efficiency.score);
        assert_eq!(q1.simplicity.score, q2.simplicity.score);
        assert_eq!(q1.confidence.score, q2.confidence.score);
    }

    #[test]
    fn test_quality_dimension_score_in_range() {
        // Test various metric values
        for &metric in &[0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0, -0.5, 1.5] {
            let dim = compute_quality_dimension(metric, "test");
            assert!(
                dim.score >= 0.0 && dim.score <= 1.0,
                "dimension score {} should be in [0.0, 1.0] for metric {}",
                dim.score,
                metric
            );
        }
    }
}
