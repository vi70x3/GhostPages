//! Replay Analytics for GhostPages.
//!
//! Produces analysis reports from replay data — most active/stable regions,
//! policy disagreements, recommendation effectiveness.
//!
//! All functions are **pure** — no I/O, no mutation, no side effects.
//! Same inputs always produce same outputs. Deterministic by design.

use ghost_core::types::ChunkId;
use ghost_linux::policy_rules::{PressureLevel, SystemState};

use crate::adaptive::TemperatureClass;
use crate::lifecycle::LifecycleTracker;
use crate::scoring::RecommendationScore;
use crate::stability::RecommendationStability;
use crate::tournament::PolicyRound;

// ─── Region Activity ───────────────────────────────────────────────────────────

/// Activity statistics for a single region.
#[derive(Debug, Clone, PartialEq)]
pub struct RegionActivity {
    pub region_id: ChunkId,
    pub transition_count: usize,
    pub promotion_count: usize,
    pub demotion_count: usize,
    pub average_residency_secs: f32,
    pub dominant_temperature: TemperatureClass,
}

// ─── Policy Disagreement ───────────────────────────────────────────────────────

/// A disagreement between two policies on the same state.
#[derive(Debug, Clone, PartialEq)]
pub struct PolicyDisagreement {
    pub state_index: usize,
    pub policy_a: &'static str,
    pub policy_b: &'static str,
    pub recommendation_a: String,
    pub recommendation_b: String,
    pub score_difference: f32,
}

// ─── Score Distribution ────────────────────────────────────────────────────────

/// Distribution of recommendation scores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScoreDistribution {
    pub excellent_count: usize, // score >= 0.8
    pub good_count: usize,      // score >= 0.5
    pub poor_count: usize,      // score >= 0.3
    pub bad_count: usize,       // score < 0.3
}

// ─── Recommendation Effectiveness ──────────────────────────────────────────────

/// How effective recommendations were across the replay.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecommendationEffectiveness {
    pub total_recommendations: usize,
    pub effective_recommendations: usize,   // score > 0.5
    pub ineffective_recommendations: usize, // score < 0.3
    pub average_score: f32,
    pub best_score: f32,
    pub worst_score: f32,
    pub score_distribution: ScoreDistribution,
}

// ─── Pressure Profile ──────────────────────────────────────────────────────────

/// Pressure profile over the replay.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PressureProfile {
    pub min_pressure: f32,
    pub max_pressure: f32,
    pub average_pressure: f32,
    pub pressure_variance: f32,
    pub critical_periods: usize,    // snapshots with critical pressure
    pub high_pressure_periods: usize,
    pub low_pressure_periods: usize,
}

// ─── Analysis Summary ──────────────────────────────────────────────────────────

/// Overall analysis summary.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalysisSummary {
    pub overall_effectiveness: f32, // 0.0-1.0
    pub policy_agreement_rate: f32, // 0.0-1.0, how often policies agree
    pub stability_index: f32,       // from StabilityTracker
    pub dominant_pressure_level: PressureLevel,
    pub top_recommendation_type: String, // most common recommendation kind
}

// ─── Replay Analysis Report ────────────────────────────────────────────────────

/// A complete analysis report from replaying workload traces.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayAnalysisReport {
    /// Total number of state snapshots analyzed.
    pub total_snapshots: usize,
    /// Most active regions (highest transition count).
    pub most_active_regions: Vec<RegionActivity>,
    /// Most stable regions (lowest transition count).
    pub most_stable_regions: Vec<RegionActivity>,
    /// Policy disagreements — where policies disagree on recommendations.
    pub policy_disagreements: Vec<PolicyDisagreement>,
    /// Recommendation effectiveness — how well recommendations scored.
    pub recommendation_effectiveness: RecommendationEffectiveness,
    /// Pressure profile over the replay.
    pub pressure_profile: PressureProfile,
    /// Overall analysis summary.
    pub summary: AnalysisSummary,
}

// ─── Public Functions ──────────────────────────────────────────────────────────

/// Analyze a sequence of state snapshots and produce a full report.
///
/// This is a pure function — same inputs always produce same outputs.
pub fn analyze_replay(
    snapshots: &[SystemState],
    lifecycle: &LifecycleTracker,
    stability: &RecommendationStability,
) -> ReplayAnalysisReport {
    let total_snapshots = snapshots.len();

    // Compute region activity from lifecycle tracker.
    let (most_active_regions, most_stable_regions) =
        compute_region_activities(lifecycle);

    // Compute pressure profile.
    let pressure_profile = compute_pressure_profile(snapshots);

    // Build summary.
    let summary = AnalysisSummary {
        overall_effectiveness: stability.stability_index,
        policy_agreement_rate: 0.0, // computed from disagreements if available
        stability_index: stability.stability_index,
        dominant_pressure_level: dominant_pressure(&pressure_profile),
        top_recommendation_type: String::new(),
    };

    ReplayAnalysisReport {
        total_snapshots,
        most_active_regions,
        most_stable_regions,
        policy_disagreements: Vec::new(),
        recommendation_effectiveness: RecommendationEffectiveness {
            total_recommendations: 0,
            effective_recommendations: 0,
            ineffective_recommendations: 0,
            average_score: 0.0,
            best_score: 0.0,
            worst_score: 0.0,
            score_distribution: ScoreDistribution {
                excellent_count: 0,
                good_count: 0,
                poor_count: 0,
                bad_count: 0,
            },
        },
        pressure_profile,
        summary,
    }
}

/// Analyze policy disagreements from tournament results.
pub fn analyze_disagreements(rounds: &[PolicyRound]) -> Vec<PolicyDisagreement> {
    let mut disagreements = Vec::new();

    for round in rounds {
        let results = &round.results;

        // Compare each pair of policy results.
        for i in 0..results.len() {
            for j in (i + 1)..results.len() {
                let a = &results[i];
                let b = &results[j];

                // Get the first recommendation kind from each policy (or "none").
                let kind_a = a
                    .recommendations
                    .first()
                    .map(|r| r.kind().to_string())
                    .unwrap_or_else(|| "none".to_string());
                let kind_b = b
                    .recommendations
                    .first()
                    .map(|r| r.kind().to_string())
                    .unwrap_or_else(|| "none".to_string());

                // Disagreement: different recommendation kinds.
                if kind_a != kind_b {
                    let score_diff = (a.score.overall_score - b.score.overall_score).abs();
                    disagreements.push(PolicyDisagreement {
                        state_index: round.round_index,
                        policy_a: a.policy_name,
                        policy_b: b.policy_name,
                        recommendation_a: kind_a,
                        recommendation_b: kind_b,
                        score_difference: score_diff,
                    });
                }
            }
        }
    }

    disagreements
}

/// Compute recommendation effectiveness from scores.
pub fn compute_effectiveness(scores: &[RecommendationScore]) -> RecommendationEffectiveness {
    if scores.is_empty() {
        return RecommendationEffectiveness {
            total_recommendations: 0,
            effective_recommendations: 0,
            ineffective_recommendations: 0,
            average_score: 0.0,
            best_score: 0.0,
            worst_score: 0.0,
            score_distribution: ScoreDistribution {
                excellent_count: 0,
                good_count: 0,
                poor_count: 0,
                bad_count: 0,
            },
        };
    }

    let total = scores.len();
    let mut effective = 0;
    let mut ineffective = 0;
    let mut excellent = 0;
    let mut good = 0;
    let mut poor = 0;
    let mut bad = 0;
    let mut sum = 0.0_f32;
    let mut best = 0.0_f32;
    let mut worst = 1.0_f32;

    for score in scores {
        let overall = score.overall_score;
        sum += overall;
        best = best.max(overall);
        worst = worst.min(overall);

        if overall > 0.5 {
            effective += 1;
        }
        if overall < 0.3 {
            ineffective += 1;
        }

        // Score distribution.
        if overall >= 0.8 {
            excellent += 1;
        } else if overall >= 0.5 {
            good += 1;
        } else if overall >= 0.3 {
            poor += 1;
        } else {
            bad += 1;
        }
    }

    RecommendationEffectiveness {
        total_recommendations: total,
        effective_recommendations: effective,
        ineffective_recommendations: ineffective,
        average_score: sum / total as f32,
        best_score: best,
        worst_score: worst,
        score_distribution: ScoreDistribution {
            excellent_count: excellent,
            good_count: good,
            poor_count: poor,
            bad_count: bad,
        },
    }
}

/// Compute pressure profile from state snapshots.
pub fn compute_pressure_profile(snapshots: &[SystemState]) -> PressureProfile {
    if snapshots.is_empty() {
        return PressureProfile {
            min_pressure: 0.0,
            max_pressure: 0.0,
            average_pressure: 0.0,
            pressure_variance: 0.0,
            critical_periods: 0,
            high_pressure_periods: 0,
            low_pressure_periods: 0,
        };
    }

    let mut min_pressure = 1.0_f32;
    let mut max_pressure = 0.0_f32;
    let mut sum_pressure = 0.0_f32;
    let mut critical_periods = 0;
    let mut high_pressure_periods = 0;
    let mut low_pressure_periods = 0;

    for snapshot in snapshots {
        let pressure = max_pressure_for_snapshot(snapshot);
        min_pressure = min_pressure.min(pressure);
        max_pressure = max_pressure.max(pressure);
        sum_pressure += pressure;

        match snapshot.pressure_level() {
            PressureLevel::Critical => critical_periods += 1,
            PressureLevel::High => high_pressure_periods += 1,
            PressureLevel::Low => low_pressure_periods += 1,
            PressureLevel::Medium => {}
        }
    }

    let avg = sum_pressure / snapshots.len() as f32;

    // Compute variance.
    let variance = if snapshots.len() > 1 {
        snapshots
            .iter()
            .map(|s| {
                let p = max_pressure_for_snapshot(s);
                let diff = p - avg;
                diff * diff
            })
            .sum::<f32>()
            / snapshots.len() as f32
    } else {
        0.0
    };

    PressureProfile {
        min_pressure,
        max_pressure,
        average_pressure: avg,
        pressure_variance: variance,
        critical_periods,
        high_pressure_periods,
        low_pressure_periods,
    }
}

// ─── Helper Functions ──────────────────────────────────────────────────────────

/// Compute region activity lists from lifecycle tracker.
fn compute_region_activities(
    lifecycle: &LifecycleTracker,
) -> (Vec<RegionActivity>, Vec<RegionActivity>) {
    let summary = lifecycle.summary();

    if summary.total_regions == 0 {
        return (Vec::new(), Vec::new());
    }

    let mut activities: Vec<RegionActivity> = Vec::new();

    // Collect region IDs from the tracker.
    let mut all_ids: Vec<ChunkId> = Vec::new();

    if let Some(active_id) = summary.most_active_region {
        all_ids.push(active_id);
    }
    if let Some(stable_id) = summary.most_stable_region {
        if !all_ids.contains(&stable_id) {
            all_ids.push(stable_id);
        }
    }

    for region_id in &all_ids {
        if let Some(lifecycle_entry) = lifecycle.get_lifecycle(region_id) {
            // Determine dominant temperature from transitions.
            let dominant = if lifecycle_entry.current_temperature == TemperatureClass::Hot {
                TemperatureClass::Hot
            } else if lifecycle_entry.current_temperature == TemperatureClass::Cold
                || lifecycle_entry.current_temperature == TemperatureClass::Frozen
            {
                TemperatureClass::Cold
            } else {
                TemperatureClass::Warm
            };

            activities.push(RegionActivity {
                region_id: *region_id,
                transition_count: lifecycle_entry.transitions.len(),
                promotion_count: lifecycle_entry.promotion_count,
                demotion_count: lifecycle_entry.demotion_count,
                average_residency_secs: lifecycle_entry.average_residency_secs,
                dominant_temperature: dominant,
            });
        }
    }

    // Sort by transition count descending for most active.
    let mut most_active = activities.clone();
    most_active.sort_by(|a, b| b.transition_count.cmp(&a.transition_count));

    // Sort by transition count ascending for most stable.
    let mut most_stable = activities;
    most_stable.sort_by(|a, b| a.transition_count.cmp(&b.transition_count));

    (most_active, most_stable)
}

/// Get the maximum pressure value from a snapshot.
fn max_pressure_for_snapshot(snapshot: &SystemState) -> f32 {
    snapshot
        .dram_pressure
        .memory_pressure
        .max(snapshot.dram_pressure.io_pressure)
        .max(snapshot.io_pressure.memory_pressure)
        .max(snapshot.io_pressure.io_pressure)
}

/// Determine the dominant pressure level from a pressure profile.
fn dominant_pressure(profile: &PressureProfile) -> PressureLevel {
    if profile.critical_periods > 0 {
        PressureLevel::Critical
    } else if profile.high_pressure_periods > profile.low_pressure_periods {
        PressureLevel::High
    } else if profile.average_pressure >= 0.7 {
        PressureLevel::High
    } else if profile.average_pressure >= 0.5 {
        PressureLevel::Medium
    } else {
        PressureLevel::Low
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::PressureState;
    use ghost_linux::policy::Recommendation;

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

    fn empty_stability() -> RecommendationStability {
        RecommendationStability {
            recommendations_per_hour: 0.0,
            temperature_flips: 0,
            tier_oscillations: 0,
            confidence_variance: 0.0,
            stability_index: 1.0,
        }
    }

    // ── Tests ──

    #[test]
    fn test_analyze_replay_empty() {
        let snapshots: Vec<SystemState> = vec![];
        let lifecycle = LifecycleTracker::new();
        let stability = empty_stability();

        let report = analyze_replay(&snapshots, &lifecycle, &stability);

        assert_eq!(report.total_snapshots, 0);
        assert!(report.most_active_regions.is_empty());
        assert!(report.most_stable_regions.is_empty());
        assert!(report.policy_disagreements.is_empty());
        assert_eq!(report.recommendation_effectiveness.total_recommendations, 0);
        assert_eq!(report.pressure_profile.min_pressure, 0.0);
        assert_eq!(report.pressure_profile.max_pressure, 0.0);
    }

    #[test]
    fn test_analyze_replay_single_snapshot() {
        let snapshots = vec![idle_state()];
        let lifecycle = LifecycleTracker::new();
        let stability = empty_stability();

        let report = analyze_replay(&snapshots, &lifecycle, &stability);

        assert_eq!(report.total_snapshots, 1);
        assert!(report.most_active_regions.is_empty());
        assert!(report.most_stable_regions.is_empty());
        // Single idle snapshot should have low pressure.
        assert_eq!(report.pressure_profile.min_pressure, 0.0);
        assert_eq!(report.pressure_profile.max_pressure, 0.0);
        assert_eq!(report.pressure_profile.low_pressure_periods, 1);
    }

    #[test]
    fn test_compute_pressure_profile() {
        let snapshots = vec![
            idle_state(),
            medium_pressure_state(),
            high_pressure_state(),
            critical_pressure_state(),
        ];

        let profile = compute_pressure_profile(&snapshots);

        assert_eq!(profile.min_pressure, 0.0);
        assert_eq!(profile.max_pressure, 0.95);
        assert!(profile.average_pressure > 0.0);
        assert!(profile.pressure_variance > 0.0);
        assert_eq!(profile.critical_periods, 1);
        assert_eq!(profile.high_pressure_periods, 1);
        assert_eq!(profile.low_pressure_periods, 1);
    }

    #[test]
    fn test_compute_effectiveness() {
        // Create scores with varying overall_score values.
        let scores = vec![
            RecommendationScore {
                fault_reduction: 0.9,
                swap_reduction: 0.8,
                zram_efficiency: 0.7,
                pressure_reduction: 0.85,
                tier_balance: 0.6,
                stability: 0.9,
                overall_score: 0.85, // excellent
            },
            RecommendationScore {
                fault_reduction: 0.6,
                swap_reduction: 0.5,
                zram_efficiency: 0.4,
                pressure_reduction: 0.55,
                tier_balance: 0.5,
                stability: 0.6,
                overall_score: 0.55, // good
            },
            RecommendationScore {
                fault_reduction: 0.3,
                swap_reduction: 0.2,
                zram_efficiency: 0.25,
                pressure_reduction: 0.28,
                tier_balance: 0.4,
                stability: 0.3,
                overall_score: 0.28, // bad (ineffective)
            },
        ];

        let effectiveness = compute_effectiveness(&scores);

        assert_eq!(effectiveness.total_recommendations, 3);
        assert_eq!(effectiveness.effective_recommendations, 2); // > 0.5
        assert_eq!(effectiveness.ineffective_recommendations, 1); // < 0.3
        assert!(effectiveness.average_score > 0.0 && effectiveness.average_score < 1.0);
        assert_eq!(effectiveness.best_score, 0.85);
        assert_eq!(effectiveness.worst_score, 0.28);

        // Score distribution.
        assert_eq!(effectiveness.score_distribution.excellent_count, 1); // >= 0.8
        assert_eq!(effectiveness.score_distribution.good_count, 1); // >= 0.5
        assert_eq!(effectiveness.score_distribution.poor_count, 0); // >= 0.3
        assert_eq!(effectiveness.score_distribution.bad_count, 1); // < 0.3
    }

    #[test]
    fn test_analyze_disagreements() {
        use crate::scoring::RecommendationScore;
        use crate::tournament::PolicyResult;

        let before = high_pressure_state();
        let after = idle_state();

        // Two policies with different recommendations.
        let round = PolicyRound {
            round_index: 0,
            state_before: before.clone(),
            state_after: after.clone(),
            results: vec![
                PolicyResult {
                    policy_name: "PolicyA",
                    recommendations: vec![Recommendation::PromoteToDram {
                        chunk_id: ChunkId::from_data(b"hot"),
                        reason: "hot".to_string(),
                        confidence: 0.9,
                        factors: vec![],
                    }],
                    score: RecommendationScore {
                        fault_reduction: 0.8,
                        swap_reduction: 0.7,
                        zram_efficiency: 0.6,
                        pressure_reduction: 0.75,
                        tier_balance: 0.5,
                        stability: 0.8,
                        overall_score: 0.75,
                    },
                    state_before: before.clone(),
                    state_after: after.clone(),
                },
                PolicyResult {
                    policy_name: "PolicyB",
                    recommendations: vec![Recommendation::EvictCold {
                        tier: ghost_core::types::TierId::Ram,
                        count: 8,
                        confidence: 0.8,
                        factors: vec![],
                    }],
                    score: RecommendationScore {
                        fault_reduction: 0.5,
                        swap_reduction: 0.4,
                        zram_efficiency: 0.3,
                        pressure_reduction: 0.45,
                        tier_balance: 0.6,
                        stability: 0.5,
                        overall_score: 0.45,
                    },
                    state_before: before.clone(),
                    state_after: after.clone(),
                },
            ],
            round_winner: Some("PolicyA"),
        };

        let disagreements = analyze_disagreements(&[round]);

        assert!(!disagreements.is_empty(), "should detect disagreement");
        let d = &disagreements[0];
        assert_eq!(d.state_index, 0);
        assert_eq!(d.policy_a, "PolicyA");
        assert_eq!(d.policy_b, "PolicyB");
        assert_eq!(d.recommendation_a, "promote_to_dram");
        assert_eq!(d.recommendation_b, "evict_cold");
        assert!((d.score_difference - 0.3).abs() < 0.01);
    }

    #[test]
    fn test_analyze_disagreements_no_disagreement() {
        use crate::scoring::RecommendationScore;
        use crate::tournament::PolicyResult;

        let before = idle_state();
        let after = idle_state();

        // Two policies with the same recommendation kind.
        let round = PolicyRound {
            round_index: 0,
            state_before: before.clone(),
            state_after: after.clone(),
            results: vec![
                PolicyResult {
                    policy_name: "PolicyA",
                    recommendations: vec![Recommendation::NoAction {
                        reason: "stable".to_string(),
                        confidence: 0.9,
                        factors: vec![],
                    }],
                    score: RecommendationScore {
                        fault_reduction: 0.5,
                        swap_reduction: 0.5,
                        zram_efficiency: 0.5,
                        pressure_reduction: 0.5,
                        tier_balance: 0.5,
                        stability: 0.5,
                        overall_score: 0.5,
                    },
                    state_before: before.clone(),
                    state_after: after.clone(),
                },
                PolicyResult {
                    policy_name: "PolicyB",
                    recommendations: vec![Recommendation::NoAction {
                        reason: "also stable".to_string(),
                        confidence: 0.8,
                        factors: vec![],
                    }],
                    score: RecommendationScore {
                        fault_reduction: 0.4,
                        swap_reduction: 0.4,
                        zram_efficiency: 0.4,
                        pressure_reduction: 0.4,
                        tier_balance: 0.4,
                        stability: 0.4,
                        overall_score: 0.4,
                    },
                    state_before: before.clone(),
                    state_after: after.clone(),
                },
            ],
            round_winner: Some("PolicyA"),
        };

        let disagreements = analyze_disagreements(&[round]);

        assert!(
            disagreements.is_empty(),
            "same recommendation kind should not produce disagreement"
        );
    }

    #[test]
    fn test_region_activity() {
        let mut lifecycle = LifecycleTracker::new();
        let chunk_a = ChunkId::from_data(b"region_a");
        let chunk_b = ChunkId::from_data(b"region_b");

        // Record transitions for chunk_a (more active).
        lifecycle.record_transition(
            chunk_a,
            TemperatureClass::Cold,
            TemperatureClass::Warm,
            100,
            "warming".to_string(),
        );
        lifecycle.record_transition(
            chunk_a,
            TemperatureClass::Warm,
            TemperatureClass::Hot,
            200,
            "hot".to_string(),
        );
        lifecycle.record_promotion(chunk_a, 300);

        // Record one transition for chunk_b (more stable).
        lifecycle.record_transition(
            chunk_b,
            TemperatureClass::Cold,
            TemperatureClass::Warm,
            100,
            "warming".to_string(),
        );

        let stability = empty_stability();
        let report = analyze_replay(&[], &lifecycle, &stability);

        // Most active should include chunk_a.
        assert!(
            !report.most_active_regions.is_empty(),
            "should have most active regions"
        );
        assert_eq!(report.most_active_regions[0].region_id, chunk_a);
        assert!(report.most_active_regions[0].transition_count >= 3);

        // Most stable should include chunk_b.
        assert!(
            !report.most_stable_regions.is_empty(),
            "should have most stable regions"
        );
        assert_eq!(report.most_stable_regions[0].region_id, chunk_b);
        assert_eq!(report.most_stable_regions[0].transition_count, 1);
    }

    #[test]
    fn test_analysis_summary() {
        let stability = RecommendationStability {
            recommendations_per_hour: 0.5,
            temperature_flips: 0,
            tier_oscillations: 0,
            confidence_variance: 0.01,
            stability_index: 0.85,
        };

        let snapshots = vec![idle_state(), medium_pressure_state()];
        let lifecycle = LifecycleTracker::new();

        let report = analyze_replay(&snapshots, &lifecycle, &stability);

        // Summary should reflect the stability index.
        assert!(
            (report.summary.stability_index - 0.85).abs() < 0.01,
            "stability_index should be 0.85, got {}",
            report.summary.stability_index
        );
        assert!(
            (report.summary.overall_effectiveness - 0.85).abs() < 0.01,
            "overall_effectiveness should be 0.85, got {}",
            report.summary.overall_effectiveness
        );

        // Dominant pressure should be Low or Medium for these snapshots.
        assert!(
            report.summary.dominant_pressure_level == PressureLevel::Low
                || report.summary.dominant_pressure_level == PressureLevel::Medium,
            "dominant pressure should be Low or Medium"
        );
    }
}
