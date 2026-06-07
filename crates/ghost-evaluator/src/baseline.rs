//! Baseline engine representing Linux's default memory behavior.
//!
//! This module provides a deliberately simplistic policy that mimics how
//! vanilla Linux handles memory pressure — no hotness awareness, no ZRAM
//! awareness, no proactive promotion. It serves as a comparison baseline
//! for GhostPages' intelligent recommendations.
//!
//! All functions are **pure** — no I/O, no mutation, no side effects.
//! Same inputs always produce same outputs. Deterministic by design.

use ghost_core::types::TierId;
use ghost_linux::policy::Recommendation;
use ghost_linux::policy_rules::{PressureLevel, SystemState};

// ─── Baseline Action ──────────────────────────────────────────────────────────

/// Actions that the Linux baseline policy can recommend.
///
/// This is a simplified subset of `Recommendation` — Linux only knows
/// how to evict, swap out, or do nothing. It does not have ZRAM or
/// proactive promotion concepts.
#[derive(Debug, Clone, PartialEq)]
pub enum BaselineAction {
    /// Linux reclaims pages (LRU-style eviction).
    Evict,
    /// Linux swaps pages out to disk swap.
    SwapOut,
    /// Linux does nothing — system is fine.
    NoAction,
}

// ─── Baseline Recommendation ──────────────────────────────────────────────────

/// A simplified recommendation from the baseline engine.
///
/// Unlike the full `Recommendation` which contains chunk-level detail,
/// baseline recommendations are coarse-grained: they indicate *what*
/// Linux would do, not *which specific chunk* it would act on.
#[derive(Debug, Clone, PartialEq)]
pub struct BaselineRecommendation {
    /// The action Linux would take.
    pub action: BaselineAction,
    /// Confidence in the action (0.0–1.0).
    pub confidence: f32,
    /// Human-readable reason for the recommendation.
    pub reason: String,
}

impl BaselineRecommendation {
    /// Create a new baseline recommendation.
    pub fn new(action: BaselineAction, confidence: f32, reason: impl Into<String>) -> Self {
        Self {
            action,
            confidence,
            reason: reason.into(),
        }
    }
}

// ─── Conversion to Recommendation ─────────────────────────────────────────────

/// Convert a `BaselineRecommendation` into a `Recommendation` for scoring.
///
/// This allows the baseline output to be evaluated using the same
/// `score_policy_evaluation` function as GhostPages recommendations.
impl From<BaselineRecommendation> for Recommendation {
    fn from(baseline: BaselineRecommendation) -> Self {
        match baseline.action {
            BaselineAction::Evict => Recommendation::EvictCold {
                tier: TierId::Ram,
                count: eviction_count_from_confidence(baseline.confidence),
                confidence: baseline.confidence,
                factors: vec![baseline.reason],
            },
            BaselineAction::SwapOut => Recommendation::MoveToDiskSwap {
                chunk_id: ghost_core::types::ChunkId::from_data(b"baseline_eviction"),
                reason: baseline.reason,
                confidence: baseline.confidence,
                factors: vec!["pressure_reactive".to_string()],
            },
            BaselineAction::NoAction => Recommendation::NoAction {
                reason: baseline.reason,
                confidence: baseline.confidence,
                factors: vec![],
            },
        }
    }
}

/// Estimate eviction count from confidence level.
/// Higher confidence = more aggressive eviction.
fn eviction_count_from_confidence(confidence: f32) -> usize {
    if confidence >= 0.9 {
        16
    } else if confidence >= 0.7 {
        8
    } else {
        4
    }
}

// ─── Linux Baseline Policy ────────────────────────────────────────────────────

/// A policy that mimics Linux's default memory behavior.
///
/// This policy is intentionally simplistic:
/// - No hotness awareness (ignores `hotness_summary` and `hotness_confidence`)
/// - No ZRAM awareness (never generates `MoveToZram`)
/// - No proactive promotion (never generates `PromoteToDram`)
/// - Pressure-reactive only: acts when pressure exceeds thresholds
/// - Uses simple utilization thresholds for decision-making
#[derive(Debug, Clone, Copy, Default)]
pub struct LinuxBaselinePolicy;

impl LinuxBaselinePolicy {
    /// Create a new baseline policy.
    pub fn new() -> Self {
        Self
    }

    /// Evaluate the system state and produce baseline recommendations.
    ///
    /// This is the primary entry point. It classifies the pressure level
    /// and generates appropriate recommendations matching Linux's behavior.
    pub fn evaluate(&self, state: &SystemState) -> Vec<BaselineRecommendation> {
        let pressure = state.pressure_level();

        match pressure {
            PressureLevel::Low => self.evaluate_low(state),
            PressureLevel::Medium => self.evaluate_medium(state),
            PressureLevel::High => self.evaluate_high(state),
            PressureLevel::Critical => self.evaluate_critical(state),
        }
    }

    /// Low pressure: Linux does nothing.
    fn evaluate_low(&self, _state: &SystemState) -> Vec<BaselineRecommendation> {
        vec![BaselineRecommendation::new(
            BaselineAction::NoAction,
            0.95,
            "system idle — no action needed",
        )]
    }

    /// Medium pressure: maybe light eviction if DRAM is getting full.
    fn evaluate_medium(&self, state: &SystemState) -> Vec<BaselineRecommendation> {
        if state.dram_utilization > 0.8 {
            // Linux starts considering eviction when DRAM is >80%
            vec![BaselineRecommendation::new(
                BaselineAction::Evict,
                0.6,
                "moderate pressure with high DRAM utilization — light eviction",
            )]
        } else {
            vec![BaselineRecommendation::new(
                BaselineAction::NoAction,
                0.8,
                "moderate pressure but DRAM utilization acceptable",
            )]
        }
    }

    /// High pressure: eviction + swap-out.
    fn evaluate_high(&self, state: &SystemState) -> Vec<BaselineRecommendation> {
        let mut recs = Vec::new();

        // Linux evicts cold pages when pressure is high
        recs.push(BaselineRecommendation::new(
            BaselineAction::Evict,
            0.8,
            "high pressure — evicting cold pages",
        ));

        // If swap utilization is also high, Linux may swap out
        if state.swap_utilization > 0.8 {
            recs.push(BaselineRecommendation::new(
                BaselineAction::SwapOut,
                0.7,
                "high pressure with high swap — swapping out pages",
            ));
        }

        recs
    }

    /// Critical pressure: aggressive eviction + swap-out.
    fn evaluate_critical(&self, state: &SystemState) -> Vec<BaselineRecommendation> {
        let mut recs = Vec::new();

        // Aggressive eviction under critical pressure
        recs.push(BaselineRecommendation::new(
            BaselineAction::Evict,
            0.95,
            "critical pressure — aggressive eviction",
        ));

        // Swap out regardless of swap utilization when critical
        recs.push(BaselineRecommendation::new(
            BaselineAction::SwapOut,
            0.85,
            "critical pressure — swapping out pages",
        ));

        // If swap is also nearly full, even more aggressive swap
        if state.swap_utilization > 0.8 {
            recs.push(BaselineRecommendation::new(
                BaselineAction::SwapOut,
                0.9,
                "critical pressure with high swap — emergency swap-out",
            ));
        }

        recs
    }
}

// ─── Evaluate Baseline (convenience function) ─────────────────────────────────

/// Evaluate system state as Linux would, returning baseline recommendations.
///
/// This is a pure function — the primary public API for the baseline engine.
pub fn evaluate_baseline(state: &SystemState) -> Vec<BaselineRecommendation> {
    LinuxBaselinePolicy::new().evaluate(state)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
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

    fn medium_pressure_high_dram_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.55,
                ..Default::default()
            },
            dram_utilization: 0.85,
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

    fn high_pressure_high_swap_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.8,
                ..Default::default()
            },
            dram_utilization: 0.85,
            swap_utilization: 0.85,
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

    fn critical_pressure_high_swap_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.95,
                ..Default::default()
            },
            dram_utilization: 0.97,
            swap_utilization: 0.9,
            zram_utilization: Some(0.6),
            io_pressure: PressureState::default(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn state_with_hotness(state: SystemState) -> SystemState {
        SystemState {
            hotness_summary: Some(ghost_core::hotness_summary::HotnessSummary {
                hot_count: 10,
                warm_count: 20,
                cold_count: 30,
                frozen_count: 0,
                total_regions: 60,
                hot_percentage: 16.7,
                warm_percentage: 33.3,
                cold_percentage: 50.0,
                frozen_percentage: 0.0,
                avg_access_count: 100,
                max_access_count: 1000,
                min_access_count: 0,
            }),
            hotness_confidence: Some(ghost_core::hotness_confidence::HotnessConfidence {
                score: 0.9,
                factors: vec![],
            }),
            ..state
        }
    }

    // ── Required tests ──

    #[test]
    fn test_baseline_idle_system() {
        let state = idle_state();
        let recs = evaluate_baseline(&state);

        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].action, BaselineAction::NoAction);
        assert!(recs[0].confidence > 0.9);
    }

    #[test]
    fn test_baseline_high_pressure() {
        let state = high_pressure_state();
        let recs = evaluate_baseline(&state);

        // High pressure should produce eviction
        assert!(
            recs.iter().any(|r| r.action == BaselineAction::Evict),
            "high pressure should produce eviction recommendation"
        );

        // With swap_utilization > 0.8, should also produce swap-out
        let high_swap_state = high_pressure_high_swap_state();
        let high_swap_recs = evaluate_baseline(&high_swap_state);
        assert!(
            high_swap_recs
                .iter()
                .any(|r| r.action == BaselineAction::SwapOut),
            "high pressure with high swap should produce swap-out"
        );
    }

    #[test]
    fn test_baseline_critical_pressure() {
        let state = critical_pressure_state();
        let recs = evaluate_baseline(&state);

        // Critical pressure should always produce eviction
        assert!(
            recs.iter().any(|r| r.action == BaselineAction::Evict),
            "critical pressure should produce eviction"
        );

        // Critical pressure should always produce swap-out
        assert!(
            recs.iter().any(|r| r.action == BaselineAction::SwapOut),
            "critical pressure should produce swap-out"
        );

        // Eviction confidence should be very high
        let evict_rec = recs
            .iter()
            .find(|r| r.action == BaselineAction::Evict)
            .unwrap();
        assert!(
            evict_rec.confidence >= 0.9,
            "critical eviction confidence should be >= 0.9"
        );

        // With high swap, should have extra swap recommendation
        let high_swap_state = critical_pressure_high_swap_state();
        let high_swap_recs = evaluate_baseline(&high_swap_state);
        let swap_count = high_swap_recs
            .iter()
            .filter(|r| r.action == BaselineAction::SwapOut)
            .count();
        assert!(
            swap_count >= 2,
            "critical + high swap should have multiple swap recommendations, got {}",
            swap_count
        );
    }

    #[test]
    fn test_baseline_no_hotness_awareness() {
        let base_state = high_pressure_state();
        let hot_state = state_with_hotness(base_state.clone());

        let base_recs = evaluate_baseline(&base_state);
        let hot_recs = evaluate_baseline(&hot_state);

        // Baseline should produce identical results regardless of hotness data
        assert_eq!(
            base_recs.len(),
            hot_recs.len(),
            "hotness should not affect recommendation count"
        );

        for (base_rec, hot_rec) in base_recs.iter().zip(hot_recs.iter()) {
            assert_eq!(
                base_rec.action, hot_rec.action,
                "hotness should not affect action"
            );
            assert_eq!(
                base_rec.confidence, hot_rec.confidence,
                "hotness should not affect confidence"
            );
            assert_eq!(
                base_rec.reason, hot_rec.reason,
                "hotness should not affect reason"
            );
        }
    }

    #[test]
    fn test_baseline_no_zram_awareness() {
        // Test with various ZRAM utilizations — baseline should never care
        let base = critical_pressure_state();

        let with_zram = SystemState {
            zram_utilization: Some(0.5),
            ..base.clone()
        };
        let without_zram = SystemState {
            zram_utilization: None,
            ..base.clone()
        };
        let full_zram = SystemState {
            zram_utilization: Some(0.95),
            ..base.clone()
        };

        let recs_with = evaluate_baseline(&with_zram);
        let recs_without = evaluate_baseline(&without_zram);
        let recs_full = evaluate_baseline(&full_zram);

        // All three should produce identical recommendations
        assert_eq!(recs_with.len(), recs_without.len());
        assert_eq!(recs_with.len(), recs_full.len());

        for ((a, b), c) in recs_with
            .iter()
            .zip(recs_without.iter())
            .zip(recs_full.iter())
        {
            assert_eq!(a.action, b.action);
            assert_eq!(b.action, c.action);
            assert_eq!(a.confidence, b.confidence);
            assert_eq!(b.confidence, c.confidence);
        }

        // Baseline should never generate MoveToZram
        for rec in &recs_with {
            let recommendation: Recommendation = rec.clone().into();
            assert!(
                !matches!(recommendation, Recommendation::MoveToZram { .. }),
                "baseline should never generate MoveToZram"
            );
        }
    }

    #[test]
    fn test_baseline_deterministic() {
        let state = high_pressure_state();

        // Run evaluation multiple times — should be identical
        for _ in 0..10 {
            let recs1 = evaluate_baseline(&state);
            let recs2 = evaluate_baseline(&state);

            assert_eq!(recs1.len(), recs2.len());
            for (r1, r2) in recs1.iter().zip(recs2.iter()) {
                assert_eq!(r1.action, r2.action);
                assert_eq!(r1.confidence, r2.confidence);
                assert_eq!(r1.reason, r2.reason);
            }
        }
    }

    #[test]
    fn test_baseline_scoreable() {
        use crate::scoring::score_policy_evaluation;

        let before = critical_pressure_state();
        let after = idle_state();

        let baseline_recs = evaluate_baseline(&before);

        // Convert baseline recommendations to Recommendations
        let recommendations: Vec<Recommendation> =
            baseline_recs.into_iter().map(Recommendation::from).collect();

        // Should be able to score without panic
        let score = score_policy_evaluation(&recommendations, &before, &after);

        // Score should be in valid range
        assert!(
            score.overall_score >= 0.0 && score.overall_score <= 1.0,
            "overall_score {} should be in [0.0, 1.0]",
            score.overall_score
        );
    }

    // ── Additional edge case tests ──

    #[test]
    fn test_baseline_medium_pressure_with_high_dram() {
        let state = medium_pressure_high_dram_state();
        let recs = evaluate_baseline(&state);

        // Medium pressure + DRAM > 0.8 should trigger eviction
        assert!(
            recs.iter().any(|r| r.action == BaselineAction::Evict),
            "medium pressure with high DRAM should trigger eviction"
        );
    }

    #[test]
    fn test_baseline_medium_pressure_low_dram() {
        let state = medium_pressure_state();
        let recs = evaluate_baseline(&state);

        // Medium pressure + DRAM < 0.8 should be NoAction
        assert!(
            recs.iter().all(|r| r.action == BaselineAction::NoAction),
            "medium pressure with low DRAM should be NoAction"
        );
    }

    #[test]
    fn test_baseline_no_promote_to_dram() {
        // Even with low pressure (where promotion might make sense),
        // baseline should never generate PromoteToDram
        let state = idle_state();
        let recs = evaluate_baseline(&state);

        for rec in &recs {
            let recommendation: Recommendation = rec.clone().into();
            assert!(
                !matches!(recommendation, Recommendation::PromoteToDram { .. }),
                "baseline should never generate PromoteToDram"
            );
        }
    }

    #[test]
    fn test_baseline_confidence_ranges() {
        let states = vec![
            idle_state(),
            medium_pressure_state(),
            high_pressure_state(),
            critical_pressure_state(),
        ];

        for state in &states {
            let recs = evaluate_baseline(state);
            for rec in &recs {
                assert!(
                    rec.confidence >= 0.0 && rec.confidence <= 1.0,
                    "confidence {} should be in [0.0, 1.0] for action {:?}",
                    rec.confidence,
                    rec.action
                );
            }
        }
    }

    #[test]
    fn test_baseline_eviction_count_from_confidence() {
        assert_eq!(eviction_count_from_confidence(0.95), 16);
        assert_eq!(eviction_count_from_confidence(0.9), 16);
        assert_eq!(eviction_count_from_confidence(0.7), 8);
        assert_eq!(eviction_count_from_confidence(0.5), 4);
        assert_eq!(eviction_count_from_confidence(0.0), 4);
    }

    #[test]
    fn test_baseline_conversion_to_recommendation() {
        let evict = BaselineRecommendation::new(BaselineAction::Evict, 0.8, "test eviction");
        let swap = BaselineRecommendation::new(BaselineAction::SwapOut, 0.7, "test swap");
        let no_action = BaselineRecommendation::new(BaselineAction::NoAction, 0.95, "test no action");

        let evict_rec: Recommendation = evict.into();
        assert!(matches!(
            evict_rec,
            Recommendation::EvictCold {
                tier: TierId::Ram,
                ..
            }
        ));

        let swap_rec: Recommendation = swap.into();
        assert!(matches!(
            swap_rec,
            Recommendation::MoveToDiskSwap { .. }
        ));

        let no_action_rec: Recommendation = no_action.into();
        assert!(matches!(no_action_rec, Recommendation::NoAction { .. }));
    }

    #[test]
    fn test_linux_baseline_policy_default() {
        let policy = LinuxBaselinePolicy::default();
        let state = idle_state();
        let recs = policy.evaluate(&state);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].action, BaselineAction::NoAction);
    }
}
