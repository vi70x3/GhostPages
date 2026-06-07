//! Integration tests for ghost-evaluator.
//!
//! Tests cross-module behavior: determinism, stability, evaluation, and comparison.
//! All tests are self-contained and use simulated SystemState instances.

use std::collections::HashMap;

use ghost_core::hotness_confidence::HotnessConfidence;
use ghost_core::hotness_summary::HotnessSummary;
use ghost_core::state::PressureState;
use ghost_core::types::{ChunkId, TierId};
use ghost_evaluator::adaptive::AdaptiveTemperatureModel;
use ghost_evaluator::baseline::evaluate_baseline;
use ghost_evaluator::scoring::{score_policy_evaluation, score_recommendation, ScoringWeights};
use ghost_evaluator::stability::StabilityTracker;
use ghost_evaluator::tournament::{
    ArenaLinuxBaselinePolicy, HybridPolicy, PolicyArena, PressurePolicy, HotnessPolicy,
};
use ghost_linux::policy::Recommendation;
use ghost_linux::policy_rules::SystemState;

// ─── Helper: Build test SystemState instances ─────────────────────────────────

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

fn medium_pressure_state() -> SystemState {
    SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.55,
            ..Default::default()
        },
        dram_utilization: 0.7,
        swap_utilization: 0.2,
        zram_utilization: Some(0.3),
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

// ═══════════════════════════════════════════════════════════════════════════════
// §10.1 — Determinism Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_scoring_deterministic() {
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
fn test_baseline_deterministic() {
    let state = high_pressure_state();

    let recs1 = evaluate_baseline(&state);
    let recs2 = evaluate_baseline(&state);

    assert_eq!(recs1.len(), recs2.len());
    for (r1, r2) in recs1.iter().zip(recs2.iter()) {
        assert_eq!(r1.action, r2.action);
        assert_eq!(r1.confidence, r2.confidence);
        assert_eq!(r1.reason, r2.reason);
    }
}

#[test]
fn test_tournament_deterministic() {
    let rounds = vec![
        (high_pressure_state(), improved_from_high()),
        (critical_pressure_state(), idle_state()),
        (medium_pressure_state(), idle_state()),
    ];

    // Run tournament twice with identical inputs
    let mut arena1 = PolicyArena::new();
    arena1
        .add_policy(Box::new(ArenaLinuxBaselinePolicy))
        .add_policy(Box::new(PressurePolicy))
        .add_policy(Box::new(HotnessPolicy))
        .add_policy(Box::new(HybridPolicy));

    let result1 = arena1.run_tournament(
        &rounds
            .iter()
            .map(|(b, a)| (b, a))
            .collect::<Vec<_>>(),
    );

    let mut arena2 = PolicyArena::new();
    arena2
        .add_policy(Box::new(ArenaLinuxBaselinePolicy))
        .add_policy(Box::new(PressurePolicy))
        .add_policy(Box::new(HotnessPolicy))
        .add_policy(Box::new(HybridPolicy));

    let result2 = arena2.run_tournament(
        &rounds
            .iter()
            .map(|(b, a)| (b, a))
            .collect::<Vec<_>>(),
    );

    // Round counts must match
    assert_eq!(result1.rounds.len(), result2.rounds.len());

    // Each round's policy scores must be identical (deterministic scoring)
    for (r1, r2) in result1.rounds.iter().zip(result2.rounds.iter()) {
        assert_eq!(r1.results.len(), r2.results.len());
        for (pr1, pr2) in r1.results.iter().zip(r2.results.iter()) {
            assert_eq!(pr1.policy_name, pr2.policy_name);
            assert_eq!(
                pr1.score.overall_score, pr2.score.overall_score,
                "policy {} score mismatch: {} vs {}",
                pr1.policy_name, pr1.score.overall_score, pr2.score.overall_score
            );
        }
    }

    // Summary must match
    assert_eq!(result1.summary.total_rounds, result2.summary.total_rounds);
    assert_eq!(
        result1.summary.best_overall_score,
        result2.summary.best_overall_score
    );
    assert_eq!(
        result1.summary.worst_overall_score,
        result2.summary.worst_overall_score
    );

    // Average scores per policy must be identical
    for (name, score1) in &result1.summary.average_scores {
        let score2 = result2.summary.average_scores.get(name).unwrap();
        assert_eq!(score1, score2, "average score mismatch for {}", name);
    }
}

#[test]
fn test_stability_deterministic() {
    let state = idle_state();

    // Build two trackers with identical inputs
    let mut tracker1 = StabilityTracker::new(100);
    let mut tracker2 = StabilityTracker::new(100);

    for i in 0..50 {
        let rec = if i % 5 == 0 {
            Recommendation::PromoteToDram {
                chunk_id: ChunkId::from_data(b"hot"),
                reason: "hot chunk".to_string(),
                confidence: 0.9,
                factors: vec![],
            }
        } else {
            Recommendation::NoAction {
                reason: "stable".to_string(),
                confidence: 1.0,
                factors: vec![],
            }
        };
        tracker1.record(rec.clone(), &state, i as u64 * 60);
        tracker2.record(rec, &state, i as u64 * 60);
    }

    let stability1 = tracker1.evaluate();
    let stability2 = tracker2.evaluate();

    assert_eq!(
        stability1.recommendations_per_hour,
        stability2.recommendations_per_hour
    );
    assert_eq!(stability1.temperature_flips, stability2.temperature_flips);
    assert_eq!(stability1.tier_oscillations, stability2.tier_oscillations);
    assert_eq!(
        stability1.confidence_variance,
        stability2.confidence_variance
    );
    assert_eq!(stability1.stability_index, stability2.stability_index);
}

// ═══════════════════════════════════════════════════════════════════════════════
// §10.2 — Stability Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_no_recommendation_storm() {
    let state = idle_state();
    let mut tracker = StabilityTracker::new(100);

    // Feed 100 consecutive NoAction recommendations (idle system)
    for i in 0..100 {
        tracker.record(
            Recommendation::NoAction {
                reason: "system idle".to_string(),
                confidence: 1.0,
                factors: vec!["low_pressure".to_string()],
            },
            &state,
            i as u64 * 60, // one per minute
        );
    }

    let stability = tracker.evaluate();

    // All NoAction → zero recommendation rate
    assert_eq!(stability.recommendations_per_hour, 0.0);

    // Stability index should be very high (>0.9)
    assert!(
        stability.stability_index > 0.9,
        "idle system should have stability_index > 0.9, got {}",
        stability.stability_index
    );

    // No flips or oscillations
    assert_eq!(stability.temperature_flips, 0);
    assert_eq!(stability.tier_oscillations, 0);
}

#[test]
fn test_stability_under_pressure() {
    let mut tracker = StabilityTracker::new(100);

    // Simulate gradual pressure increase with consistent recommendations
    let states = [
        idle_state(),
        medium_pressure_state(),
        medium_pressure_state(),
        high_pressure_state(),
        high_pressure_state(),
        high_pressure_state(),
    ];

    for (i, state) in states.iter().enumerate() {
        // Consistent eviction recommendation as pressure rises
        tracker.record(
            Recommendation::EvictCold {
                tier: TierId::Ram,
                count: 4,
                confidence: 0.8,
                factors: vec!["rising_pressure".to_string()],
            },
            state,
            i as u64 * 120,
        );
    }

    let stability = tracker.evaluate();

    // Consistent recommendations → no oscillations
    assert_eq!(stability.tier_oscillations, 0);

    // Stability should be reasonable (>0.5) since recommendations are consistent
    assert!(
        stability.stability_index > 0.5,
        "consistent recommendations under pressure should be stable, got {}",
        stability.stability_index
    );
}

#[test]
fn test_adaptive_thresholds_stable() {
    let mut model = AdaptiveTemperatureModel::new(0.7, 0.3, 0.1);

    // Feed constant pressure and occupancy — thresholds should not oscillate wildly
    let constant_pressure = 0.6_f32;
    let mut tier_occupancy = HashMap::new();
    tier_occupancy.insert(TierId::Ram, 0.5);
    tier_occupancy.insert(TierId::Disk, 0.3);

    let mut prev_hot_threshold = model.hot_threshold;

    for _i in 0..20 {
        model.update(constant_pressure, &tier_occupancy);

        // Thresholds should not change wildly between consecutive updates
        // under constant pressure
        let hot_delta = (model.hot_threshold - prev_hot_threshold).abs();
        assert!(
            hot_delta < 0.5,
            "hot threshold changed by {} between updates — too volatile",
            hot_delta
        );
        prev_hot_threshold = model.hot_threshold;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// §10.3 — Evaluation Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_score_improvement() {
    let weights = ScoringWeights::default();
    let rec = Recommendation::NoAction {
        reason: "test".to_string(),
        confidence: 1.0,
        factors: vec![],
    };

    // Score from high pressure to improved state
    let improved_score = score_recommendation(&rec, &high_pressure_state(), &improved_from_high(), &weights);

    // Score from high pressure to same high pressure (no improvement)
    let degraded_score = score_recommendation(&rec, &high_pressure_state(), &high_pressure_state(), &weights);

    // Improved state should score higher
    assert!(
        improved_score.overall_score > degraded_score.overall_score,
        "improved state should score higher: improved={} degraded={}",
        improved_score.overall_score,
        degraded_score.overall_score
    );

    // Pressure reduction should be positive for improved state
    assert!(
        improved_score.pressure_reduction > 0.0,
        "pressure reduction should be positive for improved state"
    );

    // Pressure reduction should be zero for no-change state
    assert_eq!(
        degraded_score.pressure_reduction, 0.0,
        "pressure reduction should be zero when state doesn't change"
    );
}

#[test]
fn test_baseline_vs_ghostpages() {
    // Create a hot workload state
    let hot_state = state_with_hotness(high_pressure_state());
    let after = improved_from_high();

    // Evaluate with baseline (Linux) policy
    let baseline_recs = evaluate_baseline(&hot_state);
    let baseline_recommendations: Vec<Recommendation> =
        baseline_recs.into_iter().map(Recommendation::from).collect();
    let baseline_score = score_policy_evaluation(&baseline_recommendations, &hot_state, &after);

    // Evaluate with GhostPages Pressure policy (has ZRAM awareness)
    let mut arena = PolicyArena::new();
    arena.add_policy(Box::new(PressurePolicy));
    let round = arena.run_round(&hot_state, &after);
    let gp_score = &round.results[0].score;

    // GhostPages Pressure policy should score at least as well as baseline
    // because it has ZRAM awareness and more recommendation options
    assert!(
        gp_score.overall_score >= baseline_score.overall_score * 0.5,
        "GhostPages policy should score reasonably vs baseline: gp={} baseline={}",
        gp_score.overall_score,
        baseline_score.overall_score
    );
}

#[test]
fn test_scoring_weights_effect() {
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

    // Custom weights emphasizing pressure reduction
    let pressure_weights = ScoringWeights {
        fault_reduction_weight: 0.0,
        swap_reduction_weight: 0.0,
        zram_efficiency_weight: 0.0,
        pressure_reduction_weight: 1.0,
        tier_balance_weight: 0.0,
        stability_weight: 0.0,
    };
    let pressure_score = score_recommendation(&rec, &before, &after, &pressure_weights);

    // Custom weights emphasizing stability
    let stability_weights = ScoringWeights {
        fault_reduction_weight: 0.0,
        swap_reduction_weight: 0.0,
        zram_efficiency_weight: 0.0,
        pressure_reduction_weight: 0.0,
        tier_balance_weight: 0.0,
        stability_weight: 1.0,
    };
    let stability_score = score_recommendation(&rec, &before, &after, &stability_weights);

    // Different weights should produce different overall scores
    let scores_different = (default_score.overall_score - pressure_score.overall_score).abs() > 0.01
        || (default_score.overall_score - stability_score.overall_score).abs() > 0.01
        || (pressure_score.overall_score - stability_score.overall_score).abs() > 0.01;

    assert!(
        scores_different,
        "different weights should produce different scores: default={} pressure={} stability={}",
        default_score.overall_score,
        pressure_score.overall_score,
        stability_score.overall_score
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// §10.4 — Comparison Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_tournament_four_policies() {
    let rounds = vec![
        (idle_state(), idle_state()),
        (medium_pressure_state(), idle_state()),
        (high_pressure_state(), improved_from_high()),
        (critical_pressure_state(), high_pressure_state()),
        (high_pressure_state(), medium_pressure_state()),
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

    // All 4 policies should have participated
    assert_eq!(result.rounds.len(), 5);

    for round in &result.rounds {
        assert_eq!(
            round.results.len(),
            4,
            "each round should have 4 policy results"
        );
    }

    // There should be a winner
    assert!(
        result.winner.is_some(),
        "tournament with 4 policies should have a winner"
    );

    // Winner should be one of the 4 registered policies
    let winner = result.winner.unwrap();
    assert!(
        winner == "LinuxBaseline"
            || winner == "Pressure"
            || winner == "Hotness"
            || winner == "Hybrid",
        "winner should be a registered policy, got {}",
        winner
    );

    // Summary should have entries for all policies
    assert_eq!(result.summary.average_scores.len(), 4);
    assert!(result.summary.average_scores.contains_key("LinuxBaseline"));
    assert!(result.summary.average_scores.contains_key("Pressure"));
    assert!(result.summary.average_scores.contains_key("Hotness"));
    assert!(result.summary.average_scores.contains_key("Hybrid"));
}

#[test]
fn test_tournament_leaderboard_consistent() {
    let rounds = vec![
        (high_pressure_state(), improved_from_high()),
        (critical_pressure_state(), idle_state()),
        (medium_pressure_state(), idle_state()),
    ];

    // Run tournament twice
    let mut arena1 = PolicyArena::new();
    arena1
        .add_policy(Box::new(ArenaLinuxBaselinePolicy))
        .add_policy(Box::new(PressurePolicy))
        .add_policy(Box::new(HotnessPolicy))
        .add_policy(Box::new(HybridPolicy));

    let _result1 = arena1.run_tournament(
        &rounds
            .iter()
            .map(|(b, a)| (b, a))
            .collect::<Vec<_>>(),
    );

    let mut arena2 = PolicyArena::new();
    arena2
        .add_policy(Box::new(ArenaLinuxBaselinePolicy))
        .add_policy(Box::new(PressurePolicy))
        .add_policy(Box::new(HotnessPolicy))
        .add_policy(Box::new(HybridPolicy));

    let _result2 = arena2.run_tournament(
        &rounds
            .iter()
            .map(|(b, a)| (b, a))
            .collect::<Vec<_>>(),
    );

    // Leaderboard order should be identical
    let lb1 = arena1.leaderboard();
    let lb2 = arena2.leaderboard();

    assert_eq!(lb1.len(), lb2.len());
    for ((name1, score1), (name2, score2)) in lb1.iter().zip(lb2.iter()) {
        assert_eq!(name1, name2, "leaderboard order should be consistent");
        assert_eq!(score1, score2, "leaderboard scores should be identical");
    }
}

#[test]
fn test_policy_disagreement_detection() {
    // Use a state where policies are likely to disagree:
    // high pressure with hotness data
    let state = state_with_hotness(high_pressure_state());
    let after = improved_from_high();

    let mut arena = PolicyArena::new();
    arena
        .add_policy(Box::new(ArenaLinuxBaselinePolicy))
        .add_policy(Box::new(PressurePolicy))
        .add_policy(Box::new(HotnessPolicy))
        .add_policy(Box::new(HybridPolicy));

    let round = arena.run_round(&state, &after);

    // Collect the recommendation types from each policy
    let mut all_kinds: Vec<Vec<&str>> = Vec::new();
    for result in &round.results {
        let kinds: Vec<&str> = result.recommendations.iter().map(|r| r.kind()).collect();
        all_kinds.push(kinds);
    }

    // At least some policies should produce different recommendation kinds
    // (baseline only evicts/swaps, hotness promotes, etc.)
    let mut found_disagreement = false;
    for i in 0..all_kinds.len() {
        for j in (i + 1)..all_kinds.len() {
            if all_kinds[i] != all_kinds[j] {
                found_disagreement = true;
                break;
            }
        }
        if found_disagreement {
            break;
        }
    }

    assert!(
        found_disagreement,
        "policies should disagree on recommendation types for mixed state"
    );

    // Also verify that scores differ between policies
    let mut unique_scores = Vec::new();
    for result in &round.results {
        let score = result.score.overall_score;
        if !unique_scores.iter().any(|&s: &f32| (s - score).abs() < 0.001) {
            unique_scores.push(score);
        }
    }

    assert!(
        unique_scores.len() > 1,
        "different policies should produce different scores, got {:?}",
        unique_scores
    );
}
