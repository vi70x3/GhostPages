//! # ghost-evaluator
//!
//! Recommendation scoring and evaluation for GhostPages.
//!
//! This crate provides pure, deterministic scoring functions that evaluate
//! whether GhostPages recommendations are actually useful before implementing
//! real migration. All scoring functions are pure — same inputs always
//! produce same outputs. No I/O, no mutation.

pub mod adaptive;
pub mod baseline;
pub mod evaluator_metrics;
pub mod lifecycle;
pub mod replay_analytics;
pub mod scoring;
pub mod stability;
pub mod tournament;

pub use adaptive::{AdaptiveTemperatureModel, TemperatureClass, TemperatureThresholds};
pub use baseline::{
    BaselineAction, BaselineRecommendation, LinuxBaselinePolicy, evaluate_baseline,
};
pub use evaluator_metrics::EvaluatorMetrics;
pub use lifecycle::{
    LifecycleSummary, LifecycleTracker, RegionLifecycle, TemperatureTransition,
};
pub use replay_analytics::{
    AnalysisSummary, PolicyDisagreement, PressureProfile, RecommendationEffectiveness,
    RegionActivity, ReplayAnalysisReport, ScoreDistribution, analyze_disagreements,
    analyze_replay, compute_effectiveness, compute_pressure_profile,
};
pub use scoring::{
    RecommendationScore, ScoringWeights, score_fault_reduction, score_pressure_reduction,
    score_recommendation, score_stability, score_swap_reduction, score_tier_balance,
    score_zram_efficiency, score_policy_evaluation,
};
pub use stability::{RecommendationStability, StabilityEntry, StabilityTracker};
pub use tournament::{
    ArenaLinuxBaselinePolicy, HybridPolicy, Policy, PolicyArena, PolicyResult,
    PolicyRound, PressurePolicy, HotnessPolicy, TournamentResult, TournamentSummary,
};
