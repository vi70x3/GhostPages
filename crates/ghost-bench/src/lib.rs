//! Benchmarking and workload validation for GhostPages policy evaluation.
//!
//! This crate provides synthetic workload definitions, policy comparison runs,
//! benchmark reports, policy experiments, and a persistent leaderboard.

pub mod comparison;
pub mod experiment;
pub mod leaderboard;
pub mod report;
pub mod runner;
pub mod workload;

// Re-exports
pub use comparison::{
    run_policy_comparison, run_workload_comparison, PolicyComparisonRun, WorkloadComparison,
};
pub use experiment::{
    hybrid_weight_experiment, pressure_weight_experiment, run_experiment,
    temperature_threshold_experiment, ExperimentResult, PolicyExperiment,
};
pub use leaderboard::{from_report, LeaderboardEntry, PolicyLeaderboard};
pub use report::{
    format_report_json, format_report_markdown, generate_report, BenchmarkReport,
    BenchmarkSummary, PolicyRankEntry, PolicyResultSummary, WorkloadResultSummary,
};
pub use runner::BenchmarkRunner;
pub use workload::{
    all_builtin_workloads, allocator_stress, build_server, database_cache, idle_desktop,
    memory_pressure_ramp, mixed_multitask, tier_saturation, PressureTimeDistribution,
    ScenarioMetadata, TimedSnapshot, WorkloadClass, WorkloadDefinition, WorkloadGenerator,
    WorkloadScenario,
};
