//! Policy experiment framework for GhostPages.
//!
//! Provides parameter sweep experiments to discover better policy evaluation
//! configurations. Experiments test **evaluation parameters** (scoring weights),
//! not policy internals.

use ghost_evaluator::scoring::ScoringWeights;
use ghost_evaluator::tournament::Policy;

use crate::comparison::run_policy_comparison;
use crate::workload::WorkloadScenario;

// ─── Experiment Result ────────────────────────────────────────────────────────

/// Result from testing one parameter value.
#[derive(Debug, Clone)]
pub struct ExperimentResult {
    /// The parameter value that was tested.
    pub parameter_value: f32,
    /// Average score across all workloads with this parameter value.
    pub average_score: f32,
    /// Win rate (fraction of workloads where this value was best).
    pub win_rate: f32,
    /// Stability index averaged across workloads.
    pub stability_index: f32,
}

// ─── Policy Experiment ────────────────────────────────────────────────────────

/// A policy experiment: varying parameters to discover better policies.
#[derive(Debug, Clone)]
pub struct PolicyExperiment {
    /// Name of this experiment.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Name of the parameter being varied (e.g., "pressure_weight").
    pub parameter_name: String,
    /// Default/baseline value.
    pub baseline_value: f32,
    /// Values to test.
    pub test_values: Vec<f32>,
    /// Results for each test value.
    pub results: Vec<ExperimentResult>,
    /// The value that produced the best score.
    pub best_value: f32,
    /// The best score achieved.
    pub best_score: f32,
    /// Improvement over baseline: (best - baseline) / baseline.
    pub improvement: f32,
}

// ─── Built-in Experiment Factories ─────────────────────────────────────────────

/// Create a temperature threshold experiment.
///
/// Note: This experiment tests how different scoring weight configurations
/// affect outcomes, since we cannot modify adaptive model thresholds from
/// outside the policy. Specifically, it sweeps the fault_reduction_weight
/// to find the optimal balance.
pub fn temperature_threshold_experiment() -> PolicyExperiment {
    PolicyExperiment {
        name: "temperature_threshold_sweep".to_string(),
        description: "Sweep fault reduction weight to find optimal scoring balance. \
                      Since adaptive model thresholds cannot be modified externally, \
                      this tests how scoring weight configuration affects outcomes."
            .to_string(),
        parameter_name: "fault_reduction_weight".to_string(),
        baseline_value: 0.25,
        test_values: vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
        results: Vec::new(),
        best_value: 0.25,
        best_score: 0.0,
        improvement: 0.0,
    }
}

/// Create a pressure weight experiment.
///
/// Sweeps the pressure reduction weight in scoring from 0.1 to 0.5.
pub fn pressure_weight_experiment() -> PolicyExperiment {
    PolicyExperiment {
        name: "pressure_weight_sweep".to_string(),
        description: "Sweep pressure reduction weight in scoring from 0.1 to 0.5 (baseline 0.25)."
            .to_string(),
        parameter_name: "pressure_reduction_weight".to_string(),
        baseline_value: 0.25,
        test_values: vec![0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5],
        results: Vec::new(),
        best_value: 0.25,
        best_score: 0.0,
        improvement: 0.0,
    }
}

/// Create a hybrid weight experiment.
///
/// Sweeps the hotness-vs-pressure blend ratio (swap_reduction_weight) from 0.0 to 1.0.
pub fn hybrid_weight_experiment() -> PolicyExperiment {
    PolicyExperiment {
        name: "hybrid_weight_sweep".to_string(),
        description:
            "Sweep swap reduction weight (hotness-vs-pressure blend) from 0.0 to 1.0 (baseline 0.20)."
                .to_string(),
        parameter_name: "swap_reduction_weight".to_string(),
        baseline_value: 0.20,
        test_values: vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
        results: Vec::new(),
        best_value: 0.20,
        best_score: 0.0,
        improvement: 0.0,
    }
}

// ─── Experiment Runner ────────────────────────────────────────────────────────

/// Run a policy experiment by varying the parameter and measuring the effect.
///
/// Creates modified `ScoringWeights` for each parameter value, runs the base
/// policy against all workloads with each weight configuration, and records
/// scores to determine the best parameter value.
pub fn run_experiment(
    experiment: &PolicyExperiment,
    workloads: &[WorkloadScenario],
    base_policy: &dyn Policy,
) -> PolicyExperiment {
    let mut results = Vec::new();
    let mut best_value = experiment.baseline_value;
    let mut best_score = 0.0_f32;

    for &value in &experiment.test_values {
        let weights = make_weights(&experiment.parameter_name, value);

        let mut total_score = 0.0_f32;
        let mut total_stability = 0.0_f32;
        let mut num_runs = 0usize;

        for workload in workloads {
            let run = run_policy_comparison(workload, base_policy, &weights);
            total_score += run.average_score.overall_score;
            total_stability += run.stability.stability_index;
            num_runs += 1;
        }

        let avg_score = if num_runs > 0 {
            total_score / num_runs as f32
        } else {
            0.0
        };
        let avg_stability = if num_runs > 0 {
            total_stability / num_runs as f32
        } else {
            0.0
        };

        // Win rate: fraction of workloads where this value scored highest
        // (computed later after all values are tested — for now use score as proxy)
        let win_rate = if best_score > 0.0 {
            if avg_score > best_score {
                1.0
            } else {
                0.0
            }
        } else {
            0.5
        };

        results.push(ExperimentResult {
            parameter_value: value,
            average_score: avg_score,
            win_rate,
            stability_index: avg_stability,
        });

        if avg_score > best_score {
            best_score = avg_score;
            best_value = value;
        }
    }

    // Recompute win rates: for each value, count how many workloads it wins
    if workloads.len() > 1 && experiment.test_values.len() > 1 {
        for (i, value) in experiment.test_values.iter().enumerate() {
            let mut wins = 0usize;
            for workload in workloads {
                // Find the score for this value on this workload
                let weights = make_weights(&experiment.parameter_name, *value);
                let run = run_policy_comparison(workload, base_policy, &weights);
                let this_score = run.average_score.overall_score;

                // Check if this is the best score across all values for this workload
                let is_best = experiment.test_values.iter().all(|other_val| {
                    let other_weights = make_weights(&experiment.parameter_name, *other_val);
                    let other_run = run_policy_comparison(workload, base_policy, &other_weights);
                    this_score >= other_run.average_score.overall_score
                });

                if is_best {
                    wins += 1;
                }
            }
            results[i].win_rate = if !workloads.is_empty() {
                wins as f32 / workloads.len() as f32
            } else {
                0.0
            };
        }
    }

    // Compute improvement over baseline
    let baseline_result = results.iter().find(|r| {
        (r.parameter_value - experiment.baseline_value).abs() < f32::EPSILON
    });
    let baseline_score = baseline_result.map(|r| r.average_score).unwrap_or(0.0);
    let improvement = if baseline_score > 0.0 {
        (best_score - baseline_score) / baseline_score
    } else {
        0.0
    };

    PolicyExperiment {
        name: experiment.name.clone(),
        description: experiment.description.clone(),
        parameter_name: experiment.parameter_name.clone(),
        baseline_value: experiment.baseline_value,
        test_values: experiment.test_values.clone(),
        results,
        best_value,
        best_score,
        improvement,
    }
}

/// Create modified ScoringWeights with the given parameter set to the given value.
fn make_weights(parameter_name: &str, value: f32) -> ScoringWeights {
    let mut weights = ScoringWeights::default();
    match parameter_name {
        "fault_reduction_weight" => {
            weights.fault_reduction_weight = value;
        }
        "pressure_reduction_weight" => {
            weights.pressure_reduction_weight = value;
        }
        "swap_reduction_weight" => {
            weights.swap_reduction_weight = value;
        }
        "zram_efficiency_weight" => {
            weights.zram_efficiency_weight = value;
        }
        "tier_balance_weight" => {
            weights.tier_balance_weight = value;
        }
        "stability_weight" => {
            weights.stability_weight = value;
        }
        _ => {}
    }
    weights
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_evaluator::tournament::PressurePolicy;

    fn make_test_workloads() -> Vec<WorkloadScenario> {
        use ghost_core::state::PressureState;
        use ghost_linux::policy_rules::SystemState;

        let make_scenario = |name: &str, pressure: f32| {
            let def = crate::workload::WorkloadDefinition {
                name: name.to_string(),
                class: crate::workload::WorkloadClass::MemoryPressure,
                description: "test".to_string(),
                duration_seconds: 4,
                snapshot_interval_ms: 2000,
                seed: 42,
            };

            let snapshots = vec![
                crate::workload::TimedSnapshot {
                    timestamp_ms: 0,
                    state: SystemState {
                        dram_pressure: PressureState {
                            memory_pressure: pressure,
                            ..PressureState::new()
                        },
                        dram_utilization: pressure + 0.05,
                        swap_utilization: 0.2,
                        zram_utilization: Some(0.3),
                        io_pressure: PressureState::new(),
                        hotness_summary: None,
                        hotness_confidence: None,
                    },
                },
                crate::workload::TimedSnapshot {
                    timestamp_ms: 2000,
                    state: SystemState {
                        dram_pressure: PressureState {
                            memory_pressure: (pressure - 0.2).max(0.0),
                            ..PressureState::new()
                        },
                        dram_utilization: (pressure - 0.15).max(0.0),
                        swap_utilization: 0.15,
                        zram_utilization: Some(0.4),
                        io_pressure: PressureState::new(),
                        hotness_summary: None,
                        hotness_confidence: None,
                    },
                },
            ];

            WorkloadScenario {
                definition: def,
                snapshots,
                metadata: crate::workload::ScenarioMetadata {
                    total_snapshots: 2,
                    peak_dram_pressure: pressure,
                    peak_dram_utilization: pressure + 0.05,
                    avg_dram_utilization: (pressure + (pressure - 0.15).max(0.0)) / 2.0,
                    avg_swap_utilization: 0.175,
                    pressure_time_distribution: crate::workload::PressureTimeDistribution {
                        idle_fraction: 0.0,
                        low_fraction: 0.5,
                        medium_fraction: 0.5,
                        high_fraction: 0.0,
                        critical_fraction: 0.0,
                    },
                },
            }
        };

        vec![
            make_scenario("high_pressure", 0.8),
            make_scenario("medium_pressure", 0.5),
        ]
    }

    #[test]
    fn test_temperature_threshold_experiment() {
        let exp = temperature_threshold_experiment();
        assert_eq!(exp.parameter_name, "fault_reduction_weight");
        assert_eq!(exp.baseline_value, 0.25);
        assert!(!exp.test_values.is_empty());
        assert!(exp.test_values.contains(&0.5));
    }

    #[test]
    fn test_pressure_weight_experiment() {
        let exp = pressure_weight_experiment();
        assert_eq!(exp.parameter_name, "pressure_reduction_weight");
        assert_eq!(exp.baseline_value, 0.25);
        assert!(exp.test_values.contains(&0.1));
        assert!(exp.test_values.contains(&0.5));
    }

    #[test]
    fn test_hybrid_weight_experiment() {
        let exp = hybrid_weight_experiment();
        assert_eq!(exp.parameter_name, "swap_reduction_weight");
        assert_eq!(exp.baseline_value, 0.20);
        assert!(exp.test_values.contains(&0.0));
        assert!(exp.test_values.contains(&1.0));
    }

    #[test]
    fn test_run_experiment() {
        let exp = pressure_weight_experiment();
        let workloads = make_test_workloads();
        let policy = PressurePolicy;

        let result = run_experiment(&exp, &workloads, &policy);

        assert_eq!(result.results.len(), exp.test_values.len());
        assert!(result.best_value >= 0.0);
        assert!(result.best_score >= 0.0);

        // All results should have valid scores
        for r in &result.results {
            assert!(r.average_score >= 0.0 && r.average_score <= 1.0);
            assert!(r.stability_index >= 0.0 && r.stability_index <= 1.0);
        }
    }

    #[test]
    fn test_experiment_best_value() {
        let exp = pressure_weight_experiment();
        let workloads = make_test_workloads();
        let policy = PressurePolicy;

        let result = run_experiment(&exp, &workloads, &policy);

        // Best value should be one of the test values
        assert!(
            result
                .test_values
                .iter()
                .any(|&v| (v - result.best_value).abs() < f32::EPSILON),
            "best_value {} should be in test_values",
            result.best_value
        );

        // Best score should match the result for best_value
        let best_result = result
            .results
            .iter()
            .find(|r| (r.parameter_value - result.best_value).abs() < f32::EPSILON)
            .unwrap();
        assert!((best_result.average_score - result.best_score).abs() < 0.001);
    }
}
