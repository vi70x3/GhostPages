//! Benchmark runner that ties together workloads, policies, and reporting.
//!
//! The `BenchmarkRunner` is the main entry point for running benchmarks.
//! It orchestrates workload generation, policy evaluation, and report
//! production in a deterministic, pure manner.

use ghost_evaluator::scoring::ScoringWeights;
use ghost_evaluator::tournament::Policy;

use crate::comparison::run_workload_comparison;
use crate::experiment::{run_experiment, PolicyExperiment};
use crate::report::generate_report;
use crate::workload::{WorkloadDefinition, WorkloadGenerator};

// ─── Benchmark Runner ─────────────────────────────────────────────────────────

/// Ties together workload generation, policy evaluation, and reporting.
pub struct BenchmarkRunner {
    generator: WorkloadGenerator,
    policies: Vec<Box<dyn Policy>>,
    weights: ScoringWeights,
}

impl std::fmt::Debug for BenchmarkRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BenchmarkRunner")
            .field("generator", &self.generator)
            .field("weights", &self.weights)
            .field("policy_count", &self.policies.len())
            .finish()
    }
}

impl BenchmarkRunner {
    /// Create a new benchmark runner with the given seed.
    pub fn new(seed: u64) -> Self {
        Self {
            generator: WorkloadGenerator::new(seed),
            policies: Vec::new(),
            weights: ScoringWeights::default(),
        }
    }

    /// Set the policies to evaluate. Returns `&mut Self` for chaining.
    pub fn with_policies(&mut self, policies: Vec<Box<dyn Policy>>) -> &mut Self {
        self.policies = policies;
        self
    }

    /// Set the scoring weights. Returns `&mut Self` for chaining.
    pub fn with_weights(&mut self, weights: ScoringWeights) -> &mut Self {
        self.weights = weights;
        self
    }

    /// Run a single workload through all policies.
    pub fn run_workload(
        &self,
        definition: &WorkloadDefinition,
    ) -> crate::comparison::WorkloadComparison {
        let scenario = self.generator.generate(definition);
        run_workload_comparison(&scenario, &self.policies, &self.weights)
    }

    /// Run all built-in workloads through all policies.
    pub fn run_all_builtin(&self) -> crate::report::BenchmarkReport {
        let definitions = crate::workload::all_builtin_workloads();
        self.run_workloads(&definitions)
    }

    /// Run a specific set of workload definitions.
    pub fn run_workloads(
        &self,
        definitions: &[WorkloadDefinition],
    ) -> crate::report::BenchmarkReport {
        let comparisons: Vec<crate::comparison::WorkloadComparison> = definitions
            .iter()
            .map(|def| self.run_workload(def))
            .collect();

        generate_report(comparisons)
    }

    /// Run a policy experiment.
    pub fn run_experiment(&self, experiment: &PolicyExperiment) -> PolicyExperiment {
        // Generate all workload scenarios first
        let definitions = crate::workload::all_builtin_workloads();
        let scenarios: Vec<crate::workload::WorkloadScenario> = definitions
            .iter()
            .map(|def| self.generator.generate(def))
            .collect();

        if self.policies.is_empty() {
            return experiment.clone();
        }

        run_experiment(experiment, &scenarios, self.policies[0].as_ref())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_evaluator::tournament::PressurePolicy;

    fn make_runner() -> BenchmarkRunner {
        let mut runner = BenchmarkRunner::new(42);
        let policies: Vec<Box<dyn Policy>> = vec![Box::new(PressurePolicy)];
        runner.with_policies(policies);
        runner
    }

    #[test]
    fn test_runner_new() {
        let runner = BenchmarkRunner::new(42);
        assert!(runner.policies.is_empty());
    }

    #[test]
    fn test_runner_with_policies() {
        let mut runner = BenchmarkRunner::new(42);
        let policies: Vec<Box<dyn Policy>> = vec![Box::new(PressurePolicy)];
        runner.with_policies(policies);
        assert_eq!(runner.policies.len(), 1);
    }

    #[test]
    fn test_runner_run_workload() {
        let runner = make_runner();
        let def = crate::workload::idle_desktop();
        let comparison = runner.run_workload(&def);

        assert_eq!(comparison.workload_name, "idle_desktop");
        assert_eq!(comparison.runs.len(), 1);
    }

    #[test]
    fn test_runner_run_all_builtin() {
        let runner = make_runner();
        let report = runner.run_all_builtin();

        // Should have 7 workload results
        assert_eq!(report.workload_results.len(), 7);
        assert_eq!(report.summary.total_workloads, 7);
    }

    #[test]
    fn test_runner_deterministic() {
        let runner1 = make_runner();
        let runner2 = make_runner();

        let report1 = runner1.run_all_builtin();
        let report2 = runner2.run_all_builtin();

        // Same seed should produce same results
        assert_eq!(
            report1.workload_results.len(),
            report2.workload_results.len()
        );

        for (c1, c2) in report1
            .workload_results
            .iter()
            .zip(report2.workload_results.iter())
        {
            assert_eq!(c1.workload_name, c2.workload_name);
            for (r1, r2) in c1.policy_results.iter().zip(c2.policy_results.iter()) {
                assert_eq!(r1.average_score, r2.average_score);
            }
        }
    }
}
