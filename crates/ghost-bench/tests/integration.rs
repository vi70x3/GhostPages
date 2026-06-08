//! Regression tests for ghost-bench.
//!
//! These tests verify that synthetic workload tournaments produce consistent,
//! deterministic results with expected policy ranking patterns. They serve as
//! regression guards — if a future change causes HybridPolicy to no longer
//! outperform LinuxBaseline, these tests will catch it.

use ghost_bench::*;
use ghost_evaluator::tournament::{
    ArenaLinuxBaselinePolicy, HotnessPolicy, HybridPolicy, PressurePolicy,
};

// ─── Helper ────────────────────────────────────────────────────────────────────

fn make_runner() -> BenchmarkRunner {
    let mut runner = BenchmarkRunner::new(42);
    runner.with_policies(vec![
        Box::new(ArenaLinuxBaselinePolicy),
        Box::new(PressurePolicy),
        Box::new(HotnessPolicy),
        Box::new(HybridPolicy),
    ]);
    runner
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 1: Determinism Regression Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_benchmark_deterministic() {
    let runner1 = make_runner();
    let runner2 = make_runner();

    let report1 = runner1.run_all_builtin();
    let report2 = runner2.run_all_builtin();

    // Same number of workloads
    assert_eq!(
        report1.workload_results.len(),
        report2.workload_results.len()
    );

    // Same policy ranking order
    assert_eq!(
        report1.policy_ranking.len(),
        report2.policy_ranking.len()
    );
    for (e1, e2) in report1.policy_ranking.iter().zip(report2.policy_ranking.iter()) {
        assert_eq!(e1.policy_name, e2.policy_name);
        assert_eq!(e1.rank, e2.rank);
        assert!(
            (e1.average_score - e2.average_score).abs() < f32::EPSILON,
            "average_score mismatch for {}: {} vs {}",
            e1.policy_name,
            e1.average_score,
            e2.average_score
        );
    }

    // Same per-workload results
    for (w1, w2) in report1
        .workload_results
        .iter()
        .zip(report2.workload_results.iter())
    {
        assert_eq!(w1.workload_name, w2.workload_name);
        assert_eq!(w1.winner, w2.winner);
        for (p1, p2) in w1.policy_results.iter().zip(w2.policy_results.iter()) {
            assert_eq!(p1.policy_name, p2.policy_name);
            assert!(
                (p1.average_score - p2.average_score).abs() < f32::EPSILON,
                "score mismatch for {} on {}: {} vs {}",
                p1.policy_name,
                w1.workload_name,
                p1.average_score,
                p2.average_score
            );
        }
    }
}

#[test]
fn test_workload_generation_deterministic() {
    let gen1 = WorkloadGenerator::new(42);
    let gen2 = WorkloadGenerator::new(42);
    let def = idle_desktop();

    let scenario1 = gen1.generate(&def);
    let scenario2 = gen2.generate(&def);

    assert_eq!(scenario1.snapshots.len(), scenario2.snapshots.len());
    for (s1, s2) in scenario1.snapshots.iter().zip(scenario2.snapshots.iter()) {
        assert_eq!(s1.timestamp_ms, s2.timestamp_ms);
        assert!(
            (s1.state.dram_pressure.memory_pressure - s2.state.dram_pressure.memory_pressure)
                .abs()
                < f32::EPSILON
        );
        assert!(
            (s1.state.dram_utilization - s2.state.dram_utilization).abs() < f32::EPSILON
        );
        assert!(
            (s1.state.swap_utilization - s2.state.swap_utilization).abs() < f32::EPSILON
        );
    }

    // Metadata should also match
    assert_eq!(scenario1.metadata.total_snapshots, scenario2.metadata.total_snapshots);
    assert!(
        (scenario1.metadata.peak_dram_pressure - scenario2.metadata.peak_dram_pressure).abs()
            < f32::EPSILON
    );
}

#[test]
fn test_comparison_deterministic() {
    let runner = make_runner();
    let def = memory_pressure_ramp();

    let comparison1 = runner.run_workload(&def);
    let comparison2 = runner.run_workload(&def);

    assert_eq!(comparison1.workload_name, comparison2.workload_name);
    assert_eq!(comparison1.runs.len(), comparison2.runs.len());
    assert_eq!(comparison1.winner, comparison2.winner);

    for (r1, r2) in comparison1.runs.iter().zip(comparison2.runs.iter()) {
        assert_eq!(r1.policy_name, r2.policy_name);
        assert!(
            (r1.average_score.overall_score - r2.average_score.overall_score).abs() < f32::EPSILON,
            "score mismatch for {}: {} vs {}",
            r1.policy_name,
            r1.average_score.overall_score,
            r2.average_score.overall_score
        );
        assert_eq!(r1.recommendation_count, r2.recommendation_count);
        assert_eq!(r1.active_recommendation_count, r2.active_recommendation_count);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 2: Policy Ranking Regression Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_hybrid_outperforms_baseline_on_mixed_workload() {
    let runner = make_runner();
    // mixed_multitask has hotness data and varied pressure — Hybrid should excel
    let def = mixed_multitask();
    let comparison = runner.run_workload(&def);

    let baseline_score = comparison
        .runs
        .iter()
        .find(|r| r.policy_name == "LinuxBaseline")
        .map(|r| r.average_score.overall_score)
        .unwrap_or(0.0);

    let hybrid_score = comparison
        .runs
        .iter()
        .find(|r| r.policy_name == "Hybrid")
        .map(|r| r.average_score.overall_score)
        .unwrap_or(0.0);

    assert!(
        hybrid_score > baseline_score,
        "HybridPolicy ({}) should outperform LinuxBaseline ({}) on mixed_multitask",
        hybrid_score,
        baseline_score
    );
}

#[test]
fn test_pressure_policy_outperforms_baseline_under_sustained_pressure() {
    let runner = make_runner();
    let def = allocator_stress();
    let comparison = runner.run_workload(&def);

    let baseline_score = comparison
        .runs
        .iter()
        .find(|r| r.policy_name == "LinuxBaseline")
        .map(|r| r.average_score.overall_score)
        .unwrap_or(0.0);

    let pressure_score = comparison
        .runs
        .iter()
        .find(|r| r.policy_name == "Pressure")
        .map(|r| r.average_score.overall_score)
        .unwrap_or(0.0);

    assert!(
        pressure_score > baseline_score,
        "PressurePolicy ({}) should outperform LinuxBaseline ({}) on allocator_stress",
        pressure_score,
        baseline_score
    );
}

#[test]
fn test_informed_policies_outperform_baseline_aggregate() {
    let runner = make_runner();
    let report = runner.run_all_builtin();

    let baseline_entry = report
        .policy_ranking
        .iter()
        .find(|e| e.policy_name == "LinuxBaseline")
        .expect("LinuxBaseline should be in ranking");

    // At least one informed policy should have a higher aggregate score
    let any_informed_beats_baseline = report.policy_ranking.iter().any(|e| {
        e.policy_name != "LinuxBaseline" && e.average_score > baseline_entry.average_score
    });

    assert!(
        any_informed_beats_baseline,
        "At least one informed policy should outperform LinuxBaseline in aggregate. \
         Baseline score: {}, ranking: {:?}",
        baseline_entry.average_score,
        report.policy_ranking
    );
}

#[test]
fn test_baseline_rarely_wins() {
    let runner = make_runner();
    let report = runner.run_all_builtin();

    let baseline_entry = report
        .policy_ranking
        .iter()
        .find(|e| e.policy_name == "LinuxBaseline")
        .expect("LinuxBaseline should be in ranking");

    // LinuxBaseline should win at most 1 out of 7 workloads
    assert!(
        baseline_entry.wins <= 1,
        "LinuxBaseline should rarely win, but won {} out of {} workloads",
        baseline_entry.wins,
        baseline_entry.total_workloads
    );
}

#[test]
fn test_policy_ranking_order_consistent() {
    let runner = make_runner();
    let report = runner.run_all_builtin();

    // Find positions
    let baseline_rank = report
        .policy_ranking
        .iter()
        .find(|e| e.policy_name == "LinuxBaseline")
        .map(|e| e.rank)
        .unwrap();

    let hybrid_rank = report
        .policy_ranking
        .iter()
        .find(|e| e.policy_name == "Hybrid")
        .map(|e| e.rank)
        .unwrap();

    // Hybrid should rank above (lower rank number) LinuxBaseline
    assert!(
        hybrid_rank < baseline_rank,
        "Hybrid (rank {}) should rank above LinuxBaseline (rank {})",
        hybrid_rank,
        baseline_rank
    );

    // Ranking should have all 4 policies
    assert_eq!(report.policy_ranking.len(), 4);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 3: Workload Characteristic Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_idle_desktop_low_pressure() {
    let gen = WorkloadGenerator::new(42);
    let def = idle_desktop();
    let scenario = gen.generate(&def);

    // >50% of snapshots should be at idle or low pressure (< 0.5)
    let low_pressure_count = scenario
        .snapshots
        .iter()
        .filter(|s| s.state.dram_pressure.memory_pressure < 0.5)
        .count();

    let fraction = low_pressure_count as f32 / scenario.snapshots.len() as f32;
    assert!(
        fraction > 0.5,
        "idle_desktop should have >50% low-pressure snapshots, got {:.1}%",
        fraction * 100.0
    );
}

#[test]
fn test_pressure_ramp_increasing() {
    let gen = WorkloadGenerator::new(100);
    let def = memory_pressure_ramp();
    let scenario = gen.generate(&def);

    let first_pressure = scenario
        .snapshots
        .first()
        .unwrap()
        .state
        .dram_pressure
        .memory_pressure;
    let last_pressure = scenario
        .snapshots
        .last()
        .unwrap()
        .state
        .dram_pressure
        .memory_pressure;

    assert!(
        last_pressure > first_pressure,
        "memory_pressure_ramp last snapshot pressure ({}) should be > first ({})",
        last_pressure,
        first_pressure
    );
}

#[test]
fn test_build_server_periodic_spikes() {
    let gen = WorkloadGenerator::new(200);
    let def = build_server();
    let scenario = gen.generate(&def);

    // Should have both high-pressure (>0.6) and low-pressure (<0.4) snapshots
    let has_high = scenario
        .snapshots
        .iter()
        .any(|s| s.state.dram_pressure.memory_pressure > 0.6);
    let has_low = scenario
        .snapshots
        .iter()
        .any(|s| s.state.dram_pressure.memory_pressure < 0.4);

    assert!(has_high, "build_server should have high-pressure snapshots");
    assert!(has_low, "build_server should have low-pressure snapshots");
}

#[test]
fn test_allocator_stress_oscillating() {
    let gen = WorkloadGenerator::new(500);
    let def = allocator_stress();
    let scenario = gen.generate(&def);

    // Should have both high (>0.5) and low (<0.4) pressure snapshots
    let has_high = scenario
        .snapshots
        .iter()
        .any(|s| s.state.dram_pressure.memory_pressure > 0.5);
    let has_low = scenario
        .snapshots
        .iter()
        .any(|s| s.state.dram_pressure.memory_pressure < 0.4);

    assert!(
        has_high,
        "allocator_stress should have high-pressure snapshots"
    );
    assert!(
        has_low,
        "allocator_stress should have low-pressure snapshots"
    );

    // Check for oscillation: count direction changes in pressure
    let pressures: Vec<f32> = scenario
        .snapshots
        .iter()
        .map(|s| s.state.dram_pressure.memory_pressure)
        .collect();

    let mut direction_changes = 0;
    for w in pressures.windows(3) {
        let diff1 = w[1] - w[0];
        let diff2 = w[2] - w[1];
        if diff1.signum() != diff2.signum() && diff1.abs() > 0.01 && diff2.abs() > 0.01 {
            direction_changes += 1;
        }
    }

    // With 8 cycles over 120 snapshots, there should be many direction changes
    assert!(
        direction_changes >= 4,
        "allocator_stress should have oscillating pressure, got {} direction changes",
        direction_changes
    );
}

#[test]
fn test_database_cache_has_hotness() {
    let gen = WorkloadGenerator::new(300);
    let def = database_cache();
    let scenario = gen.generate(&def);

    // Most snapshots should have hotness data
    let with_hotness = scenario
        .snapshots
        .iter()
        .filter(|s| s.state.hotness_summary.is_some())
        .count();

    let fraction = with_hotness as f32 / scenario.snapshots.len() as f32;
    assert!(
        fraction >= 0.9,
        "database_cache should have hotness data in >=90% of snapshots, got {:.1}%",
        fraction * 100.0
    );
}

#[test]
fn test_tier_saturation_progressive() {
    let gen = WorkloadGenerator::new(600);
    let def = tier_saturation();
    let scenario = gen.generate(&def);

    // DRAM utilization should generally increase in the first half (filling phase)
    let mid = scenario.snapshots.len() / 2;
    let first_half_avg: f32 = scenario.snapshots[..mid]
        .iter()
        .map(|s| s.state.dram_utilization)
        .sum::<f32>()
        / mid as f32;
    let second_half_avg: f32 = scenario.snapshots[mid..]
        .iter()
        .map(|s| s.state.dram_utilization)
        .sum::<f32>()
        / (scenario.snapshots.len() - mid) as f32;

    // Second half should have higher or similar DRAM util (stays high during ZRAM/swap fill)
    assert!(
        second_half_avg >= first_half_avg * 0.8,
        "tier_saturation second half avg DRAM util ({}) should be >= 80% of first half ({})",
        second_half_avg,
        first_half_avg
    );

    // Peak DRAM utilization should be very high
    assert!(
        scenario.metadata.peak_dram_utilization > 0.8,
        "tier_saturation peak DRAM util should be >0.8, got {}",
        scenario.metadata.peak_dram_utilization
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 4: Report Regression Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_report_contains_all_workloads() {
    let runner = make_runner();
    let report = runner.run_all_builtin();

    assert_eq!(
        report.workload_results.len(),
        7,
        "Report should have 7 workload results"
    );

    let expected = [
        "idle_desktop",
        "memory_pressure_ramp",
        "build_server",
        "database_cache",
        "mixed_multitask",
        "allocator_stress",
        "tier_saturation",
    ];

    for name in &expected {
        assert!(
            report
                .workload_results
                .iter()
                .any(|w| &w.workload_name == *name),
            "Report should contain workload '{}'",
            name
        );
    }
}

#[test]
fn test_report_policy_ranking_populated() {
    let runner = make_runner();
    let report = runner.run_all_builtin();

    assert_eq!(
        report.policy_ranking.len(),
        4,
        "Policy ranking should have 4 entries"
    );

    let expected_policies = ["LinuxBaseline", "Pressure", "Hotness", "Hybrid"];
    for name in &expected_policies {
        assert!(
            report
                .policy_ranking
                .iter()
                .any(|e| &e.policy_name == *name),
            "Policy ranking should contain '{}'",
            name
        );
    }

    // Ranks should be 1, 2, 3, 4
    let mut ranks: Vec<usize> = report.policy_ranking.iter().map(|e| e.rank).collect();
    ranks.sort();
    assert_eq!(ranks, vec![1, 2, 3, 4]);
}

#[test]
fn test_report_markdown_valid() {
    let runner = make_runner();
    let report = runner.run_all_builtin();
    let md = format_report_markdown(&report);

    // Should contain expected section headers
    assert!(md.contains("# GhostPages Benchmark Report"));
    assert!(md.contains("## Summary"));
    assert!(md.contains("## Policy Ranking"));
    assert!(md.contains("## Per-Workload Results"));

    // Should contain policy names
    assert!(md.contains("LinuxBaseline"));
    assert!(md.contains("Pressure"));
    assert!(md.contains("Hotness"));
    assert!(md.contains("Hybrid"));

    // Should contain workload names
    assert!(md.contains("idle_desktop"));
    assert!(md.contains("memory_pressure_ramp"));

    // Should contain table formatting
    assert!(md.contains("| Rank | Policy |"));
}

#[test]
fn test_report_json_parseable() {
    let runner = make_runner();
    let report = runner.run_all_builtin();
    let json = format_report_json(&report);

    // Should be valid JSON
    let parsed: serde_json::Value =
        serde_json::from_str(&json).expect("JSON report should be valid JSON");

    // Should have expected top-level fields
    assert!(parsed.get("id").is_some(), "JSON should have 'id' field");
    assert!(
        parsed.get("timestamp").is_some(),
        "JSON should have 'timestamp' field"
    );
    assert!(
        parsed.get("workload_results").is_some(),
        "JSON should have 'workload_results' field"
    );
    assert!(
        parsed.get("policy_ranking").is_some(),
        "JSON should have 'policy_ranking' field"
    );
    assert!(
        parsed.get("summary").is_some(),
        "JSON should have 'summary' field"
    );

    // Summary should have expected fields
    let summary = parsed.get("summary").unwrap();
    assert!(summary.get("total_workloads").is_some());
    assert!(summary.get("total_policies").is_some());
    assert!(summary.get("best_policy").is_some());
}

#[test]
fn test_report_best_policy_identified() {
    let runner = make_runner();
    let report = runner.run_all_builtin();

    // Best policy should be one of the 4 registered policies
    assert!(
        report.summary.best_policy == "LinuxBaseline"
            || report.summary.best_policy == "Pressure"
            || report.summary.best_policy == "Hotness"
            || report.summary.best_policy == "Hybrid",
        "Best policy should be a registered policy, got '{}'",
        report.summary.best_policy
    );

    // Best policy score should be > 0
    assert!(
        report.summary.best_policy_score > 0.0,
        "Best policy score should be > 0"
    );

    // Best policy should match the top of the ranking
    assert_eq!(
        report.summary.best_policy,
        report.policy_ranking[0].policy_name
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 5: Leaderboard Regression Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_leaderboard_from_report() {
    let runner = make_runner();
    let report = runner.run_all_builtin();
    let leaderboard = from_report(&report, 1);

    // Should have entries (4 policies × 7 workloads = 28)
    assert!(
        !leaderboard.entries.is_empty(),
        "Leaderboard should have entries"
    );
    assert_eq!(leaderboard.entries.len(), 28);

    // Version should be set
    assert!(leaderboard.version >= 1);
}

#[test]
fn test_leaderboard_top_policies() {
    let runner = make_runner();
    let report = runner.run_all_builtin();
    let leaderboard = from_report(&report, 1);

    let top = leaderboard.top_policies(4);
    assert!(!top.is_empty(), "top_policies should return entries");

    // Should be sorted by score descending
    for i in 1..top.len() {
        assert!(
            top[i - 1].score >= top[i].score,
            "top_policies should be sorted descending: {:?}",
            top
        );
    }
}

#[test]
fn test_leaderboard_workload_filtering() {
    let runner = make_runner();
    let report = runner.run_all_builtin();
    let leaderboard = from_report(&report, 1);

    use ghost_bench::workload::WorkloadClass;

    let desktop_top = leaderboard.top_for_workload(&WorkloadClass::Desktop, 10);
    // All returned entries should be Desktop class
    for entry in &desktop_top {
        assert_eq!(
            entry.workload_class,
            WorkloadClass::Desktop,
            "top_for_workload(Desktop) should only return Desktop entries"
        );
    }

    // Should have at least one Desktop entry (idle_desktop workload)
    assert!(
        !desktop_top.is_empty(),
        "Should have at least one Desktop entry"
    );

    // MemoryPressure should have entries (memory_pressure_ramp, allocator_stress, tier_saturation)
    let mem_pressure_top =
        leaderboard.top_for_workload(&WorkloadClass::MemoryPressure, 10);
    assert!(
        !mem_pressure_top.is_empty(),
        "Should have MemoryPressure entries"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 6: Experiment Regression Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_experiment_produces_results() {
    let runner = make_runner();
    let experiment = pressure_weight_experiment();

    let result = runner.run_experiment(&experiment);

    // Should have results for each test value
    assert_eq!(
        result.results.len(),
        experiment.test_values.len(),
        "Should have results for each test value"
    );

    // Each result should have a valid score
    for r in &result.results {
        assert!(
            r.average_score >= 0.0 && r.average_score <= 1.0,
            "Result score {} should be in [0, 1]",
            r.average_score
        );
    }
}

#[test]
fn test_experiment_best_value_identified() {
    let runner = make_runner();
    let experiment = pressure_weight_experiment();

    let result = runner.run_experiment(&experiment);

    // Best value should be one of the test values
    assert!(
        result
            .test_values
            .iter()
            .any(|&v| (v - result.best_value).abs() < f32::EPSILON),
        "Best value {} should be in test_values",
        result.best_value
    );

    // Best score should be > 0
    assert!(
        result.best_score > 0.0,
        "Best score should be > 0, got {}",
        result.best_score
    );

    // Best score should match the result for best_value
    let best_result = result
        .results
        .iter()
        .find(|r| (r.parameter_value - result.best_value).abs() < f32::EPSILON)
        .unwrap();
    assert!(
        (best_result.average_score - result.best_score).abs() < 0.001,
        "Best score should match the result entry"
    );
}

#[test]
fn test_experiment_improvement_measured() {
    let runner = make_runner();
    let experiment = pressure_weight_experiment();

    let result = runner.run_experiment(&experiment);

    // Improvement should be computed (can be positive, zero, or negative)
    // The key thing is that it's a valid finite number
    assert!(
        result.improvement.is_finite(),
        "Improvement should be a finite number, got {}",
        result.improvement
    );

    // Verify improvement formula: (best - baseline) / baseline
    let baseline_result = result
        .results
        .iter()
        .find(|r| (r.parameter_value - experiment.baseline_value).abs() < f32::EPSILON);
    if let Some(baseline) = baseline_result {
        if baseline.average_score > 0.0 {
            let expected_improvement =
                (result.best_score - baseline.average_score) / baseline.average_score;
            assert!(
                (result.improvement - expected_improvement).abs() < 0.001,
                "Improvement should match (best - baseline) / baseline: got {} expected {}",
                result.improvement,
                expected_improvement
            );
        }
    }
}
