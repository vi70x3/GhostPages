//! Benchmark report generation for GhostPages.
//!
//! Produces comprehensive benchmark reports across multiple workloads and
//! policies, with both human-readable (markdown) and machine-readable (JSON)
//! output formats.

use std::collections::HashMap;

use serde::Serialize;

use crate::comparison::WorkloadComparison;
use crate::workload::WorkloadClass;

// ─── Policy Rank Entry ────────────────────────────────────────────────────────

/// A policy's ranking in the global leaderboard.
#[derive(Debug, Clone, Serialize)]
pub struct PolicyRankEntry {
    /// Name of the policy.
    pub policy_name: String,
    /// Rank (1 = best).
    pub rank: usize,
    /// Average score across all workloads.
    pub average_score: f32,
    /// Number of workloads where this policy won.
    pub wins: usize,
    /// Total number of workloads evaluated.
    pub total_workloads: usize,
    /// Win rate (wins / total_workloads).
    pub win_rate: f32,
}

// ─── Benchmark Summary ────────────────────────────────────────────────────────

/// Summary statistics for the entire benchmark.
#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkSummary {
    /// Total number of workloads evaluated.
    pub total_workloads: usize,
    /// Total number of policies evaluated.
    pub total_policies: usize,
    /// Total number of rounds (workload × policy evaluations).
    pub total_rounds: usize,
    /// Name of the best overall policy.
    pub best_policy: String,
    /// Best policy's average score.
    pub best_policy_score: f32,
    /// Name of the worst overall policy.
    pub worst_policy: String,
    /// Worst policy's average score.
    pub worst_policy_score: f32,
    /// Workload class where policies differ most (highest score variance).
    pub dominant_workload_class: String,
}

// ─── Serializable Workload Result ─────────────────────────────────────────────

/// A serializable summary of per-workload comparison results.
#[derive(Debug, Clone, Serialize)]
pub struct WorkloadResultSummary {
    /// Name of the workload.
    pub workload_name: String,
    /// Name of the winning policy.
    pub winner: String,
    /// Winner's score.
    pub winner_score: f32,
    /// Per-policy results.
    pub policy_results: Vec<PolicyResultSummary>,
}

/// A serializable summary of a single policy's result in a workload.
#[derive(Debug, Clone, Serialize)]
pub struct PolicyResultSummary {
    /// Name of the policy.
    pub policy_name: String,
    /// Average overall score.
    pub average_score: f32,
    /// Total recommendations generated.
    pub recommendation_count: usize,
    /// Active (non-NoAction) recommendations.
    pub active_recommendation_count: usize,
    /// Stability index.
    pub stability_index: f32,
    /// Overall quality score.
    pub overall_quality: f32,
}

// ─── Benchmark Report (Serializable) ─────────────────────────────────────────

/// A comprehensive benchmark report across multiple workloads and policies.
#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkReport {
    /// Unique report ID (timestamp-based).
    pub id: String,
    /// ISO 8601 timestamp.
    pub timestamp: String,
    /// Per-workload comparison results (serializable summary).
    pub workload_results: Vec<WorkloadResultSummary>,
    /// Global policy ranking across all workloads.
    pub policy_ranking: Vec<PolicyRankEntry>,
    /// Summary statistics.
    pub summary: BenchmarkSummary,
}

// ─── Report Generation ────────────────────────────────────────────────────────

/// Generate a benchmark report from multiple workload comparisons.
pub fn generate_report(comparisons: Vec<WorkloadComparison>) -> BenchmarkReport {
    let timestamp = chrono::Utc::now().to_rfc3339();
    let id = format!("bench-{}", chrono::Utc::now().timestamp());

    // Collect all unique policy names
    let mut policy_names: Vec<String> = comparisons
        .iter()
        .flat_map(|c| c.runs.iter().map(|r| r.policy_name.clone()))
        .collect();
    policy_names.sort();
    policy_names.dedup();
    let total_policies = policy_names.len();
    let total_workloads = comparisons.len();
    let total_rounds: usize = comparisons.iter().map(|c| c.runs.len()).sum();

    // Compute per-policy aggregate scores
    let mut policy_score_sums: HashMap<String, (f32, usize)> = HashMap::new();
    let mut policy_wins: HashMap<String, usize> = HashMap::new();

    for comparison in &comparisons {
        for run in &comparison.runs {
            let entry = policy_score_sums
                .entry(run.policy_name.clone())
                .or_insert((0.0, 0));
            entry.0 += run.average_score.overall_score;
            entry.1 += 1;
        }

        // Count wins
        *policy_wins.entry(comparison.winner.clone()).or_insert(0) += 1;
    }

    // Build policy ranking
    let mut ranking: Vec<PolicyRankEntry> = policy_names
        .iter()
        .map(|name| {
            let (sum, count) = policy_score_sums.get(name).copied().unwrap_or((0.0, 0));
            let avg = if count > 0 { sum / count as f32 } else { 0.0 };
            let wins = policy_wins.get(name).copied().unwrap_or(0);
            PolicyRankEntry {
                policy_name: name.clone(),
                rank: 0, // filled in after sorting
                average_score: avg,
                wins,
                total_workloads,
                win_rate: if total_workloads > 0 {
                    wins as f32 / total_workloads as f32
                } else {
                    0.0
                },
            }
        })
        .collect();

    // Sort by average score descending
    ranking.sort_by(|a, b| {
        b.average_score
            .partial_cmp(&a.average_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Assign ranks
    for (i, entry) in ranking.iter_mut().enumerate() {
        entry.rank = i + 1;
    }

    // Determine best/worst
    let best = ranking.first();
    let worst = ranking.last();

    let best_policy = best.map(|e| e.policy_name.clone()).unwrap_or_default();
    let best_policy_score = best.map(|e| e.average_score).unwrap_or(0.0);
    let worst_policy = worst.map(|e| e.policy_name.clone()).unwrap_or_default();
    let worst_policy_score = worst.map(|e| e.average_score).unwrap_or(0.0);

    // Find dominant workload class (class where policies differ most)
    let dominant_workload_class = find_dominant_workload_class(&comparisons);

    // Build serializable workload results
    let workload_results: Vec<WorkloadResultSummary> = comparisons
        .into_iter()
        .map(|c| WorkloadResultSummary {
            workload_name: c.workload_name,
            winner: c.winner,
            winner_score: c.winner_score,
            policy_results: c
                .runs
                .into_iter()
                .map(|r| PolicyResultSummary {
                    policy_name: r.policy_name,
                    average_score: r.average_score.overall_score,
                    recommendation_count: r.recommendation_count,
                    active_recommendation_count: r.active_recommendation_count,
                    stability_index: r.stability.stability_index,
                    overall_quality: r.quality.overall_quality,
                })
                .collect(),
        })
        .collect();

    BenchmarkReport {
        id,
        timestamp,
        workload_results,
        policy_ranking: ranking,
        summary: BenchmarkSummary {
            total_workloads,
            total_policies,
            total_rounds,
            best_policy,
            best_policy_score,
            worst_policy,
            worst_policy_score,
            dominant_workload_class,
        },
    }
}

/// Find the workload class where policies differ most (highest score variance).
fn find_dominant_workload_class(comparisons: &[WorkloadComparison]) -> String {
    let mut class_scores: HashMap<String, Vec<f32>> = HashMap::new();

    for comparison in comparisons {
        let class_name = infer_workload_class(&comparison.workload_name);

        for run in &comparison.runs {
            class_scores
                .entry(class_name.clone())
                .or_default()
                .push(run.average_score.overall_score);
        }
    }

    let mut max_variance = -1.0_f32;
    let mut dominant = "Unknown".to_string();

    for (class, scores) in &class_scores {
        if scores.len() < 2 {
            continue;
        }
        let n = scores.len() as f32;
        let mean = scores.iter().sum::<f32>() / n;
        let variance = scores.iter().map(|s| (s - mean) * (s - mean)).sum::<f32>() / n;

        if variance > max_variance {
            max_variance = variance;
            dominant = class.clone();
        }
    }

    dominant
}

/// Infer workload class from the workload name.
fn infer_workload_class(name: &str) -> String {
    if name.contains("idle") || name.contains("desktop") {
        WorkloadClass::Desktop.to_string()
    } else if name.contains("build") {
        WorkloadClass::BuildSystem.to_string()
    } else if name.contains("pressure") || name.contains("allocator") || name.contains("tier") {
        WorkloadClass::MemoryPressure.to_string()
    } else if name.contains("database") || name.contains("cache") {
        WorkloadClass::DataSystem.to_string()
    } else if name.contains("mixed") {
        WorkloadClass::Mixed.to_string()
    } else {
        "Unknown".to_string()
    }
}

// ─── Report Formatting ────────────────────────────────────────────────────────

/// Format a benchmark report as human-readable markdown.
pub fn format_report_markdown(report: &BenchmarkReport) -> String {
    let mut md = String::new();

    // Header
    md.push_str("# GhostPages Benchmark Report\n\n");
    md.push_str(&format!("**Report ID:** `{}`\n", report.id));
    md.push_str(&format!("**Timestamp:** {}\n\n", report.timestamp));

    // Summary
    md.push_str("## Summary\n\n");
    md.push_str(&format!(
        "- **Total Workloads:** {}\n",
        report.summary.total_workloads
    ));
    md.push_str(&format!(
        "- **Total Policies:** {}\n",
        report.summary.total_policies
    ));
    md.push_str(&format!("- **Total Rounds:** {}\n", report.summary.total_rounds));
    md.push_str(&format!(
        "- **Best Policy:** `{}` (score: {:.4})\n",
        report.summary.best_policy, report.summary.best_policy_score
    ));
    md.push_str(&format!(
        "- **Worst Policy:** `{}` (score: {:.4})\n",
        report.summary.worst_policy, report.summary.worst_policy_score
    ));
    md.push_str(&format!(
        "- **Dominant Workload Class:** {}\n\n",
        report.summary.dominant_workload_class
    ));

    // Policy Ranking Table
    md.push_str("## Policy Ranking\n\n");
    md.push_str("| Rank | Policy | Avg Score | Wins | Win Rate |\n");
    md.push_str("|------|--------|-----------|------|----------|\n");
    for entry in &report.policy_ranking {
        md.push_str(&format!(
            "| {} | {} | {:.4} | {} | {:.1}% |\n",
            entry.rank,
            entry.policy_name,
            entry.average_score,
            entry.wins,
            entry.win_rate * 100.0
        ));
    }
    md.push('\n');

    // Per-Workload Results
    md.push_str("## Per-Workload Results\n\n");
    for wr in &report.workload_results {
        md.push_str(&format!("### {}\n\n", wr.workload_name));
        md.push_str(&format!(
            "**Winner:** `{}` (score: {:.4})\n\n",
            wr.winner, wr.winner_score
        ));

        md.push_str("| Policy | Avg Score | Recommendations | Active | Stability |\n");
        md.push_str("|--------|-----------|-----------------|--------|----------|\n");
        for pr in &wr.policy_results {
            md.push_str(&format!(
                "| {} | {:.4} | {} | {} | {:.4} |\n",
                pr.policy_name,
                pr.average_score,
                pr.recommendation_count,
                pr.active_recommendation_count,
                pr.stability_index
            ));
        }
        md.push('\n');
    }

    md
}

/// Format a benchmark report as machine-readable JSON.
pub fn format_report_json(report: &BenchmarkReport) -> String {
    serde_json::to_string_pretty(report).unwrap_or_else(|e| {
        format!(r#"{{"error": "failed to serialize report: {}"}}"#, e)
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_report() -> BenchmarkReport {
        let wr = WorkloadResultSummary {
            workload_name: "test_workload".to_string(),
            winner: "Hybrid".to_string(),
            winner_score: 0.68,
            policy_results: vec![
                PolicyResultSummary {
                    policy_name: "Pressure".to_string(),
                    average_score: 0.58,
                    recommendation_count: 10,
                    active_recommendation_count: 8,
                    stability_index: 0.85,
                    overall_quality: 0.72,
                },
                PolicyResultSummary {
                    policy_name: "Hybrid".to_string(),
                    average_score: 0.68,
                    recommendation_count: 12,
                    active_recommendation_count: 10,
                    stability_index: 0.75,
                    overall_quality: 0.78,
                },
            ],
        };

        BenchmarkReport {
            id: "bench-12345".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            workload_results: vec![wr],
            policy_ranking: vec![
                PolicyRankEntry {
                    policy_name: "Hybrid".to_string(),
                    rank: 1,
                    average_score: 0.68,
                    wins: 1,
                    total_workloads: 1,
                    win_rate: 1.0,
                },
                PolicyRankEntry {
                    policy_name: "Pressure".to_string(),
                    rank: 2,
                    average_score: 0.58,
                    wins: 0,
                    total_workloads: 1,
                    win_rate: 0.0,
                },
            ],
            summary: BenchmarkSummary {
                total_workloads: 1,
                total_policies: 2,
                total_rounds: 2,
                best_policy: "Hybrid".to_string(),
                best_policy_score: 0.68,
                worst_policy: "Pressure".to_string(),
                worst_policy_score: 0.58,
                dominant_workload_class: "MemoryPressure".to_string(),
            },
        }
    }

    #[test]
    fn test_generate_report() {
        use crate::comparison::{PolicyComparisonRun, WorkloadComparison};
        use ghost_evaluator::quality::{QualityDimension, RecommendationQuality};
        use ghost_evaluator::scoring::RecommendationScore;
        use ghost_evaluator::stability::RecommendationStability;

        let make_run = |name: &str, score: f32| PolicyComparisonRun {
            workload_name: "test_workload".to_string(),
            policy_name: name.to_string(),
            scores: vec![],
            average_score: RecommendationScore {
                fault_reduction: score,
                swap_reduction: score,
                zram_efficiency: score,
                pressure_reduction: score,
                tier_balance: score,
                stability: score,
                overall_score: score,
            },
            recommendation_count: 10,
            active_recommendation_count: 8,
            stability: RecommendationStability {
                recommendations_per_hour: 1.0,
                temperature_flips: 0,
                tier_oscillations: 0,
                confidence_variance: 0.01,
                stability_index: 0.85,
            },
            quality: RecommendationQuality {
                stability: QualityDimension {
                    score: 0.85,
                    label: "Stability".to_string(),
                    details: vec![],
                },
                efficiency: QualityDimension {
                    score: 0.6,
                    label: "Efficiency".to_string(),
                    details: vec![],
                },
                simplicity: QualityDimension {
                    score: 0.7,
                    label: "Simplicity".to_string(),
                    details: vec![],
                },
                confidence: QualityDimension {
                    score: 0.8,
                    label: "Confidence".to_string(),
                    details: vec![],
                },
                overall_quality: 0.72,
            },
        };

        let comparison = WorkloadComparison {
            workload_name: "test_workload".to_string(),
            runs: vec![make_run("Pressure", 0.58), make_run("Hybrid", 0.68)],
            winner: "Hybrid".to_string(),
            winner_score: 0.68,
        };

        let report = generate_report(vec![comparison]);

        assert_eq!(report.summary.total_workloads, 1);
        assert_eq!(report.summary.total_policies, 2);
        assert_eq!(report.policy_ranking.len(), 2);
    }

    #[test]
    fn test_policy_ranking() {
        let report = make_test_report();

        // Hybrid should be rank 1 (higher score: 0.68 > 0.58)
        assert_eq!(report.policy_ranking[0].policy_name, "Hybrid");
        assert_eq!(report.policy_ranking[0].rank, 1);
        assert_eq!(report.policy_ranking[1].policy_name, "Pressure");
        assert_eq!(report.policy_ranking[1].rank, 2);
    }

    #[test]
    fn test_format_report_markdown() {
        let report = make_test_report();
        let md = format_report_markdown(&report);

        assert!(md.contains("# GhostPages Benchmark Report"));
        assert!(md.contains("## Summary"));
        assert!(md.contains("## Policy Ranking"));
        assert!(md.contains("## Per-Workload Results"));
        assert!(md.contains("Hybrid"));
        assert!(md.contains("Pressure"));
    }

    #[test]
    fn test_format_report_json() {
        let report = make_test_report();
        let json = format_report_json(&report);

        // Should be valid JSON
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("should be valid JSON");
        assert!(parsed.get("id").is_some());
        assert!(parsed.get("summary").is_some());
        assert!(parsed.get("policy_ranking").is_some());
    }

    #[test]
    fn test_summary_statistics() {
        let report = make_test_report();

        assert_eq!(report.summary.best_policy, "Hybrid");
        assert!((report.summary.best_policy_score - 0.68).abs() < 0.001);
        assert_eq!(report.summary.worst_policy, "Pressure");
        assert!((report.summary.worst_policy_score - 0.58).abs() < 0.001);
    }
}
