//! Policy leaderboard / hall of fame for GhostPages.
//!
//! Tracks policy performance over time, enabling comparison of policy
//! versions and detection of improvement trends.

use serde::Serialize;

use crate::report::BenchmarkReport;
use crate::workload::WorkloadClass;

// ─── Leaderboard Entry ────────────────────────────────────────────────────────

/// A single entry in the leaderboard.
#[derive(Debug, Clone, Serialize)]
pub struct LeaderboardEntry {
    /// Name of the policy.
    pub policy_name: String,
    /// Version string (e.g., "v1.0", "v1.1-hotfix").
    pub policy_version: String,
    /// The workload class this entry is for.
    pub workload_class: WorkloadClass,
    /// The score achieved.
    pub score: f32,
    /// Stability index at the time of scoring.
    pub stability_index: f32,
    /// Rank within this workload class (1 = best).
    pub rank: usize,
    /// ISO 8601 timestamp of when this entry was recorded.
    pub timestamp: String,
}

// ─── Policy Leaderboard ───────────────────────────────────────────────────────

/// Persistent leaderboard tracking policy performance over time.
#[derive(Debug, Clone, Serialize)]
pub struct PolicyLeaderboard {
    /// All leaderboard entries.
    pub entries: Vec<LeaderboardEntry>,
    /// Version counter (incremented on each update).
    pub version: u32,
    /// ISO 8601 timestamp of last update.
    pub last_updated: String,
}

impl PolicyLeaderboard {
    /// Create a new empty leaderboard.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            version: 0,
            last_updated: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Add an entry to the leaderboard.
    pub fn add_entry(&mut self, entry: LeaderboardEntry) {
        self.entries.push(entry);
        self.version += 1;
        self.last_updated = chrono::Utc::now().to_rfc3339();
    }

    /// Return the top N policies by score (descending).
    pub fn top_policies(&self, limit: usize) -> Vec<&LeaderboardEntry> {
        let mut sorted: Vec<&LeaderboardEntry> = self.entries.iter().collect();
        sorted.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted.truncate(limit);
        sorted
    }

    /// Return the top N policies for a specific workload class.
    pub fn top_for_workload(&self, class: &WorkloadClass, limit: usize) -> Vec<&LeaderboardEntry> {
        let mut filtered: Vec<&LeaderboardEntry> = self
            .entries
            .iter()
            .filter(|e| &e.workload_class == class)
            .collect();
        filtered.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        filtered.truncate(limit);
        filtered
    }

    /// Return the history for a specific policy (all entries, oldest first).
    pub fn policy_history(&self, policy_name: &str) -> Vec<&LeaderboardEntry> {
        let mut history: Vec<&LeaderboardEntry> = self
            .entries
            .iter()
            .filter(|e| e.policy_name == policy_name)
            .collect();
        history.sort_by_key(|e| &e.timestamp);
        history
    }

    /// Check if a policy is improving (recent entries score higher than older ones).
    ///
    /// Compares the average of the most recent 3 entries vs the average of the
    /// previous 3 entries. Returns true if recent average > older average.
    pub fn is_improving(&self, policy_name: &str) -> bool {
        let history = self.policy_history(policy_name);
        if history.len() < 2 {
            return false;
        }

        let n = history.len();
        let recent_count = 3usize.min(n);
        let older_count = 3usize.min(n - recent_count);

        if older_count == 0 {
            return false;
        }

        let recent_avg: f32 = history[n - recent_count..]
            .iter()
            .map(|e| e.score)
            .sum::<f32>()
            / recent_count as f32;

        let older_avg: f32 = history[0..older_count]
            .iter()
            .map(|e| e.score)
            .sum::<f32>()
            / older_count as f32;

        recent_avg > older_avg
    }
}

impl Default for PolicyLeaderboard {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a benchmark report into leaderboard entries.
pub fn from_report(report: &BenchmarkReport, version: u32) -> PolicyLeaderboard {
    let mut leaderboard = PolicyLeaderboard::new();
    leaderboard.version = version;
    leaderboard.last_updated = report.timestamp.clone();

    for wr in &report.workload_results {
        let workload_class = infer_workload_class(&wr.workload_name);

        for pr in &wr.policy_results {
            let entry = LeaderboardEntry {
                policy_name: pr.policy_name.clone(),
                policy_version: format!("v{}", version),
                workload_class,
                score: pr.average_score,
                stability_index: pr.stability_index,
                rank: 0, // computed below
                timestamp: report.timestamp.clone(),
            };
            leaderboard.add_entry(entry);
        }
    }

    // Assign ranks within each workload class
    let classes: Vec<WorkloadClass> = leaderboard
        .entries
        .iter()
        .map(|e| e.workload_class.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    for class in classes {
        let mut indices: Vec<usize> = leaderboard
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| e.workload_class == class)
            .map(|(i, _)| i)
            .collect();

        // Sort by score descending
        indices.sort_by(|&a, &b| {
            leaderboard.entries[b]
                .score
                .partial_cmp(&leaderboard.entries[a].score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        for (rank, &idx) in indices.iter().enumerate() {
            leaderboard.entries[idx].rank = rank + 1;
        }
    }

    leaderboard
}

/// Infer workload class from the workload name.
fn infer_workload_class(name: &str) -> WorkloadClass {
    if name.contains("idle") || name.contains("desktop") {
        WorkloadClass::Desktop
    } else if name.contains("build") {
        WorkloadClass::BuildSystem
    } else if name.contains("pressure") || name.contains("allocator") || name.contains("tier") {
        WorkloadClass::MemoryPressure
    } else if name.contains("database") || name.contains("cache") {
        WorkloadClass::DataSystem
    } else if name.contains("mixed") {
        WorkloadClass::Mixed
    } else {
        // Default to Mixed for unknown
        WorkloadClass::Mixed
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(name: &str, class: WorkloadClass, score: f32, timestamp: &str) -> LeaderboardEntry {
        LeaderboardEntry {
            policy_name: name.to_string(),
            policy_version: "v1.0".to_string(),
            workload_class: class,
            score,
            stability_index: 0.8,
            rank: 0,
            timestamp: timestamp.to_string(),
        }
    }

    #[test]
    fn test_leaderboard_new() {
        let lb = PolicyLeaderboard::new();
        assert_eq!(lb.entries.len(), 0);
        assert_eq!(lb.version, 0);
    }

    #[test]
    fn test_add_entry() {
        let mut lb = PolicyLeaderboard::new();
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::Desktop,
            0.75,
            "2024-01-01T00:00:00Z",
        ));

        assert_eq!(lb.entries.len(), 1);
        assert_eq!(lb.version, 1);
        assert_eq!(lb.entries[0].policy_name, "Pressure");
    }

    #[test]
    fn test_top_policies() {
        let mut lb = PolicyLeaderboard::new();
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::Desktop,
            0.6,
            "2024-01-01T00:00:00Z",
        ));
        lb.add_entry(make_entry(
            "Hybrid",
            WorkloadClass::Desktop,
            0.8,
            "2024-01-01T00:00:00Z",
        ));
        lb.add_entry(make_entry(
            "Hotness",
            WorkloadClass::Desktop,
            0.7,
            "2024-01-01T00:00:00Z",
        ));

        let top = lb.top_policies(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].policy_name, "Hybrid");
        assert_eq!(top[1].policy_name, "Hotness");
    }

    #[test]
    fn test_top_for_workload() {
        let mut lb = PolicyLeaderboard::new();
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::Desktop,
            0.6,
            "2024-01-01T00:00:00Z",
        ));
        lb.add_entry(make_entry(
            "Hybrid",
            WorkloadClass::Desktop,
            0.8,
            "2024-01-01T00:00:00Z",
        ));
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::BuildSystem,
            0.7,
            "2024-01-01T00:00:00Z",
        ));

        let top_desktop = lb.top_for_workload(&WorkloadClass::Desktop, 10);
        assert_eq!(top_desktop.len(), 2);

        let top_build = lb.top_for_workload(&WorkloadClass::BuildSystem, 10);
        assert_eq!(top_build.len(), 1);
        assert_eq!(top_build[0].policy_name, "Pressure");
    }

    #[test]
    fn test_policy_history() {
        let mut lb = PolicyLeaderboard::new();
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::Desktop,
            0.5,
            "2024-01-01T00:00:00Z",
        ));
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::Desktop,
            0.7,
            "2024-02-01T00:00:00Z",
        ));
        lb.add_entry(make_entry(
            "Hybrid",
            WorkloadClass::Desktop,
            0.8,
            "2024-01-01T00:00:00Z",
        ));

        let history = lb.policy_history("Pressure");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].score, 0.5);
        assert_eq!(history[1].score, 0.7);
    }

    #[test]
    fn test_is_improving() {
        let mut lb = PolicyLeaderboard::new();
        // Add improving scores
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::Desktop,
            0.3,
            "2024-01-01T00:00:00Z",
        ));
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::Desktop,
            0.4,
            "2024-02-01T00:00:00Z",
        ));
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::Desktop,
            0.5,
            "2024-03-01T00:00:00Z",
        ));
        lb.add_entry(make_entry(
            "Pressure",
            WorkloadClass::Desktop,
            0.7,
            "2024-04-01T00:00:00Z",
        ));

        assert!(lb.is_improving("Pressure"));
    }

    #[test]
    fn test_from_report() {
        use crate::report::{
            BenchmarkReport, BenchmarkSummary, PolicyRankEntry, PolicyResultSummary,
            WorkloadResultSummary,
        };

        let wr = WorkloadResultSummary {
            workload_name: "idle_desktop".to_string(),
            winner: "Pressure".to_string(),
            winner_score: 0.58,
            policy_results: vec![PolicyResultSummary {
                policy_name: "Pressure".to_string(),
                average_score: 0.58,
                recommendation_count: 5,
                active_recommendation_count: 3,
                stability_index: 0.85,
                overall_quality: 0.72,
            }],
        };

        let report = BenchmarkReport {
            id: "bench-test".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            workload_results: vec![wr],
            policy_ranking: vec![PolicyRankEntry {
                policy_name: "Pressure".to_string(),
                rank: 1,
                average_score: 0.58,
                wins: 1,
                total_workloads: 1,
                win_rate: 1.0,
            }],
            summary: BenchmarkSummary {
                total_workloads: 1,
                total_policies: 1,
                total_rounds: 1,
                best_policy: "Pressure".to_string(),
                best_policy_score: 0.58,
                worst_policy: "Pressure".to_string(),
                worst_policy_score: 0.58,
                dominant_workload_class: "Desktop".to_string(),
            },
        };

        let leaderboard = from_report(&report, 1);

        assert!(!leaderboard.entries.is_empty());
        assert!(leaderboard.version >= 1);
        assert_eq!(leaderboard.entries[0].policy_name, "Pressure");
    }
}
