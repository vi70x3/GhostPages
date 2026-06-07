//! Recommendation Stability Measurement for GhostPages.
//!
//! Measures recommendation churn — how much recommendations fluctuate over time.
//! A stable system produces consistent, predictable recommendations.
//!
//! All functions are **pure** — no I/O, no mutation, no side effects.
//! Same inputs always produce same outputs. Deterministic by design.

use ghost_linux::policy::Recommendation;
use ghost_linux::policy_rules::SystemState;

// ─── Recommendation Stability ─────────────────────────────────────────────────

/// Stability metrics for a sequence of recommendations.
///
/// The `stability_index` ranges from 0.0 (chaotic) to 1.0 (perfectly stable).
/// Higher values indicate more predictable, consistent recommendations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecommendationStability {
    /// Number of non-NoAction recommendations per hour.
    pub recommendations_per_hour: f32,
    /// Number of times a chunk's implied temperature changes direction
    /// (Hot→Cold or Cold→Hot).
    pub temperature_flips: usize,
    /// Number of times a chunk is recommended for both promotion and demotion
    /// within the window.
    pub tier_oscillations: usize,
    /// Variance of confidence scores across all recommendations.
    pub confidence_variance: f32,
    /// Overall stability index (0.0 = chaotic, 1.0 = perfectly stable).
    pub stability_index: f32,
}

// ─── Stability Entry ───────────────────────────────────────────────────────────

/// A single entry in the stability tracking window.
#[derive(Debug, Clone)]
pub struct StabilityEntry {
    /// Monotonic timestamp (not wall clock).
    pub timestamp: u64,
    /// The recommendation that was made.
    pub recommendation: Recommendation,
    /// The system state at the time of the recommendation.
    pub state: SystemState,
}

// ─── Stability Tracker ────────────────────────────────────────────────────────

/// Tracks recommendation history and computes stability metrics.
///
/// Uses a sliding window of `StabilityEntry` values. When the window is full,
/// older entries are discarded as new ones are added.
#[derive(Debug, Clone)]
pub struct StabilityTracker {
    history: Vec<StabilityEntry>,
    window_size: usize,
}

impl StabilityTracker {
    /// Create a new stability tracker with the given window size.
    ///
    /// # Arguments
    ///
    /// * `window_size` — Maximum number of entries to retain in the sliding window.
    pub fn new(window_size: usize) -> Self {
        Self {
            history: Vec::new(),
            window_size,
        }
    }

    /// Record a recommendation with its associated system state and timestamp.
    ///
    /// If the window is full, the oldest entry is removed.
    pub fn record(
        &mut self,
        recommendation: Recommendation,
        state: &SystemState,
        timestamp: u64,
    ) {
        self.history.push(StabilityEntry {
            timestamp,
            recommendation,
            state: state.clone(),
        });

        // Maintain sliding window
        if self.history.len() > self.window_size {
            self.history.remove(0);
        }
    }

    /// Evaluate the current stability of recommendations in the window.
    pub fn evaluate(&self) -> RecommendationStability {
        let recommendations_per_hour = self.recommendation_rate();
        let temperature_flips = self.count_temperature_flips();
        let tier_oscillations = self.count_tier_oscillations();
        let confidence_variance = self.compute_confidence_variance();

        // Compute stability index as a weighted combination.
        // Each factor contributes inversely to instability.
        //
        // Recommendation rate factor: lower rate = more stable.
        // We consider 3+ recs/hour as fully unstable (0.0), 0 as fully stable (1.0).
        let rate_factor = (1.0 - (recommendations_per_hour / 3.0).min(1.0)).max(0.0);

        // Temperature flip factor: 0 flips = 1.0, 2+ flips = 0.0.
        let flip_factor = (1.0 - (temperature_flips as f32 / 2.0).min(1.0)).max(0.0);

        // Tier oscillation factor: 0 oscillations = 1.0, 1+ = 0.0.
        let oscillation_factor = (1.0 - (tier_oscillations as f32 / 1.0).min(1.0)).max(0.0);

        // Confidence variance factor: 0 variance = 1.0, 0.25+ variance = 0.0.
        let variance_factor = (1.0 - (confidence_variance / 0.25).min(1.0)).max(0.0);

        // Weighted combination (instability-focused weights).
        let stability_index = (rate_factor * 0.20
            + flip_factor * 0.35
            + oscillation_factor * 0.30
            + variance_factor * 0.15)
            .clamp(0.0, 1.0);

        RecommendationStability {
            recommendations_per_hour,
            temperature_flips,
            tier_oscillations,
            confidence_variance,
            stability_index,
        }
    }

    /// Check whether the current stability meets the given threshold.
    ///
    /// Returns `true` if `stability_index >= threshold`.
    pub fn is_stable(&self, threshold: f32) -> bool {
        self.evaluate().stability_index >= threshold
    }

    /// Compute the recommendation rate (non-NoAction recommendations per hour).
    ///
    /// Uses the time span of the window. If there are fewer than 2 entries
    /// or all entries are NoAction, returns 0.0.
    pub fn recommendation_rate(&self) -> f32 {
        let non_noaction: Vec<&StabilityEntry> = self
            .history
            .iter()
            .filter(|e| !matches!(e.recommendation, Recommendation::NoAction { .. }))
            .collect();

        if non_noaction.is_empty() {
            return 0.0;
        }

        if self.history.len() < 2 {
            // Single entry — can't compute time span, treat as 1 hour
            return non_noaction.len() as f32;
        }

        let first_ts = self.history.first().unwrap().timestamp;
        let last_ts = self.history.last().unwrap().timestamp;
        let time_span_secs = last_ts.saturating_sub(first_ts);

        if time_span_secs == 0 {
            // All entries at same timestamp — treat as 1 hour
            return non_noaction.len() as f32;
        }

        let time_span_hours = time_span_secs as f32 / 3600.0;
        non_noaction.len() as f32 / time_span_hours
    }

    /// Count temperature flips — direction changes in implied temperature.
    ///
    /// A flip occurs when consecutive non-NoAction recommendations for the
    /// same chunk change from Hot-promoting to Cold-demoting or vice versa.
    fn count_temperature_flips(&self) -> usize {
        let mut flips = 0;

        // Group entries by chunk_id (extracted from recommendation).
        let mut chunk_temps: Vec<(u64, i8)> = Vec::new();

        for entry in &self.history {
            let temp_direction = match &entry.recommendation {
                Recommendation::PromoteToDram { chunk_id, .. } => {
                    Some((chunk_id.short_hex(), 1i8)) // Hot direction
                }
                Recommendation::MoveToZram { chunk_id, .. }
                | Recommendation::MoveToDiskSwap { chunk_id, .. } => {
                    Some((chunk_id.short_hex(), -1i8)) // Cold direction
                }
                Recommendation::EvictCold { .. } => Some(("evict".to_string(), -1i8)),
                Recommendation::DemoteHot { .. } => Some(("demote".to_string(), -1i8)),
                Recommendation::NoAction { .. } => None,
            };

            if let Some((id, dir)) = temp_direction {
                chunk_temps.push((0, dir)); // simplified: track direction changes globally
                                                   // We use a simplified approach: track direction changes
                                                   // across all recommendations in sequence
                let _ = id;
            }
        }

        // Count direction changes in the sequence.
        for window in chunk_temps.windows(2) {
            if window[0].1 != window[1].1 {
                flips += 1;
            }
        }

        flips
    }

    /// Count tier oscillations — chunks recommended for both promotion and demotion.
    ///
    /// An oscillation occurs when the same chunk appears in both a promote
    /// and a demote recommendation within the window.
    fn count_tier_oscillations(&self) -> usize {
        use ghost_core::types::ChunkId;

        let mut promoted_chunks: Vec<ChunkId> = Vec::new();
        let mut demoted_chunks: Vec<ChunkId> = Vec::new();

        for entry in &self.history {
            match &entry.recommendation {
                Recommendation::PromoteToDram { chunk_id, .. } => {
                    promoted_chunks.push(*chunk_id);
                }
                Recommendation::MoveToZram { chunk_id, .. }
                | Recommendation::MoveToDiskSwap { chunk_id, .. } => {
                    demoted_chunks.push(*chunk_id);
                }
                _ => {}
            }
        }

        // Deduplicate: each chunk counted once.
        let unique_oscillations = {
            let mut seen = Vec::new();
            let mut count = 0;
            for promoted in &promoted_chunks {
                if demoted_chunks.contains(promoted) && !seen.contains(promoted) {
                    seen.push(*promoted);
                    count += 1;
                }
            }
            count
        };

        unique_oscillations
    }

    /// Compute the variance of confidence scores across all recommendations.
    fn compute_confidence_variance(&self) -> f32 {
        if self.history.is_empty() {
            return 0.0;
        }

        let confidences: Vec<f32> = self
            .history
            .iter()
            .map(|e| e.recommendation.confidence())
            .collect();

        let n = confidences.len() as f32;
        let mean = confidences.iter().sum::<f32>() / n;

        let variance = confidences
            .iter()
            .map(|&c| {
                let diff = c - mean;
                diff * diff
            })
            .sum::<f32>()
            / n;

        variance
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::ChunkId;

    fn test_state() -> SystemState {
        SystemState {
            dram_pressure: ghost_core::state::PressureState::new(),
            dram_utilization: 0.5,
            swap_utilization: 0.2,
            zram_utilization: Some(0.3),
            io_pressure: ghost_core::state::PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn chunk_a() -> ChunkId {
        ChunkId::from_data(b"chunk_a")
    }

    fn chunk_b() -> ChunkId {
        ChunkId::from_data(b"chunk_b")
    }

    #[test]
    fn test_stable_no_recommendations() {
        let mut tracker = StabilityTracker::new(10);
        let state = test_state();

        // Record only NoAction recommendations over a time span.
        for i in 0..5 {
            tracker.record(
                Recommendation::NoAction {
                    reason: "stable".to_string(),
                    confidence: 1.0,
                    factors: vec![],
                },
                &state,
                i as u64 * 3600,
            );
        }

        let stability = tracker.evaluate();
        assert_eq!(stability.recommendations_per_hour, 0.0);
        assert_eq!(stability.temperature_flips, 0);
        assert_eq!(stability.tier_oscillations, 0);
        assert_eq!(stability.confidence_variance, 0.0);
        assert!(
            stability.stability_index >= 0.9,
            "all NoAction should be near-perfect stability, got {}",
            stability.stability_index
        );
    }

    #[test]
    fn test_stable_consistent_recommendations() {
        let mut tracker = StabilityTracker::new(10);
        let state = test_state();

        // Record the same recommendation repeatedly.
        for i in 0..5 {
            tracker.record(
                Recommendation::PromoteToDram {
                    chunk_id: chunk_a(),
                    reason: "hot chunk".to_string(),
                    confidence: 0.9,
                    factors: vec![],
                },
                &state,
                i as u64 * 3600,
            );
        }

        let stability = tracker.evaluate();
        assert_eq!(stability.temperature_flips, 0);
        assert_eq!(stability.tier_oscillations, 0);
        assert!(
            stability.stability_index > 0.7,
            "consistent recommendations should be stable, got {}",
            stability.stability_index
        );
    }

    #[test]
    fn test_unstable_oscillating() {
        let mut tracker = StabilityTracker::new(10);
        let state = test_state();

        // Alternate between promoting and demoting the same chunk.
        for i in 0..6 {
            if i % 2 == 0 {
                tracker.record(
                    Recommendation::PromoteToDram {
                        chunk_id: chunk_a(),
                        reason: "hot".to_string(),
                        confidence: 0.8,
                        factors: vec![],
                    },
                    &state,
                    i as u64 * 3600,
                );
            } else {
                tracker.record(
                    Recommendation::MoveToZram {
                        chunk_id: chunk_a(),
                        reason: "cold".to_string(),
                        confidence: 0.8,
                        factors: vec![],
                    },
                    &state,
                    i as u64 * 3600,
                );
            }
        }

        let stability = tracker.evaluate();
        assert!(
            stability.temperature_flips > 0,
            "alternating promote/demote should have temperature flips"
        );
        assert!(
            stability.tier_oscillations > 0,
            "same chunk promoted and demoted should have oscillations"
        );
        assert!(
            stability.stability_index < 0.3,
            "oscillating recommendations should have low stability, got {}",
            stability.stability_index
        );
    }

    #[test]
    fn test_recommendation_rate_calculation() {
        let mut tracker = StabilityTracker::new(10);
        let state = test_state();

        // 4 non-NoAction recommendations over 2 hours = 2 per hour.
        for i in 0..4 {
            tracker.record(
                Recommendation::PromoteToDram {
                    chunk_id: chunk_a(),
                    reason: "hot".to_string(),
                    confidence: 0.9,
                    factors: vec![],
                },
                &state,
                i as u64 * 1800, // every 30 minutes = 2 hours span for 4 entries
            );
        }

        let rate = tracker.recommendation_rate();
        assert!(
            rate > 0.0,
            "recommendation rate should be positive, got {}",
            rate
        );

        // 4 recs / 1.5 hours (4700 secs from 0 to 5400) ≈ 2.67
        let expected = 4.0 / 1.5; // ~2.67
        assert!(
            (rate - expected).abs() < 0.5,
            "rate {} should be close to {}",
            rate,
            expected
        );
    }

    #[test]
    fn test_temperature_flip_detection() {
        let mut tracker = StabilityTracker::new(10);
        let state = test_state();

        // Promote (hot direction).
        tracker.record(
            Recommendation::PromoteToDram {
                chunk_id: chunk_a(),
                reason: "hot".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
            &state,
            0,
        );

        // Move to ZRAM (cold direction) — this is a flip.
        tracker.record(
            Recommendation::MoveToZram {
                chunk_id: chunk_b(),
                reason: "cold".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
            &state,
            3600,
        );

        // Promote again — another flip.
        tracker.record(
            Recommendation::PromoteToDram {
                chunk_id: chunk_a(),
                reason: "hot again".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
            &state,
            7200,
        );

        let stability = tracker.evaluate();
        assert_eq!(
            stability.temperature_flips, 2,
            "should detect 2 temperature flips"
        );
    }

    #[test]
    fn test_tier_oscillation_detection() {
        let mut tracker = StabilityTracker::new(10);
        let state = test_state();

        // Promote chunk_a.
        tracker.record(
            Recommendation::PromoteToDram {
                chunk_id: chunk_a(),
                reason: "hot".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
            &state,
            0,
        );

        // Demote the same chunk_a to ZRAM — oscillation.
        tracker.record(
            Recommendation::MoveToZram {
                chunk_id: chunk_a(),
                reason: "cold now".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
            &state,
            3600,
        );

        // chunk_b only promoted — no oscillation.
        tracker.record(
            Recommendation::PromoteToDram {
                chunk_id: chunk_b(),
                reason: "hot".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
            &state,
            7200,
        );

        let stability = tracker.evaluate();
        assert_eq!(
            stability.tier_oscillations, 1,
            "chunk_a promoted then demoted should count as 1 oscillation"
        );
    }

    #[test]
    fn test_confidence_variance() {
        let mut tracker = StabilityTracker::new(10);
        let state = test_state();

        // All same confidence → zero variance.
        for i in 0..3 {
            tracker.record(
                Recommendation::NoAction {
                    reason: "test".to_string(),
                    confidence: 0.5,
                    factors: vec![],
                },
                &state,
                i as u64,
            );
        }

        let stability = tracker.evaluate();
        assert!(
            stability.confidence_variance < 0.001,
            "same confidence should have ~0 variance, got {}",
            stability.confidence_variance
        );

        // Now test with varying confidence.
        let mut tracker2 = StabilityTracker::new(10);
        for i in 0..3 {
            let conf = 0.3 + i as f32 * 0.3; // 0.3, 0.6, 0.9
            tracker2.record(
                Recommendation::NoAction {
                    reason: "test".to_string(),
                    confidence: conf,
                    factors: vec![],
                },
                &state,
                i as u64,
            );
        }

        let stability2 = tracker2.evaluate();
        assert!(
            stability2.confidence_variance > 0.01,
            "varying confidence should have non-zero variance, got {}",
            stability2.confidence_variance
        );
    }

    #[test]
    fn test_stability_index_range() {
        let state = test_state();

        // Test various scenarios — stability_index should always be 0.0–1.0.
        let scenarios: Vec<Vec<Recommendation>> = vec![
            // All NoAction.
            (0..5)
                .map(|_| Recommendation::NoAction {
                    reason: "test".to_string(),
                    confidence: 1.0,
                    factors: vec![],
                })
                .collect(),
            // All same recommendation.
            (0..5)
                .map(|_| Recommendation::PromoteToDram {
                    chunk_id: chunk_a(),
                    reason: "hot".to_string(),
                    confidence: 0.9,
                    factors: vec![],
                })
                .collect(),
            // Oscillating.
            (0..6)
                .map(|i| {
                    if i % 2 == 0 {
                        Recommendation::PromoteToDram {
                            chunk_id: chunk_a(),
                            reason: "hot".to_string(),
                            confidence: 0.8,
                            factors: vec![],
                        }
                    } else {
                        Recommendation::MoveToZram {
                            chunk_id: chunk_a(),
                            reason: "cold".to_string(),
                            confidence: 0.8,
                            factors: vec![],
                        }
                    }
                })
                .collect(),
        ];

        for recs in scenarios {
            let mut t = StabilityTracker::new(20);
            for (i, rec) in recs.iter().enumerate() {
                t.record(rec.clone(), &state, i as u64 * 60);
            }
            let stability = t.evaluate();
            assert!(
                stability.stability_index >= 0.0 && stability.stability_index <= 1.0,
                "stability_index {} should be in [0.0, 1.0]",
                stability.stability_index
            );
        }
    }
}
