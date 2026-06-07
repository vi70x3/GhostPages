//! Evaluator Metrics for GhostPages.
//!
//! Prometheus metrics for the evaluator subsystem — recommendation scores,
//! policy comparisons, region promotions/demotions, stability index, and
//! policy wins.

use prometheus::{IntCounter, IntCounterVec, IntGauge, Registry};

use crate::scoring::RecommendationScore;

/// Metrics for the evaluator subsystem.
#[derive(Debug, Clone)]
pub struct EvaluatorMetrics {
    /// Recommendation score gauge (overall score, scaled by 1000 for integer gauge).
    pub recommendation_score: IntGauge,

    /// Policy comparison counter (by winner label).
    pub policy_comparison_total: IntCounterVec,

    /// Region promotions counter.
    pub region_promotions_total: IntCounter,

    /// Region demotions counter.
    pub region_demotions_total: IntCounter,

    /// Stability index gauge (scaled by 1000).
    pub stability_index: IntGauge,

    /// Policy wins counter (by policy name label).
    pub policy_wins_total: IntCounterVec,
}

impl EvaluatorMetrics {
    /// Create and register all evaluator metrics.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let recommendation_score = IntGauge::new(
            "ghost_recommendation_score",
            "Recommendation overall score (scaled by 1000)",
        )?;

        let policy_comparison_total = IntCounterVec::new(
            prometheus::Opts::new(
                "ghost_policy_comparison_total",
                "Total policy comparisons by winner",
            ),
            &["winner"],
        )?;

        let region_promotions_total = IntCounter::new(
            "ghost_region_promotions_total",
            "Total region promotions",
        )?;

        let region_demotions_total = IntCounter::new(
            "ghost_region_demotions_total",
            "Total region demotions",
        )?;

        let stability_index = IntGauge::new(
            "ghost_stability_index",
            "Current stability index (scaled by 1000)",
        )?;

        let policy_wins_total = IntCounterVec::new(
            prometheus::Opts::new(
                "ghost_policy_wins_total",
                "Total policy wins by policy name",
            ),
            &["policy"],
        )?;

        registry.register(Box::new(recommendation_score.clone()))?;
        registry.register(Box::new(policy_comparison_total.clone()))?;
        registry.register(Box::new(region_promotions_total.clone()))?;
        registry.register(Box::new(region_demotions_total.clone()))?;
        registry.register(Box::new(stability_index.clone()))?;
        registry.register(Box::new(policy_wins_total.clone()))?;

        Ok(Self {
            recommendation_score,
            policy_comparison_total,
            region_promotions_total,
            region_demotions_total,
            stability_index,
            policy_wins_total,
        })
    }

    /// Record a recommendation score (scaled to integer).
    pub fn record_recommendation_score(&self, score: &RecommendationScore) {
        let scaled = (score.overall_score.clamp(0.0, 1.0) * 1000.0).round() as i64;
        self.recommendation_score.set(scaled);
    }

    /// Record a policy comparison result.
    pub fn record_policy_comparison(&self, winner: &str) {
        self.policy_comparison_total
            .with_label_values(&[winner])
            .inc();
    }

    /// Record a region promotion.
    pub fn record_promotion(&self) {
        self.region_promotions_total.inc();
    }

    /// Record a region demotion.
    pub fn record_demotion(&self) {
        self.region_demotions_total.inc();
    }

    /// Record the current stability index.
    pub fn record_stability_index(&self, index: f32) {
        let scaled = (index.clamp(0.0, 1.0) * 1000.0).round() as i64;
        self.stability_index.set(scaled);
    }

    /// Record a policy win.
    pub fn record_policy_win(&self, policy_name: &str) {
        self.policy_wins_total
            .with_label_values(&[policy_name])
            .inc();
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_score(overall: f32) -> RecommendationScore {
        RecommendationScore {
            fault_reduction: overall,
            swap_reduction: overall,
            zram_efficiency: overall,
            pressure_reduction: overall,
            tier_balance: overall,
            stability: overall,
            overall_score: overall,
        }
    }

    #[test]
    fn test_evaluator_metrics_new() {
        let registry = Registry::new();
        let metrics = EvaluatorMetrics::new(&registry).unwrap();

        // All counters should start at 0.
        assert_eq!(metrics.region_promotions_total.get(), 0);
        assert_eq!(metrics.region_demotions_total.get(), 0);
        assert_eq!(metrics.recommendation_score.get(), 0);
        assert_eq!(metrics.stability_index.get(), 0);
    }

    #[test]
    fn test_record_recommendation_score() {
        let registry = Registry::new();
        let metrics = EvaluatorMetrics::new(&registry).unwrap();

        // Record a score of 0.75 → should set gauge to 750.
        let score = test_score(0.75);
        metrics.record_recommendation_score(&score);
        assert_eq!(metrics.recommendation_score.get(), 750);

        // Record a score of 1.0 → should set gauge to 1000.
        let score = test_score(1.0);
        metrics.record_recommendation_score(&score);
        assert_eq!(metrics.recommendation_score.get(), 1000);

        // Record a score of 0.0 → should set gauge to 0.
        let score = test_score(0.0);
        metrics.record_recommendation_score(&score);
        assert_eq!(metrics.recommendation_score.get(), 0);
    }

    #[test]
    fn test_record_policy_comparison() {
        let registry = Registry::new();
        let metrics = EvaluatorMetrics::new(&registry).unwrap();

        metrics.record_policy_comparison("Pressure");
        metrics.record_policy_comparison("Hybrid");
        metrics.record_policy_comparison("Pressure");

        // Pressure should have 2 comparisons.
        assert_eq!(
            metrics
                .policy_comparison_total
                .with_label_values(&["Pressure"])
                .get(),
            2
        );

        // Hybrid should have 1 comparison.
        assert_eq!(
            metrics
                .policy_comparison_total
                .with_label_values(&["Hybrid"])
                .get(),
            1
        );
    }

    #[test]
    fn test_record_promotion_demotion() {
        let registry = Registry::new();
        let metrics = EvaluatorMetrics::new(&registry).unwrap();

        metrics.record_promotion();
        metrics.record_promotion();
        metrics.record_promotion();
        assert_eq!(metrics.region_promotions_total.get(), 3);

        metrics.record_demotion();
        metrics.record_demotion();
        assert_eq!(metrics.region_demotions_total.get(), 2);
    }

    #[test]
    fn test_record_stability_index() {
        let registry = Registry::new();
        let metrics = EvaluatorMetrics::new(&registry).unwrap();

        // Record stability index of 0.85 → should set gauge to 850.
        metrics.record_stability_index(0.85);
        assert_eq!(metrics.stability_index.get(), 850);

        // Record stability index of 0.0.
        metrics.record_stability_index(0.0);
        assert_eq!(metrics.stability_index.get(), 0);

        // Record stability index of 1.0 → should set gauge to 1000.
        metrics.record_stability_index(1.0);
        assert_eq!(metrics.stability_index.get(), 1000);
    }

    #[test]
    fn test_record_policy_win() {
        let registry = Registry::new();
        let metrics = EvaluatorMetrics::new(&registry).unwrap();

        metrics.record_policy_win("Pressure");
        metrics.record_policy_win("Hybrid");
        metrics.record_policy_win("Pressure");
        metrics.record_policy_win("Hotness");

        // Pressure should have 2 wins.
        assert_eq!(
            metrics
                .policy_wins_total
                .with_label_values(&["Pressure"])
                .get(),
            2
        );

        // Hybrid should have 1 win.
        assert_eq!(
            metrics
                .policy_wins_total
                .with_label_values(&["Hybrid"])
                .get(),
            1
        );

        // Hotness should have 1 win.
        assert_eq!(
            metrics
                .policy_wins_total
                .with_label_values(&["Hotness"])
                .get(),
            1
        );
    }
}
