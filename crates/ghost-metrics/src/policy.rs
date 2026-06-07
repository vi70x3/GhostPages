//! Policy metrics for recommendation tracking.
//!
//! Tracks policy evaluation outcomes, recommendation types, confidence,
//! suppression events, and evaluation latency.

use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Registry};

use std::time::Duration;

/// Recommendation action types for metrics labeling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecommendationAction {
    /// Promote to a faster tier.
    Promote,
    /// Demote to a slower tier.
    Demote,
    /// No action recommended.
    NoAction,
}

impl RecommendationAction {
    /// Convert to string label for Prometheus metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            RecommendationAction::Promote => "promote",
            RecommendationAction::Demote => "demote",
            RecommendationAction::NoAction => "no_action",
        }
    }
}

/// A policy recommendation with action type and confidence.
#[derive(Debug, Clone)]
pub struct Recommendation {
    /// The action to take.
    pub action: RecommendationAction,
    /// Confidence score (0.0-1.0).
    pub confidence: f32,
}

impl Recommendation {
    /// Create a new recommendation.
    pub fn new(action: RecommendationAction, confidence: f32) -> Self {
        Self { action, confidence }
    }

    /// Create a promotion recommendation.
    pub fn promote(confidence: f32) -> Self {
        Self {
            action: RecommendationAction::Promote,
            confidence,
        }
    }

    /// Create a demotion recommendation.
    pub fn demote(confidence: f32) -> Self {
        Self {
            action: RecommendationAction::Demote,
            confidence,
        }
    }

    /// Create a no-action recommendation.
    pub fn no_action(confidence: f32) -> Self {
        Self {
            action: RecommendationAction::NoAction,
            confidence,
        }
    }
}

/// Metrics for the policy runtime subsystem.
#[derive(Debug, Clone)]
pub struct PolicyMetrics {
    /// Total recommendations by action type.
    pub recommendations_total: IntCounterVec,
    /// Total promotions recommended.
    pub promotions_total: IntCounter,
    /// Total demotions recommended.
    pub demotions_total: IntCounter,
    /// Total no-action recommendations.
    pub no_action_total: IntCounter,
    /// Current recommendation confidence (scaled by 1000).
    pub recommendation_confidence: IntGauge,
    /// Total suppressed recommendations (rate limited).
    pub suppressed_recommendations_total: IntCounter,
    /// Total cooldown hits (recommendations blocked by cooldown).
    pub cooldown_hits_total: IntCounter,
    /// Histogram of policy evaluation duration.
    pub evaluation_duration_seconds: Histogram,
}

impl PolicyMetrics {
    /// Create a new PolicyMetrics instance and register with the given registry.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let recommendations_total = IntCounterVec::new(
            prometheus::Opts::new(
                "ghostpages_policy_recommendations_total",
                "Total recommendations by action type",
            ),
            &["action_type"],
        )?;
        let promotions_total = IntCounter::new(
            "ghostpages_policy_promotions_total",
            "Total promotions recommended",
        )?;
        let demotions_total = IntCounter::new(
            "ghostpages_policy_demotions_total",
            "Total demotions recommended",
        )?;
        let no_action_total = IntCounter::new(
            "ghostpages_policy_no_action_total",
            "Total no-action recommendations",
        )?;
        let recommendation_confidence = IntGauge::new(
            "ghostpages_policy_recommendation_confidence",
            "Current recommendation confidence (scaled by 1000)",
        )?;
        let suppressed_recommendations_total = IntCounter::new(
            "ghostpages_policy_suppressed_recommendations_total",
            "Total suppressed recommendations (rate limited)",
        )?;
        let cooldown_hits_total = IntCounter::new(
            "ghostpages_policy_cooldown_hits_total",
            "Total cooldown hits (recommendations blocked by cooldown)",
        )?;
        let evaluation_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "ghostpages_policy_evaluation_duration_seconds",
                "Policy evaluation duration in seconds",
            )
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
        )?;

        registry.register(Box::new(recommendations_total.clone()))?;
        registry.register(Box::new(promotions_total.clone()))?;
        registry.register(Box::new(demotions_total.clone()))?;
        registry.register(Box::new(no_action_total.clone()))?;
        registry.register(Box::new(recommendation_confidence.clone()))?;
        registry.register(Box::new(suppressed_recommendations_total.clone()))?;
        registry.register(Box::new(cooldown_hits_total.clone()))?;
        registry.register(Box::new(evaluation_duration_seconds.clone()))?;

        Ok(Self {
            recommendations_total,
            promotions_total,
            demotions_total,
            no_action_total,
            recommendation_confidence,
            suppressed_recommendations_total,
            cooldown_hits_total,
            evaluation_duration_seconds,
        })
    }

    /// Record a policy recommendation.
    pub fn record_recommendation(&self, rec: &Recommendation) {
        let action_str = rec.action.as_str();
        self.recommendations_total
            .with_label_values(&[action_str])
            .inc();

        match rec.action {
            RecommendationAction::Promote => self.promotions_total.inc(),
            RecommendationAction::Demote => self.demotions_total.inc(),
            RecommendationAction::NoAction => self.no_action_total.inc(),
        }

        // Update confidence gauge (scaled by 1000)
        let scaled = (rec.confidence.clamp(0.0, 1.0) * 1000.0).round() as i64;
        self.recommendation_confidence.set(scaled);
    }

    /// Record a suppressed recommendation (rate limited).
    pub fn record_suppression(&self) {
        self.suppressed_recommendations_total.inc();
    }

    /// Record a cooldown hit (recommendation blocked by cooldown).
    pub fn record_cooldown_hit(&self) {
        self.cooldown_hits_total.inc();
    }

    /// Record the duration of a policy evaluation.
    pub fn record_evaluation_duration(&self, duration: Duration) {
        self.evaluation_duration_seconds
            .observe(duration.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recommendation_action_as_str() {
        assert_eq!(RecommendationAction::Promote.as_str(), "promote");
        assert_eq!(RecommendationAction::Demote.as_str(), "demote");
        assert_eq!(RecommendationAction::NoAction.as_str(), "no_action");
    }

    #[test]
    fn test_recommendation_helpers() {
        let rec = Recommendation::promote(0.8);
        assert_eq!(rec.action, RecommendationAction::Promote);
        assert_eq!(rec.confidence, 0.8);

        let rec = Recommendation::demote(0.6);
        assert_eq!(rec.action, RecommendationAction::Demote);

        let rec = Recommendation::no_action(0.9);
        assert_eq!(rec.action, RecommendationAction::NoAction);
    }

    #[test]
    fn test_policy_metrics_new() {
        let registry = Registry::new();
        let metrics = PolicyMetrics::new(&registry).unwrap();
        assert_eq!(metrics.promotions_total.get(), 0);
        assert_eq!(metrics.demotions_total.get(), 0);
        assert_eq!(metrics.no_action_total.get(), 0);
    }

    #[test]
    fn test_record_recommendation() {
        let registry = Registry::new();
        let metrics = PolicyMetrics::new(&registry).unwrap();

        metrics.record_recommendation(&Recommendation::promote(0.85));
        assert_eq!(metrics.promotions_total.get(), 1);
        assert_eq!(metrics.recommendation_confidence.get(), 850);

        metrics.record_recommendation(&Recommendation::demote(0.7));
        assert_eq!(metrics.demotions_total.get(), 1);
        assert_eq!(metrics.recommendation_confidence.get(), 700);

        metrics.record_recommendation(&Recommendation::no_action(0.95));
        assert_eq!(metrics.no_action_total.get(), 1);
        assert_eq!(metrics.recommendation_confidence.get(), 950);
    }

    #[test]
    fn test_record_suppression() {
        let registry = Registry::new();
        let metrics = PolicyMetrics::new(&registry).unwrap();

        metrics.record_suppression();
        metrics.record_suppression();
        assert_eq!(metrics.suppressed_recommendations_total.get(), 2);
    }

    #[test]
    fn test_record_cooldown_hit() {
        let registry = Registry::new();
        let metrics = PolicyMetrics::new(&registry).unwrap();

        metrics.record_cooldown_hit();
        assert_eq!(metrics.cooldown_hits_total.get(), 1);
    }

    #[test]
    fn test_record_evaluation_duration() {
        let registry = Registry::new();
        let metrics = PolicyMetrics::new(&registry).unwrap();

        metrics.record_evaluation_duration(Duration::from_millis(5));
        metrics.record_evaluation_duration(Duration::from_micros(500));
        // Histogram doesn't have a simple get(), so we just verify no panic
    }
}