//! Cooldown tracker for recommendation stability.
//!
//! Prevents rapid successive recommendations for the same region by enforcing
//! a minimum time between recommendations. Uses a [`BTreeMap`] to track
//! per-region timestamps and supports deterministic time via [`TimeProvider`].

use std::collections::BTreeMap;
use std::sync::Arc;

use ghost_core::time::TimeProvider;

use crate::policy_rules::StabilityConfig;

/// Tracks cooldown state for recommendations and suppressions per region.
///
/// Each region (identified by a string key) has an associated timestamp
/// indicating when the last recommendation or suppression occurred.
/// The tracker uses a configurable cooldown duration to determine whether
/// a new recommendation is allowed.
pub struct CooldownTracker {
    /// Maps region keys to the timestamp (seconds since epoch) of the last recommendation.
    last_recommendation: BTreeMap<String, u64>,
    /// Maps region keys to the timestamp (seconds since epoch) of the last suppression.
    last_suppression: BTreeMap<String, u64>,
    /// Stability configuration controlling cooldown durations.
    config: StabilityConfig,
    /// Time provider for getting the current timestamp.
    time_provider: Arc<dyn TimeProvider>,
}

impl CooldownTracker {
    /// Create a new cooldown tracker with the given configuration.
    pub fn new(config: StabilityConfig, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            last_recommendation: BTreeMap::new(),
            last_suppression: BTreeMap::new(),
            config,
            time_provider,
        }
    }

    /// Check if a region can receive a new recommendation.
    ///
    /// Returns `true` if no recommendation has been recorded for this region,
    /// or if enough time has elapsed since the last recommendation.
    pub fn can_recommend(&self, region_key: &str) -> bool {
        let now = self.time_provider.timestamp_secs();
        match self.last_recommendation.get(region_key) {
            Some(&last_time) => {
                now.saturating_sub(last_time) >= self.config.recommendation_cooldown_secs
            }
            None => true,
        }
    }

    /// Record that a recommendation was emitted for a region.
    pub fn record_recommendation(&mut self, region_key: &str) {
        let now = self.time_provider.timestamp_secs();
        self.last_recommendation
            .insert(region_key.to_string(), now);
    }

    /// Record that a recommendation was suppressed for a region.
    pub fn record_suppression(&mut self, region_key: &str) {
        let now = self.time_provider.timestamp_secs();
        self.last_suppression
            .insert(region_key.to_string(), now);
    }

    /// Get the remaining cooldown time (in seconds) for a region.
    ///
    /// Returns `None` if the region has no active cooldown, or if the
    /// cooldown has already expired.
    pub fn remaining_cooldown(&self, region_key: &str) -> Option<u64> {
        let now = self.time_provider.timestamp_secs();
        self.last_recommendation.get(region_key).and_then(|&last_time| {
            let elapsed = now.saturating_sub(last_time);
            if elapsed < self.config.recommendation_cooldown_secs {
                Some(self.config.recommendation_cooldown_secs - elapsed)
            } else {
                None
            }
        })
    }

    /// Prune expired entries from both recommendation and suppression maps.
    ///
    /// Removes entries where the cooldown period has fully elapsed. This
    /// prevents unbounded growth of the internal maps over long running
    /// sessions.
    pub fn prune_expired(&mut self) {
        let now = self.time_provider.timestamp_secs();
        let rec_cooldown = self.config.recommendation_cooldown_secs;
        let sup_cooldown = self.config.suppression_cooldown_secs;

        self.last_recommendation
            .retain(|_, &mut last_time| now.saturating_sub(last_time) < rec_cooldown);
        self.last_suppression
            .retain(|_, &mut last_time| now.saturating_sub(last_time) < sup_cooldown);
    }

    /// Count the number of regions with active (non-expired) cooldowns.
    pub fn active_count(&self) -> usize {
        let now = self.time_provider.timestamp_secs();
        self.last_recommendation
            .values()
            .filter(|&&last_time| now.saturating_sub(last_time) < self.config.recommendation_cooldown_secs)
            .count()
    }

    /// Count the number of regions that are currently suppressed.
    pub fn suppressed_count(&self) -> usize {
        let now = self.time_provider.timestamp_secs();
        self.last_suppression
            .values()
            .filter(|&&last_time| now.saturating_sub(last_time) < self.config.suppression_cooldown_secs)
            .count()
    }

    /// Check if a region is within its suppression cooldown.
    pub fn is_suppressed(&self, region_key: &str) -> bool {
        let now = self.time_provider.timestamp_secs();
        match self.last_suppression.get(region_key) {
            Some(&last_time) => {
                now.saturating_sub(last_time) < self.config.suppression_cooldown_secs
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::time::DeterministicTimeProvider;

    fn test_tracker(cooldown_secs: u64) -> CooldownTracker {
        let config = StabilityConfig {
            recommendation_cooldown_secs: cooldown_secs,
            suppression_cooldown_secs: cooldown_secs * 2,
            ..StabilityConfig::default()
        };
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_secs(1)));
        CooldownTracker::new(config, time_provider)
    }

    #[test]
    fn test_new_tracker_allows_all_regions() {
        let tracker = test_tracker(30);
        assert!(tracker.can_recommend("region_a"));
        assert!(tracker.can_recommend("region_b"));
    }

    #[test]
    fn test_cooldown_blocks_within_window() {
        let mut tracker = test_tracker(30);
        tracker.record_recommendation("region_a");
        assert!(!tracker.can_recommend("region_a"));
        // Other regions are unaffected
        assert!(tracker.can_recommend("region_b"));
    }

    #[test]
    fn test_remaining_cooldown() {
        let mut tracker = test_tracker(30);
        tracker.record_recommendation("region_a");
        let remaining = tracker.remaining_cooldown("region_a");
        assert!(remaining.is_some());
        assert_eq!(remaining.unwrap(), 30);
    }

    #[test]
    fn test_no_cooldown_for_unknown_region() {
        let tracker = test_tracker(30);
        assert_eq!(tracker.remaining_cooldown("unknown"), None);
    }

    #[test]
    fn test_suppression_tracking() {
        let mut tracker = test_tracker(30);
        assert!(!tracker.is_suppressed("region_a"));
        tracker.record_suppression("region_a");
        assert!(tracker.is_suppressed("region_a"));
    }

    #[test]
    fn test_prune_expired_entries() {
        let mut tracker = test_tracker(10);
        tracker.record_recommendation("region_a");
        assert_eq!(tracker.last_recommendation.len(), 1);
        // Prune without time advancing — entry should remain
        tracker.prune_expired();
        assert_eq!(tracker.last_recommendation.len(), 1);
    }
}
