//! Temperature stability checker for recommendation flapping prevention.
//!
//! Tracks temperature history per region and determines whether the temperature
//! has been stable for a configurable number of consecutive samples. Also
//! detects warming/cooling trends to provide early signals before threshold
//! crossings.

use std::collections::BTreeMap;

use ghost_core::hotness_history::TemperatureTrend;
use ghost_core::hotness_provider::Temperature;

use crate::policy_rules::StabilityConfig;

/// Tracks temperature history and determines stability for regions.
///
/// Maintains a rolling window of temperature observations per region and
/// provides methods to check stability, compute the stable temperature,
/// and detect trends (warming/cooling).
pub struct StabilityChecker {
    /// Maps region keys to their temperature history (oldest first).
    history: BTreeMap<String, Vec<Temperature>>,
    /// Stability configuration controlling window size and margins.
    config: StabilityConfig,
}

impl StabilityChecker {
    /// Create a new stability checker with the given configuration.
    pub fn new(config: StabilityConfig) -> Self {
        Self {
            history: BTreeMap::new(),
            config,
        }
    }

    /// Record a temperature observation for a region.
    ///
    /// Appends the temperature to the region's history. If the history
    /// exceeds twice the stability window size, the oldest entries are
    /// trimmed to prevent unbounded growth.
    pub fn record(&mut self, region_key: &str, temperature: Temperature) {
        let entry = self
            .history
            .entry(region_key.to_string())
            .or_insert_with(Vec::new);
        entry.push(temperature);

        // Trim to a reasonable capacity (2x the stability window)
        let max_len = self.config.temperature_stability_window * 2;
        while entry.len() > max_len {
            entry.remove(0);
        }
    }

    /// Check if temperature is stable for a region.
    ///
    /// Temperature is considered "stable" when the last N consecutive
    /// samples (where N = `temperature_stability_window`) all have the
    /// same temperature value.
    pub fn is_stable(&self, region_key: &str) -> bool {
        let history = match self.history.get(region_key) {
            Some(h) => h,
            None => return false,
        };

        if history.len() < self.config.temperature_stability_window {
            return false;
        }

        let window = &history[history.len() - self.config.temperature_stability_window..];
        let first = window[0];
        window.iter().all(|&t| t == first)
    }

    /// Get the stable temperature for a region.
    ///
    /// Returns the most common temperature in the stability window if
    /// the region is stable, or `None` if it is not.
    pub fn stable_temperature(&self, region_key: &str) -> Option<Temperature> {
        if !self.is_stable(region_key) {
            return None;
        }

        let history = self.history.get(region_key)?;
        let window = &history[history.len() - self.config.temperature_stability_window..];
        Some(window[0])
    }

    /// Detect the temperature trend for a region.
    ///
    /// Analyzes the temperature history to determine if the region is
    /// warming up, cooling down, stable, or flapping (unstable).
    pub fn trend(&self, region_key: &str) -> Option<TemperatureTrend> {
        let history = match self.history.get(region_key) {
            Some(h) => h,
            None => return Some(TemperatureTrend::Stable(Temperature::Frozen)),
        };

        if history.is_empty() {
            return Some(TemperatureTrend::Stable(Temperature::Frozen));
        }

        if history.len() < 2 {
            return Some(TemperatureTrend::Stable(history[0]));
        }

        // Count temperature changes
        let mut changes = 0;
        for i in 1..history.len() {
            if history[i] != history[i - 1] {
                changes += 1;
            }
        }

        // If more than 50% of positions changed, it's flapping
        if changes > history.len() / 2 {
            return Some(TemperatureTrend::Flapping);
        }

        let first = history[0];
        let last = history[history.len() - 1];

        if last.value() > first.value() {
            Some(TemperatureTrend::Warming(first, last))
        } else if last.value() < first.value() {
            Some(TemperatureTrend::Cooling(first, last))
        } else {
            Some(TemperatureTrend::Stable(last))
        }
    }

    /// Classify temperature with hysteresis.
    ///
    /// Given the current classification and a new raw temperature reading,
    /// apply hysteresis to prevent rapid oscillation at threshold boundaries.
    ///
    /// The hysteresis margin means:
    /// - If currently Hot, the reading must drop below `hot_threshold * (1 - margin)` to downgrade.
    /// - If currently Cold/Frozen, the reading must exceed `hot_threshold * (1 + margin)` to upgrade.
    pub fn classify_with_hysteresis(
        &self,
        region_key: &str,
        current: Temperature,
        access_count: u64,
        hot_threshold: u64,
        cold_threshold: u64,
    ) -> Temperature {
        let margin = self.config.hysteresis_margin;

        match current {
            Temperature::Hot | Temperature::Warm => {
                // Currently hot/warm — require significant cooling to downgrade
                let effective_cold = (cold_threshold as f32 * (1.0 - margin)) as u64;
                if access_count < effective_cold.max(1) {
                    Temperature::from_access_count(access_count)
                } else {
                    current
                }
            }
            Temperature::Cold | Temperature::Frozen => {
                // Currently cold/frozen — require significant heating to upgrade
                let effective_hot = (hot_threshold as f32 * (1.0 + margin)) as u64;
                if access_count >= effective_hot {
                    Temperature::from_access_count(access_count)
                } else {
                    current
                }
            }
        }
    }

    /// Get the number of recorded observations for a region.
    pub fn observation_count(&self, region_key: &str) -> usize {
        self.history
            .get(region_key)
            .map(|h| h.len())
            .unwrap_or(0)
    }

    /// Clear all history for a specific region.
    pub fn clear_region(&mut self, region_key: &str) {
        self.history.remove(region_key);
    }

    /// Clear all history.
    pub fn clear_all(&mut self) {
        self.history.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_checker(window: usize) -> StabilityChecker {
        let config = StabilityConfig {
            temperature_stability_window: window,
            hysteresis_margin: 0.1,
            ..StabilityConfig::default()
        };
        StabilityChecker::new(config)
    }

    #[test]
    fn test_empty_history_not_stable() {
        let checker = test_checker(3);
        assert!(!checker.is_stable("region_a"));
        assert_eq!(checker.stable_temperature("region_a"), None);
    }

    #[test]
    fn test_stable_after_n_same_samples() {
        let mut checker = test_checker(3);
        checker.record("region_a", Temperature::Hot);
        assert!(!checker.is_stable("region_a"));
        checker.record("region_a", Temperature::Hot);
        assert!(!checker.is_stable("region_a"));
        checker.record("region_a", Temperature::Hot);
        assert!(checker.is_stable("region_a"));
        assert_eq!(checker.stable_temperature("region_a"), Some(Temperature::Hot));
    }

    #[test]
    fn test_stability_broken_by_change() {
        let mut checker = test_checker(3);
        checker.record("region_a", Temperature::Hot);
        checker.record("region_a", Temperature::Hot);
        checker.record("region_a", Temperature::Hot);
        assert!(checker.is_stable("region_a"));
        checker.record("region_a", Temperature::Cold);
        assert!(!checker.is_stable("region_a"));
    }

    #[test]
    fn test_trend_stable() {
        let mut checker = test_checker(3);
        checker.record("region_a", Temperature::Hot);
        checker.record("region_a", Temperature::Hot);
        checker.record("region_a", Temperature::Hot);
        match checker.trend("region_a") {
            Some(TemperatureTrend::Stable(t)) => assert_eq!(t, Temperature::Hot),
            other => panic!("Expected Stable(Hot), got {:?}", other),
        }
    }

    #[test]
    fn test_trend_warming() {
        let mut checker = test_checker(3);
        // 4 samples, 2 changes = 50%, not > 50% → not flapping
        checker.record("region_a", Temperature::Frozen);
        checker.record("region_a", Temperature::Frozen);
        checker.record("region_a", Temperature::Cold);
        checker.record("region_a", Temperature::Warm);
        match checker.trend("region_a") {
            Some(TemperatureTrend::Warming(from, to)) => {
                assert_eq!(from, Temperature::Frozen);
                assert_eq!(to, Temperature::Warm);
            }
            other => panic!("Expected Warming, got {:?}", other),
        }
    }

    #[test]
    fn test_trend_cooling() {
        let mut checker = test_checker(3);
        // 4 samples, 2 changes = 50%, not > 50% → not flapping
        checker.record("region_a", Temperature::Hot);
        checker.record("region_a", Temperature::Hot);
        checker.record("region_a", Temperature::Warm);
        checker.record("region_a", Temperature::Cold);
        match checker.trend("region_a") {
            Some(TemperatureTrend::Cooling(from, to)) => {
                assert_eq!(from, Temperature::Hot);
                assert_eq!(to, Temperature::Cold);
            }
            other => panic!("Expected Cooling, got {:?}", other),
        }
    }

    #[test]
    fn test_trend_flapping() {
        let mut checker = test_checker(3);
        checker.record("region_a", Temperature::Hot);
        checker.record("region_a", Temperature::Frozen);
        checker.record("region_a", Temperature::Hot);
        checker.record("region_a", Temperature::Frozen);
        assert_eq!(checker.trend("region_a"), Some(TemperatureTrend::Flapping));
    }

    #[test]
    fn test_hysteresis_prevents_downgrade() {
        let checker = test_checker(3);
        // Currently Hot, access count just below hot threshold
        // With hysteresis margin of 0.1, effective cold threshold = 10 * 0.9 = 9
        let result = checker.classify_with_hysteresis("region_a", Temperature::Hot, 9, 100, 10);
        // 9 < 9 (effective_cold) is false since effective_cold = 9 and 9 < 9 is false
        // So it stays Hot
        assert_eq!(result, Temperature::Hot);
    }

    #[test]
    fn test_hysteresis_allows_downgrade_when_cold() {
        let checker = test_checker(3);
        // Currently Hot, access count well below cold threshold
        let result = checker.classify_with_hysteresis("region_a", Temperature::Hot, 1, 100, 10);
        // effective_cold = 10 * 0.9 = 9; 1 < 9 => downgrade
        assert_ne!(result, Temperature::Hot);
    }

    #[test]
    fn test_hysteresis_prevents_upgrade() {
        let checker = test_checker(3);
        // Currently Frozen, access count just above hot threshold
        // With hysteresis margin of 0.1, effective hot = 100 * 1.1 = 110
        let result = checker.classify_with_hysteresis("region_a", Temperature::Frozen, 105, 100, 10);
        // 105 < 110 => stays Frozen
        assert_eq!(result, Temperature::Frozen);
    }

    #[test]
    fn test_hysteresis_allows_upgrade_when_hot() {
        let checker = test_checker(3);
        // Currently Frozen, access count well above hot threshold
        let result = checker.classify_with_hysteresis("region_a", Temperature::Frozen, 120, 100, 10);
        // 120 >= 110 => upgrade
        assert_eq!(result, Temperature::Hot);
    }

    #[test]
    fn test_history_trimming() {
        let mut checker = test_checker(3);
        // Record more than 2x window size
        for _ in 0..10 {
            checker.record("region_a", Temperature::Hot);
        }
        // History should be trimmed to 2 * window = 6
        assert_eq!(checker.observation_count("region_a"), 6);
    }

    #[test]
    fn test_clear_region() {
        let mut checker = test_checker(3);
        checker.record("region_a", Temperature::Hot);
        checker.record("region_b", Temperature::Cold);
        checker.clear_region("region_a");
        assert_eq!(checker.observation_count("region_a"), 0);
        assert_eq!(checker.observation_count("region_b"), 1);
    }

    #[test]
    fn test_clear_all() {
        let mut checker = test_checker(3);
        checker.record("region_a", Temperature::Hot);
        checker.record("region_b", Temperature::Cold);
        checker.clear_all();
        assert_eq!(checker.observation_count("region_a"), 0);
        assert_eq!(checker.observation_count("region_b"), 0);
    }
}
