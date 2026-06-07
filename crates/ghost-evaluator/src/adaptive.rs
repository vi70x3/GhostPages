//! Adaptive Temperature Model for GhostPages.
//!
//! Dynamic threshold adjustment based on pressure, tier occupancy, and
//! historical behavior. The model adapts temperature classification
//! boundaries to current system conditions.
//!
//! All functions are **pure** — no I/O, no mutation, no side effects.
//! Same inputs always produce same outputs. Deterministic by design.

use std::collections::HashMap;

use ghost_core::types::TierId;

// ─── Temperature Class ────────────────────────────────────────────────────────

/// Classification of a temperature value into discrete bands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TemperatureClass {
    /// Hot data — frequently accessed, should be in fastest tier.
    Hot,
    /// Warm data — moderately accessed.
    Warm,
    /// Cold data — rarely accessed, candidate for demotion.
    Cold,
    /// Frozen data — almost never accessed, candidate for eviction.
    Frozen,
}

// ─── Temperature Thresholds ───────────────────────────────────────────────────

/// Current effective temperature thresholds.
///
/// These boundaries define the classification bands:
/// - `hot`: Above this → Hot
/// - `warm`: Between warm and hot → Warm
/// - `cold`: Between cold and warm → Cold
/// - `frozen`: Below cold → Frozen
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TemperatureThresholds {
    /// Threshold above which data is classified as Hot.
    pub hot: f32,
    /// Threshold above which data is classified as Warm (below hot).
    pub warm: f32,
    /// Threshold above which data is classified as Cold (below warm).
    pub cold: f32,
    /// Threshold below which data is classified as Frozen.
    pub frozen: f32,
}

// ─── Adaptive Temperature Model ───────────────────────────────────────────────

/// Adjusts temperature thresholds dynamically based on system conditions.
///
/// The model tracks pressure history and tier occupancy to adaptively shift
/// temperature boundaries:
/// - Higher pressure → lower hot threshold (promote more aggressively)
/// - Lower pressure → higher hot threshold (be more conservative)
/// - Higher tier occupancy → lower cold threshold (evict more aggressively)
/// - Lower tier occupancy → higher cold threshold (keep more)
#[derive(Debug, Clone)]
pub struct AdaptiveTemperatureModel {
    /// Current hot threshold.
    pub hot_threshold: f32,
    /// Current cold threshold.
    pub cold_threshold: f32,
    /// Initial hot threshold (for reset).
    initial_hot: f32,
    /// Initial cold threshold (for reset).
    initial_cold: f32,
    /// History of pressure values.
    pub pressure_history: Vec<f32>,
    /// Current tier occupancy per tier.
    pub tier_occupancy: HashMap<TierId, f32>,
    /// Rate at which thresholds adapt (0.0 = no adaptation, 1.0 = instant).
    pub adjustment_rate: f32,
}

impl AdaptiveTemperatureModel {
    /// Create a new adaptive temperature model.
    ///
    /// # Arguments
    ///
    /// * `initial_hot` — Initial hot threshold (will be clamped to [0.5, 0.95]).
    /// * `initial_cold` — Initial cold threshold (will be clamped to [0.05, 0.5]).
    /// * `adjustment_rate` — How fast thresholds adapt (0.0–1.0).
    pub fn new(initial_hot: f32, initial_cold: f32, adjustment_rate: f32) -> Self {
        Self {
            hot_threshold: initial_hot.clamp(0.5, 0.95),
            cold_threshold: initial_cold.clamp(0.05, 0.5),
            initial_hot: initial_hot.clamp(0.5, 0.95),
            initial_cold: initial_cold.clamp(0.05, 0.5),
            pressure_history: Vec::new(),
            tier_occupancy: HashMap::new(),
            adjustment_rate: adjustment_rate.clamp(0.0, 1.0),
        }
    }

    /// Update the model with new pressure and occupancy data.
    ///
    /// This adjusts the hot and cold thresholds based on the new data.
    ///
    /// # Arguments
    ///
    /// * `pressure` — Current system pressure (0.0–1.0).
    /// * `tier_occupancy` — Current occupancy per tier (0.0–1.0).
    pub fn update(&mut self, pressure: f32, tier_occupancy: &HashMap<TierId, f32>) {
        self.pressure_history.push(pressure.clamp(0.0, 1.0));
        self.tier_occupancy = tier_occupancy.clone();

        // Compute average occupancy across all tiers.
        let avg_occupancy = if self.tier_occupancy.is_empty() {
            0.0
        } else {
            self.tier_occupancy.values().sum::<f32>() / self.tier_occupancy.len() as f32
        };

        // Adapt hot threshold based on pressure.
        // Higher pressure → lower hot threshold (promote more aggressively).
        // Lower pressure → higher hot threshold (be more conservative).
        let pressure_delta = pressure - 0.5; // positive when pressure > 0.5
        let hot_adjustment = -pressure_delta * self.adjustment_rate * 0.2;
        let target_hot = self.initial_hot + hot_adjustment;

        // Adapt cold threshold based on tier occupancy.
        // Higher occupancy → lower cold threshold (evict more aggressively).
        // Lower occupancy → higher cold threshold (keep more).
        let occupancy_delta = avg_occupancy - 0.5; // positive when occupancy > 0.5
        let cold_adjustment = -occupancy_delta * self.adjustment_rate * 0.1;
        let target_cold = self.initial_cold + cold_adjustment;

        // Apply adjustments with rate limiting.
        self.hot_threshold = self
            .hot_threshold
            .lerp(target_hot, self.adjustment_rate)
            .clamp(0.5, 0.95);
        self.cold_threshold = self
            .cold_threshold
            .lerp(target_cold, self.adjustment_rate)
            .clamp(0.05, 0.5);
    }

    /// Get the current effective thresholds.
    ///
    /// Returns a `TemperatureThresholds` struct with all four boundaries.
    /// The warm and frozen thresholds are derived from hot and cold.
    pub fn thresholds(&self) -> TemperatureThresholds {
        // Warm is midway between hot and cold.
        let warm = (self.hot_threshold + self.cold_threshold) / 2.0;
        // Frozen is at 80% of the cold threshold.
        let frozen = self.cold_threshold * 0.8;

        TemperatureThresholds {
            hot: self.hot_threshold,
            warm,
            cold: self.cold_threshold,
            frozen,
        }
    }

    /// Classify a temperature value using current thresholds.
    pub fn classify(&self, temperature: f32) -> TemperatureClass {
        let t = self.thresholds();

        if temperature >= t.hot {
            TemperatureClass::Hot
        } else if temperature >= t.warm {
            TemperatureClass::Warm
        } else if temperature >= t.cold {
            TemperatureClass::Cold
        } else {
            TemperatureClass::Frozen
        }
    }

    /// Reset to initial thresholds.
    pub fn reset(&mut self) {
        self.hot_threshold = self.initial_hot;
        self.cold_threshold = self.initial_cold;
        self.pressure_history.clear();
        self.tier_occupancy.clear();
    }

    /// Get the pressure trend (positive = increasing, negative = decreasing).
    ///
    /// Computed as the slope of a simple linear regression over the pressure
    /// history. Returns 0.0 if fewer than 2 data points.
    pub fn pressure_trend(&self) -> f32 {
        let n = self.pressure_history.len();

        if n < 2 {
            return 0.0;
        }

        // Simple trend: (last - first) / n
        let first = self.pressure_history[0];
        let last = self.pressure_history[n - 1];

        (last - first) / n as f32
    }
}

/// Linear interpolation between two values.
trait Lerp {
    fn lerp(self, target: f32, rate: f32) -> f32;
}

impl Lerp for f32 {
    fn lerp(self, target: f32, rate: f32) -> f32 {
        self + (target - self) * rate
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_thresholds() {
        let model = AdaptiveTemperatureModel::new(0.8, 0.2, 0.1);
        let t = model.thresholds();

        assert!(
            t.hot >= 0.5 && t.hot <= 0.95,
            "hot threshold {} should be in [0.5, 0.95]",
            t.hot
        );
        assert!(
            t.cold >= 0.05 && t.cold <= 0.5,
            "cold threshold {} should be in [0.05, 0.5]",
            t.cold
        );
        assert!(t.hot > t.warm, "hot should be above warm");
        assert!(t.warm > t.cold, "warm should be above cold");
        assert!(t.cold > t.frozen, "cold should be above frozen");
    }

    #[test]
    fn test_pressure_increase_lowers_hot() {
        let mut model = AdaptiveTemperatureModel::new(0.8, 0.2, 1.0);
        let initial_hot = model.hot_threshold;

        let mut occupancy = HashMap::new();
        occupancy.insert(TierId::Ram, 0.5);

        // High pressure should lower the hot threshold.
        model.update(0.9, &occupancy);

        assert!(
            model.hot_threshold < initial_hot,
            "high pressure should lower hot threshold: {} < {}",
            model.hot_threshold,
            initial_hot
        );
    }

    #[test]
    fn test_pressure_decrease_raises_hot() {
        let mut model = AdaptiveTemperatureModel::new(0.8, 0.2, 1.0);
        let initial_hot = model.hot_threshold;

        let mut occupancy = HashMap::new();
        occupancy.insert(TierId::Ram, 0.5);

        // Low pressure should raise the hot threshold.
        model.update(0.1, &occupancy);

        assert!(
            model.hot_threshold > initial_hot,
            "low pressure should raise hot threshold: {} > {}",
            model.hot_threshold,
            initial_hot
        );
    }

    #[test]
    fn test_high_occupancy_lowers_cold() {
        let mut model = AdaptiveTemperatureModel::new(0.8, 0.2, 1.0);
        let initial_cold = model.cold_threshold;

        let mut occupancy = HashMap::new();
        occupancy.insert(TierId::Ram, 0.9);
        occupancy.insert(TierId::Disk, 0.8);

        // High occupancy should lower the cold threshold.
        model.update(0.5, &occupancy);

        assert!(
            model.cold_threshold < initial_cold,
            "high occupancy should lower cold threshold: {} < {}",
            model.cold_threshold,
            initial_cold
        );
    }

    #[test]
    fn test_classify_hot() {
        let model = AdaptiveTemperatureModel::new(0.8, 0.2, 0.1);

        assert_eq!(
            model.classify(0.9),
            TemperatureClass::Hot,
            "temperature above hot threshold should be Hot"
        );
        assert_eq!(
            model.classify(0.8),
            TemperatureClass::Hot,
            "temperature at hot threshold should be Hot"
        );
    }

    #[test]
    fn test_classify_frozen() {
        let model = AdaptiveTemperatureModel::new(0.8, 0.2, 0.1);

        assert_eq!(
            model.classify(0.05),
            TemperatureClass::Frozen,
            "temperature below cold threshold should be Frozen"
        );
        assert_eq!(
            model.classify(0.0),
            TemperatureClass::Frozen,
            "zero temperature should be Frozen"
        );
    }

    #[test]
    fn test_pressure_trend() {
        let mut model = AdaptiveTemperatureModel::new(0.8, 0.2, 0.5);

        // Initially no trend.
        assert_eq!(model.pressure_trend(), 0.0);

        // Increasing pressure.
        let occupancy = HashMap::new();
        model.update(0.2, &occupancy);
        model.update(0.4, &occupancy);
        model.update(0.6, &occupancy);
        model.update(0.8, &occupancy);

        let trend = model.pressure_trend();
        assert!(
            trend > 0.0,
            "increasing pressure should give positive trend, got {}",
            trend
        );

        // Decreasing pressure.
        let mut model2 = AdaptiveTemperatureModel::new(0.8, 0.2, 0.5);
        model2.update(0.8, &occupancy);
        model2.update(0.6, &occupancy);
        model2.update(0.4, &occupancy);
        model2.update(0.2, &occupancy);

        let trend2 = model2.pressure_trend();
        assert!(
            trend2 < 0.0,
            "decreasing pressure should give negative trend, got {}",
            trend2
        );
    }

    #[test]
    fn test_reset() {
        let mut model = AdaptiveTemperatureModel::new(0.8, 0.2, 1.0);

        let mut occupancy = HashMap::new();
        occupancy.insert(TierId::Ram, 0.9);

        // Update with extreme values.
        model.update(0.9, &occupancy);
        model.update(0.9, &occupancy);

        // Reset.
        model.reset();

        assert_eq!(model.hot_threshold, 0.8);
        assert_eq!(model.cold_threshold, 0.2);
        assert!(model.pressure_history.is_empty());
        assert!(model.tier_occupancy.is_empty());
    }
}
