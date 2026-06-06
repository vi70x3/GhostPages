//! Hotness confidence scoring.
//!
//! Provides confidence metrics for temperature classifications based on
//! observation duration, access stability, temperature stability, and sample count.

use crate::hotness_provider::{HotnessSnapshot, Temperature};

/// Confidence level for hotness classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfidenceLevel {
    /// High confidence (>= 0.8)
    High,
    /// Medium confidence (>= 0.5)
    Medium,
    /// Low confidence (>= 0.2)
    Low,
    /// Unknown/Very low confidence (< 0.2)
    Unknown,
}

impl ConfidenceLevel {
    /// Get the minimum score for this level.
    pub fn min_score(&self) -> f32 {
        match self {
            ConfidenceLevel::High => 0.8,
            ConfidenceLevel::Medium => 0.5,
            ConfidenceLevel::Low => 0.2,
            ConfidenceLevel::Unknown => 0.0,
        }
    }
}

/// Factors contributing to confidence score.
#[derive(Debug, Clone)]
pub enum ConfidenceFactor {
    /// Duration of observation in seconds.
    ObservationDuration(u64),
    /// Variance in access count (0.0 = stable, 1.0+ = volatile).
    AccessStability(f32),
    /// Stability of temperature classification (0.0 = stable, 1.0+ = flapping).
    TemperatureStability(f32),
    /// Number of samples collected.
    SampleCount(usize),
}

/// Confidence score for a hotness classification.
///
/// A score of 1.0 means very high confidence that the classification is accurate.
/// A score of 0.0 means very low confidence.
#[derive(Debug, Clone)]
pub struct HotnessConfidence {
    /// Overall confidence score (0.0 to 1.0).
    pub score: f32,
    /// Individual factors contributing to the score.
    pub factors: Vec<ConfidenceFactor>,
}

impl HotnessConfidence {
    /// Calculate confidence from a snapshot and historical data.
    ///
    /// Confidence is based on:
    /// - Observation duration: longer = higher confidence
    /// - Access stability: lower variance = higher confidence
    /// - Temperature stability: fewer changes = higher confidence
    /// - Sample count: more samples = higher confidence
    pub fn calculate(snapshot: &HotnessSnapshot, history: &[HotnessSnapshot]) -> Self {
        let mut factors = Vec::new();
        let mut score = 0.0;
        let mut weight_sum = 0.0;

        // Factor 1: Sample count (weight: 0.2)
        let sample_count = snapshot.samples.len();
        let sample_factor = Self::sample_count_score(sample_count);
        factors.push(ConfidenceFactor::SampleCount(sample_count));
        score += sample_factor * 0.2;
        weight_sum += 0.2;

        // Factor 2: Observation duration (weight: 0.3)
        // Calculate based on time span of history + current snapshot
        let duration = Self::calculate_duration(snapshot, history);
        let duration_factor = Self::duration_score(duration);
        factors.push(ConfidenceFactor::ObservationDuration(duration));
        score += duration_factor * 0.3;
        weight_sum += 0.3;

        // Factor 3: Access stability (weight: 0.25)
        let access_variance = Self::calculate_access_variance(snapshot, history);
        let access_factor = Self::access_stability_score(access_variance);
        factors.push(ConfidenceFactor::AccessStability(access_variance));
        score += access_factor * 0.25;
        weight_sum += 0.25;

        // Factor 4: Temperature stability (weight: 0.25)
        let temp_changes = Self::calculate_temperature_changes(snapshot, history);
        let temp_factor = Self::temperature_stability_score(temp_changes);
        factors.push(ConfidenceFactor::TemperatureStability(temp_changes));
        score += temp_factor * 0.25;
        weight_sum += 0.25;

        // Normalize score
        let normalized_score = if weight_sum > 0.0 {
            score / weight_sum
        } else {
            0.0
        };

        Self {
            score: normalized_score,
            factors,
        }
    }

    /// Get confidence level description.
    pub fn level(&self) -> ConfidenceLevel {
        if self.score >= 0.8 {
            ConfidenceLevel::High
        } else if self.score >= 0.5 {
            ConfidenceLevel::Medium
        } else if self.score >= 0.2 {
            ConfidenceLevel::Low
        } else {
            ConfidenceLevel::Unknown
        }
    }

    /// Calculate observation duration in seconds.
    fn calculate_duration(snapshot: &HotnessSnapshot, history: &[HotnessSnapshot]) -> u64 {
        if history.is_empty() {
            return 0;
        }

        let min_time = history.iter().map(|s| s.timestamp).min().unwrap_or(0);
        let max_time = snapshot.timestamp.max(
            history.iter().map(|s| s.timestamp).max().unwrap_or(0)
        );

        max_time.saturating_sub(min_time)
    }

    /// Score for sample count (more samples = higher confidence).
    fn sample_count_score(count: usize) -> f32 {
        match count {
            0..=5 => 0.2,
            6..=20 => 0.5,
            21..=50 => 0.7,
            51..=100 => 0.85,
            _ => 1.0,
        }
    }

    /// Score for observation duration (longer = higher confidence).
    fn duration_score(seconds: u64) -> f32 {
        match seconds {
            0..=10 => 0.2,
            11..=60 => 0.4,
            61..=300 => 0.6,
            301..=3600 => 0.8,
            _ => 1.0,
        }
    }

    /// Score for access variance (lower variance = higher confidence).
    fn access_stability_score(variance: f32) -> f32 {
        // Variance of 0 = perfect stability = score 1.0
        // Variance of 1.0+ = very volatile = score 0.0
        (1.0 - variance.min(1.0)).max(0.0)
    }

    /// Score for temperature changes (fewer changes = higher confidence).
    fn temperature_stability_score(changes: f32) -> f32 {
        // 0 changes = perfect stability = score 1.0
        // 5+ changes = very unstable = score 0.0
        (1.0 - (changes / 5.0)).max(0.0)
    }

    /// Calculate variance in access counts across history.
    fn calculate_access_variance(snapshot: &HotnessSnapshot, history: &[HotnessSnapshot]) -> f32 {
        if history.is_empty() {
            return 0.5; // Unknown variance
        }

        // Collect all access counts
        let mut all_counts: Vec<u64> = snapshot.samples.iter().map(|s| s.access_count).collect();
        for hist in history {
            for sample in &hist.samples {
                all_counts.push(sample.access_count);
            }
        }

        if all_counts.is_empty() {
            return 0.5;
        }

        // Calculate mean
        let sum: u64 = all_counts.iter().sum();
        let mean = sum as f32 / all_counts.len() as f32;

        if mean == 0.0 {
            return 0.0; // All zeros = stable
        }

        // Calculate coefficient of variation (normalized variance)
        let variance: f32 = all_counts.iter()
            .map(|&c| {
                let diff = c as f32 - mean;
                diff * diff
            })
            .sum::<f32>() / all_counts.len() as f32;

        // Coefficient of variation (normalized by mean)
        (variance.sqrt() / mean).min(1.0)
    }

    /// Calculate number of temperature changes across history.
    fn calculate_temperature_changes(snapshot: &HotnessSnapshot, history: &[HotnessSnapshot]) -> f32 {
        if history.is_empty() {
            return 0.0;
        }

        // For each region, count temperature transitions
        let mut total_changes = 0.0;
        let mut region_count = 0;

        // Build a map of address ranges to their temperature history
        for sample in &snapshot.samples {
            let range = sample.address_range;
            let mut temps = vec![sample.temperature];

            for hist in history.iter().rev() {
                if let Some(hist_sample) = hist.samples.iter().find(|s| s.address_range == range) {
                    temps.push(hist_sample.temperature);
                }
            }

            // Count changes
            for i in 1..temps.len() {
                if temps[i] != temps[i - 1] {
                    total_changes += 1.0;
                }
            }
            region_count += 1;
        }

        // Normalize by number of regions
        if region_count > 0 {
            total_changes / region_count as f32
        } else {
            0.0
        }
    }
}

impl std::fmt::Display for ConfidenceLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfidenceLevel::High => write!(f, "high"),
            ConfidenceLevel::Medium => write!(f, "medium"),
            ConfidenceLevel::Low => write!(f, "low"),
            ConfidenceLevel::Unknown => write!(f, "unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hotness_provider::{AddressRange, HotnessSample};

    fn create_sample(temp: Temperature, access_count: u64) -> HotnessSample {
        HotnessSample {
            address_range: AddressRange::new(0, 4096),
            temperature: temp,
            access_count,
        }
    }

    #[test]
    fn test_confidence_levels() {
        let mut confidence = HotnessConfidence {
            score: 0.9,
            factors: vec![],
        };
        assert_eq!(confidence.level(), ConfidenceLevel::High);

        confidence.score = 0.8;
        assert_eq!(confidence.level(), ConfidenceLevel::High);

        confidence.score = 0.79;
        assert_eq!(confidence.level(), ConfidenceLevel::Medium);

        confidence.score = 0.5;
        assert_eq!(confidence.level(), ConfidenceLevel::Medium);

        confidence.score = 0.49;
        assert_eq!(confidence.level(), ConfidenceLevel::Low);

        confidence.score = 0.2;
        assert_eq!(confidence.level(), ConfidenceLevel::Low);

        confidence.score = 0.19;
        assert_eq!(confidence.level(), ConfidenceLevel::Unknown);
    }

    #[test]
    fn test_sample_count_score() {
        assert_eq!(HotnessConfidence::sample_count_score(0), 0.2);
        assert_eq!(HotnessConfidence::sample_count_score(5), 0.2);
        assert_eq!(HotnessConfidence::sample_count_score(6), 0.5);
        assert_eq!(HotnessConfidence::sample_count_score(20), 0.5);
        assert_eq!(HotnessConfidence::sample_count_score(21), 0.7);
        assert_eq!(HotnessConfidence::sample_count_score(50), 0.7);
        assert_eq!(HotnessConfidence::sample_count_score(51), 0.85);
        assert_eq!(HotnessConfidence::sample_count_score(100), 0.85);
        assert_eq!(HotnessConfidence::sample_count_score(101), 1.0);
    }

    #[test]
    fn test_duration_score() {
        assert_eq!(HotnessConfidence::duration_score(0), 0.2);
        assert_eq!(HotnessConfidence::duration_score(10), 0.2);
        assert_eq!(HotnessConfidence::duration_score(11), 0.4);
        assert_eq!(HotnessConfidence::duration_score(60), 0.4);
        assert_eq!(HotnessConfidence::duration_score(61), 0.6);
        assert_eq!(HotnessConfidence::duration_score(300), 0.6);
        assert_eq!(HotnessConfidence::duration_score(301), 0.8);
        assert_eq!(HotnessConfidence::duration_score(3600), 0.8);
        assert_eq!(HotnessConfidence::duration_score(3601), 1.0);
    }

    #[test]
    fn test_access_stability_score() {
        assert_eq!(HotnessConfidence::access_stability_score(0.0), 1.0);
        assert_eq!(HotnessConfidence::access_stability_score(0.5), 0.5);
        assert_eq!(HotnessConfidence::access_stability_score(1.0), 0.0);
        assert_eq!(HotnessConfidence::access_stability_score(2.0), 0.0); // Capped
    }

    #[test]
    fn test_temperature_stability_score() {
        assert_eq!(HotnessConfidence::temperature_stability_score(0.0), 1.0);
        assert_eq!(HotnessConfidence::temperature_stability_score(2.5), 0.5);
        assert_eq!(HotnessConfidence::temperature_stability_score(5.0), 0.0);
        assert_eq!(HotnessConfidence::temperature_stability_score(10.0), 0.0); // Capped
    }

    #[test]
    fn test_calculate_with_no_history() {
        let snapshot = HotnessSnapshot {
            samples: vec![
                create_sample(Temperature::Hot, 100),
                create_sample(Temperature::Warm, 50),
            ],
            timestamp: 100,
        };

        let confidence = HotnessConfidence::calculate(&snapshot, &[]);
        assert!(confidence.score > 0.0);
        assert!(confidence.factors.len() >= 4);
    }

    #[test]
    fn test_calculate_with_history() {
        let snapshot = HotnessSnapshot {
            samples: vec![create_sample(Temperature::Hot, 100)],
            timestamp: 100,
        };

        let history = vec![
            HotnessSnapshot {
                samples: vec![create_sample(Temperature::Hot, 100)],
                timestamp: 50,
            },
            HotnessSnapshot {
                samples: vec![create_sample(Temperature::Hot, 100)],
                timestamp: 0,
            },
        ];

        let confidence = HotnessConfidence::calculate(&snapshot, &history);
        assert!(confidence.score > 0.0);
    }
}