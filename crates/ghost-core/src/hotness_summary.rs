//! Hotness summary statistics for a snapshot.
//!
//! Provides aggregated statistics about memory region temperatures,
//! including counts, percentages, and workload classification.

use crate::hotness_provider::{HotnessSnapshot, Temperature};

/// Aggregated hotness statistics for a snapshot.
///
/// Provides counts, percentages, and access statistics for all
/// memory regions in a hotness snapshot.
#[derive(Debug, Clone)]
pub struct HotnessSummary {
    /// Number of hot regions.
    pub hot_count: usize,
    /// Number of warm regions.
    pub warm_count: usize,
    /// Number of cold regions.
    pub cold_count: usize,
    /// Number of frozen regions.
    pub frozen_count: usize,
    /// Total number of regions.
    pub total_regions: usize,
    /// Percentage of hot regions (0.0-100.0).
    pub hot_percentage: f32,
    /// Percentage of warm regions (0.0-100.0).
    pub warm_percentage: f32,
    /// Percentage of cold regions (0.0-100.0).
    pub cold_percentage: f32,
    /// Percentage of frozen regions (0.0-100.0).
    pub frozen_percentage: f32,
    /// Average access count across all regions.
    pub avg_access_count: u64,
    /// Maximum access count across all regions.
    pub max_access_count: u64,
    /// Minimum access count across all regions.
    pub min_access_count: u64,
}

impl HotnessSummary {
    /// Create a summary from a hotness snapshot.
    pub fn from_snapshot(snapshot: &HotnessSnapshot) -> Self {
        let samples = &snapshot.samples;
        let total_regions = samples.len();

        if total_regions == 0 {
            return Self {
                hot_count: 0,
                warm_count: 0,
                cold_count: 0,
                frozen_count: 0,
                total_regions: 0,
                hot_percentage: 0.0,
                warm_percentage: 0.0,
                cold_percentage: 0.0,
                frozen_percentage: 0.0,
                avg_access_count: 0,
                max_access_count: 0,
                min_access_count: 0,
            };
        }

        // Count temperatures
        let hot_count = samples.iter().filter(|s| s.temperature == Temperature::Hot).count();
        let warm_count = samples.iter().filter(|s| s.temperature == Temperature::Warm).count();
        let cold_count = samples.iter().filter(|s| s.temperature == Temperature::Cold).count();
        let frozen_count = samples.iter().filter(|s| s.temperature == Temperature::Frozen).count();

        // Calculate percentages
        let total = total_regions as f32;
        let hot_percentage = (hot_count as f32 / total) * 100.0;
        let warm_percentage = (warm_count as f32 / total) * 100.0;
        let cold_percentage = (cold_count as f32 / total) * 100.0;
        let frozen_percentage = (frozen_count as f32 / total) * 100.0;

        // Calculate access statistics
        let access_counts: Vec<u64> = samples.iter().map(|s| s.access_count).collect();
        let total_access: u64 = access_counts.iter().sum();
        let avg_access_count = total_access / total_regions as u64;
        let max_access_count = *access_counts.iter().max().unwrap_or(&0);
        let min_access_count = *access_counts.iter().min().unwrap_or(&0);

        Self {
            hot_count,
            warm_count,
            cold_count,
            frozen_count,
            total_regions,
            hot_percentage,
            warm_percentage,
            cold_percentage,
            frozen_percentage,
            avg_access_count,
            max_access_count,
            min_access_count,
        }
    }

    /// Get the dominant temperature (most common).
    ///
    /// Returns the temperature classification with the highest count.
    /// In case of a tie, returns Hot > Warm > Cold > Frozen.
    pub fn dominant_temperature(&self) -> Temperature {
        if self.hot_count > 0 {
            return Temperature::Hot;
        }
        if self.warm_count > 0 {
            return Temperature::Warm;
        }
        if self.cold_count > 0 {
            return Temperature::Cold;
        }
        Temperature::Frozen
    }

    /// Check if the memory profile is "hot" (majority hot+warm).
    ///
    /// Returns true if more than 50% of regions are hot or warm.
    pub fn is_hot_workload(&self) -> bool {
        if self.total_regions == 0 {
            return false;
        }
        let active_count = self.hot_count + self.warm_count;
        active_count > self.total_regions / 2
    }

    /// Check if the memory profile is "cold" (majority cold+frozen).
    ///
    /// Returns true if more than 50% of regions are cold or frozen.
    pub fn is_cold_workload(&self) -> bool {
        if self.total_regions == 0 {
            return false;
        }
        let inactive_count = self.cold_count + self.frozen_count;
        inactive_count > self.total_regions / 2
    }

    /// Get the count of active regions (hot + warm).
    pub fn active_count(&self) -> usize {
        self.hot_count + self.warm_count
    }

    /// Get the count of inactive regions (cold + frozen).
    pub fn inactive_count(&self) -> usize {
        self.cold_count + self.frozen_count
    }

    /// Get the percentage of active regions (hot + warm).
    pub fn active_percentage(&self) -> f32 {
        self.hot_percentage + self.warm_percentage
    }

    /// Get the percentage of inactive regions (cold + frozen).
    pub fn inactive_percentage(&self) -> f32 {
        self.cold_percentage + self.frozen_percentage
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
    fn test_from_snapshot_empty() {
        let snapshot = HotnessSnapshot {
            samples: vec![],
            timestamp: 0,
        };
        let summary = HotnessSummary::from_snapshot(&snapshot);
        assert_eq!(summary.total_regions, 0);
        assert_eq!(summary.dominant_temperature(), Temperature::Frozen);
        assert!(!summary.is_hot_workload());
        assert!(!summary.is_cold_workload());
    }

    #[test]
    fn test_from_snapshot_all_hot() {
        let snapshot = HotnessSnapshot {
            samples: vec![
                create_sample(Temperature::Hot, 150),
                create_sample(Temperature::Hot, 200),
                create_sample(Temperature::Hot, 100),
            ],
            timestamp: 0,
        };
        let summary = HotnessSummary::from_snapshot(&snapshot);
        assert_eq!(summary.total_regions, 3);
        assert_eq!(summary.hot_count, 3);
        assert_eq!(summary.warm_count, 0);
        assert_eq!(summary.cold_count, 0);
        assert_eq!(summary.frozen_count, 0);
        assert_eq!(summary.hot_percentage, 100.0);
        assert_eq!(summary.dominant_temperature(), Temperature::Hot);
        assert!(summary.is_hot_workload());
        assert!(!summary.is_cold_workload());
    }

    #[test]
    fn test_from_snapshot_mixed() {
        let snapshot = HotnessSnapshot {
            samples: vec![
                create_sample(Temperature::Hot, 150),
                create_sample(Temperature::Warm, 50),
                create_sample(Temperature::Cold, 5),
                create_sample(Temperature::Frozen, 0),
            ],
            timestamp: 0,
        };
        let summary = HotnessSummary::from_snapshot(&snapshot);
        assert_eq!(summary.total_regions, 4);
        assert_eq!(summary.hot_count, 1);
        assert_eq!(summary.warm_count, 1);
        assert_eq!(summary.cold_count, 1);
        assert_eq!(summary.frozen_count, 1);
        assert_eq!(summary.hot_percentage, 25.0);
        assert_eq!(summary.warm_percentage, 25.0);
        assert_eq!(summary.cold_percentage, 25.0);
        assert_eq!(summary.frozen_percentage, 25.0);
        assert_eq!(summary.dominant_temperature(), Temperature::Hot);
        assert!(!summary.is_hot_workload());
        assert!(!summary.is_cold_workload());
    }

    #[test]
    fn test_access_statistics() {
        let snapshot = HotnessSnapshot {
            samples: vec![
                create_sample(Temperature::Hot, 100),
                create_sample(Temperature::Warm, 50),
                create_sample(Temperature::Cold, 10),
                create_sample(Temperature::Frozen, 0),
            ],
            timestamp: 0,
        };
        let summary = HotnessSummary::from_snapshot(&snapshot);
        assert_eq!(summary.avg_access_count, 40); // (100+50+10+0)/4
        assert_eq!(summary.max_access_count, 100);
        assert_eq!(summary.min_access_count, 0);
    }

    #[test]
    fn test_is_hot_workload() {
        // More than 50% hot+warm
        let snapshot = HotnessSnapshot {
            samples: vec![
                create_sample(Temperature::Hot, 100),
                create_sample(Temperature::Warm, 50),
                create_sample(Temperature::Cold, 5),
            ],
            timestamp: 0,
        };
        let summary = HotnessSummary::from_snapshot(&snapshot);
        assert!(summary.is_hot_workload());

        // Exactly 50% - not hot
        let snapshot = HotnessSnapshot {
            samples: vec![
                create_sample(Temperature::Hot, 100),
                create_sample(Temperature::Cold, 5),
            ],
            timestamp: 0,
        };
        let summary = HotnessSummary::from_snapshot(&snapshot);
        assert!(!summary.is_hot_workload());
    }

    #[test]
    fn test_is_cold_workload() {
        // More than 50% cold+frozen
        let snapshot = HotnessSnapshot {
            samples: vec![
                create_sample(Temperature::Cold, 5),
                create_sample(Temperature::Frozen, 0),
                create_sample(Temperature::Hot, 100),
            ],
            timestamp: 0,
        };
        let summary = HotnessSummary::from_snapshot(&snapshot);
        assert!(summary.is_cold_workload());
    }
}