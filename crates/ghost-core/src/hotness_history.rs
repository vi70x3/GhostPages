//! Hotness history tracking for trend analysis.
//!
//! Maintains a rolling history of hotness snapshots to detect trends
//! in temperature and access patterns over time.

use crate::hotness_provider::{AddressRange, HotnessSnapshot, Temperature};

/// Temperature trend for a region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemperatureTrend {
    /// Temperature has remained stable.
    Stable(Temperature),
    /// Temperature is warming up.
    Warming(Temperature, Temperature),
    /// Temperature is cooling down.
    Cooling(Temperature, Temperature),
    /// Temperature is flapping (unstable).
    Flapping,
}

/// Access pattern trend for a region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessTrend {
    /// Access count is increasing.
    Increasing,
    /// Access count is decreasing.
    Decreasing,
    /// Access count is stable.
    Stable,
    /// Access count is volatile.
    Volatile,
}

/// History of hotness snapshots for trend analysis.
#[derive(Debug, Clone)]
pub struct HotnessHistory {
    /// Rolling history of snapshots (oldest first).
    snapshots: Vec<HotnessSnapshot>,
    /// Maximum number of snapshots to retain.
    max_snapshots: usize,
}

impl HotnessHistory {
    /// Create a new history tracker.
    pub fn new(max_snapshots: usize) -> Self {
        Self {
            snapshots: Vec::new(),
            max_snapshots: max_snapshots.max(1),
        }
    }

    /// Add a new snapshot to the history.
    ///
    /// If the history is full, the oldest snapshot is removed.
    pub fn push(&mut self, snapshot: HotnessSnapshot) {
        self.snapshots.push(snapshot);

        // Trim to max size
        while self.snapshots.len() > self.max_snapshots {
            self.snapshots.remove(0);
        }
    }

    /// Get the temperature trend for a specific region.
    ///
    /// Analyzes the temperature history for the given address range
    /// and returns the detected trend.
    pub fn get_temperature_trend(&self, region: &AddressRange) -> TemperatureTrend {
        if self.snapshots.is_empty() {
            return TemperatureTrend::Stable(Temperature::Frozen);
        }

        // Collect temperatures for this region
        let mut temps: Vec<Temperature> = Vec::new();
        for snapshot in &self.snapshots {
            if let Some(sample) = snapshot.samples.iter().find(|s| s.address_range == *region) {
                temps.push(sample.temperature);
            }
        }

        if temps.is_empty() {
            return TemperatureTrend::Stable(Temperature::Frozen);
        }

        // Check for flapping (too many changes)
        let mut changes = 0;
        for i in 1..temps.len() {
            if temps[i] != temps[i - 1] {
                changes += 1;
            }
        }

        // If more than 50% of positions changed, it's flapping
        if changes > temps.len() / 2 {
            return TemperatureTrend::Flapping;
        }

        // Check for warming or cooling trend
        let first = temps.first().copied().unwrap_or(Temperature::Frozen);
        let last = temps.last().copied().unwrap_or(Temperature::Frozen);

        if last.value() > first.value() {
            TemperatureTrend::Warming(first, last)
        } else if last.value() < first.value() {
            TemperatureTrend::Cooling(first, last)
        } else {
            TemperatureTrend::Stable(last)
        }
    }

    /// Get the access trend for a specific region.
    ///
    /// Analyzes the access count history for the given address range
    /// and returns the detected trend.
    pub fn get_access_trend(&self, region: &AddressRange) -> AccessTrend {
        if self.snapshots.is_empty() {
            return AccessTrend::Stable;
        }

        // Collect access counts for this region
        let mut counts: Vec<u64> = Vec::new();
        for snapshot in &self.snapshots {
            if let Some(sample) = snapshot.samples.iter().find(|s| s.address_range == *region) {
                counts.push(sample.access_count);
            }
        }

        if counts.len() < 2 {
            return AccessTrend::Stable;
        }

        // Calculate trend using linear regression slope
        let n = counts.len() as f64;
        let sum_x: f64 = (0..counts.len()).map(|i| i as f64).sum();
        let sum_y: f64 = counts.iter().map(|&c| c as f64).sum();
        let sum_xy: f64 = counts.iter().enumerate().map(|(i, &c)| i as f64 * c as f64).sum();
        let sum_xx: f64 = (0..counts.len()).map(|i| (i * i) as f64).sum();

        let denominator = n * sum_xx - sum_x * sum_x;
        if denominator == 0.0 {
            return AccessTrend::Stable;
        }

        let slope = (n * sum_xy - sum_x * sum_y) / denominator;

        // Calculate variance for volatility check
        let mean = sum_y / n;
        let variance: f64 = counts.iter()
            .map(|&c| {
                let diff = c as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / n;

        let std_dev = variance.sqrt();
        let coefficient_of_variation = if mean > 0.0 { std_dev / mean } else { 0.0 };

        // Check for volatility first
        if coefficient_of_variation > 0.5 {
            return AccessTrend::Volatile;
        }

        // Determine trend based on slope
        let avg_count = sum_y / n;
        let slope_threshold = avg_count * 0.1; // 10% of mean as threshold

        if slope > slope_threshold {
            AccessTrend::Increasing
        } else if slope < -slope_threshold {
            AccessTrend::Decreasing
        } else {
            AccessTrend::Stable
        }
    }

    /// Get the number of snapshots in history.
    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    /// Check if history is empty.
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    /// Get all snapshots (oldest first).
    pub fn snapshots(&self) -> &[HotnessSnapshot] {
        &self.snapshots
    }

    /// Clear the history.
    pub fn clear(&mut self) {
        self.snapshots.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hotness_provider::HotnessSample;

    fn create_sample(addr: u64, temp: Temperature, access_count: u64) -> HotnessSample {
        HotnessSample {
            address_range: AddressRange::new(addr, addr + 4096),
            temperature: temp,
            access_count,
        }
    }

    #[test]
    fn test_empty_history() {
        let history = HotnessHistory::new(10);
        assert!(history.is_empty());
        assert_eq!(history.len(), 0);

        let region = AddressRange::new(0, 4096);
        assert_eq!(history.get_temperature_trend(&region), TemperatureTrend::Stable(Temperature::Frozen));
        assert_eq!(history.get_access_trend(&region), AccessTrend::Stable);
    }

    #[test]
    fn test_temperature_trend_stable() {
        let mut history = HotnessHistory::new(10);
        let region = AddressRange::new(0, 4096);

        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 0,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 10,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 20,
        });

        match history.get_temperature_trend(&region) {
            TemperatureTrend::Stable(t) => assert_eq!(t, Temperature::Hot),
            _ => panic!("Expected Stable(Hot)"),
        }
    }

    #[test]
    fn test_temperature_trend_warming() {
        let mut history = HotnessHistory::new(10);
        let region = AddressRange::new(0, 4096);

        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Frozen, 0)],
            timestamp: 0,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Frozen, 0)],
            timestamp: 5,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Cold, 5)],
            timestamp: 10,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Warm, 50)],
            timestamp: 20,
        });

        match history.get_temperature_trend(&region) {
            TemperatureTrend::Warming(from, to) => {
                assert_eq!(from, Temperature::Frozen);
                assert_eq!(to, Temperature::Warm);
            }
            _ => panic!("Expected Warming"),
        }
    }

    #[test]
    fn test_temperature_trend_cooling() {
        let mut history = HotnessHistory::new(10);
        let region = AddressRange::new(0, 4096);

        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 0,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 5,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Warm, 50)],
            timestamp: 10,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Cold, 5)],
            timestamp: 20,
        });

        match history.get_temperature_trend(&region) {
            TemperatureTrend::Cooling(from, to) => {
                assert_eq!(from, Temperature::Hot);
                assert_eq!(to, Temperature::Cold);
            }
            _ => panic!("Expected Cooling"),
        }
    }

    #[test]
    fn test_temperature_trend_flapping() {
        let mut history = HotnessHistory::new(10);
        let region = AddressRange::new(0, 4096);

        // Alternating temperatures
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 0,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Frozen, 0)],
            timestamp: 10,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 20,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Frozen, 0)],
            timestamp: 30,
        });

        assert_eq!(history.get_temperature_trend(&region), TemperatureTrend::Flapping);
    }

    #[test]
    fn test_access_trend_increasing() {
        let mut history = HotnessHistory::new(10);
        let region = AddressRange::new(0, 4096);

        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Cold, 10)],
            timestamp: 0,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Cold, 20)],
            timestamp: 10,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Warm, 30)],
            timestamp: 20,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Warm, 40)],
            timestamp: 30,
        });

        assert_eq!(history.get_access_trend(&region), AccessTrend::Increasing);
    }

    #[test]
    fn test_access_trend_decreasing() {
        let mut history = HotnessHistory::new(10);
        let region = AddressRange::new(0, 4096);

        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 0,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Warm, 80)],
            timestamp: 10,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Warm, 60)],
            timestamp: 20,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Cold, 40)],
            timestamp: 30,
        });

        assert_eq!(history.get_access_trend(&region), AccessTrend::Decreasing);
    }

    #[test]
    fn test_access_trend_stable() {
        let mut history = HotnessHistory::new(10);
        let region = AddressRange::new(0, 4096);

        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 0,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 10,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 20,
        });

        assert_eq!(history.get_access_trend(&region), AccessTrend::Stable);
    }

    #[test]
    fn test_access_trend_volatile() {
        let mut history = HotnessHistory::new(10);
        let region = AddressRange::new(0, 4096);

        // High variance in access counts
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 0,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Frozen, 0)],
            timestamp: 10,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 20,
        });
        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Frozen, 0)],
            timestamp: 30,
        });

        assert_eq!(history.get_access_trend(&region), AccessTrend::Volatile);
    }

    #[test]
    fn test_max_snapshots() {
        let mut history = HotnessHistory::new(3);

        for i in 0..5 {
            history.push(HotnessSnapshot {
                samples: vec![create_sample(0, Temperature::Hot, 100 + i)],
                timestamp: i as u64 * 10,
            });
        }

        assert_eq!(history.len(), 3);
    }

    #[test]
    fn test_clear() {
        let mut history = HotnessHistory::new(10);

        history.push(HotnessSnapshot {
            samples: vec![create_sample(0, Temperature::Hot, 100)],
            timestamp: 0,
        });

        assert_eq!(history.len(), 1);
        history.clear();
        assert!(history.is_empty());
    }
}