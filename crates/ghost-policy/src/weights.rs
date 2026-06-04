//! Tier weight functions for placement scoring.
//!
//! Provides utility functions for scoring tiers based on their
//! characteristics and current system pressure.

use ghost_core::state::PressureState;
use ghost_core::types::TierId;

/// Get the base weight for a tier (higher = more preferred for storage).
///
/// Weights reflect the inherent desirability of a tier:
/// - RAM is fastest → highest weight
/// - GPU VRAM is fast but limited → high weight
/// - Disk is slowest → lowest weight
/// - Simulation is for testing → lowest weight
pub fn tier_weight(tier: TierId) -> f32 {
    match tier {
        TierId::Ram => 1.0,
        TierId::GpuVram => 0.8,
        TierId::Disk => 0.3,
        TierId::Simulation => 0.1,
    }
}

/// Score a tier given current pressure, using `score = (1.0 - pressure) * tier_weight`.
///
/// Returns a value between 0.0 and 1.0. Higher scores indicate better placement targets.
/// Under high pressure, fast tiers (RAM, GPU VRAM) are penalized more heavily since
/// they are the ones under pressure.
pub fn tier_pressure_score(tier: TierId, pressure: &PressureState) -> f32 {
    let weight = tier_weight(tier);
    let pressure_factor = match tier {
        TierId::Ram => 1.0 - pressure.memory_pressure,
        TierId::GpuVram => 1.0 - pressure.vram_pressure,
        TierId::Disk => 1.0 - pressure.io_pressure,
        TierId::Simulation => 1.0,
    };
    weight * pressure_factor.clamp(0.0, 1.0)
}

/// Select the tier with the highest pressure score from a list of candidates.
///
/// Falls back to the first tier if no candidates are provided.
pub fn best_tier(tiers: &[TierId], pressure: &PressureState) -> Option<TierId> {
    if tiers.is_empty() {
        return None;
    }

    tiers
        .iter()
        .max_by(|a, b| {
            let score_a = tier_pressure_score(**a, pressure);
            let score_b = tier_pressure_score(**b, pressure);
            score_a
                .partial_cmp(&score_b)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_weight_ram_is_highest() {
        assert!(tier_weight(TierId::Ram) > tier_weight(TierId::GpuVram));
        assert!(tier_weight(TierId::GpuVram) > tier_weight(TierId::Disk));
        assert!(tier_weight(TierId::Disk) > tier_weight(TierId::Simulation));
    }

    #[test]
    fn test_tier_pressure_score_no_pressure() {
        let pressure = PressureState::new();
        // With no pressure, score equals weight
        assert!((tier_pressure_score(TierId::Ram, &pressure) - 1.0).abs() < f32::EPSILON);
        assert!((tier_pressure_score(TierId::GpuVram, &pressure) - 0.8).abs() < f32::EPSILON);
    }

    #[test]
    fn test_tier_pressure_score_under_pressure() {
        let mut pressure = PressureState::new();
        pressure.memory_pressure = 0.9;
        // RAM score should be heavily penalized
        let ram_score = tier_pressure_score(TierId::Ram, &pressure);
        assert!(ram_score < 0.2);
        // Disk score should be less affected
        let disk_score = tier_pressure_score(TierId::Disk, &pressure);
        assert!(disk_score > 0.2);
    }

    #[test]
    fn test_best_tier_no_pressure() {
        let pressure = PressureState::new();
        let tiers = vec![TierId::Ram, TierId::Disk];
        assert_eq!(best_tier(&tiers, &pressure), Some(TierId::Ram));
    }

    #[test]
    fn test_best_tier_under_ram_pressure() {
        let mut pressure = PressureState::new();
        pressure.memory_pressure = 0.95;
        let tiers = vec![TierId::Ram, TierId::Disk];
        // Under heavy RAM pressure, disk may score higher
        let best = best_tier(&tiers, &pressure);
        assert_eq!(best, Some(TierId::Disk));
    }

    #[test]
    fn test_best_tier_empty() {
        let pressure = PressureState::new();
        assert_eq!(best_tier(&[], &pressure), None);
    }
}
