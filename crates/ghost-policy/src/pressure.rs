//! Pressure-aware placement policy.
//!
//! Makes placement decisions based on per-tier pressure watermarks.
//! Uses pressure scores to select tiers and determine migration urgency.

use ghost_core::state::PressureState;
use ghost_core::transfer::TransferPriority;
use ghost_core::types::{ChunkId, ChunkMeta, TierId};

use crate::policy::PlacementPolicy;
use crate::weights::tier_pressure_score;

/// Configuration for the pressure-aware placement policy.
#[derive(Debug, Clone)]
pub struct PressureAwareConfig {
    /// Memory pressure above which RAM is avoided for new placements.
    pub ram_high_watermark: f32,

    /// Memory pressure above which RAM is completely avoided.
    pub ram_critical_watermark: f32,

    /// VRAM pressure above which GPU VRAM is avoided for new placements.
    pub vram_high_watermark: f32,

    /// VRAM pressure above which GPU VRAM is completely avoided.
    pub vram_critical_watermark: f32,

    /// IO pressure above which disk is avoided for new placements.
    pub io_high_watermark: f32,

    /// IO pressure above which disk is completely avoided.
    pub io_critical_watermark: f32,

    /// Pressure threshold for critical migration priority.
    pub critical_pressure: f32,

    /// Pressure threshold for high migration priority.
    pub high_pressure: f32,

    /// Pressure threshold for normal migration priority.
    pub normal_pressure: f32,

    /// Current timestamp in seconds, injected by the caller for deterministic behavior.
    pub current_time_secs: u64,
}

impl Default for PressureAwareConfig {
    fn default() -> Self {
        Self {
            ram_high_watermark: 0.7,
            ram_critical_watermark: 0.9,
            vram_high_watermark: 0.7,
            vram_critical_watermark: 0.9,
            io_high_watermark: 0.7,
            io_critical_watermark: 0.9,
            critical_pressure: 0.9,
            high_pressure: 0.7,
            normal_pressure: 0.5,
            current_time_secs: 0,
        }
    }
}

/// Pressure-aware placement policy.
///
/// Selects tiers and eviction victims based on current system pressure.
/// Under high pressure, avoids placing chunks on pressured tiers and
/// prioritizes migrating chunks away from pressured tiers.
#[derive(Debug, Clone)]
pub struct PressureAwarePolicy {
    config: PressureAwareConfig,
}

impl PressureAwarePolicy {
    /// Create a new pressure-aware policy with the given configuration.
    pub fn new(config: PressureAwareConfig) -> Self {
        Self { config }
    }

    /// Create a new pressure-aware policy with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(PressureAwareConfig::default())
    }

    /// Get the configuration reference.
    pub fn config(&self) -> &PressureAwareConfig {
        &self.config
    }

    /// Check if a tier is under high pressure.
    fn is_tier_pressured(&self, tier: TierId, pressure: &PressureState) -> bool {
        match tier {
            TierId::Ram => pressure.memory_pressure >= self.config.ram_high_watermark,
            TierId::GpuVram => pressure.vram_pressure >= self.config.vram_high_watermark,
            TierId::Disk => pressure.io_pressure >= self.config.io_high_watermark,
            TierId::Simulation => false,
        }
    }

    /// Check if a tier is under critical pressure.
    fn is_tier_critical(&self, tier: TierId, pressure: &PressureState) -> bool {
        match tier {
            TierId::Ram => pressure.memory_pressure >= self.config.ram_critical_watermark,
            TierId::GpuVram => pressure.vram_pressure >= self.config.vram_critical_watermark,
            TierId::Disk => pressure.io_pressure >= self.config.io_critical_watermark,
            TierId::Simulation => false,
        }
    }

    /// Get the maximum pressure across all tiers.
    fn max_pressure(&self, pressure: &PressureState) -> f32 {
        pressure.max_pressure()
    }
}

impl PlacementPolicy for PressureAwarePolicy {
    fn name(&self) -> &str {
        "pressure-aware"
    }

    fn select_target_tier(
        &self,
        _meta: &ChunkMeta,
        pressure: &PressureState,
        available_tiers: &[TierId],
    ) -> TierId {
        if available_tiers.is_empty() {
            return TierId::Disk;
        }

        // Filter out critically pressured tiers
        let viable: Vec<TierId> = available_tiers
            .iter()
            .filter(|t| !self.is_tier_critical(**t, pressure))
            .copied()
            .collect();

        if viable.is_empty() {
            // All tiers under critical pressure — pick the least pressured one
            return *available_tiers
                .iter()
                .min_by(|a, b| {
                    let score_a = tier_pressure_score(**a, pressure);
                    let score_b = tier_pressure_score(**b, pressure);
                    score_a
                        .partial_cmp(&score_b)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .unwrap_or(&available_tiers[0]);
        }

        // Among viable tiers, pick the one with the best pressure score
        viable
            .iter()
            .max_by(|a, b| {
                let score_a = tier_pressure_score(**a, pressure);
                let score_b = tier_pressure_score(**b, pressure);
                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied()
            .unwrap_or(viable[0])
    }

    fn select_viction(
        &self,
        candidates: &[(ChunkId, ChunkMeta)],
        pressure: &PressureState,
    ) -> Option<ChunkId> {
        if candidates.is_empty() {
            return None;
        }

        // Under pressure, evict from the most pressured tier first
        // Among same tier, evict the least recently accessed
        candidates
            .iter()
            .max_by(|(_, meta_a), (_, meta_b)| {
                // First, prefer evicting from pressured tiers
                let pressured_a = self.is_tier_pressured(meta_a.tier, pressure);
                let pressured_b = self.is_tier_pressured(meta_b.tier, pressure);

                match (pressured_a, pressured_b) {
                    (true, false) => std::cmp::Ordering::Greater,
                    (false, true) => std::cmp::Ordering::Less,
                    _ => {
                        // Same pressure status — evict least recently accessed
                        meta_a.last_accessed.cmp(&meta_b.last_accessed).reverse()
                        // oldest first = max by reversed
                    }
                }
            })
            .map(|(id, _)| *id)
    }

    fn should_migrate(
        &self,
        meta: &ChunkMeta,
        current_tier: TierId,
        pressure: &PressureState,
    ) -> Option<TierId> {
        // If the current tier is under critical pressure, migrate away
        if self.is_tier_critical(current_tier, pressure) {
            let candidates: Vec<TierId> = vec![TierId::Ram, TierId::GpuVram, TierId::Disk]
                .into_iter()
                .filter(|t| *t != current_tier && !self.is_tier_critical(*t, pressure))
                .collect();

            if let Some(best) = candidates.iter().max_by(|a, b| {
                let score_a = tier_pressure_score(**a, pressure);
                let score_b = tier_pressure_score(**b, pressure);
                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) {
                return Some(*best);
            }
        }

        // If the current tier is under high pressure and the chunk is cold, migrate
        if self.is_tier_pressured(current_tier, pressure) {
            let now = self.config.current_time_secs;
            let age = now.saturating_sub(meta.last_accessed);
            if age > 300 {
                // Cold chunk on pressured tier — migrate to a better tier
                let candidates: Vec<TierId> = vec![TierId::Ram, TierId::GpuVram, TierId::Disk]
                    .into_iter()
                    .filter(|t| *t != current_tier)
                    .collect();

                if let Some(best) = candidates.iter().max_by(|a, b| {
                    let score_a = tier_pressure_score(**a, pressure);
                    let score_b = tier_pressure_score(**b, pressure);
                    score_a
                        .partial_cmp(&score_b)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }) {
                    return Some(*best);
                }
            }
        }

        None
    }

    fn migration_priority(&self, _meta: &ChunkMeta, pressure: &PressureState) -> TransferPriority {
        let max = self.max_pressure(pressure);

        if max >= self.config.critical_pressure {
            TransferPriority::Critical
        } else if max >= self.config.high_pressure {
            TransferPriority::High
        } else if max >= self.config.normal_pressure {
            TransferPriority::Normal
        } else {
            TransferPriority::Low
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::ChunkMeta;

    fn make_chunk(last_accessed: u64, tier: TierId) -> ChunkMeta {
        ChunkMeta {
            id: ChunkId::from_data(b"test"),
            size: 1024,
            compressed_size: 0,
            tier,
            state: ghost_core::state::ChunkState::Stored,
            created_at: last_accessed,
            last_accessed,
            access_count: 1,
            compression: ghost_core::types::CompressionAlgorithm::None,
            checksum: [0u8; 32],
        }
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn test_pressure_aware_name() {
        let policy = PressureAwarePolicy::with_defaults();
        assert_eq!(policy.name(), "pressure-aware");
    }

    #[test]
    fn test_pressure_aware_select_target_no_pressure() {
        let policy = PressureAwarePolicy::with_defaults();
        let meta = make_chunk(now(), TierId::Ram);
        let pressure = PressureState::new();
        let tiers = vec![TierId::Ram, TierId::Disk];
        let tier = policy.select_target_tier(&meta, &pressure, &tiers);
        // No pressure → RAM has best score
        assert_eq!(tier, TierId::Ram);
    }

    #[test]
    fn test_pressure_aware_select_target_ram_pressured() {
        let policy = PressureAwarePolicy::with_defaults();
        let meta = make_chunk(now(), TierId::Ram);
        let mut pressure = PressureState::new();
        pressure.memory_pressure = 0.95; // Critical
        let tiers = vec![TierId::Ram, TierId::Disk];
        let tier = policy.select_target_tier(&meta, &pressure, &tiers);
        // RAM under critical pressure → should pick disk
        assert_eq!(tier, TierId::Disk);
    }

    #[test]
    fn test_pressure_aware_select_viction_empty() {
        let policy = PressureAwarePolicy::with_defaults();
        let pressure = PressureState::new();
        assert_eq!(policy.select_viction(&[], &pressure), None);
    }

    #[test]
    fn test_pressure_aware_select_viction_pressured_tier_first() {
        let policy = PressureAwarePolicy::with_defaults();
        let now = now();
        let old = now - 3600;

        let candidates = vec![
            (ChunkId::from_data(b"a"), make_chunk(now, TierId::Disk)),
            (ChunkId::from_data(b"b"), make_chunk(old, TierId::Ram)),
        ];

        let mut pressure = PressureState::new();
        pressure.memory_pressure = 0.8; // RAM is pressured

        let victim = policy.select_viction(&candidates, &pressure);
        // Should evict from RAM (pressured tier) even though disk chunk is older
        assert_eq!(victim, Some(ChunkId::from_data(b"b")));
    }

    #[test]
    fn test_pressure_aware_should_migrate_critical_pressure() {
        let policy = PressureAwarePolicy::with_defaults();
        let meta = make_chunk(now(), TierId::Ram);
        let mut pressure = PressureState::new();
        pressure.memory_pressure = 0.95;
        let result = policy.should_migrate(&meta, TierId::Ram, &pressure);
        assert!(result.is_some());
        assert_ne!(result.unwrap(), TierId::Ram);
    }

    #[test]
    fn test_pressure_aware_should_not_migrate_no_pressure() {
        let policy = PressureAwarePolicy::with_defaults();
        let meta = make_chunk(now(), TierId::Ram);
        let pressure = PressureState::new();
        let result = policy.should_migrate(&meta, TierId::Ram, &pressure);
        assert_eq!(result, None);
    }

    #[test]
    fn test_pressure_aware_migration_priority_critical() {
        let policy = PressureAwarePolicy::with_defaults();
        let meta = make_chunk(now(), TierId::Ram);
        let mut pressure = PressureState::new();
        pressure.memory_pressure = 0.95;
        let priority = policy.migration_priority(&meta, &pressure);
        assert_eq!(priority, TransferPriority::Critical);
    }

    #[test]
    fn test_pressure_aware_migration_priority_low() {
        let policy = PressureAwarePolicy::with_defaults();
        let meta = make_chunk(now(), TierId::Ram);
        let pressure = PressureState::new();
        let priority = policy.migration_priority(&meta, &pressure);
        assert_eq!(priority, TransferPriority::Low);
    }

    #[test]
    fn test_pressure_aware_deterministic() {
        let policy = PressureAwarePolicy::with_defaults();
        let meta = make_chunk(now(), TierId::Ram);
        let pressure = PressureState::new();
        let tiers = vec![TierId::Ram, TierId::Disk];

        for _ in 0..10 {
            let tier = policy.select_target_tier(&meta, &pressure, &tiers);
            assert_eq!(tier, TierId::Ram);
        }
    }

    #[test]
    fn test_pressure_aware_config_default() {
        let config = PressureAwareConfig::default();
        assert_eq!(config.ram_high_watermark, 0.7);
        assert_eq!(config.ram_critical_watermark, 0.9);
        assert_eq!(config.critical_pressure, 0.9);
        assert_eq!(config.high_pressure, 0.7);
        assert_eq!(config.normal_pressure, 0.5);
    }

    #[test]
    fn test_pressure_aware_all_tiers_critical() {
        let policy = PressureAwarePolicy::with_defaults();
        let meta = make_chunk(now(), TierId::Ram);
        let mut pressure = PressureState::new();
        pressure.memory_pressure = 0.95;
        pressure.vram_pressure = 0.95;
        pressure.io_pressure = 0.95;
        let tiers = vec![TierId::Ram, TierId::GpuVram, TierId::Disk];
        // All tiers critical — should pick least pressured
        let tier = policy.select_target_tier(&meta, &pressure, &tiers);
        // Should still return a tier (least bad option)
        assert!(tiers.contains(&tier));
    }
}
