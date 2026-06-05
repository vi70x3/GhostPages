//! LRU (Least Recently Used) placement policy.
//!
//! Evicts the chunk with the oldest `last_accessed` timestamp.
//! Migrates chunks based on access recency and tier preference.

use ghost_core::state::PressureState;
use ghost_core::transfer::TransferPriority;
use ghost_core::types::{ChunkId, ChunkMeta, TierId};

use crate::policy::PlacementPolicy;
use crate::weights::best_tier;

/// Configuration for the LRU placement policy.
#[derive(Debug, Clone)]
pub struct LruConfig {
    /// Hotness threshold: chunks accessed within this many seconds are "hot"
    /// and should prefer faster tiers.
    pub hotness_threshold_secs: u64,

    /// Minimum residence time in seconds before a chunk can be evicted.
    pub min_residence_secs: u64,

    /// Preferred tier for new chunks.
    pub preferred_tier: TierId,

    /// Tier to use when preferred tier is under pressure.
    pub fallback_tier: TierId,
}

impl Default for LruConfig {
    fn default() -> Self {
        Self {
            hotness_threshold_secs: 300, // 5 minutes
            min_residence_secs: 60,      // 1 minute
            preferred_tier: TierId::Ram,
            fallback_tier: TierId::Disk,
        }
    }
}

/// LRU-based placement policy.
///
/// Selects eviction victims by oldest `last_accessed` time.
/// Selects target tiers by pressure-weighted scoring.
/// Migrates hot chunks to faster tiers and cold chunks to slower tiers.
#[derive(Debug, Clone)]
pub struct LruPolicy {
    config: LruConfig,
}

impl LruPolicy {
    /// Create a new LRU policy with the given configuration.
    pub fn new(config: LruConfig) -> Self {
        Self { config }
    }

    /// Create a new LRU policy with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(LruConfig::default())
    }

    /// Get the configuration reference.
    pub fn config(&self) -> &LruConfig {
        &self.config
    }

    /// Check if a chunk is "hot" (recently accessed).
    fn is_hot(&self, meta: &ChunkMeta) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let age = now.saturating_sub(meta.last_accessed);
        age < self.config.hotness_threshold_secs
    }

    /// Check if a chunk has been resident long enough to be evictable.
    fn is_resident(&self, meta: &ChunkMeta) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let age = now.saturating_sub(meta.created_at);
        age >= self.config.min_residence_secs
    }
}

impl PlacementPolicy for LruPolicy {
    fn name(&self) -> &str {
        "lru"
    }

    fn select_target_tier(
        &self,
        meta: &ChunkMeta,
        pressure: &PressureState,
        available_tiers: &[TierId],
    ) -> TierId {
        if available_tiers.is_empty() {
            return self.config.preferred_tier;
        }

        // Hot chunks go to the best available tier
        if self.is_hot(meta) {
            if let Some(tier) = best_tier(available_tiers, pressure) {
                return tier;
            }
        }

        // Cold chunks: prefer fallback tier to save fast tier space
        if available_tiers.contains(&self.config.fallback_tier) {
            return self.config.fallback_tier;
        }

        // Default to preferred tier if available
        if available_tiers.contains(&self.config.preferred_tier) {
            return self.config.preferred_tier;
        }

        available_tiers[0]
    }

    fn select_viction(
        &self,
        candidates: &[(ChunkId, ChunkMeta)],
        _pressure: &PressureState,
    ) -> Option<ChunkId> {
        if candidates.is_empty() {
            return None;
        }

        // Filter to only resident chunks
        let resident: Vec<_> = candidates
            .iter()
            .filter(|(_, meta)| self.is_resident(meta))
            .collect();

        if resident.is_empty() {
            // All chunks are too new; evict the oldest anyway
            return candidates
                .iter()
                .min_by_key(|(_, meta)| meta.last_accessed)
                .map(|(id, _)| *id);
        }

        // Evict the least recently accessed resident chunk
        resident
            .iter()
            .min_by_key(|(_, meta)| meta.last_accessed)
            .map(|(id, _)| *id)
    }

    fn should_migrate(
        &self,
        meta: &ChunkMeta,
        current_tier: TierId,
        pressure: &PressureState,
    ) -> Option<TierId> {
        let hot = self.is_hot(meta);

        if hot && current_tier == TierId::Disk {
            // Hot chunk on disk — migrate to a faster tier
            let tiers = vec![TierId::Ram, TierId::GpuVram];
            return best_tier(&tiers, pressure);
        }

        if !hot && current_tier == TierId::Ram {
            // Cold chunk on RAM — migrate to a slower tier to free fast space
            let tiers = vec![TierId::Disk, TierId::GpuVram];
            return best_tier(&tiers, pressure);
        }

        // Under memory pressure, migrate cold chunks off RAM
        if pressure.memory_pressure > 0.7 && !hot && current_tier == TierId::Ram {
            return Some(TierId::Disk);
        }

        // Under VRAM pressure, migrate cold chunks off GPU VRAM
        if pressure.vram_pressure > 0.7 && !hot && current_tier == TierId::GpuVram {
            return Some(TierId::Disk);
        }

        None
    }

    fn migration_priority(&self, meta: &ChunkMeta, pressure: &PressureState) -> TransferPriority {
        // Critical: hot chunk on slow tier under pressure
        if self.is_hot(meta) && pressure.memory_pressure > 0.9 {
            return TransferPriority::Critical;
        }

        // High: hot chunk on slow tier, or any migration under high pressure
        if self.is_hot(meta) || pressure.memory_pressure > 0.7 {
            return TransferPriority::High;
        }

        // Normal: moderate pressure
        if pressure.memory_pressure > 0.5 {
            return TransferPriority::Normal;
        }

        // Low: everything else
        TransferPriority::Low
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::ChunkMeta;

    fn make_chunk(last_accessed: u64, created_at: u64, tier: TierId) -> ChunkMeta {
        ChunkMeta {
            id: ChunkId::from_data(b"test"),
            size: 1024,
            compressed_size: 0,
            tier,
            state: ghost_core::state::ChunkState::Stored,
            created_at,
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
    fn test_lru_name() {
        let policy = LruPolicy::with_defaults();
        assert_eq!(policy.name(), "lru");
    }

    #[test]
    fn test_lru_select_target_tier_empty() {
        let policy = LruPolicy::with_defaults();
        let meta = make_chunk(now(), now(), TierId::Ram);
        let pressure = PressureState::new();
        // Empty tiers should return preferred tier
        let tier = policy.select_target_tier(&meta, &pressure, &[]);
        assert_eq!(tier, TierId::Ram);
    }

    #[test]
    fn test_lru_select_target_tier_hot_chunk() {
        let policy = LruPolicy::with_defaults();
        let meta = make_chunk(now(), now(), TierId::Disk);
        let pressure = PressureState::new();
        let tiers = vec![TierId::Ram, TierId::Disk];
        // Hot chunk should go to best tier (RAM under no pressure)
        let tier = policy.select_target_tier(&meta, &pressure, &tiers);
        assert_eq!(tier, TierId::Ram);
    }

    #[test]
    fn test_lru_select_target_tier_cold_chunk() {
        let policy = LruPolicy::with_defaults();
        let old_time = now() - 3600; // 1 hour ago
        let meta = make_chunk(old_time, old_time, TierId::Ram);
        let pressure = PressureState::new();
        let tiers = vec![TierId::Ram, TierId::Disk];
        // Cold chunk should go to fallback tier (Disk)
        let tier = policy.select_target_tier(&meta, &pressure, &tiers);
        assert_eq!(tier, TierId::Disk);
    }

    #[test]
    fn test_lru_select_viction_empty() {
        let policy = LruPolicy::with_defaults();
        let pressure = PressureState::new();
        assert_eq!(policy.select_viction(&[], &pressure), None);
    }

    #[test]
    fn test_lru_select_viction_oldest() {
        let policy = LruPolicy::with_defaults();
        let now = now();
        let old = now - 3600;
        let older = now - 7200;

        let candidates = vec![
            (ChunkId::from_data(b"a"), make_chunk(now, now, TierId::Ram)),
            (ChunkId::from_data(b"b"), make_chunk(old, old, TierId::Ram)),
            (
                ChunkId::from_data(b"c"),
                make_chunk(older, older, TierId::Ram),
            ),
        ];

        let pressure = PressureState::new();
        let victim = policy.select_viction(&candidates, &pressure);
        // Should evict the oldest (chunk c)
        assert_eq!(victim, Some(ChunkId::from_data(b"c")));
    }

    #[test]
    fn test_lru_should_migrate_hot_from_disk() {
        let policy = LruPolicy::with_defaults();
        let meta = make_chunk(now(), now(), TierId::Disk);
        let pressure = PressureState::new();
        let result = policy.should_migrate(&meta, TierId::Disk, &pressure);
        // Hot chunk on disk should want to migrate to faster tier
        assert!(result.is_some());
        assert_ne!(result.unwrap(), TierId::Disk);
    }

    #[test]
    fn test_lru_should_migrate_cold_from_ram() {
        let policy = LruPolicy::with_defaults();
        let old_time = now() - 3600;
        let meta = make_chunk(old_time, old_time, TierId::Ram);
        let pressure = PressureState::new();
        let result = policy.should_migrate(&meta, TierId::Ram, &pressure);
        // Cold chunk on RAM should want to migrate to slower tier
        assert!(result.is_some());
    }

    #[test]
    fn test_lru_should_not_migrate_hot_on_ram() {
        let policy = LruPolicy::with_defaults();
        let meta = make_chunk(now(), now(), TierId::Ram);
        let pressure = PressureState::new();
        let result = policy.should_migrate(&meta, TierId::Ram, &pressure);
        // Hot chunk on RAM should stay
        assert_eq!(result, None);
    }

    #[test]
    fn test_lru_migration_priority_critical() {
        let policy = LruPolicy::with_defaults();
        let meta = make_chunk(now(), now(), TierId::Disk);
        let mut pressure = PressureState::new();
        pressure.memory_pressure = 0.95;
        let priority = policy.migration_priority(&meta, &pressure);
        assert_eq!(priority, TransferPriority::Critical);
    }

    #[test]
    fn test_lru_migration_priority_low() {
        let policy = LruPolicy::with_defaults();
        let old_time = now() - 3600;
        let meta = make_chunk(old_time, old_time, TierId::Ram);
        let pressure = PressureState::new();
        let priority = policy.migration_priority(&meta, &pressure);
        assert_eq!(priority, TransferPriority::Low);
    }

    #[test]
    fn test_lru_deterministic() {
        let policy = LruPolicy::with_defaults();
        let meta = make_chunk(now(), now(), TierId::Disk);
        let pressure = PressureState::new();
        let tiers = vec![TierId::Ram, TierId::Disk];

        // Same inputs should always produce same outputs
        for _ in 0..10 {
            let tier = policy.select_target_tier(&meta, &pressure, &tiers);
            assert_eq!(tier, TierId::Ram);
        }
    }

    #[test]
    fn test_lru_config_default() {
        let config = LruConfig::default();
        assert_eq!(config.hotness_threshold_secs, 300);
        assert_eq!(config.min_residence_secs, 60);
        assert_eq!(config.preferred_tier, TierId::Ram);
        assert_eq!(config.fallback_tier, TierId::Disk);
    }
}
