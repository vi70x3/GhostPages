//! Runtime tier inventory — live tier graph observation.
//!
//! [`TierInventory`] discovers and tracks all memory/storage tiers available
//! on the system. It is **observational only** — no migrations occur.
//!
//! The tier graph is built from:
//! - `/proc/meminfo` (DRAM)
//! - `/proc/swaps` (swap devices)
//! - `/sys/block/zram*` (ZRAM compressed RAM)
//! - PSI pressure metrics
//! - A simulated tier for testing

use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::SeedableRng;

use ghost_core::emitter::EventEmitter;
use ghost_core::error::{GhostError, GhostResult};
use ghost_core::events::Event;
use ghost_core::time::TimeProvider;
use ghost_core::types::TierId;

use crate::meminfo::MeminfoReader;
use crate::psi::{PsiReader, PsiResource};
use crate::swaps::SwapReader;
use crate::zram::ZramReader;

// ─── Tier Kind ─────────────────────────────────────────────────────────────────

/// Kind of storage tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TierKind {
    /// System DRAM (hot tier).
    Dram,

    /// ZRAM compressed RAM (warm tier).
    Zram,

    /// Disk swap (cold tier).
    Swap,

    /// Disk-based swap file.
    DiskSwap,

    /// GPU VRAM (warm tier, high bandwidth).
    GpuVram,

    /// Simulated tier for testing.
    Simulated,
}

impl std::fmt::Display for TierKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TierKind::Dram => write!(f, "dram"),
            TierKind::Zram => write!(f, "zram"),
            TierKind::Swap => write!(f, "swap"),
            TierKind::DiskSwap => write!(f, "disk_swap"),
            TierKind::GpuVram => write!(f, "gpu_vram"),
            TierKind::Simulated => write!(f, "simulated"),
        }
    }
}

// ─── Tier Info ─────────────────────────────────────────────────────────────────

/// Information about a single tier in the inventory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierInfo {
    /// Tier identifier.
    pub id: TierId,

    /// Kind of tier.
    pub kind: TierKind,

    /// Human-readable name.
    pub name: String,

    /// Total capacity in bytes.
    pub total_bytes: u64,

    /// Used bytes.
    pub used_bytes: u64,

    /// Available bytes.
    pub available_bytes: u64,

    /// Current pressure state.
    pub pressure: ghost_core::state::PressureState,

    /// Backend health.
    pub health: ghost_core::events::BackendHealth,

    /// Last update timestamp (seconds since epoch).
    pub last_updated: u64,

    /// Bytes classified as hot (frequently accessed).
    pub hot_bytes: u64,

    /// Bytes classified as warm (moderately accessed).
    pub warm_bytes: u64,

    /// Bytes classified as cold (rarely accessed).
    pub cold_bytes: u64,

    /// Bytes classified as frozen (essentially never accessed).
    pub frozen_bytes: u64,
}

impl TierInfo {
    /// Create a new TierInfo with default values.
    pub fn new(id: TierId, kind: TierKind, name: impl Into<String>) -> Self {
        Self {
            id,
            kind,
            name: name.into(),
            total_bytes: 0,
            used_bytes: 0,
            available_bytes: 0,
            pressure: ghost_core::state::PressureState::new(),
            health: ghost_core::events::BackendHealth::Healthy,
            last_updated: 0,
            hot_bytes: 0,
            warm_bytes: 0,
            cold_bytes: 0,
            frozen_bytes: 0,
        }
    }

    /// Get the utilization ratio (0.0 = empty, 1.0 = full).
    pub fn utilization(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            self.used_bytes as f64 / self.total_bytes as f64
        }
    }
}

// ─── Tier Inventory ────────────────────────────────────────────────────────────

/// Discovers and tracks all memory/storage tiers on the system.
///
/// The tier graph is observational only — no migrations occur.
/// All readers are used in read-only mode with graceful degradation
/// when Linux interfaces aren't available.
pub struct TierInventory {
    tiers: BTreeMap<TierId, TierInfo>,
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
}

impl TierInventory {
    /// Create a new tier inventory.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        Self {
            tiers: BTreeMap::new(),
            time_provider,
            event_emitter,
        }
    }

    /// Discover all available tiers on this system.
    ///
    /// 1. Always adds `TierId::Ram` (DRAM is always present)
    /// 2. Always adds `TierId::Simulation` (simulated tier for testing)
    /// 3. If swap devices found via SwapReader, adds swap tiers
    /// 4. If ZRAM devices found via ZramReader, adds ZRAM tiers
    /// 5. Emits `TierInventoryChanged` event
    pub fn discover(&mut self) -> GhostResult<()> {
        let timestamp = self.time_provider.timestamp_secs();

        // 1. Always add DRAM tier
        let mut dram = TierInfo::new(TierId::Ram, TierKind::Dram, "DRAM");
        dram.last_updated = timestamp;
        self.tiers.insert(TierId::Ram, dram);

        // 2. Always add simulation tier
        let mut sim = TierInfo::new(TierId::Simulation, TierKind::Simulated, "Simulation");
        sim.last_updated = timestamp;
        self.tiers.insert(TierId::Simulation, sim);

        // 3. Try to discover swap devices
        let swap_reader = SwapReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        match swap_reader.read() {
            Ok(topology) => {
                if !topology.devices.is_empty() {
                    let total_bytes = topology.total_kb * 1024;
                    let used_bytes = topology.used_kb * 1024;
                    let mut swap = TierInfo::new(
                        TierId::Disk,
                        TierKind::Swap,
                        "Swap",
                    );
                    swap.total_bytes = total_bytes;
                    swap.used_bytes = used_bytes;
                    swap.available_bytes = total_bytes.saturating_sub(used_bytes);
                    swap.last_updated = timestamp;
                    self.tiers.insert(TierId::Disk, swap);
                }
            }
            Err(_) => {
                // Graceful degradation: swap not available
            }
        }

        // 4. Try to discover ZRAM devices
        let zram_reader = ZramReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        match zram_reader.read() {
            Ok(snapshot) => {
                if !snapshot.devices.is_empty() {
                    // Use a synthetic TierId for ZRAM — we map it to GpuVram
                    // since ZRAM doesn't have its own TierId variant.
                    // In a real system, ZRAM would have a dedicated TierId.
                    // For now, we add it as a separate entry using a custom name.
                    let total_bytes = snapshot.total_comp_kb * 1024;
                    let used_bytes = snapshot.total_comp_kb * 1024;
                    let mut zram = TierInfo::new(
                        TierId::GpuVram,
                        TierKind::Zram,
                        "ZRAM",
                    );
                    zram.total_bytes = total_bytes;
                    zram.used_bytes = used_bytes;
                    zram.available_bytes = total_bytes.saturating_sub(used_bytes);
                    zram.last_updated = timestamp;
                    self.tiers.insert(TierId::GpuVram, zram);
                }
            }
            Err(_) => {
                // Graceful degradation: ZRAM not available
            }
        }

        // 5. Emit tier inventory changed event
        self.emit_inventory_changed();

        Ok(())
    }

    /// Update tier info from all readers.
    ///
    /// 1. Read memory info -> update DRAM tier
    /// 2. Read PSI -> update pressure state for each tier
    /// 3. Read swap -> update swap tier utilization
    /// 4. Read ZRAM -> update ZRAM tier utilization
    /// 5. Emit appropriate events for any changes
    pub fn refresh(&mut self) -> GhostResult<()> {
        let timestamp = self.time_provider.timestamp_secs();

        // Collect event data to emit after releasing mutable borrows
        let mut utilization_events: Vec<(String, u64, u64)> = Vec::new();

        // 1. Read memory info -> update DRAM tier
        let meminfo_reader = MeminfoReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        if let Ok(snapshot) = meminfo_reader.read() {
            if let Some(dram) = self.tiers.get_mut(&TierId::Ram) {
                let old_used = dram.used_bytes;
                dram.total_bytes = snapshot.total_kb * 1024;
                dram.available_bytes = snapshot.available_kb * 1024;
                dram.used_bytes = dram.total_bytes.saturating_sub(dram.available_bytes);
                dram.last_updated = timestamp;

                if old_used != dram.used_bytes {
                    utilization_events.push((
                        dram.name.clone(),
                        dram.used_bytes,
                        dram.total_bytes,
                    ));
                }
            }
        }

        // 2. Read PSI -> update pressure state
        let psi_reader = PsiReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        if let Ok(sample) = psi_reader.read(PsiResource::Memory) {
            if let Some(dram) = self.tiers.get_mut(&TierId::Ram) {
                let pressure_value = (sample.avg10 / 10.0).min(1.0).max(0.0) as f32;
                dram.pressure.memory_pressure = pressure_value;
                dram.last_updated = timestamp;
            }
        }

        // 3. Read swap -> update swap tier utilization
        let swap_reader = SwapReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        if let Ok(topology) = swap_reader.read() {
            if let Some(swap) = self.tiers.get_mut(&TierId::Disk) {
                let old_used = swap.used_bytes;
                swap.total_bytes = topology.total_kb * 1024;
                swap.used_bytes = topology.used_kb * 1024;
                swap.available_bytes = swap.total_bytes.saturating_sub(swap.used_bytes);
                swap.last_updated = timestamp;

                if old_used != swap.used_bytes {
                    utilization_events.push((
                        swap.name.clone(),
                        swap.used_bytes,
                        swap.total_bytes,
                    ));
                }
            }
        }

        // 4. Read ZRAM -> update ZRAM tier utilization
        let zram_reader = ZramReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        if let Ok(snapshot) = zram_reader.read() {
            if let Some(zram) = self.tiers.get_mut(&TierId::GpuVram) {
                let old_used = zram.used_bytes;
                zram.total_bytes = snapshot.total_comp_kb * 1024;
                zram.used_bytes = snapshot.total_comp_kb * 1024;
                zram.available_bytes = zram.total_bytes.saturating_sub(zram.used_bytes);
                zram.last_updated = timestamp;

                if old_used != zram.used_bytes {
                    utilization_events.push((
                        zram.name.clone(),
                        zram.used_bytes,
                        zram.total_bytes,
                    ));
                }
            }
        }

        // Emit utilization events after releasing all mutable borrows
        for (name, used, total) in utilization_events {
            self.emit_utilization_changed(&name, used, total);
        }

        Ok(())
    }

    /// Get a specific tier by ID.
    pub fn get_tier(&self, id: &TierId) -> Option<&TierInfo> {
        self.tiers.get(id)
    }

    /// Get all tiers.
    pub fn all_tiers(&self) -> &BTreeMap<TierId, TierInfo> {
        &self.tiers
    }

    /// Get tiers sorted by preference: DRAM first, then ZRAM, then swap.
    pub fn tiers_by_preference(&self) -> Vec<&TierInfo> {
        let mut tiers: Vec<&TierInfo> = self.tiers.values().collect();
        tiers.sort_by(|a, b| {
            let a_priority = tier_preference_order(&a.kind);
            let b_priority = tier_preference_order(&b.kind);
            a_priority.cmp(&b_priority)
        });
        tiers
    }

    /// Get the number of tiers in the inventory.
    pub fn tier_count(&self) -> usize {
        self.tiers.len()
    }

    /// Emit `TierInventoryChanged` event.
    fn emit_inventory_changed(&self) {
        let tier_names: Vec<String> = self
            .tiers
            .values()
            .map(|t| t.name.clone())
            .collect();
        let _ = self.event_emitter.try_emit(Event::TierInventoryChanged {
            sequence_id: 0,
            tiers: tier_names,
        });
    }

    /// Emit `TierUtilizationChanged` event.
    fn emit_utilization_changed(&self, name: &str, used_bytes: u64, total_bytes: u64) {
        let _ = self.event_emitter.try_emit(Event::TierUtilizationChanged {
            sequence_id: 0,
            tier: name.to_string(),
            used_bytes,
            total_bytes,
        });
    }
}

/// Get the preference order for a tier kind (lower = preferred).
fn tier_preference_order(kind: &TierKind) -> u8 {
    match kind {
        TierKind::Dram => 0,
        TierKind::GpuVram => 1,
        TierKind::Zram => 2,
        TierKind::Simulated => 3,
        TierKind::Swap => 4,
        TierKind::DiskSwap => 5,
    }
}

// ─── Simulated Tier Inventory ─────────────────────────────────────────────────

/// Deterministic tier inventory generator for testing.
///
/// Generates a deterministic tier graph from a seed:
/// - Configurable number of tiers
/// - Configurable sizes and utilization levels
/// - Deterministic pressure states
pub struct SimulatedTierInventory {
    seed: u64,
    tier_count: usize,
    time_provider: Arc<dyn TimeProvider>,
    event_emitter: EventEmitter,
}

impl SimulatedTierInventory {
    /// Create a new simulated tier inventory.
    pub fn new(
        seed: u64,
        tier_count: usize,
        time_provider: Arc<dyn TimeProvider>,
        event_emitter: EventEmitter,
    ) -> Self {
        Self {
            seed,
            tier_count,
            time_provider,
            event_emitter,
        }
    }

    /// Generate a deterministic tier graph from the seed.
    pub fn generate(&self) -> GhostResult<Vec<TierInfo>> {
        let mut rng = StdRng::seed_from_u64(self.seed);
        use rand::Rng;

        let mut tiers = Vec::new();
        let timestamp = self.time_provider.timestamp_secs();

        // Always include DRAM
        let dram_total: u64 = rng.gen_range(4_000_000_000..32_000_000_000); // 4-32 GB
        let dram_used: u64 = rng.gen_range(0..dram_total);
        tiers.push(TierInfo {
            id: TierId::Ram,
            kind: TierKind::Dram,
            name: "DRAM".to_string(),
            total_bytes: dram_total,
            used_bytes: dram_used,
            available_bytes: dram_total - dram_used,
            pressure: ghost_core::state::PressureState {
                memory_pressure: (dram_used as f64 / dram_total as f64).min(1.0) as f32,
                ..Default::default()
            },
            health: ghost_core::events::BackendHealth::Healthy,
            last_updated: timestamp,
            hot_bytes: 0,
            warm_bytes: 0,
            cold_bytes: 0,
            frozen_bytes: 0,
        });

        // Add additional tiers based on count
        for i in 1..self.tier_count {
            let kind = match i % 3 {
                0 => TierKind::Zram,
                1 => TierKind::Swap,
                _ => TierKind::Simulated,
            };

            let name = format!("{}_{}", kind, i);
            let total: u64 = rng.gen_range(1_000_000_000..16_000_000_000); // 1-16 GB
            let used: u64 = rng.gen_range(0..total);

            let id = match kind {
                TierKind::Zram => TierId::GpuVram,
                TierKind::Swap => TierId::Disk,
                TierKind::Simulated => TierId::Simulation,
                _ => TierId::Ram,
            };

            tiers.push(TierInfo {
                id,
                kind,
                name,
                total_bytes: total,
                used_bytes: used,
                available_bytes: total - used,
                pressure: ghost_core::state::PressureState::new(),
                health: ghost_core::events::BackendHealth::Healthy,
                last_updated: timestamp,
                hot_bytes: 0,
                warm_bytes: 0,
                cold_bytes: 0,
                frozen_bytes: 0,
            });
        }

        // Emit tier inventory changed event
        let tier_names: Vec<String> = tiers.iter().map(|t| t.name.clone()).collect();
        let _ = self.event_emitter.try_emit(Event::TierInventoryChanged {
            sequence_id: 0,
            tiers: tier_names,
        });

        Ok(tiers)
    }
}

// ─── Prometheus Metrics ───────────────────────────────────────────────────────

/// Prometheus metrics for tier inventory observation.
pub mod metrics {
    use prometheus::{Gauge, Opts, Registry};

    use ghost_core::error::{GhostError, GhostResult};

    /// Container for all tier inventory metrics.
    pub struct TierInventoryMetrics {
        /// Number of tiers discovered.
        pub tier_count: Gauge,
        /// Total bytes per tier.
        pub tier_total_bytes: Gauge,
        /// Used bytes per tier.
        pub tier_used_bytes: Gauge,
        /// Pressure per tier (0.0 = none, 1.0 = critical).
        pub tier_pressure: Gauge,
    }

    /// Register tier inventory metrics with the given registry.
    pub fn register(registry: &Registry) -> GhostResult<TierInventoryMetrics> {
        let tier_count = Gauge::with_opts(Opts::new(
            "ghost_tier_count",
            "Number of discovered memory/storage tiers",
        ))
        .map_err(|e| GhostError::Internal(e.to_string()))?;

        let tier_total_bytes = Gauge::with_opts(Opts::new(
            "ghost_tier_total_bytes",
            "Total bytes per tier",
        ))
        .map_err(|e| GhostError::Internal(e.to_string()))?;

        let tier_used_bytes = Gauge::with_opts(Opts::new(
            "ghost_tier_used_bytes",
            "Used bytes per tier",
        ))
        .map_err(|e| GhostError::Internal(e.to_string()))?;

        let tier_pressure = Gauge::with_opts(Opts::new(
            "ghost_tier_pressure",
            "Pressure per tier (0.0 = none, 1.0 = critical)",
        ))
        .map_err(|e| GhostError::Internal(e.to_string()))?;

        registry
            .register(Box::new(tier_count.clone()))
            .map_err(|e| GhostError::Internal(e.to_string()))?;
        registry
            .register(Box::new(tier_total_bytes.clone()))
            .map_err(|e| GhostError::Internal(e.to_string()))?;
        registry
            .register(Box::new(tier_used_bytes.clone()))
            .map_err(|e| GhostError::Internal(e.to_string()))?;
        registry
            .register(Box::new(tier_pressure.clone()))
            .map_err(|e| GhostError::Internal(e.to_string()))?;

        Ok(TierInventoryMetrics {
            tier_count,
            tier_total_bytes,
            tier_used_bytes,
            tier_pressure,
        })
    }

    /// Update tier inventory metrics from a tier info.
    pub fn update_tier(metrics: &TierInventoryMetrics, tier: &super::TierInfo) {
        metrics.tier_total_bytes
            .set(tier.total_bytes as f64);
        metrics.tier_used_bytes
            .set(tier.used_bytes as f64);
        metrics.tier_pressure
            .set(tier.pressure.max_pressure() as f64);
    }

    /// Update the tier count gauge.
    pub fn update_tier_count(metrics: &TierInventoryMetrics, count: usize) {
        metrics.tier_count.set(count as f64);
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::time::DeterministicTimeProvider;
    use prometheus::Registry;

    fn test_time_provider() -> Arc<dyn TimeProvider> {
        Arc::new(DeterministicTimeProvider::new(
            1_700_000_000,
            std::time::Duration::from_secs(1),
        ))
    }

    fn test_emitter() -> EventEmitter {
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        EventEmitter::new(tx)
    }

    #[test]
    fn test_tier_info_new() {
        let info = TierInfo::new(TierId::Ram, TierKind::Dram, "DRAM");
        assert_eq!(info.id, TierId::Ram);
        assert_eq!(info.kind, TierKind::Dram);
        assert_eq!(info.name, "DRAM");
        assert_eq!(info.total_bytes, 0);
        assert_eq!(info.used_bytes, 0);
        assert_eq!(info.available_bytes, 0);
        assert_eq!(info.health, ghost_core::events::BackendHealth::Healthy);
    }

    #[test]
    fn test_tier_info_utilization() {
        let mut info = TierInfo::new(TierId::Ram, TierKind::Dram, "DRAM");
        assert!((info.utilization() - 0.0).abs() < f64::EPSILON);

        info.total_bytes = 1000;
        info.used_bytes = 500;
        assert!((info.utilization() - 0.5).abs() < f64::EPSILON);

        info.used_bytes = 1000;
        assert!((info.utilization() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_tier_kind_display() {
        assert_eq!(format!("{}", TierKind::Dram), "dram");
        assert_eq!(format!("{}", TierKind::Zram), "zram");
        assert_eq!(format!("{}", TierKind::Swap), "swap");
        assert_eq!(format!("{}", TierKind::DiskSwap), "disk_swap");
        assert_eq!(format!("{}", TierKind::GpuVram), "gpu_vram");
        assert_eq!(format!("{}", TierKind::Simulated), "simulated");
    }

    #[test]
    fn test_tier_inventory_new() {
        let inventory = TierInventory::new(test_time_provider(), test_emitter());
        assert_eq!(inventory.tier_count(), 0);
        assert!(inventory.all_tiers().is_empty());
    }

    #[test]
    fn test_tier_preference_order() {
        assert!(tier_preference_order(&TierKind::Dram) < tier_preference_order(&TierKind::GpuVram));
        assert!(tier_preference_order(&TierKind::GpuVram) < tier_preference_order(&TierKind::Zram));
        assert!(tier_preference_order(&TierKind::Zram) < tier_preference_order(&TierKind::Swap));
        assert!(tier_preference_order(&TierKind::Swap) < tier_preference_order(&TierKind::DiskSwap));
    }

    #[test]
    fn test_simulated_deterministic() {
        let tp = test_time_provider();
        let emitter1 = test_emitter();
        let emitter2 = test_emitter();

        let sim1 = SimulatedTierInventory::new(42, 3, tp.clone(), emitter1);
        let sim2 = SimulatedTierInventory::new(42, 3, tp, emitter2);

        let tiers1 = sim1.generate().unwrap();
        let tiers2 = sim2.generate().unwrap();

        assert_eq!(tiers1.len(), tiers2.len());
        for (t1, t2) in tiers1.iter().zip(tiers2.iter()) {
            assert_eq!(t1.name, t2.name);
            assert_eq!(t1.kind, t2.kind);
            assert_eq!(t1.total_bytes, t2.total_bytes);
            assert_eq!(t1.used_bytes, t2.used_bytes);
        }
    }

    #[test]
    fn test_simulated_different_seeds() {
        let tp = test_time_provider();

        let sim1 = SimulatedTierInventory::new(42, 3, tp.clone(), test_emitter());
        let sim2 = SimulatedTierInventory::new(99, 3, tp, test_emitter());

        let tiers1 = sim1.generate().unwrap();
        let tiers2 = sim2.generate().unwrap();

        // Different seeds should produce different values
        let any_different = tiers1.iter().zip(tiers2.iter()).any(|(t1, t2)| {
            t1.total_bytes != t2.total_bytes || t1.used_bytes != t2.used_bytes
        });
        assert!(any_different, "Different seeds should produce different tier graphs");
    }

    #[test]
    fn test_simulated_emits_events() {
        let tp = test_time_provider();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let emitter = EventEmitter::new(tx);

        let sim = SimulatedTierInventory::new(42, 3, tp, emitter);
        let _tiers = sim.generate().unwrap();

        // Should have received a TierInventoryChanged event
        let record = rx.try_recv().expect("should have received an event");
        match record.event {
            Event::TierInventoryChanged { tiers, .. } => {
                assert!(!tiers.is_empty());
            }
            other => panic!("expected TierInventoryChanged, got {:?}", other),
        }
    }

    #[test]
    fn test_metrics_register() {
        let registry = Registry::new();
        let result = metrics::register(&registry);
        assert!(result.is_ok());
    }

    #[test]
    fn test_metrics_update_tier() {
        let registry = Registry::new();
        let m = metrics::register(&registry).unwrap();
        let info = TierInfo {
            id: TierId::Ram,
            kind: TierKind::Dram,
            name: "DRAM".to_string(),
            total_bytes: 16_000_000_000,
            used_bytes: 8_000_000_000,
            available_bytes: 8_000_000_000,
            pressure: ghost_core::state::PressureState::new(),
            health: ghost_core::events::BackendHealth::Healthy,
            last_updated: 1_700_000_000,
            hot_bytes: 0,
            warm_bytes: 0,
            cold_bytes: 0,
            frozen_bytes: 0,
        };
        metrics::update_tier(&m, &info);
        metrics::update_tier_count(&m, 1);
    }
}
