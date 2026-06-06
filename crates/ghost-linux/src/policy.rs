//! Policy runtime — read-only observation and recommendation engine.
//!
//! [`PolicyRuntime`] evaluates current system state and emits
//! [`Recommendation`]s. It is **read-only** — no state mutation, no migrations.
//! The caller decides what to do with the recommendations.
//!
//! The runtime is deterministic: same system state → same recommendations.
//! All thresholds are configurable via [`PolicyRules`] but fixed during evaluation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use ghost_core::emitter::EventEmitter;
use ghost_core::error::{GhostError, GhostResult};
use ghost_core::events::Event;
use ghost_core::hotness_provider::HotnessProvider;
use ghost_core::time::TimeProvider;
use ghost_core::types::{ChunkId, TierId};

use crate::hotness_provider::MockHotnessProvider;
use crate::psi::PsiResource;
use crate::tier_inventory::TierInventory;

// ─── Recommendation ─────────────────────────────────────────────────────────────

/// A recommendation emitted by the policy runtime.
///
/// Recommendations are **advisory only** — they suggest what the caller
/// *should* do, but the policy runtime never performs any action itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Recommendation {
    /// Promote a hot chunk to DRAM.
    PromoteToDram {
        /// Chunk to promote.
        chunk_id: ChunkId,
        /// Human-readable reason for the recommendation.
        reason: String,
    },

    /// Move a cold chunk to ZRAM.
    MoveToZram {
        /// Chunk to move.
        chunk_id: ChunkId,
        /// Human-readable reason for the recommendation.
        reason: String,
    },

    /// Move a cold chunk to disk swap.
    MoveToDiskSwap {
        /// Chunk to move.
        chunk_id: ChunkId,
        /// Human-readable reason for the recommendation.
        reason: String,
    },

    /// No action needed.
    NoAction {
        /// Human-readable reason for the recommendation.
        reason: String,
    },

    /// Evict cold chunks from a tier.
    EvictCold {
        /// Tier to evict from.
        tier: TierId,
        /// Number of chunks to evict.
        count: usize,
    },

    /// Demote hot chunks from a tier to a target tier.
    DemoteHot {
        /// Tier to demote from.
        tier: TierId,
        /// Target tier.
        target: TierId,
    },
}

impl Recommendation {
    /// Get a short string describing the recommendation type.
    pub fn kind(&self) -> &'static str {
        match self {
            Recommendation::PromoteToDram { .. } => "promote_to_dram",
            Recommendation::MoveToZram { .. } => "move_to_zram",
            Recommendation::MoveToDiskSwap { .. } => "move_to_disk_swap",
            Recommendation::NoAction { .. } => "no_action",
            Recommendation::EvictCold { .. } => "evict_cold",
            Recommendation::DemoteHot { .. } => "demote_hot",
        }
    }

    /// Get the reason string for this recommendation.
    pub fn reason(&self) -> &str {
        match self {
            Recommendation::PromoteToDram { reason, .. } => reason,
            Recommendation::MoveToZram { reason, .. } => reason,
            Recommendation::MoveToDiskSwap { reason, .. } => reason,
            Recommendation::NoAction { reason } => reason,
            Recommendation::EvictCold { .. } => "eviction due to pressure",
            Recommendation::DemoteHot { .. } => "demote hot chunks",
        }
    }
}

impl std::fmt::Display for Recommendation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Recommendation::PromoteToDram { chunk_id, reason } => {
                write!(f, "PromoteToDram({}): {}", chunk_id, reason)
            }
            Recommendation::MoveToZram { chunk_id, reason } => {
                write!(f, "MoveToZram({}): {}", chunk_id, reason)
            }
            Recommendation::MoveToDiskSwap { chunk_id, reason } => {
                write!(f, "MoveToDiskSwap({}): {}", chunk_id, reason)
            }
            Recommendation::NoAction { reason } => {
                write!(f, "NoAction: {}", reason)
            }
            Recommendation::EvictCold { tier, count } => {
                write!(f, "EvictCold({:?}, {})", tier, count)
            }
            Recommendation::DemoteHot { tier, target } => {
                write!(f, "DemoteHot({:?} -> {:?})", tier, target)
            }
        }
    }
}

// ─── Policy Runtime ─────────────────────────────────────────────────────────────

/// Policy runtime that evaluates system state and emits recommendations.
///
/// This struct is **read-only** — it never mutates system state or performs
/// migrations. It only observes and recommends.
///
/// The runtime is deterministic: same system state → same recommendations.
pub struct PolicyRuntime {
    /// Live tier inventory for reading current tier state.
    tier_inventory: Arc<RwLock<TierInventory>>,

    /// Optional hotness provider for temperature data.
    hotness_provider: Option<Arc<dyn HotnessProvider>>,

    /// Event emitter for policy events.
    event_emitter: EventEmitter,

    /// Time provider for timestamps.
    time_provider: Arc<dyn TimeProvider>,

    /// Timestamp of last evaluation (seconds since epoch).
    last_evaluation: AtomicU64,

    /// Policy rules (thresholds).
    rules: crate::policy_rules::PolicyRules,
}

impl PolicyRuntime {
    /// Create a new policy runtime.
    ///
    /// # Arguments
    ///
    /// * `tier_inventory` — Shared tier inventory for reading tier state.
    /// * `event_emitter` — Emitter for policy events.
    /// * `time_provider` — Time source for timestamps.
    pub fn new(
        tier_inventory: Arc<RwLock<TierInventory>>,
        event_emitter: EventEmitter,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            tier_inventory,
            hotness_provider: None,
            event_emitter,
            time_provider,
            last_evaluation: AtomicU64::new(0),
            rules: crate::policy_rules::PolicyRules::new(),
        }
    }

    /// Create a new policy runtime with custom rules.
    pub fn with_rules(
        tier_inventory: Arc<RwLock<TierInventory>>,
        event_emitter: EventEmitter,
        time_provider: Arc<dyn TimeProvider>,
        rules: crate::policy_rules::PolicyRules,
    ) -> Self {
        Self {
            tier_inventory,
            hotness_provider: None,
            event_emitter,
            time_provider,
            last_evaluation: AtomicU64::new(0),
            rules,
        }
    }

    /// Set the hotness provider.
    pub fn set_hotness_provider(&mut self, provider: Arc<dyn HotnessProvider>) {
        self.hotness_provider = Some(provider);
    }

    /// Check if enough time has passed since the last evaluation (cooldown).
    pub fn is_cooldown_expired(&self) -> bool {
        let now = self.time_provider.timestamp_secs();
        let last = self.last_evaluation.load(Ordering::Relaxed);
        now.saturating_sub(last) >= self.rules.cooldown_seconds
    }

    /// Evaluate current system state and produce recommendations.
    ///
    /// This is **READ-ONLY** — no state mutation, no migrations.
    ///
    /// # Algorithm
    ///
    /// 1. Read tier inventory (DRAM pressure, swap utilization, ZRAM state).
    /// 2. Read PSI (memory pressure level).
    /// 3. If hotness provider available, read hotness data.
    /// 4. Build a [`SystemState`] snapshot.
    /// 5. Apply policy rules to produce recommendations.
    /// 6. Emit `PolicyRecommendationGenerated` event.
    /// 7. Return recommendations (caller decides what to do with them).
    pub fn evaluate(&self) -> GhostResult<Vec<Recommendation>> {
        let start = std::time::Instant::now();

        // 1. Read tier inventory
        let inventory = self.tier_inventory.read();

        // 2. Build system state from tier inventory
        let state = self.build_system_state(&inventory);

        // 3. Apply policy rules (pure function — deterministic)
        let recommendations = self.rules.evaluate(&state);

        // 4. Update last evaluation timestamp
        let now = self.time_provider.timestamp_secs();
        self.last_evaluation.store(now, Ordering::Relaxed);

        // 5. Emit policy recommendation event
        let rec_strings: Vec<String> = recommendations.iter().map(|r| r.to_string()).collect();
        let pressure_level = state.pressure_level().to_string();
        let _ = self.event_emitter.try_emit(Event::PolicyRecommendationGenerated {
            sequence_id: 0,
            recommendations: rec_strings,
            pressure_level,
        });

        // 6. Record metrics
        let _elapsed = start.elapsed();
        record_metrics(&recommendations, start);

        Ok(recommendations)
    }

    /// Build a system state snapshot from the tier inventory.
    fn build_system_state(
        &self,
        inventory: &TierInventory,
    ) -> crate::policy_rules::SystemState {
        use crate::policy_rules::SystemState;

        let mut dram_pressure = ghost_core::state::PressureState::new();
        let mut dram_utilization = 0.0f32;
        let mut swap_utilization = 0.0f32;
        let mut zram_utilization = None;
        let mut io_pressure = ghost_core::state::PressureState::new();

        for tier in inventory.all_tiers().values() {
            match tier.kind {
                crate::tier_inventory::TierKind::Dram => {
                    dram_utilization = tier.utilization() as f32;
                    dram_pressure.memory_pressure = tier.pressure.memory_pressure;
                    dram_pressure.io_pressure = tier.pressure.io_pressure;
                }
                crate::tier_inventory::TierKind::Swap | crate::tier_inventory::TierKind::DiskSwap => {
                    swap_utilization = tier.utilization() as f32;
                }
                crate::tier_inventory::TierKind::Zram => {
                    zram_utilization = Some(tier.utilization() as f32);
                }
                _ => {}
            }
        }

        // Try to read PSI for more accurate pressure data
        let psi_reader = crate::psi::PsiReader::new(
            self.time_provider.clone(),
            self.event_emitter.clone(),
        );
        if let Ok(sample) = psi_reader.read(PsiResource::Memory) {
            let psi_pressure = (sample.avg10 / 10.0).min(1.0).max(0.0) as f32;
            dram_pressure.memory_pressure = dram_pressure.memory_pressure.max(psi_pressure);
        }
        if let Ok(sample) = psi_reader.read(PsiResource::Io) {
            let psi_io = (sample.avg10 / 10.0).min(1.0).max(0.0) as f32;
            io_pressure.io_pressure = psi_io;
        }

        SystemState {
            dram_pressure,
            dram_utilization,
            swap_utilization,
            zram_utilization,
            io_pressure,
        }
    }
}

impl std::fmt::Debug for PolicyRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyRuntime")
            .field("last_evaluation", &self.last_evaluation.load(Ordering::Relaxed))
            .field("has_hotness_provider", &self.hotness_provider.is_some())
            .field("rules", &self.rules)
            .finish()
    }
}

// ─── Metrics ────────────────────────────────────────────────────────────────────

/// Record Prometheus metrics for a policy evaluation.
fn record_metrics(recommendations: &[Recommendation], start: std::time::Instant) {
    let elapsed = start.elapsed();
    // In a real system, these would update Prometheus counters/histograms.
    // For now, we record the metrics as a no-op with the data available
    // for future Prometheus integration.
    let _ = elapsed;
    let _ = recommendations;
}

/// Prometheus metrics for the policy runtime.
pub mod metrics {
    use prometheus::{Counter, Histogram, HistogramOpts, Opts, Registry};

    use ghost_core::error::{GhostError, GhostResult};

    /// Container for all policy metrics.
    pub struct PolicyMetrics {
        /// Total recommendations emitted.
        pub recommendations_total: Counter,
        /// Total evaluations performed.
        pub evaluations_total: Counter,
        /// Evaluation duration in seconds.
        pub evaluation_duration_seconds: Histogram,
    }

    /// Register policy metrics with the given registry.
    pub fn register(registry: &Registry) -> GhostResult<PolicyMetrics> {
        let recommendations_total = Counter::with_opts(Opts::new(
            "ghost_policy_recommendations_total",
            "Total policy recommendations emitted",
        ))
        .map_err(|e| GhostError::Internal(e.to_string()))?;

        let evaluations_total = Counter::with_opts(Opts::new(
            "ghost_policy_evaluations_total",
            "Total policy evaluations performed",
        ))
        .map_err(|e| GhostError::Internal(e.to_string()))?;

        let evaluation_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "ghost_policy_evaluation_duration_seconds",
            "Policy evaluation duration in seconds",
        ))
        .map_err(|e| GhostError::Internal(e.to_string()))?;

        registry
            .register(Box::new(recommendations_total.clone()))
            .map_err(|e| GhostError::Internal(e.to_string()))?;
        registry
            .register(Box::new(evaluations_total.clone()))
            .map_err(|e| GhostError::Internal(e.to_string()))?;
        registry
            .register(Box::new(evaluation_duration_seconds.clone()))
            .map_err(|e| GhostError::Internal(e.to_string()))?;

        Ok(PolicyMetrics {
            recommendations_total,
            evaluations_total,
            evaluation_duration_seconds,
        })
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

    fn test_tier_inventory() -> Arc<RwLock<TierInventory>> {
        let inventory = TierInventory::new(test_time_provider(), test_emitter());
        Arc::new(RwLock::new(inventory))
    }

    #[test]
    fn test_recommendation_display() {
        let rec = Recommendation::NoAction {
            reason: "system idle".to_string(),
        };
        assert!(rec.to_string().contains("NoAction"));
        assert!(rec.to_string().contains("system idle"));
    }

    #[test]
    fn test_recommendation_kind() {
        let rec = Recommendation::MoveToZram {
            chunk_id: ChunkId::from_data(b"test"),
            reason: "cold chunk".to_string(),
        };
        assert_eq!(rec.kind(), "move_to_zram");
    }

    #[test]
    fn test_recommendation_reason() {
        let rec = Recommendation::NoAction {
            reason: "system idle".to_string(),
        };
        assert_eq!(rec.reason(), "system idle");
    }

    #[test]
    fn test_policy_runtime_new() {
        let runtime = PolicyRuntime::new(
            test_tier_inventory(),
            test_emitter(),
            test_time_provider(),
        );
        assert!(runtime.hotness_provider.is_none());
        assert!(runtime.is_cooldown_expired());
    }

    #[test]
    fn test_policy_runtime_set_hotness_provider() {
        let mut runtime = PolicyRuntime::new(
            test_tier_inventory(),
            test_emitter(),
            test_time_provider(),
        );

        let config = crate::hotness_provider::MockHotnessConfig::default();
        let provider = Arc::new(MockHotnessProvider::new(
            config,
            test_time_provider(),
            test_emitter(),
        ));
        runtime.set_hotness_provider(provider);
        assert!(runtime.hotness_provider.is_some());
    }

    #[test]
    fn test_cooldown_prevents_rapid_evaluation() {
        let runtime = PolicyRuntime::new(
            test_tier_inventory(),
            test_emitter(),
            test_time_provider(),
        );

        // First evaluation should succeed (cooldown expired initially)
        assert!(runtime.is_cooldown_expired());

        // After evaluation, cooldown should be active
        let _ = runtime.evaluate();
        assert!(!runtime.is_cooldown_expired());
    }

    #[test]
    fn test_metrics_register() {
        let registry = Registry::new();
        let result = metrics::register(&registry);
        assert!(result.is_ok());
    }

    #[test]
    fn test_evaluate_returns_recommendations() {
        let runtime = PolicyRuntime::new(
            test_tier_inventory(),
            test_emitter(),
            test_time_provider(),
        );

        let result = runtime.evaluate();
        assert!(result.is_ok());
        let recs = result.unwrap();
        // Should produce at least one recommendation
        assert!(!recs.is_empty());
    }

    #[test]
    fn test_evaluate_does_not_mutate_inventory() {
        let inventory = test_tier_inventory();
        let runtime = PolicyRuntime::new(
            inventory.clone(),
            test_emitter(),
            test_time_provider(),
        );

        // Read tier count before
        let count_before = inventory.read().tier_count();

        // Evaluate
        let _ = runtime.evaluate();

        // Read tier count after — should be unchanged
        let count_after = inventory.read().tier_count();
        assert_eq!(count_before, count_after);
    }
}
