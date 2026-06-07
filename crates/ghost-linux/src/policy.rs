//! Policy runtime — read-only observation and recommendation engine.
//!
//! [`PolicyRuntime`] evaluates current system state and emits
//! [`Recommendation`]s. It is **read-only** — no state mutation, no migrations.
//! The caller decides what to do with the recommendations.
//!
//! The runtime is deterministic: same system state → same recommendations.
//! All thresholds are configurable via [`PolicyRules`] but fixed during evaluation.
//!
//! Stability mechanisms (hysteresis, cooldowns, confidence thresholds) are
//! applied to prevent recommendation flapping and churn.

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

use crate::cooldown::CooldownTracker;
use crate::hotness_provider::MockHotnessProvider;
use crate::policy_rules::{PolicyRules, StabilityConfig, SystemState};
use crate::psi::PsiResource;
use crate::stability::StabilityChecker;
use crate::tier_inventory::TierInventory;

// ─── Recommendation ─────────────────────────────────────────────────────────────

/// A recommendation emitted by the policy runtime.
///
/// Recommendations are **advisory only** — they suggest what the caller
/// *should* do, but the policy runtime never performs any action itself.
///
/// Each recommendation carries a `confidence` score (0.0–1.0) and a list
/// of `factors` that contributed to the decision. Higher confidence means
/// the engine is more certain about the recommendation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Recommendation {
    /// Promote a hot chunk to DRAM.
    PromoteToDram {
        /// Chunk to promote.
        chunk_id: ChunkId,
        /// Human-readable reason for the recommendation.
        reason: String,
        /// Confidence score (0.0–1.0).
        confidence: f32,
        /// Factors that contributed to this recommendation.
        factors: Vec<String>,
    },

    /// Move a cold chunk to ZRAM.
    MoveToZram {
        /// Chunk to move.
        chunk_id: ChunkId,
        /// Human-readable reason for the recommendation.
        reason: String,
        /// Confidence score (0.0–1.0).
        confidence: f32,
        /// Factors that contributed to this recommendation.
        factors: Vec<String>,
    },

    /// Move a cold chunk to disk swap.
    MoveToDiskSwap {
        /// Chunk to move.
        chunk_id: ChunkId,
        /// Human-readable reason for the recommendation.
        reason: String,
        /// Confidence score (0.0–1.0).
        confidence: f32,
        /// Factors that contributed to this recommendation.
        factors: Vec<String>,
    },

    /// No action needed.
    NoAction {
        /// Human-readable reason for the recommendation.
        reason: String,
        /// Confidence score (0.0–1.0).
        confidence: f32,
        /// Factors that contributed to this recommendation.
        factors: Vec<String>,
    },

    /// Evict cold chunks from a tier.
    EvictCold {
        /// Tier to evict from.
        tier: TierId,
        /// Number of chunks to evict.
        count: usize,
        /// Confidence score (0.0–1.0).
        confidence: f32,
        /// Factors that contributed to this recommendation.
        factors: Vec<String>,
    },

    /// Demote hot chunks from a tier to a target tier.
    DemoteHot {
        /// Tier to demote from.
        tier: TierId,
        /// Target tier.
        target: TierId,
        /// Confidence score (0.0–1.0).
        confidence: f32,
        /// Factors that contributed to this recommendation.
        factors: Vec<String>,
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
            Recommendation::NoAction { reason, .. } => reason,
            Recommendation::EvictCold { .. } => "eviction due to pressure",
            Recommendation::DemoteHot { .. } => "demote hot chunks",
        }
    }

    /// Get the confidence score for this recommendation.
    pub fn confidence(&self) -> f32 {
        match self {
            Recommendation::PromoteToDram { confidence, .. } => *confidence,
            Recommendation::MoveToZram { confidence, .. } => *confidence,
            Recommendation::MoveToDiskSwap { confidence, .. } => *confidence,
            Recommendation::NoAction { confidence, .. } => *confidence,
            Recommendation::EvictCold { confidence, .. } => *confidence,
            Recommendation::DemoteHot { confidence, .. } => *confidence,
        }
    }

    /// Get the factors that contributed to this recommendation.
    pub fn factors(&self) -> &[String] {
        match self {
            Recommendation::PromoteToDram { factors, .. } => factors,
            Recommendation::MoveToZram { factors, .. } => factors,
            Recommendation::MoveToDiskSwap { factors, .. } => factors,
            Recommendation::NoAction { factors, .. } => factors,
            Recommendation::EvictCold { factors, .. } => factors,
            Recommendation::DemoteHot { factors, .. } => factors,
        }
    }
}

impl std::fmt::Display for Recommendation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Recommendation::PromoteToDram { chunk_id, reason, confidence, .. } => {
                write!(f, "PromoteToDram({}): {} (confidence={:.2})", chunk_id, reason, confidence)
            }
            Recommendation::MoveToZram { chunk_id, reason, confidence, .. } => {
                write!(f, "MoveToZram({}): {} (confidence={:.2})", chunk_id, reason, confidence)
            }
            Recommendation::MoveToDiskSwap { chunk_id, reason, confidence, .. } => {
                write!(f, "MoveToDiskSwap({}): {} (confidence={:.2})", chunk_id, reason, confidence)
            }
            Recommendation::NoAction { reason, confidence, .. } => {
                write!(f, "NoAction: {} (confidence={:.2})", reason, confidence)
            }
            Recommendation::EvictCold { tier, count, confidence, .. } => {
                write!(f, "EvictCold({:?}, {}) (confidence={:.2})", tier, count, confidence)
            }
            Recommendation::DemoteHot { tier, target, confidence, .. } => {
                write!(f, "DemoteHot({:?} -> {:?}) (confidence={:.2})", tier, target, confidence)
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
/// Stability mechanisms (hysteresis, cooldowns, confidence thresholds) are
/// applied to prevent recommendation flapping.
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
    rules: PolicyRules,

    /// Stability configuration for hysteresis and cooldowns.
    stability_config: StabilityConfig,

    /// Cooldown tracker for per-region recommendation rate limiting.
    cooldown_tracker: CooldownTracker,

    /// Stability checker for temperature history and trend analysis.
    stability_checker: StabilityChecker,
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
        let stability_config = StabilityConfig::default();
        let cooldown_tracker = CooldownTracker::new(
            stability_config.clone(),
            time_provider.clone(),
        );
        let stability_checker = StabilityChecker::new(stability_config.clone());
        Self {
            tier_inventory,
            hotness_provider: None,
            event_emitter,
            time_provider,
            last_evaluation: AtomicU64::new(0),
            rules: PolicyRules::new(),
            stability_config,
            cooldown_tracker,
            stability_checker,
        }
    }

    /// Create a new policy runtime with custom rules.
    pub fn with_rules(
        tier_inventory: Arc<RwLock<TierInventory>>,
        event_emitter: EventEmitter,
        time_provider: Arc<dyn TimeProvider>,
        rules: PolicyRules,
    ) -> Self {
        let stability_config = StabilityConfig::default();
        let cooldown_tracker = CooldownTracker::new(
            stability_config.clone(),
            time_provider.clone(),
        );
        let stability_checker = StabilityChecker::new(stability_config.clone());
        Self {
            tier_inventory,
            hotness_provider: None,
            event_emitter,
            time_provider,
            last_evaluation: AtomicU64::new(0),
            rules,
            stability_config,
            cooldown_tracker,
            stability_checker,
        }
    }

    /// Create a new policy runtime with custom stability configuration.
    pub fn with_stability(
        tier_inventory: Arc<RwLock<TierInventory>>,
        event_emitter: EventEmitter,
        time_provider: Arc<dyn TimeProvider>,
        rules: PolicyRules,
        stability_config: StabilityConfig,
    ) -> Self {
        let cooldown_tracker = CooldownTracker::new(
            stability_config.clone(),
            time_provider.clone(),
        );
        let stability_checker = StabilityChecker::new(stability_config.clone());
        Self {
            tier_inventory,
            hotness_provider: None,
            event_emitter,
            time_provider,
            last_evaluation: AtomicU64::new(0),
            rules,
            stability_config,
            cooldown_tracker,
            stability_checker,
        }
    }

    /// Set the hotness provider.
    pub fn set_hotness_provider(&mut self, provider: Arc<dyn HotnessProvider>) {
        self.hotness_provider = Some(provider);
    }

    /// Get the tier inventory for observation.
    pub fn tier_inventory(&self) -> &Arc<RwLock<TierInventory>> {
        &self.tier_inventory
    }

    /// Get the number of active cooldowns.
    pub fn active_cooldowns(&self) -> usize {
        self.cooldown_tracker.active_count()
    }

    /// Get the number of suppressed recommendations.
    pub fn suppressed_count(&self) -> usize {
        self.cooldown_tracker.suppressed_count()
    }

    /// Get the timestamp of the last evaluation.
    pub fn last_evaluation_time(&self) -> u64 {
        self.last_evaluation.load(Ordering::Relaxed)
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
    /// 5. Apply policy rules to produce raw recommendations.
    /// 6. Apply stability filters (confidence threshold).
    /// 7. Apply cooldown filters (per-region rate limiting).
    /// 8. Sort by confidence, limit count.
    /// 9. Emit `PolicyRecommendationGenerated` event.
    /// 10. Return recommendations (caller decides what to do with them).
    pub fn evaluate(&self) -> GhostResult<Vec<Recommendation>> {
        let start = std::time::Instant::now();

        // 1. Read tier inventory
        let inventory = self.tier_inventory.read();

        // 2. Build system state from tier inventory
        let state = self.build_system_state(&inventory);

        // 3. Apply policy rules (pure function — deterministic)
        let raw_recommendations = self.rules.evaluate(&state);

        // 4. Apply stability filters (confidence threshold)
        let stable = self.apply_stability(raw_recommendations);

        // 5. Apply cooldown filters (per-region rate limiting)
        let filtered = self.apply_cooldowns(stable);

        // 6. Sort by confidence (descending), limit count
        let limited = self.limit_recommendations(filtered);

        // 7. Update last evaluation timestamp
        let now = self.time_provider.timestamp_secs();
        self.last_evaluation.store(now, Ordering::Relaxed);

        // 8. Emit policy recommendation event
        let rec_strings: Vec<String> = limited.iter().map(|r| r.to_string()).collect();
        let pressure_level = state.pressure_level().to_string();
        let _ = self.event_emitter.try_emit(Event::PolicyRecommendationGenerated {
            sequence_id: 0,
            recommendations: rec_strings,
            pressure_level,
        });

        // 9. Record metrics
        let _elapsed = start.elapsed();
        record_metrics(&limited, start);

        Ok(limited)
    }

    /// Apply stability filters: remove recommendations below confidence threshold.
    fn apply_stability(&self, recommendations: Vec<Recommendation>) -> Vec<Recommendation> {
        recommendations
            .into_iter()
            .filter(|r| r.confidence() >= self.stability_config.min_confidence_threshold)
            .collect()
    }

    /// Apply cooldown filters: suppress recommendations for regions in cooldown.
    fn apply_cooldowns(&self, recommendations: Vec<Recommendation>) -> Vec<Recommendation> {
        recommendations
            .into_iter()
            .filter(|r| {
                let region_key = format!("{:?}", std::mem::discriminant(r));
                self.cooldown_tracker.can_recommend(&region_key)
            })
            .collect()
    }

    /// Sort recommendations by confidence (descending) and limit the total count.
    fn limit_recommendations(&self, mut recommendations: Vec<Recommendation>) -> Vec<Recommendation> {
        recommendations.sort_by(|a, b| {
            b.confidence()
                .partial_cmp(&a.confidence())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let max = self.stability_config.max_recommendations_per_cycle;
        if recommendations.len() > max {
            recommendations.truncate(max);
        }
        recommendations
    }

    /// Build a system state snapshot from the tier inventory.
    fn build_system_state(
        &self,
        inventory: &TierInventory,
    ) -> SystemState {
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
            hotness_summary: None,
            hotness_confidence: None,
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
            confidence: 1.0,
            factors: vec![],
        };
        assert!(rec.to_string().contains("NoAction"));
        assert!(rec.to_string().contains("system idle"));
    }

    #[test]
    fn test_recommendation_kind() {
        let rec = Recommendation::MoveToZram {
            chunk_id: ChunkId::from_data(b"test"),
            reason: "cold chunk".to_string(),
            confidence: 0.8,
            factors: vec![],
        };
        assert_eq!(rec.kind(), "move_to_zram");
    }

    #[test]
    fn test_recommendation_reason() {
        let rec = Recommendation::NoAction {
            reason: "system idle".to_string(),
            confidence: 1.0,
            factors: vec![],
        };
        assert_eq!(rec.reason(), "system idle");
    }

    #[test]
    fn test_recommendation_confidence() {
        let rec = Recommendation::NoAction {
            reason: "test".to_string(),
            confidence: 0.75,
            factors: vec![],
        };
        assert!((rec.confidence() - 0.75).abs() < f32::EPSILON);
    }

    #[test]
    fn test_recommendation_factors() {
        let rec = Recommendation::NoAction {
            reason: "test".to_string(),
            confidence: 1.0,
            factors: vec!["pressure".to_string(), "hotness".to_string()],
        };
        assert_eq!(rec.factors().len(), 2);
        assert_eq!(rec.factors()[0], "pressure");
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

    #[test]
    fn test_policy_runtime_with_stability_config() {
        let runtime = PolicyRuntime::with_stability(
            test_tier_inventory(),
            test_emitter(),
            test_time_provider(),
            PolicyRules::new(),
            StabilityConfig::default(),
        );
        assert!(runtime.is_cooldown_expired());
    }
}
