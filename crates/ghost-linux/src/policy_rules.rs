//! Policy rules engine for GhostPages.
//!
//! [`PolicyRules`] defines configurable thresholds that drive recommendation
//! decisions. [`SystemState`] captures a snapshot of current system pressure
//! and utilization. The [`PolicyRules::evaluate`] method is a **pure function**
//! — given the same [`SystemState`], it always produces the same
//! [`Vec<Recommendation>`].

use serde::{Deserialize, Serialize};

use ghost_core::hotness_confidence::HotnessConfidence;
use ghost_core::hotness_summary::HotnessSummary;
use ghost_core::state::PressureState;
use ghost_core::types::TierId;

use crate::policy::Recommendation;

// ─── System State ────────────────────────────────────────────────────────────────

/// A point-in-time snapshot of system pressure and utilization.
///
/// This is the input to [`PolicyRules::evaluate`]. All fields are
/// snapshot values — the rules engine never reads live system state.
#[derive(Debug, Clone)]
pub struct SystemState {
    /// Current memory pressure from PSI or similar source.
    pub dram_pressure: PressureState,

    /// DRAM utilization ratio (0.0 = empty, 1.0 = full).
    pub dram_utilization: f32,

    /// Swap utilization ratio (0.0 = empty, 1.0 = full).
    pub swap_utilization: f32,

    /// ZRAM utilization ratio (None if ZRAM is not available).
    pub zram_utilization: Option<f32>,

    /// I/O pressure from PSI or similar source.
    pub io_pressure: PressureState,

    /// Aggregated hotness summary from the hotness provider.
    ///
    /// When `None`, the rules engine falls back to pressure-only evaluation.
    pub hotness_summary: Option<HotnessSummary>,

    /// Confidence score for the hotness data.
    ///
    /// When `None`, hotness-based recommendations are skipped.
    pub hotness_confidence: Option<HotnessConfidence>,
}

impl SystemState {
    /// Classify the overall pressure level from the state.
    pub fn pressure_level(&self) -> PressureLevel {
        let max_pressure = self
            .dram_pressure
            .memory_pressure
            .max(self.dram_pressure.io_pressure)
            .max(self.io_pressure.memory_pressure)
            .max(self.io_pressure.io_pressure);

        if max_pressure >= 0.9 {
            PressureLevel::Critical
        } else if max_pressure >= 0.7 {
            PressureLevel::High
        } else if max_pressure >= 0.5 {
            PressureLevel::Medium
        } else {
            PressureLevel::Low
        }
    }
}

/// Pressure level classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PressureLevel {
    /// System is idle or lightly loaded.
    Low,

    /// System is under moderate pressure.
    Medium,

    /// System is under high pressure — action recommended.
    High,

    /// System is under critical pressure — immediate action recommended.
    Critical,
}

impl std::fmt::Display for PressureLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PressureLevel::Low => write!(f, "low"),
            PressureLevel::Medium => write!(f, "medium"),
            PressureLevel::High => write!(f, "high"),
            PressureLevel::Critical => write!(f, "critical"),
        }
    }
}

// ─── Policy Rules ───────────────────────────────────────────────────────────────

/// Configurable thresholds that drive policy recommendations.
///
/// All thresholds are fixed during evaluation — they are set at construction
/// time and never mutated during rule evaluation. This ensures deterministic
/// output for the same input state.
#[derive(Debug, Clone)]
pub struct PolicyRules {
    /// Pressure threshold above which DRAM is considered "high" (default: 0.7).
    pub dram_high_threshold: f32,

    /// Pressure threshold above which DRAM is considered "critical" (default: 0.9).
    pub dram_critical_threshold: f32,

    /// Swap utilization threshold above which swap is considered "high" (default: 0.8).
    pub swap_high_threshold: f32,

    /// ZRAM utilization threshold above which ZRAM is considered "high" (default: 0.8).
    pub zram_high_threshold: f32,

    /// Minimum seconds between successive recommendation bursts (default: 60).
    pub cooldown_seconds: u64,

    // ── Hotness-aware fields ───────────────────────────────────────────────────

    /// Weight of hotness data in recommendation decisions (0.0–1.0, default: 0.3).
    ///
    /// A value of 0.0 means hotness is ignored; 1.0 means hotness dominates.
    pub hotness_weight: f32,

    /// Weight of pressure data in recommendation decisions (0.0–1.0, default: 0.7).
    ///
    /// Pressure is always the primary signal; hotness is advisory.
    pub pressure_weight: f32,

    /// Minimum confidence score required to act on hotness data (default: 0.3).
    ///
    /// Hotness recommendations are only emitted when the confidence score
    /// meets or exceeds this threshold.
    pub min_confidence: f32,

    /// Preferred tier for hot regions (default: TierId::Ram / DRAM).
    pub hot_region_preference: TierId,

    /// Preferred tier for cold regions (default: TierId::Disk).
    pub cold_region_preference: TierId,

    /// Hot region percentage threshold (default: 25.0).
    ///
    /// When more than this percentage of regions are hot, the engine
    /// may recommend PromoteToDram even under moderate pressure.
    pub hot_region_pct_threshold: f32,

    /// Frozen region percentage threshold (default: 50.0).
    ///
    /// When more than this percentage of regions are frozen, the engine
    /// recommends moving cold data to disk.
    pub frozen_region_pct_threshold: f32,

    /// Warm region percentage threshold (default: 40.0).
    ///
    /// When more than this percentage of regions are warm and ZRAM is
    /// available, the engine may recommend MoveToZram.
    pub warm_region_pct_threshold: f32,
}

impl Default for PolicyRules {
    fn default() -> Self {
        Self {
            dram_high_threshold: 0.7,
            dram_critical_threshold: 0.9,
            swap_high_threshold: 0.8,
            zram_high_threshold: 0.8,
            cooldown_seconds: 60,
            hotness_weight: 0.3,
            pressure_weight: 0.7,
            min_confidence: 0.3,
            hot_region_preference: TierId::Ram,
            cold_region_preference: TierId::Disk,
            hot_region_pct_threshold: 25.0,
            frozen_region_pct_threshold: 50.0,
            warm_region_pct_threshold: 40.0,
        }
    }
}

impl PolicyRules {
    /// Create policy rules with default thresholds.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create policy rules with custom thresholds.
    pub fn with_thresholds(
        dram_high: f32,
        dram_critical: f32,
        swap_high: f32,
        zram_high: f32,
        cooldown_seconds: u64,
    ) -> Self {
        Self {
            dram_high_threshold: dram_high,
            dram_critical_threshold: dram_critical,
            swap_high_threshold: swap_high,
            zram_high_threshold: zram_high,
            cooldown_seconds,
            ..Self::default()
        }
    }

    /// Create policy rules with hotness-aware configuration.
    pub fn with_hotness(
        hotness_weight: f32,
        pressure_weight: f32,
        min_confidence: f32,
        hot_region_preference: TierId,
        cold_region_preference: TierId,
    ) -> Self {
        Self {
            hotness_weight: hotness_weight.clamp(0.0, 1.0),
            pressure_weight: pressure_weight.clamp(0.0, 1.0),
            min_confidence: min_confidence.clamp(0.0, 1.0),
            hot_region_preference,
            cold_region_preference,
            ..Self::default()
        }
    }

    // ── Evaluation ─────────────────────────────────────────────────────────────

    /// Evaluate the given system state and produce recommendations.
    ///
    /// This is a **pure function** — no I/O, no mutation, no side effects.
    /// The same `SystemState` always produces the same recommendations.
    ///
    /// # Rules
    ///
    /// 1. **Critical DRAM pressure** → `EvictCold` + `MoveToZram` (if ZRAM available)
    ///    or `MoveToDiskSwap` (if no ZRAM).
    /// 2. **High DRAM pressure + ZRAM available** → `MoveToZram`.
    /// 3. **High DRAM pressure + no ZRAM + swap low** → `MoveToDiskSwap`.
    /// 4. **High DRAM pressure + swap also high** → `EvictCold` from DRAM.
    /// 5. **Low DRAM pressure + hot chunks** → `PromoteToDram`.
    /// 6. **Low pressure everywhere** → `NoAction`.
    /// 7. **Hotness-aware rules** (when confidence ≥ min_confidence):
    ///    - Hot regions > threshold + DRAM pressure → `PromoteToDram` for hottest.
    ///    - Frozen regions > threshold → `MoveToDiskSwap` for coldest.
    ///    - Warm regions > threshold + ZRAM available → `MoveToZram`.
    pub fn evaluate(&self, state: &SystemState) -> Vec<Recommendation> {
        // 1. Pressure-based recommendations (existing)
        let pressure_recs = self.evaluate_pressure(state);

        // 2. Hotness-based recommendations (only if confidence is sufficient)
        let hotness_recs = if let (Some(summary), Some(confidence)) =
            (&state.hotness_summary, &state.hotness_confidence)
        {
            if confidence.score >= self.min_confidence {
                self.evaluate_hotness(state, summary, confidence)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // 3. Merge and deduplicate
        self.merge_recommendations(pressure_recs, hotness_recs)
    }

    // ── Pressure-based evaluation ──────────────────────────────────────────────

    /// Evaluate pressure-based rules (original logic).
    fn evaluate_pressure(&self, state: &SystemState) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();
        let pressure_level = state.pressure_level();

        match pressure_level {
            PressureLevel::Critical => {
                // Critical: evict cold chunks from DRAM, move to ZRAM or swap
                recommendations.push(Recommendation::EvictCold {
                    tier: TierId::Ram,
                    count: self.eviction_count(state),
                    confidence: 1.0,
                    factors: vec![format!(
                        "critical DRAM pressure ({:.0}%)",
                        state.dram_pressure.memory_pressure * 100.0
                    )],
                });

                if state.zram_utilization.is_some() {
                    recommendations.push(Recommendation::MoveToZram {
                        chunk_id: self.eviction_chunk_id(state),
                        reason: format!(
                            "critical DRAM pressure ({:.0}%) — move cold chunks to ZRAM",
                            state.dram_pressure.memory_pressure * 100.0
                        ),
                        confidence: 1.0,
                        factors: vec!["critical_pressure".to_string(), "zram_available".to_string()],
                    });
                } else if state.swap_utilization < self.swap_high_threshold {
                    recommendations.push(Recommendation::MoveToDiskSwap {
                        chunk_id: self.eviction_chunk_id(state),
                        reason: format!(
                            "critical DRAM pressure ({:.0}%) — move cold chunks to disk swap",
                            state.dram_pressure.memory_pressure * 100.0
                        ),
                        confidence: 1.0,
                        factors: vec!["critical_pressure".to_string(), "swap_available".to_string()],
                    });
                }
            }

            PressureLevel::High => {
                // High pressure: move cold chunks to next tier
                if state.zram_utilization.is_some()
                    && state
                        .zram_utilization
                        .map_or(false, |u| u < self.zram_high_threshold)
                {
                    recommendations.push(Recommendation::MoveToZram {
                        chunk_id: self.eviction_chunk_id(state),
                        reason: format!(
                            "high DRAM pressure ({:.0}%) — ZRAM available ({:.0}% full)",
                            state.dram_pressure.memory_pressure * 100.0,
                            state.zram_utilization.unwrap_or(0.0) * 100.0
                        ),
                        confidence: 1.0,
                        factors: vec!["high_pressure".to_string(), "zram_available".to_string()],
                    });
                } else if state.swap_utilization < self.swap_high_threshold {
                    recommendations.push(Recommendation::MoveToDiskSwap {
                        chunk_id: self.eviction_chunk_id(state),
                        reason: format!(
                            "high DRAM pressure ({:.0}%) — swap available ({:.0}% full)",
                            state.dram_pressure.memory_pressure * 100.0,
                            state.swap_utilization * 100.0
                        ),
                        confidence: 1.0,
                        factors: vec!["high_pressure".to_string(), "swap_available".to_string()],
                    });
                } else {
                    // Both ZRAM and swap are full — evict cold chunks
                    recommendations.push(Recommendation::EvictCold {
                        tier: TierId::Ram,
                        count: self.eviction_count(state),
                        confidence: 1.0,
                        factors: vec![
                            "high_pressure".to_string(),
                            "zram_full".to_string(),
                            "swap_full".to_string(),
                        ],
                    });
                }
            }

            PressureLevel::Medium => {
                // Medium pressure: demote cold chunks from warm tiers
                if state.zram_utilization.is_some() {
                    recommendations.push(Recommendation::DemoteHot {
                        tier: TierId::GpuVram,
                        target: TierId::Disk,
                        confidence: 0.8,
                        factors: vec!["medium_pressure".to_string()],
                    });
                }
            }

            PressureLevel::Low => {
                // Low pressure: no action needed, or promote hot chunks
                if state.dram_pressure.memory_pressure < 0.3 {
                    recommendations.push(Recommendation::NoAction {
                        reason: format!(
                            "DRAM pressure low ({:.0}%) — no action needed",
                            state.dram_pressure.memory_pressure * 100.0
                        ),
                        confidence: 1.0,
                        factors: vec!["low_pressure".to_string()],
                    });
                }
            }
        }

        recommendations
    }

    // ── Hotness-based evaluation ───────────────────────────────────────────────

    /// Evaluate hotness-based rules.
    ///
    /// Hotness recommendations are weighted by confidence score.
    /// They are advisory — pressure takes precedence when urgent.
    fn evaluate_hotness(
        &self,
        state: &SystemState,
        summary: &HotnessSummary,
        confidence: &HotnessConfidence,
    ) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();
        let hotness_confidence = confidence.score;

        // Rule 1: Hot regions above threshold + DRAM has pressure → PromoteToDram
        if summary.hot_percentage > self.hot_region_pct_threshold
            && state.dram_pressure.memory_pressure >= self.dram_high_threshold
        {
            let weighted_confidence = hotness_confidence * self.hotness_weight;
            recommendations.push(Recommendation::PromoteToDram {
                chunk_id: self.hotness_chunk_id(summary, state),
                reason: format!(
                    "hot regions {:.0}% above threshold {:.0}% — promote hottest to DRAM",
                    summary.hot_percentage, self.hot_region_pct_threshold
                ),
                confidence: weighted_confidence,
                factors: vec![
                    format!("hot_regions={:.0}%", summary.hot_percentage),
                    format!("hotness_confidence={:.2}", hotness_confidence),
                    format!("dram_pressure={:.0}%", state.dram_pressure.memory_pressure * 100.0),
                ],
            });
        }

        // Rule 1b: Hot regions above threshold + LOW DRAM pressure → PromoteToDram
        // (Low pressure means idle system — good time to optimize hot data placement)
        if summary.hot_percentage > self.hot_region_pct_threshold
            && state.dram_pressure.memory_pressure < self.dram_high_threshold
        {
            let weighted_confidence = hotness_confidence * self.hotness_weight;
            recommendations.push(Recommendation::PromoteToDram {
                chunk_id: self.hotness_chunk_id(summary, state),
                reason: format!(
                    "hot regions {:.0}% above threshold {:.0}% — optimize placement in idle system",
                    summary.hot_percentage, self.hot_region_pct_threshold
                ),
                confidence: weighted_confidence,
                factors: vec![
                    format!("hot_regions={:.0}%", summary.hot_percentage),
                    format!("hotness_confidence={:.2}", hotness_confidence),
                    "low_pressure_optimization".to_string(),
                ],
            });
        }

        // Rule 2: Frozen regions above threshold → MoveToDiskSwap
        if summary.frozen_percentage > self.frozen_region_pct_threshold {
            let weighted_confidence = hotness_confidence * self.hotness_weight;
            recommendations.push(Recommendation::MoveToDiskSwap {
                chunk_id: self.coldness_chunk_id(summary, state),
                reason: format!(
                    "frozen regions {:.0}% above threshold {:.0}% — move coldest to disk",
                    summary.frozen_percentage, self.frozen_region_pct_threshold
                ),
                confidence: weighted_confidence,
                factors: vec![
                    format!("frozen_regions={:.0}%", summary.frozen_percentage),
                    format!("hotness_confidence={:.2}", hotness_confidence),
                ],
            });
        }

        // Rule 3: Warm regions above threshold + ZRAM available → MoveToZram
        if summary.warm_percentage > self.warm_region_pct_threshold
            && state.zram_utilization.is_some()
            && state
                .zram_utilization
                .map_or(false, |u| u < self.zram_high_threshold)
        {
            let weighted_confidence = hotness_confidence * self.hotness_weight;
            recommendations.push(Recommendation::MoveToZram {
                chunk_id: self.warmth_chunk_id(summary, state),
                reason: format!(
                    "warm regions {:.0}% above threshold {:.0}% — move warm to ZRAM",
                    summary.warm_percentage, self.warm_region_pct_threshold
                ),
                confidence: weighted_confidence,
                factors: vec![
                    format!("warm_regions={:.0}%", summary.warm_percentage),
                    format!("hotness_confidence={:.2}", hotness_confidence),
                    "zram_available".to_string(),
                ],
            });
        }

        recommendations
    }

    // ── Recommendation Merging ─────────────────────────────────────────────────

    /// Merge pressure and hotness recommendations.
    ///
    /// When pressure and hotness recommendations conflict:
    /// - Weight by confidence
    /// - Prefer the higher-confidence recommendation
    /// - If confidence is similar, prefer pressure-based (more urgent)
    fn merge_recommendations(
        &self,
        pressure_recs: Vec<Recommendation>,
        hotness_recs: Vec<Recommendation>,
    ) -> Vec<Recommendation> {
        let mut merged = Vec::new();

        // Always include pressure-based recommendations first
        // (they are the primary signal)
        merged.extend(pressure_recs);

        for hotness_rec in hotness_recs {
            // Check if this hotness recommendation conflicts with any pressure rec
            let conflict = merged.iter().any(|pressure_rec| {
                Self::recommendations_conflict(&hotness_rec, pressure_rec)
            });

            if conflict {
                // Check if the conflicting pressure recommendation is NoAction
                // If so, hotness wins (actionable > do-nothing)
                let has_noaction_conflict = merged.iter().any(|p| {
                    matches!(p, Recommendation::NoAction { .. })
                        && Self::recommendations_conflict(&hotness_rec, p)
                });

                if has_noaction_conflict {
                    // Hotness wins over NoAction — replace NoAction with hotness recommendation
                    merged.retain(|p| !matches!(p, Recommendation::NoAction { .. }));
                    merged.push(hotness_rec);
                } else {
                    // Keep the higher-confidence recommendation
                    // Pressure wins on tie (it's more urgent)
                    let pressure_confidence = merged
                        .iter()
                        .filter(|p| Self::recommendations_conflict(&hotness_rec, p))
                        .map(|p| p.confidence())
                        .fold(0.0f32, f32::max);

                    let hotness_confidence = hotness_rec.confidence();

                    // Hotness needs to exceed pressure confidence by a margin to override
                    // This ensures pressure takes precedence when urgent
                    if hotness_confidence > pressure_confidence + self.hotness_weight {
                        // Replace conflicting pressure recs with hotness rec
                        merged.retain(|p| !Self::recommendations_conflict(&hotness_rec, p));
                        merged.push(hotness_rec);
                    }
                    // Otherwise, keep the pressure recommendation (it wins)
                }
            } else {
                // No conflict — add the hotness recommendation
                merged.push(hotness_rec);
            }
        }

        merged
    }

    /// Check if two recommendations conflict (same action type on same target).
    fn recommendations_conflict(a: &Recommendation, b: &Recommendation) -> bool {
        match (a, b) {
            // PromoteToDram conflicts with MoveToZram/MoveToDiskSwap
            (Recommendation::PromoteToDram { .. }, Recommendation::MoveToZram { .. }) => true,
            (Recommendation::PromoteToDram { .. }, Recommendation::MoveToDiskSwap { .. }) => true,
            (Recommendation::MoveToZram { .. }, Recommendation::PromoteToDram { .. }) => true,
            (Recommendation::MoveToDiskSwap { .. }, Recommendation::PromoteToDram { .. }) => true,
            // MoveToDiskSwap conflicts with MoveToZram (different destinations)
            (Recommendation::MoveToDiskSwap { .. }, Recommendation::MoveToZram { .. }) => true,
            (Recommendation::MoveToZram { .. }, Recommendation::MoveToDiskSwap { .. }) => true,
            // EvictCold conflicts with PromoteToDram
            (Recommendation::EvictCold { .. }, Recommendation::PromoteToDram { .. }) => true,
            (Recommendation::PromoteToDram { .. }, Recommendation::EvictCold { .. }) => true,
            // Same kind with same chunk_id
            (
                Recommendation::PromoteToDram { chunk_id: a, .. },
                Recommendation::PromoteToDram { chunk_id: b, .. },
            ) => a == b,
            (
                Recommendation::MoveToZram { chunk_id: a, .. },
                Recommendation::MoveToZram { chunk_id: b, .. },
            ) => a == b,
            (
                Recommendation::MoveToDiskSwap { chunk_id: a, .. },
                Recommendation::MoveToDiskSwap { chunk_id: b, .. },
            ) => a == b,
            _ => false,
        }
    }

    // ── Helper methods ─────────────────────────────────────────────────────────

    /// Determine how many chunks to evict based on pressure level.
    fn eviction_count(&self, state: &SystemState) -> usize {
        let pressure = state.dram_pressure.memory_pressure;
        if pressure >= 0.95 {
            16
        } else if pressure >= 0.9 {
            8
        } else if pressure >= 0.8 {
            4
        } else {
            2
        }
    }

    /// Deterministically derive a chunk ID from system state for eviction.
    fn eviction_chunk_id(&self, state: &SystemState) -> ghost_core::types::ChunkId {
        use blake3::Hasher;

        let mut hasher = Hasher::new();
        hasher.update(&state.dram_pressure.memory_pressure.to_le_bytes());
        hasher.update(&state.dram_pressure.io_pressure.to_le_bytes());
        hasher.update(&state.dram_utilization.to_le_bytes());
        hasher.update(&state.swap_utilization.to_le_bytes());
        if let Some(zram) = state.zram_utilization {
            hasher.update(&zram.to_le_bytes());
        }

        let hash = hasher.finalize();
        ghost_core::types::ChunkId(*hash.as_bytes())
    }

    /// Deterministically derive a chunk ID for hotness-based promotion.
    fn hotness_chunk_id(
        &self,
        _summary: &HotnessSummary,
        state: &SystemState,
    ) -> ghost_core::types::ChunkId {
        use blake3::Hasher;

        let mut hasher = Hasher::new();
        hasher.update(b"hotness_promote");
        hasher.update(&state.dram_pressure.memory_pressure.to_le_bytes());
        hasher.update(&state.dram_utilization.to_le_bytes());

        let hash = hasher.finalize();
        ghost_core::types::ChunkId(*hash.as_bytes())
    }

    /// Deterministically derive a chunk ID for coldness-based disk move.
    fn coldness_chunk_id(
        &self,
        _summary: &HotnessSummary,
        state: &SystemState,
    ) -> ghost_core::types::ChunkId {
        use blake3::Hasher;

        let mut hasher = Hasher::new();
        hasher.update(b"coldness_evict");
        hasher.update(&state.dram_pressure.memory_pressure.to_le_bytes());
        hasher.update(&state.swap_utilization.to_le_bytes());

        let hash = hasher.finalize();
        ghost_core::types::ChunkId(*hash.as_bytes())
    }

    /// Deterministically derive a chunk ID for warmth-based ZRAM move.
    fn warmth_chunk_id(
        &self,
        _summary: &HotnessSummary,
        state: &SystemState,
    ) -> ghost_core::types::ChunkId {
        use blake3::Hasher;

        let mut hasher = Hasher::new();
        hasher.update(b"warmth_zram");
        hasher.update(&state.dram_pressure.memory_pressure.to_le_bytes());
        if let Some(zram) = state.zram_utilization {
            hasher.update(&zram.to_le_bytes());
        }

        let hash = hasher.finalize();
        ghost_core::types::ChunkId(*hash.as_bytes())
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::ChunkId;

    fn idle_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState::new(),
            dram_utilization: 0.3,
            swap_utilization: 0.1,
            zram_utilization: Some(0.2),
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn high_dram_pressure_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.8,
                ..Default::default()
            },
            dram_utilization: 0.85,
            swap_utilization: 0.3,
            zram_utilization: Some(0.4),
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn critical_dram_pressure_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.95,
                ..Default::default()
            },
            dram_utilization: 0.97,
            swap_utilization: 0.5,
            zram_utilization: Some(0.6),
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    #[test]
    fn test_pressure_level_low() {
        let state = idle_state();
        assert_eq!(state.pressure_level(), PressureLevel::Low);
    }

    #[test]
    fn test_pressure_level_critical() {
        let state = critical_dram_pressure_state();
        assert_eq!(state.pressure_level(), PressureLevel::Critical);
    }

    #[test]
    fn test_evaluate_idle_system() {
        let rules = PolicyRules::new();
        let state = idle_state();
        let recs = rules.evaluate(&state);

        // Idle system should produce NoAction
        assert!(
            recs.iter()
                .any(|r| matches!(r, Recommendation::NoAction { .. })),
            "expected NoAction for idle system, got {:?}",
            recs
        );
    }

    #[test]
    fn test_evaluate_high_dram_with_zram() {
        let rules = PolicyRules::new();
        let state = high_dram_pressure_state();
        let recs = rules.evaluate(&state);

        // High pressure + ZRAM available → MoveToZram
        assert!(
            recs.iter()
                .any(|r| matches!(r, Recommendation::MoveToZram { .. })),
            "expected MoveToZram for high pressure with ZRAM, got {:?}",
            recs
        );
    }

    #[test]
    fn test_evaluate_critical_dram() {
        let rules = PolicyRules::new();
        let state = critical_dram_pressure_state();
        let recs = rules.evaluate(&state);

        // Critical pressure → EvictCold + MoveToZram
        assert!(
            recs.iter()
                .any(|r| matches!(r, Recommendation::EvictCold { .. })),
            "expected EvictCold for critical pressure, got {:?}",
            recs
        );
        assert!(
            recs.iter()
                .any(|r| matches!(r, Recommendation::MoveToZram { .. })),
            "expected MoveToZram for critical pressure with ZRAM, got {:?}",
            recs
        );
    }

    #[test]
    fn test_evaluate_deterministic() {
        let rules = PolicyRules::new();
        let state = high_dram_pressure_state();

        let recs1 = rules.evaluate(&state);
        let recs2 = rules.evaluate(&state);

        // Same state must produce same number and types of recommendations
        assert_eq!(recs1.len(), recs2.len());
        for (r1, r2) in recs1.iter().zip(recs2.iter()) {
            assert!(
                std::mem::discriminant(r1) == std::mem::discriminant(r2),
                "recommendations differ: {:?} vs {:?}",
                r1,
                r2
            );
        }
    }

    #[test]
    fn test_evaluate_high_pressure_no_zram() {
        let rules = PolicyRules::new();
        let state = SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.8,
                ..Default::default()
            },
            dram_utilization: 0.85,
            swap_utilization: 0.3,
            zram_utilization: None,
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        };

        let recs = rules.evaluate(&state);

        // High pressure + no ZRAM + swap low → MoveToDiskSwap
        assert!(
            recs.iter()
                .any(|r| matches!(r, Recommendation::MoveToDiskSwap { .. })),
            "expected MoveToDiskSwap for high pressure without ZRAM, got {:?}",
            recs
        );
    }

    #[test]
    fn test_evaluate_high_pressure_zram_full() {
        let rules = PolicyRules::new();
        let state = SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.8,
                ..Default::default()
            },
            dram_utilization: 0.85,
            swap_utilization: 0.3,
            zram_utilization: Some(0.9), // ZRAM is full
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        };

        let recs = rules.evaluate(&state);

        // High pressure + ZRAM full → should NOT recommend MoveToZram
        assert!(
            !recs
                .iter()
                .any(|r| matches!(r, Recommendation::MoveToZram { .. })),
            "should not recommend MoveToZram when ZRAM is full, got {:?}",
            recs
        );
    }

    #[test]
    fn test_eviction_count_scales_with_pressure() {
        let rules = PolicyRules::new();

        let low = SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.5,
                ..Default::default()
            },
            dram_utilization: 0.5,
            swap_utilization: 0.1,
            zram_utilization: None,
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        };

        let critical = SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.97,
                ..Default::default()
            },
            dram_utilization: 0.97,
            swap_utilization: 0.1,
            zram_utilization: None,
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        };

        assert!(rules.eviction_count(&critical) > rules.eviction_count(&low));
    }

    #[test]
    fn test_eviction_chunk_id_deterministic() {
        let rules = PolicyRules::new();
        let state = high_dram_pressure_state();

        let id1 = rules.eviction_chunk_id(&state);
        let id2 = rules.eviction_chunk_id(&state);

        assert_eq!(id1, id2, "eviction_chunk_id must be deterministic");
    }

    #[test]
    fn test_custom_thresholds() {
        let rules = PolicyRules::with_thresholds(0.5, 0.8, 0.7, 0.7, 30);
        assert_eq!(rules.dram_high_threshold, 0.5);
        assert_eq!(rules.dram_critical_threshold, 0.8);
        assert_eq!(rules.swap_high_threshold, 0.7);
        assert_eq!(rules.zram_high_threshold, 0.7);
        assert_eq!(rules.cooldown_seconds, 30);
    }

    #[test]
    fn test_pressure_level_display() {
        assert_eq!(format!("{}", PressureLevel::Low), "low");
        assert_eq!(format!("{}", PressureLevel::Medium), "medium");
        assert_eq!(format!("{}", PressureLevel::High), "high");
        assert_eq!(format!("{}", PressureLevel::Critical), "critical");
    }

    #[test]
    fn test_hotness_aware_default_rules() {
        let rules = PolicyRules::new();
        assert_eq!(rules.hotness_weight, 0.3);
        assert_eq!(rules.pressure_weight, 0.7);
        assert_eq!(rules.min_confidence, 0.3);
        assert_eq!(rules.hot_region_preference, TierId::Ram);
        assert_eq!(rules.cold_region_preference, TierId::Disk);
    }

    #[test]
    fn test_with_hotness_config() {
        let rules = PolicyRules::with_hotness(
            0.5,
            0.5,
            0.4,
            TierId::Ram,
            TierId::Disk,
        );
        assert_eq!(rules.hotness_weight, 0.5);
        assert_eq!(rules.pressure_weight, 0.5);
        assert_eq!(rules.min_confidence, 0.4);
    }

    #[test]
    fn test_with_hotness_clamps_values() {
        let rules = PolicyRules::with_hotness(1.5, -0.5, 2.0, TierId::Ram, TierId::Disk);
        assert_eq!(rules.hotness_weight, 1.0);
        assert_eq!(rules.pressure_weight, 0.0);
        assert_eq!(rules.min_confidence, 1.0);
    }

    #[test]
    fn test_recommendations_conflict() {
        let promote = Recommendation::PromoteToDram {
            chunk_id: ChunkId::from_data(b"test"),
            reason: "hot".to_string(),
            confidence: 0.8,
            factors: vec![],
        };
        let move_zram = Recommendation::MoveToZram {
            chunk_id: ChunkId::from_data(b"test"),
            reason: "cold".to_string(),
            confidence: 0.8,
            factors: vec![],
        };
        let move_disk = Recommendation::MoveToDiskSwap {
            chunk_id: ChunkId::from_data(b"test"),
            reason: "cold".to_string(),
            confidence: 0.8,
            factors: vec![],
        };
        let no_action = Recommendation::NoAction {
            reason: "idle".to_string(),
            confidence: 1.0,
            factors: vec![],
        };

        // Action recommendations conflict with each other
        assert!(PolicyRules::recommendations_conflict(&promote, &move_zram));
        assert!(PolicyRules::recommendations_conflict(&promote, &move_disk));
        assert!(PolicyRules::recommendations_conflict(&move_zram, &move_disk));

        // NoAction does NOT conflict with anything
        // (it means "pressure has nothing to do" — shouldn't block hotness actions)
        assert!(!PolicyRules::recommendations_conflict(&promote, &no_action));
        assert!(!PolicyRules::recommendations_conflict(&no_action, &move_zram));
        assert!(!PolicyRules::recommendations_conflict(&move_zram, &no_action));
        assert!(!PolicyRules::recommendations_conflict(&no_action, &no_action));
    }
}
