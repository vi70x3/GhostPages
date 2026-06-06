//! Policy rules engine for GhostPages.
//!
//! [`PolicyRules`] defines configurable thresholds that drive recommendation
//! decisions. [`SystemState`] captures a snapshot of current system pressure
//! and utilization. The [`PolicyRules::evaluate`] method is a **pure function**
//! — given the same [`SystemState`], it always produces the same
//! [`Vec<Recommendation>`].

use serde::{Deserialize, Serialize};

use ghost_core::state::PressureState;
use ghost_core::types::TierId;

use crate::policy::Recommendation;

// ─── System State ────────────────────────────────────────────────────────────────

/// A point-in-time snapshot of system pressure and utilization.
///
/// This is the input to [`PolicyRules::evaluate`]. All fields are
/// snapshot values — the rules engine never reads live system state.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl Default for PolicyRules {
    fn default() -> Self {
        Self {
            dram_high_threshold: 0.7,
            dram_critical_threshold: 0.9,
            swap_high_threshold: 0.8,
            zram_high_threshold: 0.8,
            cooldown_seconds: 60,
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
        }
    }

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
    pub fn evaluate(&self, state: &SystemState) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();
        let pressure_level = state.pressure_level();

        match pressure_level {
            PressureLevel::Critical => {
                // Critical: evict cold chunks from DRAM, move to ZRAM or swap
                recommendations.push(Recommendation::EvictCold {
                    tier: TierId::Ram,
                    count: self.eviction_count(state),
                });

                if state.zram_utilization.is_some() {
                    recommendations.push(Recommendation::MoveToZram {
                        chunk_id: self.eviction_chunk_id(state),
                        reason: format!(
                            "critical DRAM pressure ({:.0}%) — move cold chunks to ZRAM",
                            state.dram_pressure.memory_pressure * 100.0
                        ),
                    });
                } else if state.swap_utilization < self.swap_high_threshold {
                    recommendations.push(Recommendation::MoveToDiskSwap {
                        chunk_id: self.eviction_chunk_id(state),
                        reason: format!(
                            "critical DRAM pressure ({:.0}%) — move cold chunks to disk swap",
                            state.dram_pressure.memory_pressure * 100.0
                        ),
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
                    });
                } else if state.swap_utilization < self.swap_high_threshold {
                    recommendations.push(Recommendation::MoveToDiskSwap {
                        chunk_id: self.eviction_chunk_id(state),
                        reason: format!(
                            "high DRAM pressure ({:.0}%) — swap available ({:.0}% full)",
                            state.dram_pressure.memory_pressure * 100.0,
                            state.swap_utilization * 100.0
                        ),
                    });
                } else {
                    // Both ZRAM and swap are full — evict cold chunks
                    recommendations.push(Recommendation::EvictCold {
                        tier: TierId::Ram,
                        count: self.eviction_count(state),
                    });
                }
            }

            PressureLevel::Medium => {
                // Medium pressure: demote cold chunks from warm tiers
                if state.zram_utilization.is_some() {
                    recommendations.push(Recommendation::DemoteHot {
                        tier: TierId::GpuVram,
                        target: TierId::Disk,
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
                    });
                }
            }
        }

        recommendations
    }

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
    ///
    /// This uses a hash of the state to pick a representative chunk — the
    /// actual chunk selection would come from the hotness provider in a real
    /// system. This is deterministic: same state → same chunk ID.
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
}
