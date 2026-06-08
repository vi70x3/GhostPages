//! Recommendation Explanation for GhostPages.
//!
//! Provides human-readable explanations for why a recommendation was made,
//! including contributing factors, confidence explanations, and alternatives.
//!
//! All functions are **pure** — no I/O, no mutation, no side effects.
//! Same inputs always produce same outputs. Deterministic by design.

use ghost_linux::policy::Recommendation;
use ghost_linux::policy_rules::SystemState;

// ─── Reason Category ──────────────────────────────────────────────────────────

/// Primary reason category for a recommendation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReasonCategory {
    /// Recommendation driven by memory pressure response.
    PressureResponse,
    /// Recommendation driven by hotness data (access frequency).
    HotnessDriven,
    /// Recommendation to evict cold data.
    ColdEviction,
    /// Recommendation to optimize tier placement.
    TierOptimization,
    /// Recommendation to preserve system stability.
    StabilityPreservation,
    /// No action is needed.
    NoActionNeeded,
}

impl std::fmt::Display for ReasonCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReasonCategory::PressureResponse => write!(f, "Pressure Response"),
            ReasonCategory::HotnessDriven => write!(f, "Hotness Driven"),
            ReasonCategory::ColdEviction => write!(f, "Cold Eviction"),
            ReasonCategory::TierOptimization => write!(f, "Tier Optimization"),
            ReasonCategory::StabilityPreservation => write!(f, "Stability Preservation"),
            ReasonCategory::NoActionNeeded => write!(f, "No Action Needed"),
        }
    }
}

// ─── Explanation Factor ───────────────────────────────────────────────────────

/// A single contributing factor to a recommendation.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplanationFactor {
    /// Factor name (e.g., "DRAM pressure").
    pub name: String,
    /// Current value of the factor.
    pub value: f32,
    /// The threshold that triggered this factor.
    pub threshold: f32,
    /// Human-readable description.
    pub description: String,
}

// ─── Confidence Level ─────────────────────────────────────────────────────────

/// Classification of confidence into discrete levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfidenceLevel {
    /// High confidence (>= 0.8).
    High,
    /// Medium confidence (0.5–0.8).
    Medium,
    /// Low confidence (< 0.5).
    Low,
}

impl std::fmt::Display for ConfidenceLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfidenceLevel::High => write!(f, "high"),
            ConfidenceLevel::Medium => write!(f, "medium"),
            ConfidenceLevel::Low => write!(f, "low"),
        }
    }
}

/// Explanation of the confidence score.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfidenceExplanation {
    /// The confidence score (0.0–1.0).
    pub confidence: f32,
    /// Discrete confidence level.
    pub level: ConfidenceLevel,
    /// Human-readable reasoning for the confidence.
    pub reasoning: String,
}

// ─── Alternative Action ───────────────────────────────────────────────────────

/// An alternative action that was considered but not chosen.
#[derive(Debug, Clone, PartialEq)]
pub struct AlternativeAction {
    /// Description of the alternative action.
    pub action: String,
    /// Why this alternative was considered but not chosen.
    pub reason: String,
    /// What confidence this alternative would have had.
    pub confidence: f32,
}

// ─── Recommendation Explanation ───────────────────────────────────────────────

/// Human-readable explanation for why a recommendation was made.
#[derive(Debug, Clone, PartialEq)]
pub struct RecommendationExplanation {
    /// The recommendation being explained.
    pub recommendation: Recommendation,
    /// Primary reason category.
    pub reason_category: ReasonCategory,
    /// Detailed reason text.
    pub reason_detail: String,
    /// Contributing factors.
    pub contributing_factors: Vec<ExplanationFactor>,
    /// Confidence explanation.
    pub confidence_explanation: ConfidenceExplanation,
    /// Alternative actions considered.
    pub alternatives: Vec<AlternativeAction>,
}

// ─── Explanation Functions ────────────────────────────────────────────────────

/// Generate a human-readable explanation for a single recommendation.
///
/// The explanation references actual state values (pressure levels, utilization
/// ratios, hotness data) to justify the recommendation.
pub fn explain_recommendation(
    recommendation: &Recommendation,
    state: &SystemState,
) -> RecommendationExplanation {
    match recommendation {
        Recommendation::PromoteToDram {
            chunk_id,
            reason,
            confidence,
            factors,
        } => explain_promote_to_dram(chunk_id, reason, confidence, factors, state),
        Recommendation::MoveToZram {
            chunk_id,
            reason,
            confidence,
            factors,
        } => explain_move_to_zram(chunk_id, reason, confidence, factors, state),
        Recommendation::MoveToDiskSwap {
            chunk_id,
            reason,
            confidence,
            factors,
        } => explain_move_to_disk_swap(chunk_id, reason, confidence, factors, state),
        Recommendation::EvictCold {
            tier,
            count,
            confidence,
            factors,
        } => explain_evict_cold(*tier, *count, confidence, factors, state),
        Recommendation::DemoteHot {
            tier,
            target,
            confidence,
            factors,
        } => explain_demote_hot(*tier, *target, confidence, factors, state),
        Recommendation::NoAction {
            reason,
            confidence,
            factors,
        } => explain_no_action(reason, confidence, factors, state),
    }
}

/// Generate explanations for a batch of recommendations.
pub fn explain_recommendations(
    recommendations: &[Recommendation],
    state: &SystemState,
) -> Vec<RecommendationExplanation> {
    recommendations
        .iter()
        .map(|rec| explain_recommendation(rec, state))
        .collect()
}

/// Format an explanation into a human-readable string.
pub fn format_explanation(explanation: &RecommendationExplanation) -> String {
    let mut output = String::new();

    // Header
    output.push_str(&format!(
        "=== Recommendation: {} ===\n",
        explanation.recommendation.kind()
    ));
    output.push_str(&format!(
        "Category: {}\n",
        explanation.reason_category
    ));
    output.push_str(&format!("Detail: {}\n", explanation.reason_detail));

    // Contributing factors
    if !explanation.contributing_factors.is_empty() {
        output.push_str("\nContributing Factors:\n");
        for factor in &explanation.contributing_factors {
            output.push_str(&format!(
                "  - {}: {:.4} (threshold: {:.4}) — {}\n",
                factor.name, factor.value, factor.threshold, factor.description
            ));
        }
    }

    // Confidence
    let conf = &explanation.confidence_explanation;
    output.push_str(&format!(
        "\nConfidence: {:.4} ({})\n  {}\n",
        conf.confidence, conf.level, conf.reasoning
    ));

    // Alternatives
    if !explanation.alternatives.is_empty() {
        output.push_str("\nAlternatives Considered:\n");
        for alt in &explanation.alternatives {
            output.push_str(&format!(
                "  - {} (confidence: {:.4}): {}\n",
                alt.action, alt.confidence, alt.reason
            ));
        }
    }

    output
}

// ─── Per-Variant Explanation Builders ─────────────────────────────────────────

fn explain_promote_to_dram(
    chunk_id: &ghost_core::types::ChunkId,
    reason: &str,
    confidence: &f32,
    factors: &[String],
    state: &SystemState,
) -> RecommendationExplanation {
    let mem_pressure = state.dram_pressure.memory_pressure;
    let dram_util = state.dram_utilization;

    let contributing_factors = vec![
        ExplanationFactor {
            name: "DRAM memory pressure".to_string(),
            value: mem_pressure,
            threshold: 0.7,
            description: if mem_pressure < 0.5 {
                "DRAM pressure is low — promotion is safe".to_string()
            } else if mem_pressure < 0.7 {
                "DRAM pressure is moderate — promotion is feasible".to_string()
            } else {
                format!("DRAM pressure is high ({:.2}) — promotion risky but hot data justifies it", mem_pressure)
            },
        },
        ExplanationFactor {
            name: "DRAM utilization".to_string(),
            value: dram_util,
            threshold: 0.85,
            description: if dram_util < 0.7 {
                "DRAM has ample free capacity".to_string()
            } else if dram_util < 0.85 {
                "DRAM is moderately utilized — space available for promotion".to_string()
            } else {
                format!("DRAM is heavily utilized ({:.2}) — promotion may increase pressure", dram_util)
            },
        },
    ];

    // Add hotness factor if available
    let mut all_factors = contributing_factors;
    if let Some(ref hotness) = state.hotness_summary {
        all_factors.push(ExplanationFactor {
            name: "Hotness summary".to_string(),
            value: hotness.hot_percentage / 100.0,
            threshold: 0.5,
            description: format!(
                "Hotness data shows this region is frequently accessed (avg hotness: {:.2})",
                hotness.hot_percentage / 100.0
            ),
        });
    }

    // Add user-provided factors
    for factor_str in factors {
        all_factors.push(ExplanationFactor {
            name: "Policy factor".to_string(),
            value: 1.0,
            threshold: 0.5,
            description: factor_str.clone(),
        });
    }

    let confidence_explanation = build_confidence_explanation(
        *confidence,
        if mem_pressure < 0.5 {
            "High confidence due to low DRAM pressure and available capacity"
        } else {
            "Moderate confidence — DRAM pressure is elevated but hotness justifies promotion"
        },
    );

    let alternatives = vec![
        AlternativeAction {
            action: "MoveToZram".to_string(),
            reason: "Not chosen because the chunk is hot — ZRAM compression would increase latency".to_string(),
            confidence: (0.3 + mem_pressure * 0.3).clamp(0.0, 1.0),
        },
        AlternativeAction {
            action: "NoAction".to_string(),
            reason: "Not chosen because promoting hot data to DRAM reduces page faults".to_string(),
            confidence: 0.2,
        },
    ];

    RecommendationExplanation {
        recommendation: Recommendation::PromoteToDram {
            chunk_id: *chunk_id,
            reason: reason.to_string(),
            confidence: *confidence,
            factors: factors.to_vec(),
        },
        reason_category: if state.hotness_summary.is_some() {
            ReasonCategory::HotnessDriven
        } else {
            ReasonCategory::TierOptimization
        },
        reason_detail: format!(
            "Chunk {} is frequently accessed (hot). Promoting to DRAM reduces page faults and improves latency. DRAM utilization is {:.0}%, memory pressure is {:.0}%.",
            chunk_id.short_hex(),
            dram_util * 100.0,
            mem_pressure * 100.0
        ),
        contributing_factors: all_factors,
        confidence_explanation,
        alternatives,
    }
}

fn explain_move_to_zram(
    chunk_id: &ghost_core::types::ChunkId,
    reason: &str,
    confidence: &f32,
    factors: &[String],
    state: &SystemState,
) -> RecommendationExplanation {
    let zram_util = state.zram_utilization.unwrap_or(0.0);
    let mem_pressure = state.dram_pressure.memory_pressure;

    let contributing_factors = vec![
        ExplanationFactor {
            name: "ZRAM utilization".to_string(),
            value: zram_util,
            threshold: 0.8,
            description: if zram_util < 0.5 {
                "ZRAM has ample capacity for compressed pages".to_string()
            } else if zram_util < 0.8 {
                "ZRAM is moderately utilized — space available".to_string()
            } else {
                format!("ZRAM is nearly full ({:.2}) — may need to consider disk swap", zram_util)
            },
        },
        ExplanationFactor {
            name: "DRAM memory pressure".to_string(),
            value: mem_pressure,
            threshold: 0.5,
            description: format!(
                "DRAM pressure at {:.0}% — moving cold data to ZRAM frees DRAM for hot data",
                mem_pressure * 100.0
            ),
        },
    ];

    let mut all_factors = contributing_factors;
    for factor_str in factors {
        all_factors.push(ExplanationFactor {
            name: "Policy factor".to_string(),
            value: 1.0,
            threshold: 0.5,
            description: factor_str.clone(),
        });
    }

    let confidence_explanation = build_confidence_explanation(
        *confidence,
        if zram_util < 0.7 {
            "High confidence — ZRAM has capacity and chunk is cold"
        } else {
            "Moderate confidence — ZRAM is filling up but still viable"
        },
    );

    let alternatives = vec![
        AlternativeAction {
            action: "MoveToDiskSwap".to_string(),
            reason: if zram_util > 0.8 {
                "Viable alternative — ZRAM is nearly full".to_string()
            } else {
                "Not chosen — ZRAM compression is more efficient than disk swap".to_string()
            },
            confidence: if zram_util > 0.8 { 0.6 } else { 0.3 },
        },
        AlternativeAction {
            action: "NoAction".to_string(),
            reason: "Not chosen — freeing DRAM by moving cold data improves overall performance".to_string(),
            confidence: 0.2,
        },
    ];

    RecommendationExplanation {
        recommendation: Recommendation::MoveToZram {
            chunk_id: *chunk_id,
            reason: reason.to_string(),
            confidence: *confidence,
            factors: factors.to_vec(),
        },
        reason_category: ReasonCategory::ColdEviction,
        reason_detail: format!(
            "Chunk {} is cold/infrequently accessed. Moving to ZRAM compresses the data and frees DRAM. ZRAM utilization is {:.0}%, DRAM pressure is {:.0}%.",
            chunk_id.short_hex(),
            zram_util * 100.0,
            mem_pressure * 100.0
        ),
        contributing_factors: all_factors,
        confidence_explanation,
        alternatives,
    }
}

fn explain_move_to_disk_swap(
    chunk_id: &ghost_core::types::ChunkId,
    reason: &str,
    confidence: &f32,
    factors: &[String],
    state: &SystemState,
) -> RecommendationExplanation {
    let zram_util = state.zram_utilization.unwrap_or(0.0);
    let swap_util = state.swap_utilization;
    let mem_pressure = state.dram_pressure.memory_pressure;

    let contributing_factors = vec![
        ExplanationFactor {
            name: "ZRAM utilization".to_string(),
            value: zram_util,
            threshold: 0.9,
            description: format!(
                "ZRAM is at {:.0}% — {}",
                zram_util * 100.0,
                if zram_util > 0.9 {
                    "nearly full, compression is inefficient"
                } else {
                    "has some capacity but chunk is very cold"
                }
            ),
        },
        ExplanationFactor {
            name: "Swap utilization".to_string(),
            value: swap_util,
            threshold: 0.85,
            description: if swap_util < 0.6 {
                "Swap has ample capacity".to_string()
            } else if swap_util < 0.85 {
                "Swap is moderately utilized".to_string()
            } else {
                format!("Swap is heavily utilized ({:.2}) — consider eviction first", swap_util)
            },
        },
        ExplanationFactor {
            name: "DRAM memory pressure".to_string(),
            value: mem_pressure,
            threshold: 0.7,
            description: format!(
                "DRAM pressure at {:.0}% — disk swap is last resort to relieve pressure",
                mem_pressure * 100.0
            ),
        },
    ];

    let mut all_factors = contributing_factors;
    for factor_str in factors {
        all_factors.push(ExplanationFactor {
            name: "Policy factor".to_string(),
            value: 1.0,
            threshold: 0.5,
            description: factor_str.clone(),
        });
    }

    let confidence_explanation = build_confidence_explanation(
        *confidence,
        if zram_util > 0.9 {
            "High confidence — ZRAM is full, disk swap is necessary"
        } else {
            "Moderate confidence — disk swap is last resort, consider alternatives"
        },
    );

    let alternatives = vec![
        AlternativeAction {
            action: "EvictCold".to_string(),
            reason: "Could evict instead of swapping — avoids disk I/O entirely".to_string(),
            confidence: if mem_pressure > 0.8 { 0.7 } else { 0.4 },
        },
        AlternativeAction {
            action: "MoveToZram".to_string(),
            reason: if zram_util > 0.9 {
                "Not viable — ZRAM is nearly full".to_string()
            } else {
                "Not chosen — ZRAM has capacity but chunk is too cold for compression to help".to_string()
            },
            confidence: if zram_util > 0.9 { 0.1 } else { 0.5 },
        },
    ];

    RecommendationExplanation {
        recommendation: Recommendation::MoveToDiskSwap {
            chunk_id: *chunk_id,
            reason: reason.to_string(),
            confidence: *confidence,
            factors: factors.to_vec(),
        },
        reason_category: ReasonCategory::PressureResponse,
        reason_detail: format!(
            "Chunk {} is very cold. ZRAM is at {:.0}% utilization, swap at {:.0}%. Disk swap is needed to free DRAM (pressure at {:.0}%).",
            chunk_id.short_hex(),
            zram_util * 100.0,
            swap_util * 100.0,
            mem_pressure * 100.0
        ),
        contributing_factors: all_factors,
        confidence_explanation,
        alternatives,
    }
}

fn explain_evict_cold(
    tier: ghost_core::types::TierId,
    count: usize,
    confidence: &f32,
    factors: &[String],
    state: &SystemState,
) -> RecommendationExplanation {
    let mem_pressure = state.dram_pressure.memory_pressure;
    let dram_util = state.dram_utilization;

    let contributing_factors = vec![
        ExplanationFactor {
            name: "DRAM memory pressure".to_string(),
            value: mem_pressure,
            threshold: 0.7,
            description: format!(
                "DRAM pressure at {:.0}% — {}",
                mem_pressure * 100.0,
                if mem_pressure >= 0.9 {
                    "critical — immediate eviction needed"
                } else if mem_pressure >= 0.7 {
                    "high — eviction recommended"
                } else {
                    "moderate — eviction may help"
                }
            ),
        },
        ExplanationFactor {
            name: "DRAM utilization".to_string(),
            value: dram_util,
            threshold: 0.85,
            description: format!(
                "DRAM utilization at {:.0}% — eviction of {} chunks frees memory",
                dram_util * 100.0,
                count
            ),
        },
    ];

    let mut all_factors = contributing_factors;
    for factor_str in factors {
        all_factors.push(ExplanationFactor {
            name: "Policy factor".to_string(),
            value: 1.0,
            threshold: 0.5,
            description: factor_str.clone(),
        });
    }

    let confidence_explanation = build_confidence_explanation(
        *confidence,
        if mem_pressure >= 0.8 {
            "High confidence — critical pressure demands immediate eviction"
        } else {
            "Moderate confidence — eviction helps but pressure is not critical"
        },
    );

    let alternatives = vec![
        AlternativeAction {
            action: "MoveToZram".to_string(),
            reason: "Could move to ZRAM instead of evicting — preserves data but uses compressed memory".to_string(),
            confidence: if state.zram_utilization.unwrap_or(1.0) < 0.7 {
                0.6
            } else {
                0.2
            },
        },
        AlternativeAction {
            action: "DemoteHot".to_string(),
            reason: "Not chosen — demoting hot data would hurt performance".to_string(),
            confidence: 0.1,
        },
    ];

    RecommendationExplanation {
        recommendation: Recommendation::EvictCold {
            tier,
            count,
            confidence: *confidence,
            factors: factors.to_vec(),
        },
        reason_category: ReasonCategory::ColdEviction,
        reason_detail: format!(
            "Evicting {} cold chunk(s) from tier {:?} to free DRAM. Memory pressure is at {:.0}%, utilization at {:.0}%.",
            count,
            tier,
            mem_pressure * 100.0,
            dram_util * 100.0
        ),
        contributing_factors: all_factors,
        confidence_explanation,
        alternatives,
    }
}

fn explain_demote_hot(
    tier: ghost_core::types::TierId,
    target: ghost_core::types::TierId,
    confidence: &f32,
    factors: &[String],
    state: &SystemState,
) -> RecommendationExplanation {
    let mem_pressure = state.dram_pressure.memory_pressure;
    let dram_util = state.dram_utilization;

    let contributing_factors = vec![
        ExplanationFactor {
            name: "DRAM memory pressure".to_string(),
            value: mem_pressure,
            threshold: 0.85,
            description: format!(
                "DRAM pressure at {:.0}% — critical level requires emergency measures",
                mem_pressure * 100.0
            ),
        },
        ExplanationFactor {
            name: "DRAM utilization".to_string(),
            value: dram_util,
            threshold: 0.9,
            description: format!(
                "DRAM utilization at {:.0}% — near capacity, emergency demotion needed",
                dram_util * 100.0
            ),
        },
    ];

    let mut all_factors = contributing_factors;
    for factor_str in factors {
        all_factors.push(ExplanationFactor {
            name: "Policy factor".to_string(),
            value: 1.0,
            threshold: 0.5,
            description: factor_str.clone(),
        });
    }

    let confidence_explanation = build_confidence_explanation(
        *confidence,
        if mem_pressure >= 0.9 {
            "High confidence — emergency demotion justified by critical pressure"
        } else {
            "Moderate confidence — demotion is a trade-off between pressure and performance"
        },
    );

    let alternatives = vec![
        AlternativeAction {
            action: "EvictCold".to_string(),
            reason: "Could evict cold data instead — less performance impact but may not free enough memory".to_string(),
            confidence: 0.5,
        },
        AlternativeAction {
            action: "MoveToDiskSwap".to_string(),
            reason: "Could move to disk swap — slower but frees more memory".to_string(),
            confidence: if state.swap_utilization < 0.7 { 0.4 } else { 0.2 },
        },
    ];

    RecommendationExplanation {
        recommendation: Recommendation::DemoteHot {
            tier,
            target,
            confidence: *confidence,
            factors: factors.to_vec(),
        },
        reason_category: ReasonCategory::PressureResponse,
        reason_detail: format!(
            "Emergency demotion of hot chunks from tier {:?} to {:?}. Despite hotness data, critical DRAM pressure ({:.0}%) and utilization ({:.0}%) require immediate action.",
            tier,
            target,
            mem_pressure * 100.0,
            dram_util * 100.0
        ),
        contributing_factors: all_factors,
        confidence_explanation,
        alternatives,
    }
}

fn explain_no_action(
    reason: &str,
    confidence: &f32,
    factors: &[String],
    state: &SystemState,
) -> RecommendationExplanation {
    let mem_pressure = state.dram_pressure.memory_pressure;
    let dram_util = state.dram_utilization;
    let swap_util = state.swap_utilization;

    let contributing_factors = vec![
        ExplanationFactor {
            name: "DRAM memory pressure".to_string(),
            value: mem_pressure,
            threshold: 0.5,
            description: format!(
                "DRAM pressure at {:.0}% — {}",
                mem_pressure * 100.0,
                if mem_pressure < 0.3 {
                    "low — system is stable"
                } else if mem_pressure < 0.5 {
                    "moderate — within acceptable range"
                } else {
                    "elevated but no action threshold not yet exceeded"
                }
            ),
        },
        ExplanationFactor {
            name: "DRAM utilization".to_string(),
            value: dram_util,
            threshold: 0.85,
            description: format!(
                "DRAM utilization at {:.0}% — no capacity concerns",
                dram_util * 100.0
            ),
        },
        ExplanationFactor {
            name: "Swap utilization".to_string(),
            value: swap_util,
            threshold: 0.7,
            description: format!(
                "Swap utilization at {:.0}% — no swap pressure",
                swap_util * 100.0
            ),
        },
    ];

    let mut all_factors = contributing_factors;
    for factor_str in factors {
        all_factors.push(ExplanationFactor {
            name: "Policy factor".to_string(),
            value: 1.0,
            threshold: 0.5,
            description: factor_str.clone(),
        });
    }

    let is_stable = mem_pressure < 0.5 && swap_util < 0.5;
    let confidence_explanation = build_confidence_explanation(
        *confidence,
        if is_stable {
            "High confidence — system is stable, no pressure thresholds exceeded"
        } else {
            "Moderate confidence — system is manageable but not ideal"
        },
    );

    let alternatives = vec![
        AlternativeAction {
            action: "PromoteToDram".to_string(),
            reason: "Not needed — no hot data to promote and DRAM has capacity".to_string(),
            confidence: 0.1,
        },
        AlternativeAction {
            action: "MoveToZram".to_string(),
            reason: "Not needed — no cold data to move and system is balanced".to_string(),
            confidence: 0.1,
        },
    ];

    RecommendationExplanation {
        recommendation: Recommendation::NoAction {
            reason: reason.to_string(),
            confidence: *confidence,
            factors: factors.to_vec(),
        },
        reason_category: ReasonCategory::NoActionNeeded,
        reason_detail: format!(
            "System is stable. DRAM pressure at {:.0}%, utilization at {:.0}%, swap at {:.0}%. No thresholds exceeded — current placement is appropriate.",
            mem_pressure * 100.0,
            dram_util * 100.0,
            swap_util * 100.0
        ),
        contributing_factors: all_factors,
        confidence_explanation,
        alternatives,
    }
}

// ─── Helper Functions ─────────────────────────────────────────────────────────

/// Build a `ConfidenceExplanation` from a raw confidence value and reasoning.
fn build_confidence_explanation(confidence: f32, reasoning: &str) -> ConfidenceExplanation {
    let level = if confidence >= 0.8 {
        ConfidenceLevel::High
    } else if confidence >= 0.5 {
        ConfidenceLevel::Medium
    } else {
        ConfidenceLevel::Low
    };

    ConfidenceExplanation {
        confidence,
        level,
        reasoning: reasoning.to_string(),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::PressureState;
    use ghost_core::types::ChunkId;

    // ── Helper functions ──

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

    fn high_pressure_state() -> SystemState {
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

    fn critical_pressure_state() -> SystemState {
        SystemState {
            dram_pressure: PressureState {
                memory_pressure: 0.95,
                ..Default::default()
            },
            dram_utilization: 0.97,
            swap_utilization: 0.5,
            zram_utilization: Some(0.9),
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    fn chunk_a() -> ChunkId {
        ChunkId::from_data(b"chunk_a")
    }

    // ── Required tests ──

    #[test]
    fn test_explain_promote_to_dram() {
        let state = high_pressure_state();
        let rec = Recommendation::PromoteToDram {
            chunk_id: chunk_a(),
            reason: "hot chunk".to_string(),
            confidence: 0.9,
            factors: vec!["high_access".to_string()],
        };

        let explanation = explain_recommendation(&rec, &state);

        assert!(matches!(explanation.reason_category, ReasonCategory::HotnessDriven | ReasonCategory::TierOptimization));
        assert!(!explanation.contributing_factors.is_empty());
        assert!(explanation
            .reason_detail
            .contains("frequently accessed"));
        assert!(explanation.confidence_explanation.confidence > 0.5);
        assert!(!explanation.alternatives.is_empty());

        // Check that factors reference actual state values
        let has_pressure_factor = explanation
            .contributing_factors
            .iter()
            .any(|f| f.name.contains("DRAM") && f.value > 0.0);
        assert!(
            has_pressure_factor,
            "should have a DRAM pressure factor with actual value"
        );
    }

    #[test]
    fn test_explain_move_to_zram() {
        let state = high_pressure_state();
        let rec = Recommendation::MoveToZram {
            chunk_id: chunk_a(),
            reason: "cold chunk".to_string(),
            confidence: 0.85,
            factors: vec![],
        };

        let explanation = explain_recommendation(&rec, &state);

        assert_eq!(explanation.reason_category, ReasonCategory::ColdEviction);
        assert!(explanation.reason_detail.contains("cold"));
        assert!(explanation.reason_detail.contains("ZRAM"));
        assert!(!explanation.contributing_factors.is_empty());
        assert!(!explanation.alternatives.is_empty());

        // Check ZRAM utilization factor
        let has_zram_factor = explanation
            .contributing_factors
            .iter()
            .any(|f| f.name.contains("ZRAM"));
        assert!(has_zram_factor, "should have a ZRAM utilization factor");
    }

    #[test]
    fn test_explain_move_to_disk_swap() {
        let state = critical_pressure_state();
        let rec = Recommendation::MoveToDiskSwap {
            chunk_id: chunk_a(),
            reason: "very cold".to_string(),
            confidence: 0.7,
            factors: vec![],
        };

        let explanation = explain_recommendation(&rec, &state);

        assert_eq!(explanation.reason_category, ReasonCategory::PressureResponse);
        assert!(explanation.reason_detail.contains("Disk swap"));
        assert!(explanation.reason_detail.contains("ZRAM"));
        assert!(!explanation.contributing_factors.is_empty());
        assert!(!explanation.alternatives.is_empty());
    }

    #[test]
    fn test_explain_evict_cold() {
        let state = critical_pressure_state();
        let rec = Recommendation::EvictCold {
            tier: ghost_core::types::TierId::Ram,
            count: 8,
            confidence: 0.95,
            factors: vec!["critical_pressure".to_string()],
        };

        let explanation = explain_recommendation(&rec, &state);

        assert_eq!(explanation.reason_category, ReasonCategory::ColdEviction);
        assert!(explanation.reason_detail.contains("Evicting"));
        assert!(explanation.reason_detail.contains("8"));
        assert!(!explanation.contributing_factors.is_empty());
        assert!(!explanation.alternatives.is_empty());
    }

    #[test]
    fn test_explain_demote_hot() {
        let state = critical_pressure_state();
        let rec = Recommendation::DemoteHot {
            tier: ghost_core::types::TierId::Ram,
            target: ghost_core::types::TierId::Disk,
            confidence: 0.8,
            factors: vec!["emergency".to_string()],
        };

        let explanation = explain_recommendation(&rec, &state);

        assert_eq!(explanation.reason_category, ReasonCategory::PressureResponse);
        assert!(explanation.reason_detail.contains("Emergency"));
        assert!(explanation.reason_detail.contains("critical"));
        assert!(!explanation.contributing_factors.is_empty());
        assert!(!explanation.alternatives.is_empty());
    }

    #[test]
    fn test_explain_no_action() {
        let state = idle_state();
        let rec = Recommendation::NoAction {
            reason: "system stable".to_string(),
            confidence: 0.95,
            factors: vec!["low_pressure".to_string()],
        };

        let explanation = explain_recommendation(&rec, &state);

        assert_eq!(explanation.reason_category, ReasonCategory::NoActionNeeded);
        assert!(explanation.reason_detail.contains("stable"));
        assert!(explanation.reason_detail.contains("DRAM pressure"));
        assert!(!explanation.contributing_factors.is_empty());
        assert!(!explanation.alternatives.is_empty());
    }

    #[test]
    fn test_explain_recommendations_batch() {
        let state = high_pressure_state();
        let recs = vec![
            Recommendation::PromoteToDram {
                chunk_id: chunk_a(),
                reason: "hot".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
            Recommendation::NoAction {
                reason: "stable".to_string(),
                confidence: 0.8,
                factors: vec![],
            },
        ];

        let explanations = explain_recommendations(&recs, &state);
        assert_eq!(explanations.len(), 2);
        assert!(matches!(explanations[0].reason_category, ReasonCategory::HotnessDriven | ReasonCategory::TierOptimization));




        assert_eq!(
            explanations[1].reason_category,
            ReasonCategory::NoActionNeeded
        );
    }

    #[test]
    fn test_format_explanation_readable() {
        let state = high_pressure_state();
        let rec = Recommendation::PromoteToDram {
            chunk_id: chunk_a(),
            reason: "hot chunk".to_string(),
            confidence: 0.9,
            factors: vec![],
        };

        let explanation = explain_recommendation(&rec, &state);
        let formatted = format_explanation(&explanation);

        assert!(formatted.contains("promote_to_dram"));
        assert!(formatted.contains("Contributing Factors"));
        assert!(formatted.contains("Confidence"));
        assert!(formatted.contains("Alternatives"));
        assert!(formatted.contains("DRAM"));
    }

    #[test]
    fn test_confidence_level_classification() {
        // High confidence
        let high = build_confidence_explanation(0.9, "test");
        assert_eq!(high.level, ConfidenceLevel::High);

        let high_boundary = build_confidence_explanation(0.8, "test");
        assert_eq!(high_boundary.level, ConfidenceLevel::High);

        // Medium confidence
        let medium = build_confidence_explanation(0.6, "test");
        assert_eq!(medium.level, ConfidenceLevel::Medium);

        let medium_boundary = build_confidence_explanation(0.5, "test");
        assert_eq!(medium_boundary.level, ConfidenceLevel::Medium);

        // Low confidence
        let low = build_confidence_explanation(0.3, "test");
        assert_eq!(low.level, ConfidenceLevel::Low);

        let low_boundary = build_confidence_explanation(0.0, "test");
        assert_eq!(low_boundary.level, ConfidenceLevel::Low);
    }

    #[test]
    fn test_alternative_actions() {
        let state = high_pressure_state();

        // Each recommendation type should have alternatives
        let recs = vec![
            Recommendation::PromoteToDram {
                chunk_id: chunk_a(),
                reason: "hot".to_string(),
                confidence: 0.9,
                factors: vec![],
            },
            Recommendation::MoveToZram {
                chunk_id: chunk_a(),
                reason: "cold".to_string(),
                confidence: 0.8,
                factors: vec![],
            },
            Recommendation::MoveToDiskSwap {
                chunk_id: chunk_a(),
                reason: "very cold".to_string(),
                confidence: 0.7,
                factors: vec![],
            },
            Recommendation::EvictCold {
                tier: ghost_core::types::TierId::Ram,
                count: 4,
                confidence: 0.95,
                factors: vec![],
            },
            Recommendation::DemoteHot {
                tier: ghost_core::types::TierId::Ram,
                target: ghost_core::types::TierId::Disk,
                confidence: 0.8,
                factors: vec![],
            },
            Recommendation::NoAction {
                reason: "stable".to_string(),
                confidence: 1.0,
                factors: vec![],
            },
        ];

        for rec in &recs {
            let explanation = explain_recommendation(rec, &state);
            assert!(
                !explanation.alternatives.is_empty(),
                "recommendation {:?} should have alternatives",
                rec.kind()
            );
        }
    }

    #[test]
    fn test_explanation_deterministic() {
        let state = high_pressure_state();
        let rec = Recommendation::PromoteToDram {
            chunk_id: chunk_a(),
            reason: "hot".to_string(),
            confidence: 0.9,
            factors: vec!["test".to_string()],
        };

        let e1 = explain_recommendation(&rec, &state);
        let e2 = explain_recommendation(&rec, &state);

        assert_eq!(e1.reason_category, e2.reason_category);
        assert_eq!(e1.reason_detail, e2.reason_detail);
        assert_eq!(
            e1.confidence_explanation.confidence,
            e2.confidence_explanation.confidence
        );
        assert_eq!(
            e1.contributing_factors.len(),
            e2.contributing_factors.len()
        );
        assert_eq!(e1.alternatives.len(), e2.alternatives.len());
    }
}
