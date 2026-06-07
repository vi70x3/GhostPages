//! Region Lifecycle Tracking for GhostPages.
//!
//! Tracks temperature transitions for memory regions over time, providing
//! insights into promotion/demotion patterns and region stability.
//!
//! All functions are **pure** — no I/O, no mutation, no side effects.
//! Same inputs always produce same outputs. Deterministic by design.

use std::collections::HashMap;

use ghost_core::types::ChunkId;

use crate::adaptive::TemperatureClass;

// ─── Temperature Transition ───────────────────────────────────────────────────

/// A single temperature transition for a region.
#[derive(Debug, Clone, PartialEq)]
pub struct TemperatureTransition {
    /// The previous temperature class.
    pub from: TemperatureClass,
    /// The new temperature class.
    pub to: TemperatureClass,
    /// Monotonic timestamp of the transition.
    pub timestamp: u64,
    /// Human-readable reason for the transition.
    pub reason: String,
}

// ─── Region Lifecycle ─────────────────────────────────────────────────────────

/// Tracks a single region's lifecycle through temperature transitions.
#[derive(Debug, Clone)]
pub struct RegionLifecycle {
    /// The region (chunk) being tracked.
    pub region_id: ChunkId,
    /// Current temperature class of the region.
    pub current_temperature: TemperatureClass,
    /// History of temperature transitions.
    pub transitions: Vec<TemperatureTransition>,
    /// Total number of promotions (to a hotter tier).
    pub promotion_count: usize,
    /// Total number of demotions (to a colder tier).
    pub demotion_count: usize,
    /// Average time spent in each temperature class (in seconds).
    pub average_residency_secs: f32,
}

// ─── Lifecycle Summary ───────────────────────────────────────────────────────

/// Aggregate lifecycle statistics across all tracked regions.
#[derive(Debug, Clone)]
pub struct LifecycleSummary {
    /// Total number of regions being tracked.
    pub total_regions: usize,
    /// Number of regions currently classified as Hot.
    pub hot_regions: usize,
    /// Number of regions currently classified as Cold.
    pub cold_regions: usize,
    /// Number of regions currently classified as Frozen.
    pub frozen_regions: usize,
    /// Total number of promotions across all regions.
    pub total_promotions: usize,
    /// Total number of demotions across all regions.
    pub total_demotions: usize,
    /// The region with the most transitions (most active).
    pub most_active_region: Option<ChunkId>,
    /// The region with the fewest transitions (most stable).
    pub most_stable_region: Option<ChunkId>,
}

// ─── Lifecycle Tracker ────────────────────────────────────────────────────────

/// Tracks lifecycle information for multiple regions.
#[derive(Debug, Clone)]
pub struct LifecycleTracker {
    regions: HashMap<ChunkId, RegionLifecycle>,
}

impl LifecycleTracker {
    /// Create a new empty lifecycle tracker.
    pub fn new() -> Self {
        Self {
            regions: HashMap::new(),
        }
    }

    /// Record a temperature transition for a region.
    ///
    /// If the region is not yet tracked, it will be added with the
    /// given `to` temperature as its current state.
    pub fn record_transition(
        &mut self,
        region_id: ChunkId,
        from: TemperatureClass,
        to: TemperatureClass,
        timestamp: u64,
        reason: String,
    ) {
        let entry = self.regions.entry(region_id).or_insert_with(|| RegionLifecycle {
            region_id,
            current_temperature: from.clone(),
            transitions: Vec::new(),
            promotion_count: 0,
            demotion_count: 0,
            average_residency_secs: 0.0,
        });

        entry.current_temperature = to.clone();
        entry.transitions.push(TemperatureTransition {
            from,
            to,
            timestamp,
            reason,
        });

        // Update average residency.
        self.update_residency(region_id);
    }

    /// Record a promotion for a region (move to a hotter tier).
    pub fn record_promotion(&mut self, region_id: ChunkId, timestamp: u64) {
        let entry = self.regions.entry(region_id).or_insert_with(|| RegionLifecycle {
            region_id,
            current_temperature: TemperatureClass::Hot,
            transitions: Vec::new(),
            promotion_count: 0,
            demotion_count: 0,
            average_residency_secs: 0.0,
        });

        entry.promotion_count += 1;
        entry.current_temperature = TemperatureClass::Hot;
        entry.transitions.push(TemperatureTransition {
            from: TemperatureClass::Warm,
            to: TemperatureClass::Hot,
            timestamp,
            reason: "promotion".to_string(),
        });

        self.update_residency(region_id);
    }

    /// Record a demotion for a region (move to a colder tier).
    pub fn record_demotion(&mut self, region_id: ChunkId, timestamp: u64) {
        let entry = self.regions.entry(region_id).or_insert_with(|| RegionLifecycle {
            region_id,
            current_temperature: TemperatureClass::Cold,
            transitions: Vec::new(),
            promotion_count: 0,
            demotion_count: 0,
            average_residency_secs: 0.0,
        });

        entry.demotion_count += 1;
        entry.current_temperature = TemperatureClass::Cold;
        entry.transitions.push(TemperatureTransition {
            from: TemperatureClass::Warm,
            to: TemperatureClass::Cold,
            timestamp,
            reason: "demotion".to_string(),
        });

        self.update_residency(region_id);
    }

    /// Get the lifecycle for a specific region.
    pub fn get_lifecycle(&self, region_id: &ChunkId) -> Option<&RegionLifecycle> {
        self.regions.get(region_id)
    }

    /// Compute aggregate lifecycle statistics across all tracked regions.
    pub fn summary(&self) -> LifecycleSummary {
        let total_regions = self.regions.len();

        let hot_regions = self
            .regions
            .values()
            .filter(|r| r.current_temperature == TemperatureClass::Hot)
            .count();

        let cold_regions = self
            .regions
            .values()
            .filter(|r| r.current_temperature == TemperatureClass::Cold)
            .count();

        let frozen_regions = self
            .regions
            .values()
            .filter(|r| r.current_temperature == TemperatureClass::Frozen)
            .count();

        let total_promotions: usize = self.regions.values().map(|r| r.promotion_count).sum();
        let total_demotions: usize = self.regions.values().map(|r| r.demotion_count).sum();

        let most_active_region = self.most_active_region();
        let most_stable_region = self.most_stable_region();

        LifecycleSummary {
            total_regions,
            hot_regions,
            cold_regions,
            frozen_regions,
            total_promotions,
            total_demotions,
            most_active_region,
            most_stable_region,
        }
    }

    /// Find the region with the most transitions (most active).
    pub fn most_active_region(&self) -> Option<ChunkId> {
        self.regions
            .values()
            .max_by_key(|r| r.transitions.len())
            .map(|r| r.region_id)
    }

    /// Find the region with the fewest transitions (most stable).
    pub fn most_stable_region(&self) -> Option<ChunkId> {
        self.regions
            .values()
            .min_by_key(|r| r.transitions.len())
            .map(|r| r.region_id)
    }

    /// Update the average residency for a region.
    fn update_residency(&mut self, region_id: ChunkId) {
        if let Some(entry) = self.regions.get_mut(&region_id) {
            if entry.transitions.len() < 2 {
                entry.average_residency_secs = 0.0;
                return;
            }

            let total_time = entry
                .transitions
                .last()
                .unwrap()
                .timestamp
                .saturating_sub(entry.transitions.first().unwrap().timestamp);

            let num_periods = entry.transitions.len() as f32;
            entry.average_residency_secs = if num_periods > 0.0 {
                total_time as f32 / num_periods
            } else {
                0.0
            };
        }
    }
}

impl Default for LifecycleTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk_a() -> ChunkId {
        ChunkId::from_data(b"region_a")
    }

    fn chunk_b() -> ChunkId {
        ChunkId::from_data(b"region_b")
    }

    fn chunk_c() -> ChunkId {
        ChunkId::from_data(b"region_c")
    }

    #[test]
    fn test_new_tracker_empty() {
        let tracker = LifecycleTracker::new();
        let summary = tracker.summary();

        assert_eq!(summary.total_regions, 0);
        assert_eq!(summary.hot_regions, 0);
        assert_eq!(summary.cold_regions, 0);
        assert_eq!(summary.frozen_regions, 0);
        assert_eq!(summary.total_promotions, 0);
        assert_eq!(summary.total_demotions, 0);
        assert!(summary.most_active_region.is_none());
        assert!(summary.most_stable_region.is_none());
    }

    #[test]
    fn test_record_transition() {
        let mut tracker = LifecycleTracker::new();

        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Cold,
            TemperatureClass::Hot,
            100,
            "access spike".to_string(),
        );

        let lifecycle = tracker.get_lifecycle(&chunk_a()).unwrap();
        assert_eq!(lifecycle.current_temperature, TemperatureClass::Hot);
        assert_eq!(lifecycle.transitions.len(), 1);
        assert_eq!(lifecycle.transitions[0].from, TemperatureClass::Cold);
        assert_eq!(lifecycle.transitions[0].to, TemperatureClass::Hot);
        assert_eq!(lifecycle.transitions[0].timestamp, 100);
        assert_eq!(lifecycle.transitions[0].reason, "access spike");
    }

    #[test]
    fn test_promotion_demotion_counters() {
        let mut tracker = LifecycleTracker::new();

        tracker.record_promotion(chunk_a(), 100);
        tracker.record_promotion(chunk_a(), 200);
        tracker.record_demotion(chunk_a(), 300);

        let lifecycle = tracker.get_lifecycle(&chunk_a()).unwrap();
        assert_eq!(lifecycle.promotion_count, 2);
        assert_eq!(lifecycle.demotion_count, 1);

        // Summary should reflect totals.
        let summary = tracker.summary();
        assert_eq!(summary.total_promotions, 2);
        assert_eq!(summary.total_demotions, 1);
    }

    #[test]
    fn test_lifecycle_summary() {
        let mut tracker = LifecycleTracker::new();

        // chunk_a: Hot (promoted).
        tracker.record_promotion(chunk_a(), 100);

        // chunk_b: Cold (demoted).
        tracker.record_demotion(chunk_b(), 100);

        // chunk_c: Frozen (transitioned to Frozen).
        tracker.record_transition(
            chunk_c(),
            TemperatureClass::Cold,
            TemperatureClass::Frozen,
            100,
            "no access".to_string(),
        );

        let summary = tracker.summary();
        assert_eq!(summary.total_regions, 3);
        assert_eq!(summary.hot_regions, 1);
        assert_eq!(summary.cold_regions, 1);
        assert_eq!(summary.frozen_regions, 1);
        assert_eq!(summary.total_promotions, 1);
        assert_eq!(summary.total_demotions, 1);
    }

    #[test]
    fn test_most_active_region() {
        let mut tracker = LifecycleTracker::new();

        // chunk_a: 3 transitions.
        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Cold,
            TemperatureClass::Warm,
            100,
            "warming".to_string(),
        );
        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Warm,
            TemperatureClass::Hot,
            200,
            "hot".to_string(),
        );
        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Hot,
            TemperatureClass::Warm,
            300,
            "cooling".to_string(),
        );

        // chunk_b: 1 transition.
        tracker.record_transition(
            chunk_b(),
            TemperatureClass::Cold,
            TemperatureClass::Warm,
            100,
            "warming".to_string(),
        );

        assert_eq!(tracker.most_active_region(), Some(chunk_a()));
    }

    #[test]
    fn test_most_stable_region() {
        let mut tracker = LifecycleTracker::new();

        // chunk_a: 3 transitions.
        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Cold,
            TemperatureClass::Warm,
            100,
            "warming".to_string(),
        );
        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Warm,
            TemperatureClass::Hot,
            200,
            "hot".to_string(),
        );
        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Hot,
            TemperatureClass::Warm,
            300,
            "cooling".to_string(),
        );

        // chunk_b: 1 transition (most stable).
        tracker.record_transition(
            chunk_b(),
            TemperatureClass::Cold,
            TemperatureClass::Warm,
            100,
            "warming".to_string(),
        );

        assert_eq!(tracker.most_stable_region(), Some(chunk_b()));
    }

    #[test]
    fn test_temperature_transition_chain() {
        let mut tracker = LifecycleTracker::new();

        // Hot → Warm → Cold → Frozen.
        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Hot,
            TemperatureClass::Warm,
            100,
            "cooling".to_string(),
        );
        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Warm,
            TemperatureClass::Cold,
            200,
            "cold".to_string(),
        );
        tracker.record_transition(
            chunk_a(),
            TemperatureClass::Cold,
            TemperatureClass::Frozen,
            300,
            "frozen".to_string(),
        );

        let lifecycle = tracker.get_lifecycle(&chunk_a()).unwrap();
        assert_eq!(lifecycle.transitions.len(), 3);
        assert_eq!(lifecycle.current_temperature, TemperatureClass::Frozen);

        // Verify the chain.
        assert_eq!(lifecycle.transitions[0].from, TemperatureClass::Hot);
        assert_eq!(lifecycle.transitions[0].to, TemperatureClass::Warm);
        assert_eq!(lifecycle.transitions[1].from, TemperatureClass::Warm);
        assert_eq!(lifecycle.transitions[1].to, TemperatureClass::Cold);
        assert_eq!(lifecycle.transitions[2].from, TemperatureClass::Cold);
        assert_eq!(lifecycle.transitions[2].to, TemperatureClass::Frozen);
    }

    #[test]
    fn test_multiple_regions() {
        let mut tracker = LifecycleTracker::new();

        // Track multiple regions independently.
        tracker.record_promotion(chunk_a(), 100);
        tracker.record_demotion(chunk_b(), 100);
        tracker.record_transition(
            chunk_c(),
            TemperatureClass::Warm,
            TemperatureClass::Hot,
            100,
            "hot".to_string(),
        );

        // Each region should have its own lifecycle.
        let a = tracker.get_lifecycle(&chunk_a()).unwrap();
        let b = tracker.get_lifecycle(&chunk_b()).unwrap();
        let c = tracker.get_lifecycle(&chunk_c()).unwrap();

        assert_eq!(a.promotion_count, 1);
        assert_eq!(a.demotion_count, 0);

        assert_eq!(b.promotion_count, 0);
        assert_eq!(b.demotion_count, 1);

        assert_eq!(c.promotion_count, 0);
        assert_eq!(c.demotion_count, 0);
        assert_eq!(c.current_temperature, TemperatureClass::Hot);

        // Summary should aggregate.
        let summary = tracker.summary();
        assert_eq!(summary.total_regions, 3);
        assert_eq!(summary.total_promotions, 1);
        assert_eq!(summary.total_demotions, 1);
    }
}
