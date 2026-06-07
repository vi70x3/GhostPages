//! Hotness tracker for chunk access pattern analysis.
//!
//! Maintains a map of chunk hotness scores, updated on each access.
//! Provides methods to query hotness, find hot/cold chunks, and
//! periodically decay recency scores.
//!
//! Optionally accepts a [`HotnessProvider`] to integrate external hotness
//! data (e.g., from DAMON on Linux) with the internal chunk-level tracking.
//!
//! The tracker maintains a [`HotnessState`] that aggregates summary statistics,
//! confidence scoring, and trend history from provider samples.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ghost_core::hotness::ChunkHotness;
use ghost_core::hotness_confidence::HotnessConfidence;
use ghost_core::hotness_history::HotnessHistory;
use ghost_core::hotness_provider::{HotnessProvider, HotnessSnapshot, Temperature};
use ghost_core::hotness_summary::HotnessSummary;
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::types::{ChunkId, TierId};

use parking_lot::RwLock;

use crate::trace_log::TraceLog;

/// Aggregated hotness state from provider samples.
///
/// Combines summary statistics, confidence scoring, and trend history
/// into a single snapshot that can be queried by the orchestrator
/// and emitted as events.
#[derive(Debug, Clone)]
pub struct HotnessState {
    /// Aggregated temperature counts and percentages.
    pub summary: HotnessSummary,
    /// Confidence score for the current classification.
    pub confidence: HotnessConfidence,
    /// Rolling history of snapshots for trend analysis.
    pub history: HotnessHistory,
    /// Timestamp of the last update (seconds since epoch).
    pub last_update: u64,
}

impl HotnessState {
    /// Create an empty hotness state with default values.
    pub fn empty() -> Self {
        Self {
            summary: HotnessSummary::from_snapshot(&HotnessSnapshot {
                samples: vec![],
                timestamp: 0,
            }),
            confidence: HotnessConfidence::calculate(
                &HotnessSnapshot {
                    samples: vec![],
                    timestamp: 0,
                },
                &[],
            ),
            history: HotnessHistory::new(64),
            last_update: 0,
        }
    }

    /// Update the state from a new provider snapshot.
    pub fn update(&mut self, snapshot: HotnessSnapshot) {
        let now = snapshot.timestamp;
        let history_snapshots: Vec<HotnessSnapshot> = self.history.snapshots().to_vec();

        self.summary = HotnessSummary::from_snapshot(&snapshot);
        self.confidence = HotnessConfidence::calculate(&snapshot, &history_snapshots);
        self.history.push(snapshot);
        self.last_update = now;
    }
}

/// SUBSYSTEM: Runtime State Owner
///
/// Hotness tracker that maintains access pattern analysis for all chunks.
///
/// When a [`HotnessProvider`] is set via [`set_hotness_provider`](HotnessTracker::set_hotness_provider),
/// the tracker can integrate external hotness observations (e.g., from DAMON)
/// with its internal chunk-level access tracking.
///
/// The tracker maintains a [`HotnessState`] that is updated each time
/// [`sample_hotness`](HotnessTracker::sample_hotness) is called.
pub struct HotnessTracker {
    hotness_map: Arc<RwLock<BTreeMap<ChunkId, ChunkHotness>>>,
    trace_log: Arc<TraceLog>,
    /// Maximum number of chunks to track.
    max_tracked: usize,
    /// Optional external hotness provider.
    hotness_provider: Arc<RwLock<Option<Arc<dyn HotnessProvider>>>>,
    /// Aggregated hotness state from provider samples.
    hotness_state: RwLock<HotnessState>,
    /// Interval between automatic hotness samples.
    sampling_interval: RwLock<Duration>,
}

impl HotnessTracker {
    /// Create a new hotness tracker.
    pub fn new(max_tracked: usize, trace_log: Arc<TraceLog>) -> Self {
        Self {
            hotness_map: Arc::new(RwLock::new(BTreeMap::new())),
            trace_log,
            max_tracked,
            hotness_provider: Arc::new(RwLock::new(None)),
            hotness_state: RwLock::new(HotnessState::empty()),
            sampling_interval: RwLock::new(Duration::from_secs(60)),
        }
    }

    /// Create a new hotness tracker with a custom sampling interval.
    pub fn with_sampling_interval(
        max_tracked: usize,
        trace_log: Arc<TraceLog>,
        interval: Duration,
    ) -> Self {
        Self {
            hotness_map: Arc::new(RwLock::new(BTreeMap::new())),
            trace_log,
            max_tracked,
            hotness_provider: Arc::new(RwLock::new(None)),
            hotness_state: RwLock::new(HotnessState::empty()),
            sampling_interval: RwLock::new(interval),
        }
    }

    /// Set an external hotness provider.
    ///
    /// When set, the tracker will periodically call `sample()` on the provider
    /// to integrate external hotness observations with internal tracking.
    ///
    /// Pass `None` to clear the provider.
    pub fn set_hotness_provider(&self, provider: Option<Arc<dyn HotnessProvider>>) {
        let mut guard = self.hotness_provider.write();
        *guard = provider;
    }

    /// Get the currently configured hotness provider, if any.
    pub fn hotness_provider(&self) -> Option<Arc<dyn HotnessProvider>> {
        self.hotness_provider.read().clone()
    }

    /// Check if a hotness provider is configured.
    pub fn has_hotness_provider(&self) -> bool {
        self.hotness_provider.read().is_some()
    }

    /// Get the current sampling interval.
    pub fn sampling_interval(&self) -> Duration {
        *self.sampling_interval.read()
    }

    /// Set the sampling interval.
    pub fn set_sampling_interval(&self, interval: Duration) {
        *self.sampling_interval.write() = interval;
    }

    /// Sample hotness from the configured provider and update the hotness state.
    ///
    /// Returns the updated [`HotnessState`] on success, or `None` if
    /// no provider is configured or the provider fails.
    pub fn sample_hotness(&self) -> Option<HotnessState> {
        let provider = self.hotness_provider.read().clone()?;
        let snapshot = provider.sample().ok()?;
        let mut state = self.hotness_state.write();
        state.update(snapshot);
        Some(state.clone())
    }

    /// Get the current hotness state.
    pub fn get_hotness_state(&self) -> HotnessState {
        self.hotness_state.read().clone()
    }

    /// Get regions classified as hot or warm (active regions).
    ///
    /// Returns samples from the most recent snapshot in history
    /// that have temperature Hot or Warm.
    pub fn get_hot_regions(&self) -> Vec<(String, Temperature)> {
        let state = self.hotness_state.read();
        let mut regions = Vec::new();

        if let Some(latest) = state.history.snapshots().last() {
            for sample in &latest.samples {
                if sample.temperature.is_active() {
                    regions.push((
                        format!("{:x}-{:x}", sample.address_range.start, sample.address_range.end),
                        sample.temperature,
                    ));
                }
            }
        }

        regions
    }

    /// Get regions classified as cold or frozen (inactive regions).
    ///
    /// Returns samples from the most recent snapshot in history
    /// that have temperature Cold or Frozen.
    pub fn get_cold_regions(&self) -> Vec<(String, Temperature)> {
        let state = self.hotness_state.read();
        let mut regions = Vec::new();

        if let Some(latest) = state.history.snapshots().last() {
            for sample in &latest.samples {
                if sample.temperature.is_inactive() {
                    regions.push((
                        format!("{:x}-{:x}", sample.address_range.start, sample.address_range.end),
                        sample.temperature,
                    ));
                }
            }
        }

        regions
    }

    /// Record an access to a chunk.
    pub fn record_access(&self, chunk_id: ChunkId, size: usize) {
        let now = current_timestamp();
        let mut map = self.hotness_map.write();

        let hotness = map.entry(chunk_id).or_insert_with(|| {
            let h = ChunkHotness::new(chunk_id, now);
            self.trace_log.record(TraceEvent::ChunkCreated {
                chunk_id,
                size: 0,
                tier: TierId::Ram,
                timestamp: now,
            });
            h
        });

        hotness.record_access(now, size);
    }

    /// Get the hotness score for a chunk.
    pub fn get_hotness(&self, chunk_id: &ChunkId) -> Option<ChunkHotness> {
        let map = self.hotness_map.read();
        map.get(chunk_id).copied()
    }

    /// Get all chunks with their hotness scores.
    pub fn all_hotness(&self) -> Vec<(ChunkId, ChunkHotness)> {
        let map = self.hotness_map.read();
        map.iter().map(|(id, h)| (*id, *h)).collect()
    }

    /// Find chunks that are considered "hot" (above the given threshold).
    pub fn find_hot_chunks(&self, threshold: f32) -> Vec<(ChunkId, ChunkHotness)> {
        let map = self.hotness_map.read();
        map.iter()
            .filter(|(_, h)| h.score >= threshold)
            .map(|(id, h)| (*id, *h))
            .collect()
    }

    /// Find chunks that are considered "cold" (below the given threshold).
    pub fn find_cold_chunks(&self, threshold: f32) -> Vec<(ChunkId, ChunkHotness)> {
        let map = self.hotness_map.read();
        map.iter()
            .filter(|(_, h)| h.score < threshold)
            .map(|(id, h)| (*id, *h))
            .collect()
    }

    /// Update recency decay for all tracked chunks.
    ///
    /// Should be called periodically to decay recency scores for chunks
    /// that haven't been accessed recently.
    pub fn decay_all(&self) {
        let now = current_timestamp();
        let mut map = self.hotness_map.write();
        for hotness in map.values_mut() {
            hotness.update_recency(now);
        }
    }

    /// Remove a chunk from tracking.
    pub fn remove(&self, chunk_id: &ChunkId) -> Option<ChunkHotness> {
        let mut map = self.hotness_map.write();
        map.remove(chunk_id)
    }

    /// Get the number of tracked chunks.
    pub fn len(&self) -> usize {
        let map = self.hotness_map.read();
        map.len()
    }

    /// Check if no chunks are being tracked.
    pub fn is_empty(&self) -> bool {
        let map = self.hotness_map.read();
        map.is_empty()
    }

    /// Clear all tracked hotness data.
    pub fn clear(&self) {
        let mut map = self.hotness_map.write();
        map.clear();
    }

    /// Get the top N hottest chunks.
    pub fn top_n(&self, n: usize) -> Vec<(ChunkId, ChunkHotness)> {
        let map = self.hotness_map.read();
        let mut entries: Vec<_> = map.iter().map(|(id, h)| (*id, *h)).collect();
        entries.sort_by(|a, b| b.1.score.partial_cmp(&a.1.score).unwrap_or(std::cmp::Ordering::Equal));
        entries.into_iter().take(n).collect()
    }

    /// Get the top N coldest chunks.
    pub fn bottom_n(&self, n: usize) -> Vec<(ChunkId, ChunkHotness)> {
        let map = self.hotness_map.read();
        let mut entries: Vec<_> = map.iter().map(|(id, h)| (*id, *h)).collect();
        entries.sort_by(|a, b| a.1.score.partial_cmp(&b.1.score).unwrap_or(std::cmp::Ordering::Equal));
        entries.into_iter().take(n).collect()
    }
}

impl Default for HotnessTracker {
    fn default() -> Self {
        Self::new(10_000, Arc::new(TraceLog::new(1000)))
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::trace::current_timestamp;

    fn test_trace_log() -> Arc<TraceLog> {
        Arc::new(TraceLog::new(1000))
    }

    fn test_chunk_id(seed: u8) -> ChunkId {
        let mut id = [0u8; 32];
        id[0] = seed;
        ChunkId(id)
    }

    #[test]
    fn test_hotness_tracker_new() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_hotness_tracker_record_access() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        let id = test_chunk_id(1);
        tracker.record_access(id, 1024);

        assert_eq!(tracker.len(), 1);
        let hotness = tracker.get_hotness(&id).unwrap();
        assert_eq!(hotness.access_count, 1);
        assert_eq!(hotness.total_bytes, 1024);
    }

    #[test]
    fn test_hotness_tracker_find_hot_chunks() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        let id = test_chunk_id(1);

        // Access many times to make it hot
        for _ in 0..20 {
            tracker.record_access(id, 1024);
        }

        let hot = tracker.find_hot_chunks(0.5);
        assert!(!hot.is_empty());
    }

    #[test]
    fn test_hotness_tracker_find_cold_chunks() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        let id = test_chunk_id(1);

        // Single access — score ~0.456, not < 0.2
        tracker.record_access(id, 1024);
        let cold = tracker.find_cold_chunks(0.2);
        assert!(cold.is_empty());

        // Score ~0.456 < 0.5, so should be found
        let warm = tracker.find_cold_chunks(0.5);
        assert!(!warm.is_empty());
    }

    #[test]
    fn test_hotness_tracker_remove() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        let id = test_chunk_id(1);
        tracker.record_access(id, 1024);

        let removed = tracker.remove(&id);
        assert!(removed.is_some());
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_hotness_tracker_clear() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        for i in 0..10 {
            tracker.record_access(test_chunk_id(i), 1024);
        }
        assert_eq!(tracker.len(), 10);

        tracker.clear();
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_hotness_tracker_top_n() {
        let tracker = HotnessTracker::new(1000, test_trace_log());

        // Chunk 1: many accesses
        for _ in 0..20 {
            tracker.record_access(test_chunk_id(1), 1024);
        }
        // Chunk 2: few accesses
        tracker.record_access(test_chunk_id(2), 1024);

        let top = tracker.top_n(1);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].0, test_chunk_id(1));
    }

    #[test]
    fn test_hotness_tracker_bottom_n() {
        let tracker = HotnessTracker::new(1000, test_trace_log());

        // Chunk 1: many accesses
        for _ in 0..20 {
            tracker.record_access(test_chunk_id(1), 1024);
        }
        // Chunk 2: few accesses
        tracker.record_access(test_chunk_id(2), 1024);

        let bottom = tracker.bottom_n(1);
        assert_eq!(bottom.len(), 1);
        assert_eq!(bottom[0].0, test_chunk_id(2));
    }

    #[test]
    fn test_hotness_tracker_decay_all() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        let id = test_chunk_id(1);
        tracker.record_access(id, 1024);

        // Decay should not panic
        tracker.decay_all();

        let hotness = tracker.get_hotness(&id).unwrap();
        assert_eq!(hotness.access_count, 1); // Count unchanged
    }

    #[test]
    fn test_hotness_tracker_default() {
        let tracker = HotnessTracker::default();
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_hotness_tracker_set_provider() {
        let tracker = HotnessTracker::new(1000, test_trace_log());

        // Initially no provider
        assert!(!tracker.has_hotness_provider());
        assert!(tracker.hotness_provider().is_none());

        // Setting None should be a no-op
        tracker.set_hotness_provider(None);
        assert!(!tracker.has_hotness_provider());
    }

    #[test]
    fn test_hotness_state_empty() {
        let state = HotnessState::empty();
        assert_eq!(state.summary.total_regions, 0);
        assert_eq!(state.last_update, 0);
        assert!(state.history.is_empty());
    }

    #[test]
    fn test_hotness_tracker_sampling_interval() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        assert_eq!(tracker.sampling_interval(), Duration::from_secs(60));

        let custom = HotnessTracker::with_sampling_interval(
            1000,
            test_trace_log(),
            Duration::from_secs(30),
        );
        assert_eq!(custom.sampling_interval(), Duration::from_secs(30));
    }

    #[test]
    fn test_hotness_tracker_get_hotness_state() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        let state = tracker.get_hotness_state();
        // Without a provider, state should be empty
        assert_eq!(state.summary.total_regions, 0);
        assert_eq!(state.last_update, 0);
    }

    #[test]
    fn test_hotness_tracker_get_hot_regions_empty() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        let hot = tracker.get_hot_regions();
        assert!(hot.is_empty());
    }

    #[test]
    fn test_hotness_tracker_get_cold_regions_empty() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        let cold = tracker.get_cold_regions();
        assert!(cold.is_empty());
    }
}
