//! Hotness tracker for chunk access pattern analysis.
//!
//! Maintains a map of chunk hotness scores, updated on each access.
//! Provides methods to query hotness, find hot/cold chunks, and
//! periodically decay recency scores.

use std::collections::HashMap;
use std::sync::Arc;

use ghost_core::hotness::ChunkHotness;
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::types::{ChunkId, TierId};

use parking_lot::RwLock;

use crate::trace_log::TraceLog;

/// Hotness tracker that maintains access pattern analysis for all chunks.
pub struct HotnessTracker {
    hotness_map: Arc<RwLock<HashMap<ChunkId, ChunkHotness>>>,
    trace_log: Arc<TraceLog>,
    /// Maximum number of chunks to track.
    max_tracked: usize,
}

impl HotnessTracker {
    /// Create a new hotness tracker.
    pub fn new(max_tracked: usize, trace_log: Arc<TraceLog>) -> Self {
        Self {
            hotness_map: Arc::new(RwLock::new(HashMap::with_capacity(max_tracked))),
            trace_log,
            max_tracked,
        }
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

// ─── Tests ────────────────────────────────────────────────────────────────────

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
        for i in 0..20 {
            tracker.record_access(id, 1024);
        }

        let hot = tracker.find_hot_chunks(0.5);
        assert!(!hot.is_empty());
    }

    #[test]
    fn test_hotness_tracker_find_cold_chunks() {
        let tracker = HotnessTracker::new(1000, test_trace_log());
        let id = test_chunk_id(1);

        // Single access with old timestamp = cold (recency decays)
        let now = current_timestamp();
        tracker.record_access(id, 1024);
        // Manually decay by recording with a much later timestamp
        // Since we can't directly set time, we use the hotness tracker's
        // decay mechanism: record at time, then the score is based on recency
        // Actually, a single access has high recency (1.0), so it won't be cold.
        // Instead, verify that find_cold_chunks returns empty for a single access
        // (which is correct behavior - a single recent access is not "cold")
        let cold = tracker.find_cold_chunks(0.2);
        // Score after single access: ~0.456 (not < 0.2), so should be empty
        assert!(cold.is_empty());

        // But if we use a very low threshold, it should still not be cold
        // because the score is ~0.456
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
}
