//! Chunk hotness tracking for pressure-driven migration.
//!
//! Tracks access frequency, recency, and regularity to compute a composite
//! hotness score for each chunk. Higher scores indicate hotter (more frequently
//! accessed) chunks that should be kept in faster tiers.

use crate::types::ChunkId;

/// Access history entry for regularity tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessRecord {
    /// Timestamp of the access.
    pub timestamp: u64,
    /// Size of the access in bytes.
    pub size: usize,
}

/// Hotness score components for a chunk.
///
/// The composite hotness score is a weighted combination of:
/// - **Frequency**: How often the chunk is accessed
/// - **Recency**: How recently the chunk was accessed
/// - **Regularity**: How predictable the access pattern is
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChunkHotness {
    /// Chunk identifier.
    pub chunk_id: ChunkId,
    /// Total number of accesses.
    pub access_count: u64,
    /// Timestamp of the last access.
    pub last_accessed: u64,
    /// Timestamp of the first access.
    pub first_accessed: u64,
    /// Composite hotness score in [0.0, 1.0].
    pub score: f32,
    /// Frequency component in [0.0, 1.0].
    pub frequency_score: f32,
    /// Recency component in [0.0, 1.0].
    pub recency_score: f32,
    /// Regularity component in [0.0, 1.0].
    pub regularity_score: f32,
    /// Total bytes accessed across all operations.
    pub total_bytes: u64,
}

impl ChunkHotness {
    /// Create a new hotness tracker for a chunk.
    pub fn new(chunk_id: ChunkId, now: u64) -> Self {
        Self {
            chunk_id,
            access_count: 0,
            last_accessed: now,
            first_accessed: now,
            score: 0.0,
            frequency_score: 0.0,
            recency_score: 0.0,
            regularity_score: 0.0,
            total_bytes: 0,
        }
    }

    /// Record an access to this chunk.
    pub fn record_access(&mut self, now: u64, size: usize) {
        self.access_count += 1;
        self.last_accessed = now;
        self.total_bytes += size as u64;

        // Update first_accessed only on the very first access
        if self.access_count == 1 {
            self.first_accessed = now;
        }

        // Recency decays exponentially: most recent = 1.0
        // Half-life of 60 seconds
        let age = now.saturating_sub(self.last_accessed);
        self.recency_score = if age == 0 {
            1.0
        } else {
            (-0.693 * age as f64 / 60.0).exp() as f32
        };

        self.recompute_score();
    }

    /// Compute the composite hotness score from components.
    pub fn recompute_score(&mut self) {
        // Frequency: sigmoid-like curve saturating at ~100 accesses
        self.frequency_score = 1.0 - 1.0 / (1.0 + self.access_count as f32 / 10.0);

        // Recency: exponential decay with 60s half-life
        // Already updated in record_access, but recompute for explicit calls
        // (last_accessed is already set, so this is a no-op unless called separately)

        // Regularity: based on coefficient of variation of inter-access intervals
        // High regularity = predictable pattern = keep in fast tier
        // This is a simplified placeholder; full implementation would track intervals
        self.regularity_score = if self.access_count > 2 {
            0.5 // Moderate regularity for chunks with multiple accesses
        } else {
            0.1 // Low regularity for rarely accessed chunks
        };

        // Weighted combination
        self.score = 0.4 * self.frequency_score
            + 0.4 * self.recency_score
            + 0.2 * self.regularity_score;

        // Clamp to [0.0, 1.0]
        self.score = self.score.clamp(0.0, 1.0);
    }

    /// Update recency based on the current time.
    ///
    /// Should be called periodically to decay recency for chunks that
    /// haven't been accessed recently.
    pub fn update_recency(&mut self, now: u64) {
        let age = now.saturating_sub(self.last_accessed);
        self.recency_score = if age == 0 {
            1.0
        } else {
            (-0.693 * age as f64 / 60.0).exp() as f32
        };
        self.recompute_score();
    }

    /// Check if this chunk is considered "hot" (should be in a fast tier).
    pub fn is_hot(&self) -> bool {
        self.score >= 0.5
    }

    /// Check if this chunk is considered "cold" (candidate for eviction).
    pub fn is_cold(&self) -> bool {
        self.score < 0.2
    }

    /// Get the access frequency (accesses per second since first access).
    pub fn access_frequency(&self, now: u64) -> f64 {
        let elapsed = now.saturating_sub(self.first_accessed);
        if elapsed == 0 {
            return 0.0;
        }
        self.access_count as f64 / elapsed as f64
    }
}

impl Default for ChunkHotness {
    fn default() -> Self {
        Self::new(ChunkId([0u8; 32]), 0)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_chunk_id() -> ChunkId {
        let mut id = [0u8; 32];
        id[0] = 1;
        ChunkId(id)
    }

    #[test]
    fn test_chunk_hotness_new() {
        let hotness = ChunkHotness::new(test_chunk_id(), 1000);
        assert_eq!(hotness.access_count, 0);
        assert_eq!(hotness.score, 0.0);
        assert!(!hotness.is_hot());
        assert!(hotness.is_cold());
    }

    #[test]
    fn test_chunk_hotness_record_access() {
        let mut hotness = ChunkHotness::new(test_chunk_id(), 1000);
        hotness.record_access(1000, 1024);
        assert_eq!(hotness.access_count, 1);
        assert_eq!(hotness.last_accessed, 1000);
        assert_eq!(hotness.total_bytes, 1024);
        assert!(hotness.recency_score > 0.9); // Just accessed
    }

    #[test]
    fn test_chunk_hotness_is_hot_after_many_accesses() {
        let mut hotness = ChunkHotness::new(test_chunk_id(), 0);
        for i in 0..20 {
            hotness.record_access(i * 10, 1024);
        }
        assert!(hotness.is_hot());
        assert!(!hotness.is_cold());
    }

    #[test]
    fn test_chunk_hotness_is_cold_when_idle() {
        let mut hotness = ChunkHotness::new(test_chunk_id(), 0);
        hotness.record_access(0, 1024);
        hotness.update_recency(10_000); // 10k seconds later
        assert!(hotness.is_cold());
    }

    #[test]
    fn test_chunk_hotness_recompute_score() {
        let mut hotness = ChunkHotness::new(test_chunk_id(), 0);
        hotness.record_access(0, 1024);
        hotness.record_access(100, 2048);
        hotness.recompute_score();
        assert!(hotness.score > 0.0);
        assert!(hotness.score <= 1.0);
    }

    #[test]
    fn test_chunk_hotness_access_frequency() {
        let mut hotness = ChunkHotness::new(test_chunk_id(), 0);
        hotness.record_access(0, 1024);
        hotness.record_access(100, 1024);
        let freq = hotness.access_frequency(100);
        assert!(freq > 0.0);
    }

    #[test]
    fn test_chunk_hotness_score_clamped() {
        let mut hotness = ChunkHotness::new(test_chunk_id(), 0);
        // Simulate extreme values
        hotness.frequency_score = 1.0;
        hotness.recency_score = 1.0;
        hotness.regularity_score = 1.0;
        hotness.recompute_score();
        assert!(hotness.score <= 1.0);
    }

    #[test]
    fn test_chunk_hotness_first_accessed() {
        let mut hotness = ChunkHotness::new(test_chunk_id(), 500);
        hotness.record_access(1000, 1024);
        hotness.record_access(2000, 1024);
        // first_accessed should be set to the time of the first record_access call
        assert_eq!(hotness.first_accessed, 1000);
    }

    #[test]
    fn test_chunk_hotness_default() {
        let hotness = ChunkHotness::default();
        assert_eq!(hotness.access_count, 0);
        assert_eq!(hotness.score, 0.0);
    }
}
