//! Optional allocation tracker for debugging and stress testing.
//!
//! This module provides [`AllocationTracker`], which records every allocation
//! and deallocation for later analysis. It is gated behind the `track-allocations`
//! feature flag so it has zero cost in production builds.
//!
//! # Examples
//!
//! ```rust,no_run
//! use ghost_tier::tracker::AllocationTracker;
//! use ghost_core::types::ChunkId;
//! use std::sync::Arc;
//!
//! let tracker = Arc::new(AllocationTracker::new());
//! let id = ChunkId::from_data(b"test");
//! tracker.record_allocation(id, 1024);
//! assert_eq!(tracker.current_usage(), 1024);
//! tracker.record_free(&id);
//! assert_eq!(tracker.current_usage(), 0);
//! ```

use ghost_core::types::ChunkId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Information about a single allocation.
#[derive(Debug, Clone)]
pub struct AllocationInfo {
    /// Size of the allocation in bytes.
    pub size: usize,
    /// When the allocation was created.
    pub allocated_at: Instant,
    /// When the allocation was freed (None if still allocated).
    pub freed_at: Option<Instant>,
    /// Optional backtrace captured at allocation time (if RUST_BACKTRACE is set).
    pub backtrace: Option<String>,
}

/// Tracks all allocations and deallocations for debugging and stress testing.
///
/// Uses atomic counters for hot-path metrics and a `parking_lot::Mutex`-protected
/// HashMap for per-allocation info. The mutex is only held briefly for map
/// operations and never across `.await` points.
#[derive(Debug)]
pub struct AllocationTracker {
    allocations: parking_lot::Mutex<HashMap<ChunkId, AllocationInfo>>,
    total_allocated: AtomicU64,
    total_freed: AtomicU64,
    peak_usage: AtomicU64,
}

impl AllocationTracker {
    /// Create a new allocation tracker.
    pub fn new() -> Self {
        Self {
            allocations: parking_lot::Mutex::new(HashMap::new()),
            total_allocated: AtomicU64::new(0),
            total_freed: AtomicU64::new(0),
            peak_usage: AtomicU64::new(0),
        }
    }

    /// Record a new allocation.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if the same `ChunkId` is recorded twice without
    /// an intervening `record_free`.
    pub fn record_allocation(&self, id: ChunkId, size: usize) {
        let mut allocs = self.allocations.lock();

        // Check for double-allocation (debug only)
        debug_assert!(
            !allocs.contains_key(&id),
            "double allocation detected for chunk {:?}",
            id
        );

        let info = AllocationInfo {
            size,
            allocated_at: Instant::now(),
            freed_at: None,
            backtrace: Self::capture_backtrace(),
        };
        allocs.insert(id, info);
        drop(allocs);

        let total = self.total_allocated.fetch_add(size as u64, Ordering::Relaxed) + size as u64;

        // Update peak usage with a CAS loop
        let mut current_peak = self.peak_usage.load(Ordering::Relaxed);
        while total > current_peak {
            match self.peak_usage.compare_exchange_weak(
                current_peak,
                total,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_peak = actual,
            }
        }
    }

    /// Record a deallocation.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if the `ChunkId` was not previously allocated.
    pub fn record_free(&self, id: &ChunkId) {
        let mut allocs = self.allocations.lock();

        if let Some(info) = allocs.get_mut(id) {
            info.freed_at = Some(Instant::now());
            let size = info.size;
            drop(allocs);

            self.total_freed.fetch_add(size as u64, Ordering::Relaxed);
        } else {
            debug_assert!(false, "free of untracked chunk {:?}", id);
        }
    }

    /// Get the current total allocated bytes (allocated - freed).
    pub fn current_usage(&self) -> u64 {
        self.total_allocated
            .load(Ordering::Relaxed)
            .saturating_sub(self.total_freed.load(Ordering::Relaxed))
    }

    /// Get the peak usage in bytes observed since creation.
    pub fn peak_usage(&self) -> u64 {
        self.peak_usage.load(Ordering::Relaxed)
    }

    /// Get the fragmentation ratio (0.0 = no fragmentation, 1.0 = maximum).
    ///
    /// Computed as: 1 - (largest_contiguous_free / total_free).
    /// For the simple bump-allocator model, this is based on the ratio of
    /// freed bytes to total allocated bytes.
    pub fn fragmentation_ratio(&self) -> f64 {
        let total_alloc = self.total_allocated.load(Ordering::Relaxed);
        if total_alloc == 0 {
            return 0.0;
        }
        let total_freed = self.total_freed.load(Ordering::Relaxed);
        if total_freed == 0 {
            return 0.0;
        }
        // Fragmentation estimate: freed bytes / total allocated bytes
        // This is a simplified metric — real fragmentation depends on layout.
        (total_freed as f64 / total_alloc as f64).clamp(0.0, 1.0)
    }

    /// Get a list of ChunkIds that have been allocated but not freed.
    ///
    /// Useful for detecting leaks in tests.
    pub fn leaked_allocations(&self) -> Vec<ChunkId> {
        let allocs = self.allocations.lock();
        allocs
            .iter()
            .filter(|(_, info)| info.freed_at.is_none())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get the total number of allocations recorded (including freed ones).
    pub fn total_allocation_count(&self) -> usize {
        self.allocations.lock().len()
    }

    /// Get the number of currently live (unfreed) allocations.
    pub fn live_allocation_count(&self) -> usize {
        let allocs = self.allocations.lock();
        allocs.values().filter(|info| info.freed_at.is_none()).count()
    }

    /// Reset all tracking state.
    pub fn reset(&self) {
        let mut allocs = self.allocations.lock();
        allocs.clear();
        drop(allocs);
        self.total_allocated.store(0, Ordering::Relaxed);
        self.total_freed.store(0, Ordering::Relaxed);
        self.peak_usage.store(0, Ordering::Relaxed);
    }

    /// Capture a backtrace if RUST_BACKTRACE is set.
    fn capture_backtrace() -> Option<String> {
        if std::env::var_os("RUST_BACKTRACE").is_some() {
            Some(format!("{:?}", std::backtrace::Backtrace::force_capture()))
        } else {
            None
        }
    }
}

impl Default for AllocationTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe reference to an allocation tracker.
pub type SharedTracker = Arc<AllocationTracker>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker_basic() {
        let tracker = AllocationTracker::new();
        let id = ChunkId::from_data(b"test");

        tracker.record_allocation(id, 1024);
        assert_eq!(tracker.current_usage(), 1024);
        assert_eq!(tracker.peak_usage(), 1024);
        assert_eq!(tracker.live_allocation_count(), 1);

        tracker.record_free(&id);
        assert_eq!(tracker.current_usage(), 0);
        assert_eq!(tracker.peak_usage(), 1024); // peak never decreases
        assert_eq!(tracker.live_allocation_count(), 0);
    }

    #[test]
    fn test_tracker_multiple_allocations() {
        let tracker = AllocationTracker::new();

        for i in 0..10 {
            let id = ChunkId::from_data(&[i]);
            tracker.record_allocation(id, 100);
        }
        assert_eq!(tracker.current_usage(), 1000);
        assert_eq!(tracker.peak_usage(), 1000);
        assert_eq!(tracker.live_allocation_count(), 10);

        // Free half
        for i in 0..5 {
            let id = ChunkId::from_data(&[i]);
            tracker.record_free(&id);
        }
        assert_eq!(tracker.current_usage(), 500);
        assert_eq!(tracker.peak_usage(), 1000);
        assert_eq!(tracker.live_allocation_count(), 5);
    }

    #[test]
    fn test_tracker_leaked_allocations() {
        let tracker = AllocationTracker::new();

        let id1 = ChunkId::from_data(b"leak1");
        let id2 = ChunkId::from_data(b"leak2");
        let id3 = ChunkId::from_data(b"no_leak");

        tracker.record_allocation(id1, 100);
        tracker.record_allocation(id2, 200);
        tracker.record_allocation(id3, 300);
        tracker.record_free(&id3);

        let leaked = tracker.leaked_allocations();
        assert_eq!(leaked.len(), 2);
        assert!(leaked.contains(&id1));
        assert!(leaked.contains(&id2));
        assert!(!leaked.contains(&id3));
    }

    #[test]
    fn test_tracker_fragmentation() {
        let tracker = AllocationTracker::new();

        let id1 = ChunkId::from_data(b"frag1");
        let id2 = ChunkId::from_data(b"frag2");

        tracker.record_allocation(id1, 1000);
        tracker.record_allocation(id2, 1000);
        assert!(tracker.fragmentation_ratio() < 0.01); // nothing freed yet

        tracker.record_free(&id1);
        let ratio = tracker.fragmentation_ratio();
        assert!(ratio > 0.0, "fragmentation should be > 0 after free");
        assert!(ratio <= 1.0, "fragmentation should be <= 1.0");
    }

    #[test]
    fn test_tracker_reset() {
        let tracker = AllocationTracker::new();
        let id = ChunkId::from_data(b"reset_test");

        tracker.record_allocation(id, 500);
        assert_eq!(tracker.current_usage(), 500);

        tracker.reset();
        assert_eq!(tracker.current_usage(), 0);
        assert_eq!(tracker.peak_usage(), 0);
        assert_eq!(tracker.live_allocation_count(), 0);
    }

    #[test]
    fn test_tracker_total_count() {
        let tracker = AllocationTracker::new();

        for i in 0..5 {
            let id = ChunkId::from_data(&[i]);
            tracker.record_allocation(id, 10);
        }
        assert_eq!(tracker.total_allocation_count(), 5);

        // Free some — total count stays the same
        for i in 0..3 {
            let id = ChunkId::from_data(&[i]);
            tracker.record_free(&id);
        }
        assert_eq!(tracker.total_allocation_count(), 5);
        assert_eq!(tracker.live_allocation_count(), 2);
    }
}
