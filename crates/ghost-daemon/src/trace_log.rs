//! Append-only trace log for the GhostPages daemon.
//!
//! Records every meaningful event in the transfer pipeline for
//! observability, debugging, and replay.

use std::sync::Arc;

use ghost_core::trace::{current_timestamp, TraceEvent};

use parking_lot::Mutex;

/// Append-only event log.
///
/// Records every event in the transfer pipeline with timestamps.
/// When the log reaches capacity, oldest events are discarded.
pub struct TraceLog {
    events: Arc<Mutex<Vec<TraceEvent>>>,
    max_events: usize,
    clock: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
}

impl std::fmt::Debug for TraceLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceLog")
            .field("max_events", &self.max_events)
            .field("event_count", &self.events.lock().len())
            .finish()
    }
}

impl TraceLog {
    /// Create a new trace log with the given maximum capacity.
    pub fn new(max_events: usize) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::with_capacity(max_events.min(1024)))),
            max_events,
            clock: None,
        }
    }

    /// Set a mock clock function for deterministic testing.
    ///
    /// When set, `record_now` will use this function instead of `current_timestamp()`
    /// to generate timestamps for events.
    pub fn set_clock(&mut self, clock: Arc<dyn Fn() -> u64 + Send + Sync>) {
        self.clock = Some(clock);
    }

    /// Record a new trace event.
    ///
    /// If the log is at capacity, the oldest event is removed to make room.
    pub fn record(&self, event: TraceEvent) {
        let mut events = self.events.lock();
        if events.len() >= self.max_events {
            // Remove the oldest event (front of vec)
            // For efficiency with large logs, a ring buffer would be better,
            // but for Phase 1 this is simple and correct.
            events.remove(0);
        }
        events.push(event);
    }

    /// Get all recorded events.
    pub fn get_events(&self) -> Vec<TraceEvent> {
        self.events.lock().clone()
    }

    /// Get events recorded since the given timestamp (inclusive).
    pub fn get_events_since(&self, timestamp: u64) -> Vec<TraceEvent> {
        self.events
            .lock()
            .iter()
            .filter(|e| e.timestamp() >= timestamp)
            .cloned()
            .collect()
    }

    /// Get the number of events currently stored.
    pub fn len(&self) -> usize {
        self.events.lock().len()
    }

    /// Check if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.events.lock().is_empty()
    }

    /// Clear all events from the log.
    pub fn clear(&self) {
        self.events.lock().clear();
    }

    /// Get the maximum capacity of the log.
    pub fn capacity(&self) -> usize {
        self.max_events
    }

    /// Get the current timestamp and record an event in one call.
    ///
    /// If a mock clock is set via `set_clock`, uses the mock clock for the
    /// timestamp instead of the real clock.
    pub fn record_now(&self, mut event: TraceEvent) {
        if let Some(ref clock) = self.clock {
            let ts = clock();
            set_timestamp(&mut event, ts);
        }
        self.record(event);
    }
}

/// Helper to set the timestamp on any TraceEvent variant.
fn set_timestamp(event: &mut TraceEvent, ts: u64) {
    match event {
        TraceEvent::ChunkCreated { timestamp, .. } => *timestamp = ts,
        TraceEvent::ChunkStateChanged { timestamp, .. } => *timestamp = ts,
        TraceEvent::ChunkDeleted { timestamp, .. } => *timestamp = ts,
        TraceEvent::TransferQueued { timestamp, .. } => *timestamp = ts,
        TraceEvent::TransferStarted { timestamp, .. } => *timestamp = ts,
        TraceEvent::TransferCompleted { timestamp, .. } => *timestamp = ts,
        TraceEvent::TransferFailed { timestamp, .. } => *timestamp = ts,
        TraceEvent::TransferRetry { timestamp, .. } => *timestamp = ts,
        TraceEvent::TransferCancelled { timestamp, .. } => *timestamp = ts,
        TraceEvent::PressureSample { timestamp, .. } => *timestamp = ts,
        TraceEvent::PressureAlert { timestamp, .. } => *timestamp = ts,
        TraceEvent::PolicyDecision { timestamp, .. } => *timestamp = ts,
        TraceEvent::Eviction { timestamp, .. } => *timestamp = ts,
        TraceEvent::DaemonStarted { timestamp, .. } => *timestamp = ts,
        TraceEvent::DaemonStopping { timestamp, .. } => *timestamp = ts,
        TraceEvent::BackendRegistered { timestamp, .. } => *timestamp = ts,
        TraceEvent::WorkerSpawned { timestamp, .. } => *timestamp = ts,
        TraceEvent::WorkerStopped { timestamp, .. } => *timestamp = ts,
        TraceEvent::IpcRequestReceived { timestamp, .. } => *timestamp = ts,
        TraceEvent::IpcResponseSent { timestamp, .. } => *timestamp = ts,
        TraceEvent::IpcConnectionAccepted { timestamp, .. } => *timestamp = ts,
        TraceEvent::IpcConnectionClosed { timestamp, .. } => *timestamp = ts,
        TraceEvent::CompressionStarted { timestamp, .. } => *timestamp = ts,
        TraceEvent::CompressionCompleted { timestamp, .. } => *timestamp = ts,
        TraceEvent::DecompressionStarted { timestamp, .. } => *timestamp = ts,
        TraceEvent::DecompressionCompleted { timestamp, .. } => *timestamp = ts,
    }
}

impl Default for TraceLog {
    fn default() -> Self {
        Self::new(10000)
    }
}

impl Clone for TraceLog {
    fn clone(&self) -> Self {
        Self {
            events: Arc::new(Mutex::new(self.events.lock().clone())),
            max_events: self.max_events,
            clock: self.clock.clone(),
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::ChunkState;
    use ghost_core::types::{ChunkId, TierId};

    #[test]
    fn test_trace_log_new() {
        let log = TraceLog::new(100);
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert_eq!(log.capacity(), 100);
    }

    #[test]
    fn test_trace_log_record() {
        let log = TraceLog::new(100);
        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test"),
            size: 1024,
            tier: TierId::Ram,
            timestamp: current_timestamp(),
        };
        log.record(event);
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn test_trace_log_get_events() {
        let log = TraceLog::new(100);
        let event1 = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"chunk1"),
            size: 100,
            tier: TierId::Ram,
            timestamp: 1000,
        };
        let event2 = TraceEvent::ChunkStateChanged {
            chunk_id: ChunkId::from_data(b"chunk2"),
            from: ChunkState::Allocated,
            to: ChunkState::Stored,
            timestamp: 2000,
        };
        log.record(event1.clone());
        log.record(event2.clone());

        let events = log.get_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].timestamp(), 1000);
        assert_eq!(events[1].timestamp(), 2000);
    }

    #[test]
    fn test_trace_log_get_events_since() {
        let log = TraceLog::new(100);
        let event1 = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"chunk1"),
            size: 100,
            tier: TierId::Ram,
            timestamp: 1000,
        };
        let event2 = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"chunk2"),
            size: 200,
            tier: TierId::Ram,
            timestamp: 2000,
        };
        let event3 = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"chunk3"),
            size: 300,
            tier: TierId::Ram,
            timestamp: 3000,
        };
        log.record(event1);
        log.record(event2.clone());
        log.record(event3.clone());

        let events = log.get_events_since(2000);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].timestamp(), 2000);
        assert_eq!(events[1].timestamp(), 3000);
    }

    #[test]
    fn test_trace_log_clear() {
        let log = TraceLog::new(100);
        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test"),
            size: 100,
            tier: TierId::Ram,
            timestamp: 1000,
        };
        log.record(event);
        assert_eq!(log.len(), 1);
        log.clear();
        assert!(log.is_empty());
    }

    #[test]
    fn test_trace_log_capacity_overflow() {
        let log = TraceLog::new(3);
        for i in 0..5 {
            let event = TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(format!("chunk{}", i).as_bytes()),
                size: 100,
                tier: TierId::Ram,
                timestamp: i as u64,
            };
            log.record(event);
        }
        // Should only keep the last 3 events
        assert_eq!(log.len(), 3);
        let events = log.get_events();
        assert_eq!(events.len(), 3);
        // Oldest should be timestamp 2 (first two were evicted)
        assert_eq!(events[0].timestamp(), 2);
        assert_eq!(events[1].timestamp(), 3);
        assert_eq!(events[2].timestamp(), 4);
    }

    #[test]
    fn test_trace_log_clone() {
        let log = TraceLog::new(100);
        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test"),
            size: 100,
            tier: TierId::Ram,
            timestamp: 1000,
        };
        log.record(event);

        let cloned = log.clone();
        assert_eq!(cloned.len(), 1);

        // Modifying clone should not affect original
        let event2 = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test2"),
            size: 200,
            tier: TierId::Ram,
            timestamp: 2000,
        };
        cloned.record(event2);
        assert_eq!(log.len(), 1);
        assert_eq!(cloned.len(), 2);
    }

    #[test]
    fn test_trace_log_default() {
        let log = TraceLog::default();
        assert_eq!(log.capacity(), 10000);
        assert!(log.is_empty());
    }

    #[test]
    fn test_trace_log_mock_clock() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let mock_time = Arc::new(AtomicU64::new(42));
        let mock_time_clone = mock_time.clone();
        let clock = Arc::new(move || mock_time_clone.load(Ordering::Relaxed));

        let mut log = TraceLog::new(100);
        log.set_clock(clock);

        let event = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test"),
            size: 100,
            tier: TierId::Ram,
            timestamp: 9999, // Will be overwritten by mock clock
        };
        log.record_now(event);

        let events = log.get_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].timestamp(), 42);

        // Advance mock clock and record again
        mock_time.store(100, Ordering::Relaxed);
        let event2 = TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"test2"),
            size: 200,
            tier: TierId::Ram,
            timestamp: 9999,
        };
        log.record_now(event2);

        let events = log.get_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].timestamp(), 100);
    }
}
