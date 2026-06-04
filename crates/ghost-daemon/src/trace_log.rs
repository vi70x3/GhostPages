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
#[derive(Debug)]
pub struct TraceLog {
    events: Arc<Mutex<Vec<TraceEvent>>>,
    max_events: usize,
}

impl TraceLog {
    /// Create a new trace log with the given maximum capacity.
    pub fn new(max_events: usize) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::with_capacity(max_events.min(1024)))),
            max_events,
        }
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
    pub fn record_now(&self, event: TraceEvent) {
        self.record(event);
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
}
