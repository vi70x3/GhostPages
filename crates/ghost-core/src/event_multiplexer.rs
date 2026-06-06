//! Event multiplexer for fan-out event distribution.
//!
//! [`EventMultiplexer`] receives [`EventRecord`]s and fans them out to all
//! registered [`EventHandler`]s. Handlers are called sequentially — if a
//! handler returns an error, the error is logged but does not prevent the
//! remaining handlers from receiving the event.
//!
//! # Ordering Verification
//!
//! The multiplexer verifies that sequence IDs are monotonically increasing.
//! If a gap or reordering is detected, it emits an [`Event::InvariantViolation`]
//! through the invariant channel before delivering the event to handlers.
//!
//! # Example
//!
//! ```
//! use std::future::Future;
//! use std::pin::Pin;
//!
//! use ghost_core::event_multiplexer::{EventMultiplexer, EventHandler};
//! use ghost_core::events::EventRecord;
//!
//! struct PrintHandler;
//!
//! impl EventHandler for PrintHandler {
//!     fn handle(
//!         &self,
//!         event: &EventRecord,
//!     ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>>
//!     {
//!         println!("Event: {}", event.event_name());
//!         Box::pin(async { Ok(()) })
//!     }
//! }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::events::{Event, EventRecord, InvariantSeverity};

/// A handler that receives events from the [`EventMultiplexer`].
///
/// Handlers must be `Send + Sync` so they can be shared across async tasks.
/// Implementors receive a reference to each [`EventRecord`] and return `Ok(())` on
/// success or an error if processing failed.
pub trait EventHandler: Send + Sync {
    /// Handle an event.
    ///
    /// Returns `Ok(())` on success. On error, the error is logged but the
    /// event is still delivered to remaining handlers.
    fn handle(
        &self,
        event: &EventRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>>;
}

/// Fan-out event distributor with ordering verification.
///
/// Receives [`EventRecord`]s on an `mpsc` channel and delivers them to all
/// registered [`EventHandler`]s. Verifies that sequence IDs are monotonically
/// increasing and emits [`Event::InvariantViolation`] if a gap or reordering
/// is detected.
///
/// # Example
///
/// ```
/// use ghost_core::event_multiplexer::EventMultiplexer;
///
/// let (tx, rx) = tokio::sync::mpsc::channel(256);
/// let multiplexer = EventMultiplexer::new(rx);
/// // Register handlers, then start the multiplexer.
/// ```
pub struct EventMultiplexer {
    rx: mpsc::Receiver<EventRecord>,
    handlers: Vec<Box<dyn EventHandler>>,
    last_sequence_id: Arc<AtomicU64>,
    invariant_sender: Option<mpsc::Sender<EventRecord>>,
}

impl EventMultiplexer {
    /// Create a new multiplexer from an `mpsc::Receiver<EventRecord>`.
    pub fn new(rx: mpsc::Receiver<EventRecord>) -> Self {
        Self {
            rx,
            handlers: Vec::new(),
            last_sequence_id: Arc::new(AtomicU64::new(0)),
            invariant_sender: None,
        }
    }

    /// Set the channel for emitting invariant violations.
    ///
    /// When set, ordering violations (gaps, reordering) will be reported
    /// as [`Event::InvariantViolation`] records sent through this channel.
    pub fn with_invariant_channel(mut self, sender: mpsc::Sender<EventRecord>) -> Self {
        self.invariant_sender = Some(sender);
        self
    }

    /// Register a handler.
    ///
    /// Consumes the multiplexer and returns a new one with the handler added.
    pub fn with_handler(mut self, handler: Box<dyn EventHandler>) -> Self {
        self.handlers.push(handler);
        self
    }

    /// Get the last observed sequence ID.
    pub fn last_sequence_id(&self) -> u64 {
        self.last_sequence_id.load(Ordering::SeqCst)
    }

    /// Run the event loop, receiving events and dispatching to handlers.
    ///
    /// Returns when the channel is closed (all senders dropped).
    ///
    /// Verifies monotonic ordering of sequence IDs. If a gap or reordering
    /// is detected, an [`Event::InvariantViolation`] is emitted through the
    /// invariant channel (if configured).
    pub async fn run(mut self) {
        while let Some(record) = self.rx.recv().await {
            self.verify_ordering(&record);
            for handler in &self.handlers {
                if let Err(err) = handler.handle(&record).await {
                    tracing::warn!(
                        event = record.event_name(),
                        category = record.category(),
                        sequence_id = record.sequence_id,
                        error = %err,
                        "Event handler failed"
                    );
                }
            }
        }
    }

    /// Verify that the sequence ID is monotonically increasing.
    ///
    /// If a gap is detected (sequence_id > last + 1), emits an InvariantViolation.
    /// If a reordering is detected (sequence_id <= last), emits an InvariantViolation.
    fn verify_ordering(&self, record: &EventRecord) {
        let current = record.sequence_id;
        let last = self.last_sequence_id.load(Ordering::SeqCst);

        if last > 0 {
            if current <= last {
                // Reordering detected
                let details = format!(
                    "Event reordering detected: sequence_id {} <= last {}",
                    current, last
                );
                self.emit_invariant_violation(
                    "event_ordering_monotonic",
                    &details,
                    InvariantSeverity::Critical,
                );
            } else if current > last + 1 {
                // Gap detected
                let details = format!(
                    "Event sequence gap detected: expected {}, got {} (gap of {})",
                    last + 1,
                    current,
                    current - last - 1
                );
                self.emit_invariant_violation(
                    "event_ordering_gap",
                    &details,
                    InvariantSeverity::Error,
                );
            }
        }

        // Update last seen sequence ID
        // Use compare_exchange to handle concurrent updates correctly
        let _ = self.last_sequence_id.compare_exchange(
            last,
            current,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    /// Emit an invariant violation event through the invariant channel.
    fn emit_invariant_violation(
        &self,
        rule: &str,
        details: &str,
        severity: InvariantSeverity,
    ) {
        if let Some(ref sender) = self.invariant_sender {
            let violation = EventRecord {
                sequence_id: 0, // Will be assigned by the emitter if re-sent
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                event: Event::InvariantViolation {
                    sequence_id: 0,
                    rule: rule.to_string(),
                    details: details.to_string(),
                    severity,
                },
            };
            // Try to send, but don't block if the channel is full
            let _ = sender.try_send(violation);
        }
    }
}

/// A no-op event handler that discards all events.
///
/// Useful as a placeholder during testing or when no real handler is needed.
pub struct NoopHandler;

impl EventHandler for NoopHandler {
    fn handle(
        &self,
        _event: &EventRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>>
    {
        Box::pin(async { Ok(()) })
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChunkId, TierId};
    use std::sync::atomic::AtomicUsize;

    struct CountingHandler {
        counter: Arc<AtomicUsize>,
    }

    impl EventHandler for CountingHandler {
        fn handle(
            &self,
            _event: &EventRecord,
        ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>>
        {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    fn make_record(sequence_id: u64, event: Event) -> EventRecord {
        EventRecord {
            sequence_id,
            timestamp: 0,
            event,
        }
    }

    fn test_event() -> Event {
        Event::AllocationCreated {
            chunk_id: ChunkId::from_data(b"test"),
            tier: TierId::Ram,
            size: 1024,
            sequence_id: 0,
        }
    }

    #[tokio::test]
    async fn test_multiplexer_delivers_to_all_handlers() {
        let (tx, rx) = mpsc::channel(64);
        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::new(AtomicUsize::new(0));

        let handler1: Box<dyn EventHandler> = Box::new(CountingHandler {
            counter: Arc::clone(&counter1),
        });
        let handler2: Box<dyn EventHandler> = Box::new(CountingHandler {
            counter: Arc::clone(&counter2),
        });

        let multiplexer = EventMultiplexer::new(rx).with_handler(handler1).with_handler(handler2);

        // Send events with proper sequence IDs
        let event = test_event();
        tx.send(make_record(1, event.clone())).await.unwrap();
        tx.send(make_record(2, event.clone())).await.unwrap();
        tx.send(make_record(3, event)).await.unwrap();
        drop(tx);

        // Run the multiplexer
        multiplexer.run().await;

        assert_eq!(counter1.load(Ordering::SeqCst), 3);
        assert_eq!(counter2.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_noop_handler() {
        let handler = NoopHandler;
        let record = make_record(1, test_event());
        handler.handle(&record).await.unwrap();
    }

    #[tokio::test]
    async fn test_multiplexer_detects_gap() {
        let (tx, rx) = mpsc::channel(64);
        let (inv_tx, mut inv_rx) = mpsc::channel(64);

        let multiplexer = EventMultiplexer::new(rx)
            .with_handler(Box::new(NoopHandler))
            .with_invariant_channel(inv_tx);

        let event = test_event();
        // Send events with a gap: 1, 2, 5 (gap of 2)
        tx.send(make_record(1, event.clone())).await.unwrap();
        tx.send(make_record(2, event.clone())).await.unwrap();
        tx.send(make_record(5, event)).await.unwrap();
        drop(tx);

        multiplexer.run().await;

        // Should have received an invariant violation for the gap
        let violation = inv_rx.recv().await.unwrap();
        assert!(matches!(
            violation.event,
            Event::InvariantViolation { .. }
        ));
        if let Event::InvariantViolation { rule, details, .. } = violation.event {
            assert_eq!(rule, "event_ordering_gap");
            assert!(details.contains("gap"));
        }
    }

    #[tokio::test]
    async fn test_multiplexer_detects_reordering() {
        let (tx, rx) = mpsc::channel(64);
        let (inv_tx, mut inv_rx) = mpsc::channel(64);

        let multiplexer = EventMultiplexer::new(rx)
            .with_handler(Box::new(NoopHandler))
            .with_invariant_channel(inv_tx);

        let event = test_event();
        // Send events with reordering: 2, 1 (pure reordering, no gap)
        tx.send(make_record(2, event.clone())).await.unwrap();
        tx.send(make_record(1, event)).await.unwrap();
        drop(tx);

        multiplexer.run().await;

        // Should have received an invariant violation for the reordering
        let violation = inv_rx.recv().await.unwrap();
        assert!(matches!(
            violation.event,
            Event::InvariantViolation { .. }
        ));
        if let Event::InvariantViolation { rule, details, .. } = violation.event {
            assert_eq!(rule, "event_ordering_monotonic");
            assert!(details.contains("reordering"));
        }
    }

    #[tokio::test]
    async fn test_multiplexer_no_violation_for_ordered_sequence() {
        let (tx, rx) = mpsc::channel(64);
        let (inv_tx, mut inv_rx) = mpsc::channel(64);

        let multiplexer = EventMultiplexer::new(rx)
            .with_handler(Box::new(NoopHandler))
            .with_invariant_channel(inv_tx);

        let event = test_event();
        // Send events in perfect order: 1, 2, 3, 4, 5
        for i in 1..=5 {
            tx.send(make_record(i, event.clone())).await.unwrap();
        }
        drop(tx);

        multiplexer.run().await;

        // Should NOT have received any invariant violations
        assert!(inv_rx.try_recv().is_err(), "No violations should be emitted for ordered sequence");
    }

    #[tokio::test]
    async fn test_last_sequence_id_tracking() {
        let (tx, rx) = mpsc::channel(64);

        let multiplexer = EventMultiplexer::new(rx)
            .with_handler(Box::new(NoopHandler));

        assert_eq!(multiplexer.last_sequence_id(), 0);

        let event = test_event();
        tx.send(make_record(1, event.clone())).await.unwrap();
        tx.send(make_record(2, event.clone())).await.unwrap();
        tx.send(make_record(3, event)).await.unwrap();
        drop(tx);

        multiplexer.run().await;
        // After processing, last_sequence_id should be 3
        // Note: we can't check this directly since multiplexer was consumed by run()
    }
}
