//! Event multiplexer for fan-out event distribution.
//!
//! [`EventMultiplexer`] receives events and fans them out to all registered
//! [`EventHandler`]s. Handlers are called sequentially — if a handler returns
//! an error, the error is logged but does not prevent the remaining handlers
//! from receiving the event.
//!
//! # Example
//!
//! ```
//! use std::future::Future;
//! use std::pin::Pin;
//!
//! use ghost_core::event_multiplexer::{EventMultiplexer, EventHandler};
//! use ghost_core::events::Event;
//!
//! struct PrintHandler;
//!
//! impl EventHandler for PrintHandler {
//!     fn handle(
//!         &self,
//!         event: &Event,
//!     ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>>
//!     {
//!         println!("Event: {}", event.event_name());
//!         Box::pin(async { Ok(()) })
//!     }
//! }
//! ```

use std::future::Future;
use std::pin::Pin;

use tokio::sync::mpsc;

use crate::events::Event;

/// A handler that receives events from the [`EventMultiplexer`].
///
/// Handlers must be `Send + Sync` so they can be shared across async tasks.
/// Implementors receive a reference to each event and return `Ok(())` on
/// success or an error if processing failed.
pub trait EventHandler: Send + Sync {
    /// Handle an event.
    ///
    /// Returns `Ok(())` on success. On error, the error is logged but the
    /// event is still delivered to remaining handlers.
    fn handle(
        &self,
        event: &Event,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>>;
}

/// Fan-out event distributor.
///
/// Receives events on an `mpsc` channel and delivers them to all registered
/// [`EventHandler`]s.
///
/// # Example
///
/// ```
/// use ghost_core::event_multiplexer::EventMultiplexer;
/// use ghost_core::events::Event;
///
/// let (tx, rx) = tokio::sync::mpsc::channel(256);
/// let multiplexer = EventMultiplexer::new(rx);
/// // Register handlers, then start the multiplexer.
/// ```
pub struct EventMultiplexer {
    rx: mpsc::Receiver<Event>,
    handlers: Vec<Box<dyn EventHandler>>,
}

impl EventMultiplexer {
    /// Create a new multiplexer from an `mpsc::Receiver<Event>`.
    pub fn new(rx: mpsc::Receiver<Event>) -> Self {
        Self {
            rx,
            handlers: Vec::new(),
        }
    }

    /// Register a handler.
    ///
    /// Consumes the multiplexer and returns a new one with the handler added.
    pub fn with_handler(mut self, handler: Box<dyn EventHandler>) -> Self {
        self.handlers.push(handler);
        self
    }

    /// Run the event loop, receiving events and dispatching to handlers.
    ///
    /// Returns when the channel is closed (all senders dropped).
    pub async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            for handler in &self.handlers {
                if let Err(err) = handler.handle(&event).await {
                    tracing::warn!(
                        event = event.event_name(),
                        category = event.category(),
                        error = %err,
                        "Event handler failed"
                    );
                }
            }
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
        _event: &Event,
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CountingHandler {
        counter: Arc<AtomicUsize>,
    }

    impl EventHandler for CountingHandler {
        fn handle(
            &self,
            _event: &Event,
        ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>>
        {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
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

        // Send events
        let event = Event::AllocationCreated {
            chunk_id: ChunkId::from_data(b"test"),
            tier: TierId::Ram,
            size: 1024,
            sequence_id: 0,
        };
        tx.send(event.clone()).await.unwrap();
        tx.send(event.clone()).await.unwrap();
        tx.send(event).await.unwrap();
        drop(tx);

        // Run the multiplexer
        multiplexer.run().await;

        assert_eq!(counter1.load(Ordering::SeqCst), 3);
        assert_eq!(counter2.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_noop_handler() {
        let handler = NoopHandler;
        let event = Event::OperationFailed {
            operation: "test".to_string(),
            reason: "test".to_string(),
            sequence_id: 0,
        };
        handler.handle(&event).await.unwrap();
    }
}
