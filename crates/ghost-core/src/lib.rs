//! # ghost-core
//!
//! Core types, errors, and utilities for the GhostPages memory-tiering system.
//!
//! This crate provides the foundational types used throughout the system:
//! - [`ChunkId`]: Content-addressed identifier using blake3 hashing
//! - [`ChunkMeta`]: Metadata for stored chunks
//! - [`TierId`]: Memory tier identifiers
//! - [`GhostError`]: Unified error type
//! - [`ChunkState`]: Chunk lifecycle state machine
//! - [`StateMachine`]: State transition enforcement
//! - [`PressureState`]: System pressure model
//! - [`TransferJob`]: Transfer job tracking
//! - [`TraceEvent`]: Trace log events
//! - [`Event`]: Unified event taxonomy for observability
//! - [`EventEmitter`]: Typed event emission
//! - [`EventMultiplexer`]: Fan-out event distribution
//! - [`TracingHandler`]: Event → structured tracing spans
//! - [`TransferDevice`]: Backend-neutral transfer device abstraction
//! - [`DmaPipeline`]: DMA-oriented transfer pipeline

pub mod dma_pipeline;
pub mod emitter;
pub mod error;
pub mod event_multiplexer;
pub mod events;
pub mod hardware;
pub mod hotness;
pub mod invariant_registry;
pub mod io_abstraction;
pub mod io_events;
pub mod state;
pub mod state_ownership;
pub mod time;
pub mod trace;
pub mod transfer;
pub mod tracing_bridge;
pub mod types;

// Re-export commonly used types
pub use emitter::EventEmitter;
pub use error::{GhostError, GhostResult};
pub use event_multiplexer::{EventHandler, EventMultiplexer};
pub use events::{BackendHealth, Event, EventRecord, InvariantSeverity};
pub use io_abstraction::{IoCompletion, IoRequest, IoScheduler};
pub use io_events::{IoEvent, IoOperation};
pub use state::{ChunkState, PressureState, StateMachine};
pub use time::{DeterministicClock, RealTimeProvider, TimeProvider};
pub use trace::{EvictionReason, TraceEvent};
pub use transfer::{TransferJob, TransferPriority, TransferState};
pub use types::{ChunkId, ChunkMeta, CompressionAlgorithm, TierId};
