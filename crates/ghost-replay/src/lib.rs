//! Trace recording and replay system for GhostPages.
//!
//! This crate provides functionality to record migration events and replay
//! them for tuning, A/B testing, regression testing, and offline
//! experimentation.
//!
//! # Phase 0 Status
//!
//! This is a skeleton implementation. Full trace recording and replay
//! functionality will be implemented in Phase 5.

#![warn(missing_docs)]

/// Trace event types.
pub mod event;

/// Trace recorder.
pub mod recorder;

/// Trace replayer.
pub mod replayer;

pub use event::TraceEvent;
pub use recorder::TraceRecorder;
pub use replayer::TraceReplayer;
