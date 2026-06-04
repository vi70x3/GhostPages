//! Placement policy trait and implementations for GhostPages.
//!
//! This module defines the [`PlacementPolicy`] trait that determines:
//! - **What** migrates (hotness threshold)
//! - **When** migration occurs (pressure triggers)
//! - **Priority** (LRU, LFU, custom)
//! - **Eviction order** (which chunk leaves a tier first)
//!
//! The policy is completely backend-agnostic — it knows nothing about
//! how or where data is stored.

pub mod policy;

pub use policy::{MigrationDecision, PlacementPolicy, PolicyError, SystemState, TierUsage};
