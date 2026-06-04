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

pub mod lru;
pub mod policy;
pub mod pressure;
pub mod weights;

pub use lru::{LruConfig, LruPolicy};
pub use policy::{PlacementPolicy, PolicyError};
pub use pressure::{PressureAwareConfig, PressureAwarePolicy};
pub use weights::{best_tier, tier_pressure_score, tier_weight};
