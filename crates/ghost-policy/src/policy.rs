//! Placement policy trait definition.
//!
//! This module defines the [`PlacementPolicy`] trait — the core abstraction
//! for deciding where chunks should reside in the memory hierarchy.

use async_trait::async_trait;
use ghost_core::types::{ChunkId, ChunkMeta, PressureLevel, TierId};
use std::collections::HashMap;

/// Errors from placement policy operations.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    /// No migration candidates available.
    #[error("no migration candidates available")]
    NoCandidates,

    /// Policy configuration error.
    #[error("invalid policy configuration: {0}")]
    InvalidConfig(String),

    /// Internal policy error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Usage statistics for a single tier.
#[derive(Debug, Clone, Default)]
pub struct TierUsage {
    /// Total capacity in bytes.
    pub capacity: usize,

    /// Currently used bytes.
    pub used: usize,

    /// Number of chunks stored in this tier.
    pub chunk_count: usize,
}

impl TierUsage {
    /// Create a new TierUsage.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            used: 0,
            chunk_count: 0,
        }
    }

    /// Get the usage ratio (0.0 = empty, 1.0 = full).
    pub fn usage_ratio(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.used as f64 / self.capacity as f64
        }
    }
}

/// System state snapshot provided to the placement policy.
///
/// Contains all information needed to make migration and eviction decisions.
#[derive(Debug, Clone)]
pub struct SystemState {
    /// Usage statistics per tier.
    pub tier_usage: HashMap<TierId, TierUsage>,

    /// Metadata for all known chunks.
    pub chunks: HashMap<ChunkId, ChunkMeta>,

    /// Current system memory pressure level.
    pub pressure_level: PressureLevel,
}

impl SystemState {
    /// Create a new empty system state.
    pub fn new() -> Self {
        Self {
            tier_usage: HashMap::new(),
            chunks: HashMap::new(),
            pressure_level: PressureLevel::Normal,
        }
    }
}

impl Default for SystemState {
    fn default() -> Self {
        Self::new()
    }
}

/// A migration decision produced by the placement policy.
#[derive(Debug, Clone)]
pub struct MigrationDecision {
    /// The chunk to migrate.
    pub chunk_id: ChunkId,

    /// Source tier.
    pub source_tier: TierId,

    /// Target tier.
    pub target_tier: TierId,

    /// Migration priority (higher = more urgent).
    pub priority: u8,
}

impl MigrationDecision {
    /// Create a new migration decision.
    pub fn new(
        chunk_id: ChunkId,
        source_tier: TierId,
        target_tier: TierId,
        priority: u8,
    ) -> Self {
        Self {
            chunk_id,
            source_tier,
            target_tier,
            priority,
        }
    }
}

/// Placement policy trait — decides where chunks should reside.
///
/// This trait is completely backend-agnostic. It only makes migration
/// and eviction decisions based on system state and chunk metadata.
///
/// # Concurrency
///
/// Implementations must be `Send + Sync + 'static`. The trait uses
/// `async-trait` so all methods are async.
#[async_trait]
pub trait PlacementPolicy: Send + Sync + 'static {
    /// Policy name for logging and debugging.
    fn name(&self) -> &str;

    /// Given current system state, decide which chunks should migrate and where.
    ///
    /// Called periodically and on pressure events.
    async fn decide_migrations(
        &self,
        state: &SystemState,
    ) -> Result<Vec<MigrationDecision>, PolicyError>;

    /// Given a tier under pressure, decide which chunk to evict.
    ///
    /// Returns the ChunkId of the chunk that should be evicted.
    async fn decide_eviction(
        &self,
        tier: TierId,
        candidates: &[ChunkMeta],
    ) -> Result<ChunkId, PolicyError>;

    /// Record an access event for hotness tracking.
    async fn record_access(&self, chunk_id: &ChunkId) -> Result<(), PolicyError>;

    /// Get the hotness score for a chunk (higher = hotter).
    async fn hotness(&self, chunk_id: &ChunkId) -> Result<f64, PolicyError>;
}
