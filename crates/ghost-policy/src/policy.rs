//! Placement policy trait definition.
//!
//! This module defines the [`PlacementPolicy`] trait — the core abstraction
//! for deciding where chunks should reside in the memory hierarchy.
//!
//! The policy is a **pure function**: no async, no mutation, no side effects,
//! deterministic, stateless, and replay-safe. It knows nothing about
//! StorageBackends or how/where data is stored.

use ghost_core::state::PressureState;
use ghost_core::transfer::TransferPriority;
use ghost_core::types::{ChunkId, ChunkMeta, TierId};

/// Errors from placement policy operations.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    /// No candidates available for eviction.
    #[error("no eviction candidates available")]
    NoCandidates,

    /// No target tier available.
    #[error("no target tier available")]
    NoTargetTier,

    /// Policy configuration error.
    #[error("invalid policy configuration: {0}")]
    InvalidConfig(String),
}

/// Placement policy trait — decides where chunks should reside.
///
/// This trait is completely backend-agnostic. It only makes placement,
/// eviction, and migration decisions based on chunk metadata and pressure.
///
/// # Design Rules
///
/// - **Pure**: No mutation, no side effects, no async
/// - **Deterministic**: Same inputs → same outputs
/// - **Stateless**: No internal state
/// - **Replay-safe**: Can be called repeatedly with the same result
/// - **No backend knowledge**: Policies know nothing about StorageBackends
///
/// Implementations must be `Send + Sync + 'static`.
pub trait PlacementPolicy: Send + Sync + 'static {
    /// Policy name for logging and debugging.
    fn name(&self) -> &str;

    /// Select the best target tier for storing a new chunk.
    ///
    /// Given the chunk's metadata, current system pressure, and available tiers,
    /// returns the tier where the chunk should be placed.
    fn select_target_tier(
        &self,
        meta: &ChunkMeta,
        pressure: &PressureState,
        available_tiers: &[TierId],
    ) -> TierId;

    /// Select the best eviction victim from a list of candidates.
    ///
    /// Given candidate chunks and current pressure, returns the ChunkId
    /// of the chunk that should be evicted first.
    fn select_viction(
        &self,
        candidates: &[(ChunkId, ChunkMeta)],
        pressure: &PressureState,
    ) -> Option<ChunkId>;

    /// Determine if a chunk should be migrated to a different tier.
    ///
    /// Returns `Some(target_tier)` if migration is recommended,
    /// or `None` if the chunk should stay on its current tier.
    fn should_migrate(
        &self,
        meta: &ChunkMeta,
        current_tier: TierId,
        pressure: &PressureState,
    ) -> Option<TierId>;

    /// Calculate the priority for a migration involving this chunk.
    ///
    /// Higher priority migrations should be scheduled first.
    fn migration_priority(&self, meta: &ChunkMeta, pressure: &PressureState) -> TransferPriority;
}
