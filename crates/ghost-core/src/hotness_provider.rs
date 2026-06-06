//! Hotness provider trait for DAMON integration.
//!
//! This module defines the [`HotnessProvider`] trait — an abstraction over
//! hotness data sources. Implementations live in other crates (e.g.,
//! `ghost-linux` provides `MockHotnessProvider` and a future
//! `DamonHotnessProvider`).
//!
//! The trait is intentionally minimal so that a future DAMON-based provider
//! can plug in without changes to consumers.

use crate::error::GhostError;

// ─── Data Types ─────────────────────────────────────────────────────────────────

/// Snapshot of memory hotness from a provider.
#[derive(Debug, Clone)]
pub struct HotnessSnapshot {
    /// Sampled address ranges with their access data.
    pub samples: Vec<HotnessSample>,
    /// Timestamp when the sample was taken (seconds since epoch).
    pub timestamp: u64,
}

/// A single hotness sample for an address range.
#[derive(Debug, Clone)]
pub struct HotnessSample {
    /// The memory address range this sample covers.
    pub address_range: AddressRange,
    /// Number of accesses observed in this range.
    pub access_count: u64,
    /// Temperature classification based on access frequency.
    pub temperature: Temperature,
}

/// A memory address range (inclusive start, exclusive end).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddressRange {
    /// Start address (inclusive).
    pub start: u64,
    /// End address (exclusive).
    pub end: u64,
}

impl AddressRange {
    /// Create a new address range.
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Get the size of this range in bytes.
    pub fn size(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }
}

/// Temperature classification for a memory region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Temperature {
    /// Hot: very frequently accessed.
    Hot,
    /// Warm: moderately accessed.
    Warm,
    /// Cold: rarely accessed.
    Cold,
    /// Frozen: essentially never accessed.
    Frozen,
}

impl Temperature {
    /// Classify temperature based on access count thresholds.
    ///
    /// Thresholds:
    /// - `>= 100` accesses → Hot
    /// - `>= 20` accesses → Warm
    /// - `>= 1` access → Cold
    /// - `0` accesses → Frozen
    pub fn from_access_count(count: u64) -> Self {
        match count {
            c if c >= 100 => Temperature::Hot,
            c if c >= 20 => Temperature::Warm,
            c if c >= 1 => Temperature::Cold,
            _ => Temperature::Frozen,
        }
    }
}

impl std::fmt::Display for Temperature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Temperature::Hot => write!(f, "hot"),
            Temperature::Warm => write!(f, "warm"),
            Temperature::Cold => write!(f, "cold"),
            Temperature::Frozen => write!(f, "frozen"),
        }
    }
}

// ─── HotnessProvider Trait ──────────────────────────────────────────────────────

/// Trait for hotness data providers.
///
/// Phase 1: `MockHotnessProvider` (in `ghost-linux`) for simulation and testing.
/// Phase 2: `DamonHotnessProvider` (in `ghost-linux`) for real DAMON integration.
///
/// Implementors must be `Send + Sync` so they can be shared across async tasks.
pub trait HotnessProvider: Send + Sync {
    /// Sample current hotness data.
    ///
    /// Returns a [`HotnessSnapshot`] with the current state of memory hotness,
    /// or a [`GhostError`] if sampling fails.
    fn sample(&self) -> Result<HotnessSnapshot, GhostError>;

    /// Get the provider name (for logging/debugging).
    fn provider_name(&self) -> &str;

    /// Check if the provider is available on this system.
    ///
    /// Returns `true` if the provider can produce data, `false` otherwise.
    /// For example, a DAMON-based provider would return `false` on non-Linux
    /// systems or kernels without DAMON support.
    fn is_available(&self) -> bool;
}
