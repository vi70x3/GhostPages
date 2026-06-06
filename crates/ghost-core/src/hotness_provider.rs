//! Hotness provider abstraction for memory temperature classification.
//!
//! This module provides the core types and trait for sampling memory hotness
//! data from various sources (DAMON, mock providers, etc.).
//!
//! The trait is intentionally minimal so that a future DAMON-based provider
//! can plug in without changes to consumers.

use crate::error::GhostError;
use crate::types::TierId;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

    /// Convert temperature to recommended tier placement.
    ///
    /// - Hot → RAM (fastest access)
    /// - Warm → RAM or GPU VRAM (high bandwidth)
    /// - Cold → ZRAM or Disk (compressed or paged)
    /// - Frozen → Disk (cold storage)
    pub fn to_tier(&self) -> TierId {
        match self {
            Temperature::Hot => TierId::Ram,
            Temperature::Warm => TierId::Ram, // Could also be GpuVram in some configs
            Temperature::Cold => TierId::Disk,
            Temperature::Frozen => TierId::Disk,
        }
    }

    /// Get numeric value for comparison (higher = hotter).
    ///
    /// Returns: Hot=3, Warm=2, Cold=1, Frozen=0
    pub fn value(&self) -> u8 {
        match self {
            Temperature::Hot => 3,
            Temperature::Warm => 2,
            Temperature::Cold => 1,
            Temperature::Frozen => 0,
        }
    }

    /// Check if temperature is "active" (hot or warm).
    ///
    /// Active regions should be kept in fast tiers.
    pub fn is_active(&self) -> bool {
        matches!(self, Temperature::Hot | Temperature::Warm)
    }

    /// Check if temperature is "inactive" (cold or frozen).
    ///
    /// Inactive regions can be safely moved to slower tiers.
    pub fn is_inactive(&self) -> bool {
        matches!(self, Temperature::Cold | Temperature::Frozen)
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
    /// Returns a snapshot of all monitored memory regions with their
    /// current access counts and temperature classifications.
    fn sample(&self) -> Result<HotnessSnapshot, GhostError>;

    /// Get the provider's name for logging/debugging.
    fn name(&self) -> &'static str;
}