//! Time provider trait for deterministic time sources.
//!
//! Provides an abstraction over time measurement, allowing tests and
//! deterministic replays to control time progression rather than relying
//! on wall-clock time.

use std::time::{Duration, Instant};

/// Trait for providing time measurements.
///
/// This allows components to be parameterized over their time source,
/// enabling deterministic behavior in tests and replay scenarios.
pub trait TimeProvider: Send + Sync + 'static {
    /// Returns the current time as an `Instant`.
    ///
    /// In the real-time implementation, this returns `Instant::now()`.
    /// In the deterministic implementation, it returns a simulated time.
    fn now(&self) -> Instant;

    /// Returns the current timestamp as seconds since the Unix epoch.
    fn timestamp_secs(&self) -> u64;

    /// Returns the elapsed duration since a given instant.
    fn elapsed(&self, since: Instant) -> Duration {
        self.now() - since
    }
}

/// Real-time provider that delegates to `Instant::now()`.
///
/// This is the default time provider used in production.
#[derive(Debug, Clone, Default)]
pub struct RealTimeProvider;

impl TimeProvider for RealTimeProvider {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn timestamp_secs(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

/// Legacy deterministic time provider — use `DeterministicClock` instead.
///
/// Kept for backward compatibility. Wraps `DeterministicClock` internally.
#[derive(Debug, Clone)]
pub struct DeterministicTimeProvider {
    inner: DeterministicClock,
}

impl DeterministicTimeProvider {
    /// Create a new deterministic time provider.
    pub fn new(start_secs: u64, _step: Duration) -> Self {
        Self {
            inner: DeterministicClock::new(start_secs),
        }
    }

    /// Advance time by one step.
    pub fn advance(&self) {
        // No-op: DeterministicClock uses explicit `advance(duration)`.
    }

    /// Set the current time to a specific timestamp (seconds since epoch).
    pub fn set_time(&mut self, timestamp_secs: u64) {
        self.inner = DeterministicClock::new(timestamp_secs);
    }
}

impl Default for DeterministicTimeProvider {
    fn default() -> Self {
        Self::new(1_700_000_000, Duration::from_millis(1))
    }
}

impl TimeProvider for DeterministicTimeProvider {
    fn now(&self) -> Instant {
        self.inner.now()
    }

    fn timestamp_secs(&self) -> u64 {
        self.inner.timestamp_secs()
    }
}

/// Deterministic time provider using a base instant and offset tracking.
#[derive(Debug)]
pub struct DeterministicClock {
    base: Instant,
    offset_nanos: std::sync::atomic::AtomicU64,
}

impl DeterministicClock {
    /// Create a new deterministic clock starting at the given time offset.
    pub fn new(start_timestamp_secs: u64) -> Self {
        Self {
            base: Instant::now(),
            offset_nanos: std::sync::atomic::AtomicU64::new(start_timestamp_secs * 1_000_000_000),
        }
    }

    /// Advance time by the given duration.
    pub fn advance(&self, duration: Duration) {
        self.offset_nanos.fetch_add(
            duration.as_nanos() as u64,
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    /// Get the current simulated timestamp in seconds since epoch.
    pub fn timestamp_secs(&self) -> u64 {
        self.offset_nanos.load(std::sync::atomic::Ordering::SeqCst) / 1_000_000_000
    }

    /// Get the current simulated `Instant`.
    pub fn now(&self) -> Instant {
        self.base + Duration::from_nanos(self.offset_nanos.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// Get elapsed duration since a simulated instant.
    pub fn elapsed(&self, since: Instant) -> Duration {
        self.now() - since
    }
}

impl Clone for DeterministicClock {
    fn clone(&self) -> Self {
        Self {
            base: self.base,
            offset_nanos: std::sync::atomic::AtomicU64::new(
                self.offset_nanos.load(std::sync::atomic::Ordering::SeqCst),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_real_time_provider() {
        let provider = RealTimeProvider;
        let now = provider.now();
        let _ = provider.elapsed(now);
        let ts = provider.timestamp_secs();
        assert!(ts > 1_600_000_000); // After 2020
    }

    #[test]
    fn test_deterministic_clock() {
        let clock = DeterministicClock::new(1_700_000_000);
        let ts = clock.timestamp_secs();
        assert_eq!(ts, 1_700_000_000);

        clock.advance(Duration::from_secs(5));
        let ts2 = clock.timestamp_secs();
        assert_eq!(ts2, 1_700_000_005);

        let now = clock.now();
        let elapsed = clock.elapsed(now);
        assert_eq!(elapsed, Duration::from_secs(0));
    }

    #[test]
    fn test_deterministic_clock_advance() {
        let clock = DeterministicClock::new(0);
        assert_eq!(clock.timestamp_secs(), 0);

        clock.advance(Duration::from_millis(100));
        assert_eq!(clock.timestamp_secs(), 0); // Still < 1 second

        clock.advance(Duration::from_millis(900));
        assert_eq!(clock.timestamp_secs(), 1); // Now 1 second
    }
}
