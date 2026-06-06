//! Backend health tracking for the GhostPages daemon.
//!
//! Monitors storage backend health, tracks failure counts, and determines
//! when backends are degraded, unavailable, or recovering.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ghost_core::emitter::EventEmitter;
use ghost_core::events::{BackendHealth as CoreBackendHealth, Event};
use ghost_core::types::TierId;

/// Health status of a storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendHealth {
    /// Backend is operating normally.
    Healthy,

    /// Backend is experiencing intermittent failures.
    Degraded,

    /// Backend is not responding and cannot serve requests.
    Unavailable,

    /// Backend is being probed after recovery.
    Recovering,
}

impl std::fmt::Display for BackendHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendHealth::Healthy => write!(f, "healthy"),
            BackendHealth::Degraded => write!(f, "degraded"),
            BackendHealth::Unavailable => write!(f, "unavailable"),
            BackendHealth::Recovering => write!(f, "recovering"),
        }
    }
}

/// Configuration for health tracking behavior.
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Number of failures before a backend is marked degraded.
    pub degraded_threshold: u64,

    /// Number of failures before a backend is marked unavailable.
    pub unavailable_threshold: u64,

    /// Time window for counting failures.
    pub failure_window: Duration,

    /// Interval between recovery probes when a backend is unavailable.
    pub recovery_probe_interval: Duration,

    /// Number of successful probes required to mark a backend as recovered.
    pub recovery_success_threshold: u64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            degraded_threshold: 3,
            unavailable_threshold: 10,
            failure_window: Duration::from_secs(60),
            recovery_probe_interval: Duration::from_secs(5),
            recovery_success_threshold: 3,
        }
    }
}

/// SUBSYSTEM: Runtime State Owner
///
/// Tracks health status for all registered storage backends.
#[derive(Debug)]
pub struct HealthTracker {
    states: BTreeMap<TierId, BackendHealth>,
    failure_counts: BTreeMap<TierId, AtomicU64>,
    last_failure: BTreeMap<TierId, Instant>,
    recovery_successes: BTreeMap<TierId, AtomicU64>,
    config: HealthConfig,
    /// Optional event emitter for unified event taxonomy.
    event_emitter: Option<EventEmitter>,
}

impl HealthTracker {
    /// Create a new health tracker with the given configuration.
    pub fn new(config: HealthConfig) -> Self {
        Self {
            states: BTreeMap::new(),
            failure_counts: BTreeMap::new(),
            last_failure: BTreeMap::new(),
            recovery_successes: BTreeMap::new(),
            config,
            event_emitter: None,
        }
    }

    /// Set the event emitter for unified event taxonomy.
    pub fn set_event_emitter(&mut self, emitter: EventEmitter) {
        self.event_emitter = Some(emitter);
    }

    /// Create a new health tracker with default configuration.
    pub fn default() -> Self {
        Self::new(HealthConfig::default())
    }

    /// Register a backend for health tracking.
    pub fn register(&mut self, tier: TierId) {
        self.states.entry(tier).or_insert(BackendHealth::Healthy);
        self.failure_counts
            .entry(tier)
            .or_insert_with(|| AtomicU64::new(0));
        self.recovery_successes
            .entry(tier)
            .or_insert_with(|| AtomicU64::new(0));
    }

    /// Record a failure for the given backend tier.
    ///
    /// Updates the health state based on failure count thresholds.
    pub fn record_failure(&mut self, tier: TierId) {
        let count = self
            .failure_counts
            .entry(tier)
            .or_insert_with(|| AtomicU64::new(0));
        let failures = count.fetch_add(1, Ordering::Relaxed) + 1;
        self.last_failure.insert(tier, Instant::now());

        let old_state = self.states.get(&tier).copied();
        let state = self.states.entry(tier).or_insert(BackendHealth::Healthy);
        let prev = *state;

        if failures >= self.config.unavailable_threshold {
            *state = BackendHealth::Unavailable;
        } else if failures >= self.config.degraded_threshold {
            *state = BackendHealth::Degraded;
        }

        let new_state = *state;
        if prev != new_state {
            if let Some(ref emitter) = self.event_emitter {
                let old_core = old_state.map(|s| match s {
                    BackendHealth::Healthy => CoreBackendHealth::Healthy,
                    BackendHealth::Degraded => CoreBackendHealth::Degraded,
                    BackendHealth::Unavailable => CoreBackendHealth::Unavailable,
                    BackendHealth::Recovering => CoreBackendHealth::Recovering,
                });
                let new_core = match new_state {
                    BackendHealth::Degraded => CoreBackendHealth::Degraded,
                    BackendHealth::Unavailable => CoreBackendHealth::Unavailable,
                    _ => CoreBackendHealth::Healthy,
                };
                let _ = emitter.try_emit(Event::BackendHealthChanged {
                    tier,
                    old: old_core.unwrap_or(CoreBackendHealth::Healthy),
                    new: new_core,
                    sequence_id: 0,
                });
            }
        }
    }

    /// Record a success for the given backend tier.
    ///
    /// If the backend was degraded, decrements the failure count.
    /// If the backend was recovering, increments the recovery success counter.
    pub fn record_success(&mut self, tier: TierId) {
        if let Some(state) = self.states.get_mut(&tier) {
            let prev = *state;
            match state {
                BackendHealth::Degraded => {
                    if let Some(count) = self.failure_counts.get(&tier) {
                        // Decrement but don't go below 0
                        let current = count.load(Ordering::Relaxed);
                        if current > 0 {
                            count.fetch_sub(1, Ordering::Relaxed);
                        }
                        // If we've decremented below degraded threshold, mark healthy
                        if current <= self.config.degraded_threshold {
                            *state = BackendHealth::Healthy;
                        }
                    }
                }
                BackendHealth::Recovering => {
                    let successes = self
                        .recovery_successes
                        .entry(tier)
                        .or_insert_with(|| AtomicU64::new(0));
                    let s = successes.fetch_add(1, Ordering::Relaxed) + 1;
                    if s >= self.config.recovery_success_threshold {
                        *state = BackendHealth::Healthy;
                        // Reset counters
                        if let Some(count) = self.failure_counts.get(&tier) {
                            count.store(0, Ordering::Relaxed);
                        }
                        successes.store(0, Ordering::Relaxed);
                    }
                }
                _ => {}
            }
            // Emit event if state changed to Healthy
            let new_state = *state;
            if prev != new_state && new_state == BackendHealth::Healthy {
                if let Some(ref emitter) = self.event_emitter {
                    let old_core = match prev {
                        BackendHealth::Degraded => CoreBackendHealth::Degraded,
                        BackendHealth::Unavailable => CoreBackendHealth::Unavailable,
                        BackendHealth::Recovering => CoreBackendHealth::Recovering,
                        _ => CoreBackendHealth::Healthy,
                    };
                    let _ = emitter.try_emit(Event::BackendHealthChanged {
                        tier,
                        old: old_core,
                        new: CoreBackendHealth::Healthy,
                        sequence_id: 0,
                    });
                }
            }
        }
    }

    /// Get the current health status of a backend.
    pub fn health(&self, tier: TierId) -> Option<BackendHealth> {
        self.states.get(&tier).copied()
    }

    /// Get the current failure count for a backend.
    pub fn failure_count(&self, tier: TierId) -> u64 {
        self.failure_counts
            .get(&tier)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Check if a backend is available for operations.
    pub fn is_available(&self, tier: TierId) -> bool {
        self.states
            .get(&tier)
            .map(|s| matches!(s, BackendHealth::Healthy | BackendHealth::Degraded))
            .unwrap_or(false)
    }

    /// Initiate recovery probing for an unavailable backend.
    pub fn begin_recovery(&mut self, tier: TierId) {
        if let Some(state) = self.states.get_mut(&tier) {
            if matches!(state, BackendHealth::Unavailable) {
                let old = CoreBackendHealth::Unavailable;
                *state = BackendHealth::Recovering;
                if let Some(s) = self.recovery_successes.get(&tier) {
                    s.store(0, Ordering::Relaxed);
                }
                if let Some(ref emitter) = self.event_emitter {
                    let _ = emitter.try_emit(Event::BackendHealthChanged {
                        tier,
                        old,
                        new: CoreBackendHealth::Recovering,
                        sequence_id: 0,
                    });
                }
            }
        }
    }

    /// Get all backend health states.
    pub fn all_states(&self) -> &BTreeMap<TierId, BackendHealth> {
        &self.states
    }

    /// Get the time of the last recorded failure for a tier.
    pub fn last_failure_time(&self, tier: TierId) -> Option<Instant> {
        self.last_failure.get(&tier).copied()
    }

    /// Reset all tracking state for a tier.
    pub fn reset(&mut self, tier: TierId) {
        self.states.insert(tier, BackendHealth::Healthy);
        if let Some(count) = self.failure_counts.get(&tier) {
            count.store(0, Ordering::Relaxed);
        }
        if let Some(s) = self.recovery_successes.get(&tier) {
            s.store(0, Ordering::Relaxed);
        }
        self.last_failure.remove(&tier);
    }
}

impl Default for HealthTracker {
    fn default() -> Self {
        Self::default()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_tracker_new() {
        let tracker = HealthTracker::default();
        assert!(tracker.states.is_empty());
    }

    #[test]
    fn test_register_backend() {
        let mut tracker = HealthTracker::default();
        tracker.register(TierId::Ram);
        assert_eq!(tracker.health(TierId::Disk), None);
    }

    #[test]
    fn test_record_failure_transitions_to_degraded() {
        let config = HealthConfig {
            degraded_threshold: 3,
            unavailable_threshold: 10,
            ..Default::default()
        };
        let mut tracker = HealthTracker::new(config);
        tracker.register(TierId::Ram);

        tracker.record_failure(TierId::Ram);
        tracker.record_failure(TierId::Ram);
        assert_eq!(tracker.health(TierId::Ram), Some(BackendHealth::Healthy));

        tracker.record_failure(TierId::Ram);
        assert_eq!(tracker.health(TierId::Ram), Some(BackendHealth::Degraded));
    }

    #[test]
    fn test_record_failure_transitions_to_unavailable() {
        let config = HealthConfig {
            degraded_threshold: 2,
            unavailable_threshold: 5,
            ..Default::default()
        };
        let mut tracker = HealthTracker::new(config);
        tracker.register(TierId::Ram);

        for _ in 0..5 {
            tracker.record_failure(TierId::Ram);
        }
        assert_eq!(tracker.health(TierId::Ram), Some(BackendHealth::Unavailable));
    }

    #[test]
    fn test_is_available() {
        let config = HealthConfig {
            degraded_threshold: 2,
            unavailable_threshold: 5,
            ..Default::default()
        };
        let mut tracker = HealthTracker::new(config);
        tracker.register(TierId::Ram);

        assert!(tracker.is_available(TierId::Ram));

        tracker.record_failure(TierId::Ram);
        tracker.record_failure(TierId::Ram);
        // Degraded is still available
        assert!(tracker.is_available(TierId::Ram));

        tracker.record_failure(TierId::Ram);
        tracker.record_failure(TierId::Ram);
        tracker.record_failure(TierId::Ram);
        // Unavailable
        assert!(!tracker.is_available(TierId::Ram));
    }

    #[test]
    fn test_begin_recovery() {
        let config = HealthConfig {
            degraded_threshold: 1,
            unavailable_threshold: 2,
            recovery_success_threshold: 2,
            ..Default::default()
        };
        let mut tracker = HealthTracker::new(config);
        tracker.register(TierId::Ram);

        tracker.record_failure(TierId::Ram);
        tracker.record_failure(TierId::Ram);
        assert_eq!(tracker.health(TierId::Ram), Some(BackendHealth::Unavailable));

        tracker.begin_recovery(TierId::Ram);
        assert_eq!(tracker.health(TierId::Ram), Some(BackendHealth::Recovering));

        tracker.record_success(TierId::Ram);
        assert_eq!(tracker.health(TierId::Ram), Some(BackendHealth::Recovering));

        tracker.record_success(TierId::Ram);
        assert_eq!(tracker.health(TierId::Ram), Some(BackendHealth::Healthy));
    }

    #[test]
    fn test_reset() {
        let config = HealthConfig {
            degraded_threshold: 1,
            unavailable_threshold: 2,
            ..Default::default()
        };
        let mut tracker = HealthTracker::new(config);
        tracker.register(TierId::Ram);

        tracker.record_failure(TierId::Ram);
        tracker.record_failure(TierId::Ram);
        assert_eq!(tracker.health(TierId::Ram), Some(BackendHealth::Unavailable));

        tracker.reset(TierId::Ram);
        assert_eq!(tracker.health(TierId::Ram), Some(BackendHealth::Healthy));
        assert_eq!(tracker.failure_count(TierId::Ram), 0);
    }

    #[test]
    fn test_failure_count() {
        let mut tracker = HealthTracker::default();
        tracker.register(TierId::Ram);

        assert_eq!(tracker.failure_count(TierId::Ram), 0);
        tracker.record_failure(TierId::Ram);
        assert_eq!(tracker.failure_count(TierId::Ram), 1);
        tracker.record_failure(TierId::Ram);
        assert_eq!(tracker.failure_count(TierId::Ram), 2);
    }

    #[test]
    fn test_health_display() {
        assert_eq!(format!("{}", BackendHealth::Healthy), "healthy");
        assert_eq!(format!("{}", BackendHealth::Degraded), "degraded");
        assert_eq!(format!("{}", BackendHealth::Unavailable), "unavailable");
        assert_eq!(format!("{}", BackendHealth::Recovering), "recovering");
    }
}
