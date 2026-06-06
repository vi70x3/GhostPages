//! Backpressure controller for overload management.
//!
//! Monitors system pressure and adjusts transfer concurrency to prevent
//! cascading failures. Implements a three-tier response:
//! - **Throttle**: Reduce concurrency when pressure is elevated
//! - **Reject**: Block non-critical transfers when pressure is high
//! - **Critical-Only**: Only allow critical transfers when pressure is critical

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::state::PressureState;
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::transfer::TransferPriority;
use ghost_core::types::TierId;

use crate::config::BackpressureConfig;
use crate::trace_log::TraceLog;

/// Current backpressure action to apply to incoming transfers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureAction {
    /// No backpressure; accept all transfers.
    Allow,
    /// Throttle: accept critical and high priority, delay others.
    Throttle,
    /// Reject: only accept critical transfers.
    Reject,
    /// Critical-only: reject everything except critical transfers.
    CriticalOnly,
}

impl BackpressureAction {
    /// Check if this action allows a given transfer priority.
    pub fn allows(&self, priority: TransferPriority) -> bool {
        match self {
            BackpressureAction::Allow => true,
            BackpressureAction::Throttle => {
                matches!(priority, TransferPriority::Critical | TransferPriority::High)
            }
            BackpressureAction::Reject => {
                matches!(priority, TransferPriority::Critical)
            }
            BackpressureAction::CriticalOnly => {
                matches!(priority, TransferPriority::Critical)
            }
        }
    }
}

impl std::fmt::Display for BackpressureAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackpressureAction::Allow => write!(f, "allow"),
            BackpressureAction::Throttle => write!(f, "throttle"),
            BackpressureAction::Reject => write!(f, "reject"),
            BackpressureAction::CriticalOnly => write!(f, "critical-only"),
        }
    }
}

/// Statistics for backpressure decisions.
#[derive(Debug, Clone, Default)]
pub struct BackpressureStats {
    /// Total number of evaluations performed.
    pub evaluations: u64,
    /// Number of times each action was taken.
    pub allow_count: u64,
    /// Number of times throttle action was taken.
    pub throttle_count: u64,
    /// Number of times reject action was taken.
    pub reject_count: u64,
    /// Number of times critical-only action was taken.
    pub critical_only_count: u64,
    /// Number of transfers rejected due to backpressure.
    pub transfers_rejected: u64,
    /// Number of transfers throttled due to backpressure.
    pub transfers_throttled: u64,
    /// Timestamp of the last action change.
    pub last_action_change: u64,
    /// Current consecutive evaluations at the same action.
    pub consecutive_same_action: u64,
}

/// Backpressure controller that monitors system pressure and adjusts transfer
/// concurrency to prevent cascading failures under overload.
pub struct BackpressureController {
    config: BackpressureConfig,
    trace_log: Arc<TraceLog>,
    /// Optional event emitter for unified event taxonomy.
    event_emitter: Option<EventEmitter>,
    /// Current backpressure action.
    current_action: Arc<AtomicU8>,
    /// Statistics.
    stats: Arc<std::sync::Mutex<BackpressureStats>>,
    /// Timestamp when the last pressure evaluation was performed.
    last_evaluation: Arc<std::sync::Mutex<Instant>>,
    /// Timestamp when pressure last exceeded the throttle threshold.
    pressure_since: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl BackpressureController {
    /// Create a new backpressure controller.
    pub fn new(config: BackpressureConfig, trace_log: Arc<TraceLog>) -> Self {
        Self {
            config,
            trace_log,
            event_emitter: None,
            current_action: Arc::new(AtomicU8::new(0)), // Allow = 0
            stats: Arc::new(std::sync::Mutex::new(BackpressureStats::default())),
            last_evaluation: Arc::new(std::sync::Mutex::new(Instant::now())),
            pressure_since: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Set the event emitter for unified event taxonomy.
    pub fn set_event_emitter(&mut self, emitter: EventEmitter) {
        self.event_emitter = Some(emitter);
    }

    /// Evaluate the current pressure and determine the appropriate action.
    ///
    /// Considers both overall system pressure and I/O-specific pressure:
    /// - I/O pressure soft limit: throttles I/O-heavy operations
    /// - I/O pressure hard limit: rejects all non-critical transfers
    /// - Queue depth threshold: escalates backpressure when queue is deep
    pub fn evaluate(&self, pressure: &PressureState) -> BackpressureAction {
        let max_pressure = pressure.max_pressure();
        let now = Instant::now();

        // I/O pressure-specific escalation:
        // If I/O pressure exceeds the hard limit or queue depth is very high,
        // escalate directly to Reject regardless of overall pressure.
        let io_escalation = if pressure.io_pressure >= self.config.io_pressure_hard_limit
            || pressure.queue_depth > self.config.queue_depth_threshold * 2
        {
            Some(BackpressureAction::Reject)
        } else if pressure.io_pressure >= self.config.io_pressure_soft_limit
            || pressure.queue_depth > self.config.queue_depth_threshold
        {
            Some(BackpressureAction::Throttle)
        } else {
            None
        };

        let action = if let Some(io_action) = io_escalation {
            // I/O escalation takes precedence when it's more severe than
            // what overall pressure would dictate
            let overall_action = if max_pressure >= self.config.critical_threshold {
                BackpressureAction::CriticalOnly
            } else if max_pressure >= self.config.reject_threshold {
                BackpressureAction::Reject
            } else if max_pressure >= self.config.throttle_threshold {
                BackpressureAction::Throttle
            } else {
                BackpressureAction::Allow
            };
            // Pick the more restrictive of the two
            if io_action as u8 > overall_action as u8 {
                io_action
            } else {
                overall_action
            }
        } else if max_pressure >= self.config.critical_threshold {
            BackpressureAction::CriticalOnly
        } else if max_pressure >= self.config.reject_threshold {
            BackpressureAction::Reject
        } else if max_pressure >= self.config.throttle_threshold {
            BackpressureAction::Throttle
        } else {
            // Check cooldown: if we were recently under pressure, wait before resuming
            if let Some(pressure_start) = *self.pressure_since.lock().unwrap() {
                let cooldown = Duration::from_secs(self.config.cooldown_secs);
                if now.duration_since(pressure_start) < cooldown {
                    // Still in cooldown, maintain throttle
                    BackpressureAction::Throttle
                } else {
                    // Cooldown expired, clear pressure state
                    *self.pressure_since.lock().unwrap() = None;
                    BackpressureAction::Allow
                }
            } else {
                BackpressureAction::Allow
            }
        };

        // Track pressure start time
        if action != BackpressureAction::Allow && self.pressure_since.lock().unwrap().is_none() {
            *self.pressure_since.lock().unwrap() = Some(now);
        }

        // Update action atomically
        let action_byte = match action {
            BackpressureAction::Allow => 0u8,
            BackpressureAction::Throttle => 1u8,
            BackpressureAction::Reject => 2u8,
            BackpressureAction::CriticalOnly => 3u8,
        };

        let prev_action = self.current_action.swap(action_byte, Ordering::SeqCst);

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.evaluations += 1;
            match action {
                BackpressureAction::Allow => stats.allow_count += 1,
                BackpressureAction::Throttle => stats.throttle_count += 1,
                BackpressureAction::Reject => stats.reject_count += 1,
                BackpressureAction::CriticalOnly => stats.critical_only_count += 1,
            }

            if prev_action != action_byte {
                stats.last_action_change = current_timestamp();
                stats.consecutive_same_action = 0;
            } else {
                stats.consecutive_same_action += 1;
            }
        }

        // Emit trace event on action change
        if prev_action != action_byte {
            self.trace_log.record(TraceEvent::PressureEscalated {
                memory_pressure: pressure.memory_pressure,
                vram_pressure: pressure.vram_pressure,
                io_pressure: pressure.io_pressure,
                timestamp: current_timestamp(),
            });

            // Emit unified event for backpressure activation
            if let Some(ref emitter) = self.event_emitter {
                let level = match action {
                    BackpressureAction::Throttle => "throttle",
                    BackpressureAction::Reject => "reject",
                    BackpressureAction::CriticalOnly => "critical-only",
                    _ => "allow",
                };
                let _ = emitter.try_emit(Event::BackpressureActivated {
                    tier: TierId::Ram,
                    level: level.to_string(),
                    sequence_id: 0,
                });
            }
        } else if action == BackpressureAction::Allow && prev_action != 0 {
            // Emit unified event for backpressure deactivation
            if let Some(ref emitter) = self.event_emitter {
                let _ = emitter.try_emit(Event::BackpressureDeactivated {
                    tier: TierId::Ram,
                    sequence_id: 0,
                });
            }
        }

        *self.last_evaluation.lock().unwrap() = now;

        action
    }

    /// Check if a transfer with the given priority should be allowed.
    pub fn should_allow(&self, priority: TransferPriority) -> bool {
        let action = self.current_action();
        let allowed = action.allows(priority);

        if !allowed {
            let mut stats = self.stats.lock().unwrap();
            stats.transfers_rejected += 1;
        }

        allowed
    }

    /// Get the current backpressure action.
    pub fn current_action(&self) -> BackpressureAction {
        match self.current_action.load(Ordering::SeqCst) {
            0 => BackpressureAction::Allow,
            1 => BackpressureAction::Throttle,
            2 => BackpressureAction::Reject,
            3 => BackpressureAction::CriticalOnly,
            _ => BackpressureAction::Allow,
        }
    }

    /// Get a snapshot of the backpressure statistics.
    pub fn stats(&self) -> BackpressureStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get the evaluation interval from config.
    pub fn evaluation_interval(&self) -> Duration {
        Duration::from_millis(self.config.evaluation_interval_ms)
    }

    /// Run the backpressure evaluation loop.
    ///
    /// This should be spawned as a background task. It periodically evaluates
    /// the pressure state and adjusts the backpressure action.
    pub async fn run(
        &self,
        mut pressure_rx: tokio::sync::watch::Receiver<PressureState>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut ticker = tokio::time::interval(self.evaluation_interval());

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let pressure = *pressure_rx.borrow();
                    self.evaluate(&pressure);
                }
                _ = pressure_rx.changed() => {
                    let pressure = *pressure_rx.borrow();
                    self.evaluate(&pressure);
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Backpressure controller shutting down");
                        break;
                    }
                }
            }
        }
    }
}

impl std::fmt::Debug for BackpressureController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackpressureController")
            .field("config", &self.config)
            .field("current_action", &self.current_action())
            .field("stats", &self.stats())
            .finish()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_trace_log() -> Arc<TraceLog> {
        Arc::new(TraceLog::new(1000))
    }

    fn test_config() -> BackpressureConfig {
        BackpressureConfig::default()
    }

    #[test]
    fn test_backpressure_action_allows() {
        assert!(BackpressureAction::Allow.allows(TransferPriority::Low));
        assert!(BackpressureAction::Allow.allows(TransferPriority::Critical));

        assert!(!BackpressureAction::Throttle.allows(TransferPriority::Low));
        assert!(BackpressureAction::Throttle.allows(TransferPriority::High));
        assert!(BackpressureAction::Throttle.allows(TransferPriority::Critical));

        assert!(!BackpressureAction::Reject.allows(TransferPriority::High));
        assert!(BackpressureAction::Reject.allows(TransferPriority::Critical));

        assert!(!BackpressureAction::CriticalOnly.allows(TransferPriority::High));
        assert!(BackpressureAction::CriticalOnly.allows(TransferPriority::Critical));
    }

    #[test]
    fn test_backpressure_controller_no_pressure() {
        let controller = BackpressureController::new(test_config(), test_trace_log());
        let pressure = PressureState::new();
        let action = controller.evaluate(&pressure);
        assert_eq!(action, BackpressureAction::Allow);
    }

    #[test]
    fn test_backpressure_controller_throttle() {
        let controller = BackpressureController::new(test_config(), test_trace_log());
        let pressure = PressureState {
            memory_pressure: 0.75,
            vram_pressure: 0.1,
            io_pressure: 0.1,
            queue_depth: 0,
            throughput_bps: 0,
        };
        let action = controller.evaluate(&pressure);
        assert_eq!(action, BackpressureAction::Throttle);
    }

    #[test]
    fn test_backpressure_controller_reject() {
        let controller = BackpressureController::new(test_config(), test_trace_log());
        let pressure = PressureState {
            memory_pressure: 0.9,
            vram_pressure: 0.1,
            io_pressure: 0.1,
            queue_depth: 0,
            throughput_bps: 0,
        };
        let action = controller.evaluate(&pressure);
        assert_eq!(action, BackpressureAction::Reject);
    }

    #[test]
    fn test_backpressure_controller_critical() {
        let controller = BackpressureController::new(test_config(), test_trace_log());
        let pressure = PressureState {
            memory_pressure: 0.99,
            vram_pressure: 0.1,
            io_pressure: 0.1,
            queue_depth: 0,
            throughput_bps: 0,
        };
        let action = controller.evaluate(&pressure);
        assert_eq!(action, BackpressureAction::CriticalOnly);
    }

    #[test]
    fn test_backpressure_should_allow() {
        let controller = BackpressureController::new(test_config(), test_trace_log());
        let pressure = PressureState {
            memory_pressure: 0.75,
            vram_pressure: 0.1,
            io_pressure: 0.1,
            queue_depth: 0,
            throughput_bps: 0,
        };
        controller.evaluate(&pressure);

        assert!(!controller.should_allow(TransferPriority::Low));
        assert!(controller.should_allow(TransferPriority::Critical));
    }

    #[test]
    fn test_backpressure_stats() {
        let controller = BackpressureController::new(test_config(), test_trace_log());
        let pressure = PressureState {
            memory_pressure: 0.99,
            vram_pressure: 0.1,
            io_pressure: 0.1,
            queue_depth: 0,
            throughput_bps: 0,
        };
        controller.evaluate(&pressure);
        controller.should_allow(TransferPriority::Low);

        let stats = controller.stats();
        assert_eq!(stats.evaluations, 1);
        assert_eq!(stats.critical_only_count, 1);
        assert_eq!(stats.transfers_rejected, 1);
    }

    #[test]
    fn test_backpressure_action_display() {
        assert_eq!(format!("{}", BackpressureAction::Allow), "allow");
        assert_eq!(format!("{}", BackpressureAction::Throttle), "throttle");
        assert_eq!(format!("{}", BackpressureAction::Reject), "reject");
        assert_eq!(
            format!("{}", BackpressureAction::CriticalOnly),
            "critical-only"
        );
    }

    #[test]
    fn test_backpressure_config_default() {
        let config = BackpressureConfig::default();
        assert!((config.throttle_threshold - 0.7).abs() < f32::EPSILON);
        assert!((config.reject_threshold - 0.85).abs() < f32::EPSILON);
        assert!((config.critical_threshold - 0.95).abs() < f32::EPSILON);
        assert!(config.enabled);
    }
}
