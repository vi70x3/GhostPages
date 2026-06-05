//! Configuration for the GhostPages daemon transfer engine.
//!
//! Defines all configuration types for the orchestrator, scheduler,
//! worker pool, transfer queue, health tracking, and retry behavior.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for the transfer orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    /// Maximum number of jobs in the transfer queue.
    pub queue_capacity: usize,

    /// Number of concurrent worker tasks.
    pub worker_count: usize,

    /// Maximum number of retry attempts for a failed transfer.
    pub max_retries: u32,

    /// Base delay in milliseconds for exponential backoff on retries.
    pub retry_base_delay_ms: u64,

    /// Maximum delay in milliseconds for exponential backoff.
    pub max_retry_delay_ms: u64,

    /// Whether to enable compression during transfers.
    pub enable_compression: bool,

    /// Maximum number of events to retain in the trace log.
    pub trace_max_events: usize,

    /// Timeout in seconds for graceful shutdown.
    pub shutdown_timeout_secs: u64,

    /// Interval in milliseconds between pressure samples from backends.
    pub pressure_sample_interval_ms: u64,

    /// EMA smoothing factor for pressure readings (0.0-1.0, lower = smoother).
    pub pressure_smoothing_factor: f32,

    /// Interval in milliseconds between auto-migration checks.
    pub auto_migration_interval_ms: u64,

    /// Number of pressure history entries to retain in the ring buffer.
    pub pressure_history_size: usize,

    /// Whether to enable automatic pressure-driven migration.
    pub enable_auto_migration: bool,
    /// Enable deterministic mode for replay equivalence. When true, the orchestrator will use a fixed RNG seed and deterministic timestamps.
    pub deterministic_mode: bool,

    /// RNG seed for deterministic random number generation. When set, the orchestrator
    /// creates a ChaCha8Rng seeded with this value and passes it to components that need randomness.
    pub rng_seed: Option<u64>,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            worker_count: 4,
            max_retries: 3,
            retry_base_delay_ms: 1000,
            max_retry_delay_ms: 30000,
            enable_compression: true,
            trace_max_events: 10000,
            shutdown_timeout_secs: 30,
            pressure_sample_interval_ms: 1000,
            pressure_smoothing_factor: 0.3,
            auto_migration_interval_ms: 5000,
            pressure_history_size: 256,
            enable_auto_migration: true,
            deterministic_mode: false,
            rng_seed: Some(42),
        }
    }
}

/// Configuration for the transfer scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    /// Maximum number of concurrent transfers allowed.
    pub max_concurrent_transfers: usize,

    /// Whether to enable priority ordering (critical > high > normal > low).
    pub priority_ordering: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_transfers: 8,
            priority_ordering: true,
        }
    }
}

/// Configuration for the worker pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPoolConfig {
    /// Number of worker tasks.
    pub worker_count: usize,

    /// Maximum retry attempts per job.
    pub max_retries: u32,

    /// Base delay for retry backoff (ms).
    pub retry_base_delay_ms: u64,

    /// Maximum retry delay (ms).
    pub max_retry_delay_ms: u64,

    /// Whether to compress data during transfer.
    pub enable_compression: bool,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            worker_count: 4,
            max_retries: 3,
            retry_base_delay_ms: 1000,
            max_retry_delay_ms: 30000,
            enable_compression: true,
        }
    }
}

/// Current status of the orchestrator.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorStatus {
    /// Current queue depth.
    pub queue_depth: usize,

    /// Whether the queue is full.
    pub queue_full: bool,

    /// Number of active workers.
    pub active_workers: u64,

    /// Total jobs submitted since startup.
    pub jobs_submitted: u64,

    /// Total jobs completed since startup.
    pub jobs_completed: u64,

    /// Total jobs failed since startup.
    pub jobs_failed: u64,

    /// Total jobs cancelled since startup.
    pub jobs_cancelled: u64,

    /// Total bytes transferred since startup.
    pub bytes_transferred: u64,

    /// Total transfer time in milliseconds.
    pub total_transfer_time_ms: u64,

    /// Number of events in the trace log.
    pub trace_event_count: usize,

    /// Whether the orchestrator is shutting down.
    pub shutting_down: bool,
}

/// Configuration for backend health tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Number of failures before a backend is marked degraded.
    pub degraded_threshold: u64,

    /// Number of failures before a backend is marked unavailable.
    pub unavailable_threshold: u64,

    /// Time window for counting failures (seconds).
    pub failure_window_secs: u64,

    /// Interval between recovery probes when a backend is unavailable (seconds).
    pub recovery_probe_interval_secs: u64,

    /// Number of successful probes required to mark a backend as recovered.
    pub recovery_success_threshold: u64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            degraded_threshold: 3,
            unavailable_threshold: 10,
            failure_window_secs: 60,
            recovery_probe_interval_secs: 5,
            recovery_success_threshold: 3,
        }
    }
}

/// Configuration for retry behavior with bounded exponential backoff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,

    /// Base delay before the first retry (ms).
    pub base_delay_ms: u64,

    /// Maximum delay cap for exponential backoff (ms).
    pub max_delay_ms: u64,

    /// Multiplier for exponential backoff.
    pub backoff_multiplier: f64,

    /// Jitter factor (0.0 = no jitter, 1.0 = full jitter).
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 30_000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.25,
        }
    }
}

impl RetryConfig {
    /// Calculate the delay for a given retry attempt.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(0);
        }

        let base = self.base_delay_ms as f64;
        let multiplier = self.backoff_multiplier.powi((attempt - 1) as i32);
        let delay_ms = (base * multiplier).min(self.max_delay_ms as f64);
        let jitter_range = delay_ms * self.jitter_factor;
        let jittered = delay_ms - (jitter_range * 0.5); // Simplified jitter

        Duration::from_millis(jittered.max(0.0) as u64)
    }
}

/// Configuration for the migration engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Maximum number of concurrent migration operations.
    pub max_concurrent_migrations: usize,

    /// Hotness threshold above which a chunk is considered "hot" and eligible for promotion.
    pub hot_threshold: f32,

    /// Hotness threshold below which a chunk is considered "cold" and eligible for eviction.
    pub cold_threshold: f32,

    /// Minimum interval between migration evaluations for the same chunk (seconds).
    pub min_migration_interval_secs: u64,

    /// Maximum number of chunks to migrate in a single evaluation cycle.
    pub max_migrations_per_cycle: usize,

    /// Whether to enable automatic promotion of hot chunks to faster tiers.
    pub enable_promotion: bool,

    /// Whether to enable automatic eviction of cold chunks from pressured tiers.
    pub enable_eviction: bool,

    /// Pressure threshold above which eviction is triggered.
    pub eviction_pressure_threshold: f32,

    /// Size limit in bytes for chunks eligible for migration.
    pub max_chunk_size_for_migration: usize,

    /// Timeout in seconds for a single migration operation.
    pub migration_timeout_secs: u64,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            max_concurrent_migrations: 2,
            hot_threshold: 0.5,
            cold_threshold: 0.2,
            min_migration_interval_secs: 60,
            max_migrations_per_cycle: 16,
            enable_promotion: true,
            enable_eviction: true,
            eviction_pressure_threshold: 0.7,
            max_chunk_size_for_migration: 256 * 1024 * 1024, // 256 MB
            migration_timeout_secs: 120,
        }
    }
}

/// Configuration for the backpressure controller.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackpressureConfig {
    /// Pressure threshold for throttling non-critical transfers (0.0-1.0).
    pub throttle_threshold: f32,

    /// Pressure threshold for rejecting all non-critical transfers (0.0-1.0).
    pub reject_threshold: f32,

    /// Pressure threshold for critical-only mode (0.0-1.0).
    pub critical_threshold: f32,

    /// Interval in milliseconds between backpressure evaluations.
    pub evaluation_interval_ms: u64,

    /// Whether to enable backpressure-based transfer throttling.
    pub enabled: bool,

    /// Cooldown period in seconds after pressure subsides before resuming normal operations.
    pub cooldown_secs: u64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            throttle_threshold: 0.7,
            reject_threshold: 0.85,
            critical_threshold: 0.95,
            evaluation_interval_ms: 1000,
            enabled: true,
            cooldown_secs: 10,
        }
    }
}

/// Configuration for the Prometheus metrics HTTP exporter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsExporterConfig {
    /// Address to bind the HTTP server to.
    pub bind_address: String,

    /// Port to bind the HTTP server to.
    pub port: u16,

    /// Whether to enable the metrics exporter.
    pub enabled: bool,
}

impl Default for MetricsExporterConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".to_string(),
            port: 9090,
            enabled: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orchestrator_config_default() {
        let config = OrchestratorConfig::default();
        assert_eq!(config.queue_capacity, 1024);
        assert_eq!(config.worker_count, 4);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_base_delay_ms, 1000);
        assert_eq!(config.max_retry_delay_ms, 30000);
        assert!(config.enable_compression);
        assert_eq!(config.trace_max_events, 10000);
        assert_eq!(config.shutdown_timeout_secs, 30);
    }

    #[test]
    fn test_scheduler_config_default() {
        let config = SchedulerConfig::default();
        assert_eq!(config.max_concurrent_transfers, 8);
        assert!(config.priority_ordering);
    }

    #[test]
    fn test_worker_pool_config_default() {
        let config = WorkerPoolConfig::default();
        assert_eq!(config.worker_count, 4);
        assert_eq!(config.max_retries, 3);
        assert!(config.enable_compression);
    }

    #[test]
    fn test_orchestrator_status_default() {
        let status = OrchestratorStatus::default();
        assert_eq!(status.queue_depth, 0);
        assert!(!status.queue_full);
        assert_eq!(status.active_workers, 0);
        assert!(!status.shutting_down);
    }

    #[test]
    fn test_health_config_default() {
        let config = HealthConfig::default();
        assert_eq!(config.degraded_threshold, 3);
        assert_eq!(config.unavailable_threshold, 10);
        assert_eq!(config.failure_window_secs, 60);
        assert_eq!(config.recovery_probe_interval_secs, 5);
        assert_eq!(config.recovery_success_threshold, 3);
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 30_000);
        assert!((config.backoff_multiplier - 2.0).abs() < f64::EPSILON);
        assert!((config.jitter_factor - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_config_delay_capped() {
        let config = RetryConfig {
            base_delay_ms: 1000,
            max_delay_ms: 2000,
            backoff_multiplier: 10.0,
            jitter_factor: 0.0,
            ..Default::default()
        };
        let delay = config.delay_for_attempt(10);
        assert_eq!(delay, Duration::from_millis(2000));
    }

    #[test]
    fn test_migration_config_default() {
        let config = MigrationConfig::default();
        assert_eq!(config.max_concurrent_migrations, 2);
        assert!((config.hot_threshold - 0.5).abs() < f32::EPSILON);
        assert!((config.cold_threshold - 0.2).abs() < f32::EPSILON);
        assert!(config.enable_promotion);
        assert!(config.enable_eviction);
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
