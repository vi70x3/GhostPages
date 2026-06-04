//! Configuration for the GhostPages daemon transfer engine.
//!
//! Defines all configuration types for the orchestrator, scheduler,
//! worker pool, and transfer queue.

use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl Default for OrchestratorStatus {
    fn default() -> Self {
        Self {
            queue_depth: 0,
            queue_full: false,
            active_workers: 0,
            jobs_submitted: 0,
            jobs_completed: 0,
            jobs_failed: 0,
            jobs_cancelled: 0,
            bytes_transferred: 0,
            total_transfer_time_ms: 0,
            trace_event_count: 0,
            shutting_down: false,
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
}
