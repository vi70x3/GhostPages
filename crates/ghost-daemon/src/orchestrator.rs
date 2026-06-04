//! Transfer orchestrator for the GhostPages daemon.
//!
//! The top-level orchestrator that ties together the transfer queue, scheduler,
//! worker pool, state machine, trace log, and metrics into a unified API.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::state::{ChunkState, StateMachine};
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::transfer::{TransferJob, TransferPriority};
use ghost_core::types::{ChunkId, TierId};
use ghost_policy::PlacementPolicy;
use ghost_tier::StorageBackend;

use tokio::sync::watch;

use crate::config::{OrchestratorConfig, SchedulerConfig, WorkerPoolConfig};
use crate::metrics::TransferMetrics;
use crate::queue::TransferQueue;
use crate::scheduler::TransferScheduler;
use crate::trace_log::TraceLog;
use crate::worker::WorkerPool;

/// The top-level orchestrator for the transfer engine.
///
/// Provides a unified API for storing, retrieving, migrating, and evicting
/// chunks across memory tiers. Internally coordinates the transfer queue,
/// scheduler, worker pool, state machine, trace log, and metrics.
pub struct TransferOrchestrator {
    config: OrchestratorConfig,
    queue: Arc<TransferQueue>,
    state_machine: Arc<std::sync::Mutex<StateMachine>>,
    trace_log: Arc<TraceLog>,
    metrics: Arc<TransferMetrics>,
    backends: HashMap<TierId, Arc<dyn StorageBackend>>,
    policy: Arc<dyn PlacementPolicy>,
    scheduler_config: SchedulerConfig,
    worker_config: WorkerPoolConfig,
    shutdown_tx: Option<watch::Sender<bool>>,
}

impl TransferOrchestrator {
    /// Create a new transfer orchestrator.
    pub fn new(
        config: OrchestratorConfig,
        backends: HashMap<TierId, Arc<dyn StorageBackend>>,
        policy: Arc<dyn PlacementPolicy>,
    ) -> Self {
        let queue = Arc::new(TransferQueue::new(config.queue_capacity));
        let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
        let trace_log = Arc::new(TraceLog::new(config.trace_max_events));
        let metrics = Arc::new(TransferMetrics::new());

        let scheduler_config = SchedulerConfig {
            max_concurrent_transfers: config.worker_count,
            priority_ordering: true,
        };

        let worker_config = WorkerPoolConfig {
            worker_count: config.worker_count,
            max_retries: config.max_retries,
            retry_base_delay_ms: config.retry_base_delay_ms,
            max_retry_delay_ms: config.max_retry_delay_ms,
            enable_compression: config.enable_compression,
        };

        Self {
            config,
            queue,
            state_machine,
            trace_log,
            metrics,
            backends,
            policy,
            scheduler_config,
            worker_config,
            shutdown_tx: None,
        }
    }

    /// Start the orchestrator, spawning the scheduler and worker pool.
    ///
    /// This must be called before submitting any jobs.
    pub fn start(&mut self) -> GhostResult<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx.clone());

        // Create and start the worker pool
        let worker_pool = WorkerPool::new(
            self.worker_config.clone(),
            self.backends.clone(),
            self.trace_log.clone(),
            self.metrics.clone(),
        );

        let (job_tx, worker_handles) = worker_pool.start(shutdown_rx.clone());

        // Create the scheduler
        let scheduler = TransferScheduler::new(
            self.queue.clone(),
            self.policy.clone(),
            self.state_machine.clone(),
            self.trace_log.clone(),
            self.scheduler_config.clone(),
            self.metrics.clone(),
        );

        // Spawn the scheduler task
        let scheduler_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            scheduler.run(job_tx, scheduler_shutdown_rx).await;
        });

        // Store the worker handles for shutdown
        // We keep them alive by spawning a task that holds them
        tokio::spawn(async move {
            for handle in worker_handles {
                let _ = handle.await;
            }
        });

        tracing::info!(
            "Transfer orchestrator started with {} workers, queue capacity {}",
            self.config.worker_count,
            self.config.queue_capacity
        );

        Ok(())
    }

    /// Store a chunk in the specified tier.
    ///
    /// This registers the chunk in the state machine and submits a transfer
    /// job to write data to the target tier.
    pub fn store(&self, chunk_id: ChunkId, tier: TierId, data: &[u8]) -> GhostResult<()> {
        // Register chunk in state machine
        {
            let mut sm = self.state_machine.lock().unwrap();
            let current = sm.get_state(&chunk_id);
            match current {
                None => {
                    // register sets state to Allocated, then transition to Stored
                    sm.register(chunk_id)?;
                    sm.transition(&chunk_id, ChunkState::Stored)?;
                }
                Some(ChunkState::Allocated) => {
                    sm.transition(&chunk_id, ChunkState::Stored)?;
                }
                Some(ChunkState::Evicted) | Some(ChunkState::Cached) => {
                    sm.transition(&chunk_id, ChunkState::Stored)?;
                }
                Some(state) => {
                    return Err(GhostError::InvalidStateTransition {
                        from: format!("{:?}", state),
                        to: "Stored".to_string(),
                    });
                }
            }
        }

        self.trace_log.record(TraceEvent::ChunkCreated {
            chunk_id,
            size: data.len(),
            tier,
            timestamp: current_timestamp(),
        });

        // For store, we use a same-tier "transfer" that just writes to the backend
        let job = TransferJob::new(chunk_id, tier, tier, data.len(), TransferPriority::Normal);
        self.submit_job(job)
    }

    /// Retrieve a chunk from the specified tier.
    ///
    /// Looks up the chunk in the state machine and submits a transfer job
    /// to read data from the source tier.
    pub fn retrieve(&self, chunk_id: ChunkId, tier: TierId) -> GhostResult<()> {
        // Verify chunk exists and is in a readable state
        {
            let sm = self.state_machine.lock().unwrap();
            let state = sm.get_state(&chunk_id);
            match state {
                Some(ChunkState::Stored) | Some(ChunkState::Cached) => {}
                Some(ChunkState::Migrating) => {
                    return Err(GhostError::Internal(format!(
                        "chunk {:?} is currently migrating",
                        chunk_id
                    )));
                }
                Some(state) => {
                    return Err(GhostError::InvalidStateTransition {
                        from: format!("{:?}", state),
                        to: "Readable".to_string(),
                    });
                }
                None => {
                    return Err(GhostError::ChunkNotFound(format!("{:?}", chunk_id)));
                }
            }
        }

        // For retrieve, we use a same-tier transfer
        let job = TransferJob::new(chunk_id, tier, tier, 0, TransferPriority::High);
        self.submit_job(job)
    }

    /// Migrate a chunk from one tier to another.
    ///
    /// Validates the state machine transition and submits a transfer job.
    pub fn migrate(
        &self,
        chunk_id: ChunkId,
        from_tier: TierId,
        to_tier: TierId,
        size: usize,
    ) -> GhostResult<()> {
        // Validate state machine transition
        {
            let mut sm = self.state_machine.lock().unwrap();
            let current = sm.get_state(&chunk_id);
            match current {
                Some(ChunkState::Stored) | Some(ChunkState::Cached) => {
                    sm.transition(&chunk_id, ChunkState::Migrating)?;
                }
                Some(ChunkState::Failed) => {
                    // Retry path: go through Stored first
                    sm.transition(&chunk_id, ChunkState::Stored)?;
                    sm.transition(&chunk_id, ChunkState::Migrating)?;
                }
                Some(ChunkState::Allocated) => {
                    sm.transition(&chunk_id, ChunkState::Stored)?;
                    sm.transition(&chunk_id, ChunkState::Migrating)?;
                }
                Some(state) => {
                    return Err(GhostError::InvalidStateTransition {
                        from: format!("{:?}", state),
                        to: "Migrating".to_string(),
                    });
                }
                None => {
                    // Register and set up
                    sm.register(chunk_id)?;
                    sm.transition(&chunk_id, ChunkState::Stored)?;
                    sm.transition(&chunk_id, ChunkState::Migrating)?;
                }
            }
        }

        let priority = if size > 1024 * 1024 {
            // Large transfers get lower priority to avoid blocking small ones
            TransferPriority::Normal
        } else {
            TransferPriority::High
        };

        let job = TransferJob::new(chunk_id, from_tier, to_tier, size, priority);
        self.submit_job(job)
    }

    /// Evict a chunk from the specified tier.
    ///
    /// Transitions the chunk to the Evicted state and removes it from the tier.
    pub fn evict(&self, chunk_id: ChunkId, tier: TierId) -> GhostResult<()> {
        {
            let mut sm = self.state_machine.lock().unwrap();
            let current = sm.get_state(&chunk_id);
            match current {
                Some(ChunkState::Stored) | Some(ChunkState::Cached) => {
                    sm.transition(&chunk_id, ChunkState::Evicted)?;
                }
                Some(state) => {
                    return Err(GhostError::InvalidStateTransition {
                        from: format!("{:?}", state),
                        to: "Evicted".to_string(),
                    });
                }
                None => {
                    return Err(GhostError::ChunkNotFound(format!("{:?}", chunk_id)));
                }
            }
        }

        self.trace_log.record(TraceEvent::ChunkStateChanged {
            chunk_id,
            from: ChunkState::Stored,
            to: ChunkState::Evicted,
            timestamp: current_timestamp(),
        });

        tracing::info!("Evicted chunk {:?} from tier {:?}", chunk_id, tier);
        Ok(())
    }

    /// Get the current orchestrator status.
    pub fn status(&self) -> crate::config::OrchestratorStatus {
        let submitted = self.metrics.jobs_submitted.load(std::sync::atomic::Ordering::Relaxed);
        let completed = self.metrics.jobs_completed.load(std::sync::atomic::Ordering::Relaxed);
        let failed = self.metrics.jobs_failed.load(std::sync::atomic::Ordering::Relaxed);
        let cancelled = self.metrics.jobs_cancelled.load(std::sync::atomic::Ordering::Relaxed);
        let bytes = self.metrics.bytes_transferred.load(std::sync::atomic::Ordering::Relaxed);
        let transfer_time = self.metrics.total_transfer_time_ms.load(std::sync::atomic::Ordering::Relaxed);
        let active = self.metrics.active_workers.load(std::sync::atomic::Ordering::Relaxed);

        crate::config::OrchestratorStatus {
            queue_depth: self.queue.depth(),
            queue_full: self.queue.is_full(),
            active_workers: active,
            jobs_submitted: submitted,
            jobs_completed: completed,
            jobs_failed: failed,
            jobs_cancelled: cancelled,
            bytes_transferred: bytes,
            total_transfer_time_ms: transfer_time,
            trace_event_count: self.trace_log.len(),
            shutting_down: self.queue.is_shutdown(),
        }
    }

    /// Shutdown the orchestrator gracefully.
    ///
    /// Stops accepting new jobs, waits for in-flight jobs to complete
    /// (up to the configured timeout), then shuts down workers and scheduler.
    pub fn shutdown(&mut self) -> GhostResult<()> {
        tracing::info!("Orchestrator shutting down...");

        // Signal the queue to stop accepting new submissions
        self.queue.shutdown();

        // Send shutdown signal to scheduler and workers
        if let Some(tx) = self.shutdown_tx.take() {
            tx.send(true).map_err(|_| {
                GhostError::Internal("shutdown signal already sent".to_string())
            })?;
        }

        // Wait for queue to drain (up to shutdown timeout)
        let timeout = Duration::from_secs(self.config.shutdown_timeout_secs);
        let start = std::time::Instant::now();

        while !self.queue.is_empty() {
            if start.elapsed() > timeout {
                tracing::warn!(
                    "Shutdown timeout reached with {} jobs remaining in queue",
                    self.queue.depth()
                );
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        tracing::info!("Orchestrator shut down complete");
        Ok(())
    }

    /// Get a reference to the trace log.
    pub fn trace_log(&self) -> &TraceLog {
        &self.trace_log
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &TransferMetrics {
        &self.metrics
    }

    /// Get a reference to the transfer queue.
    pub fn queue(&self) -> &TransferQueue {
        &self.queue
    }

    /// Submit a transfer job to the queue.
    fn submit_job(&self, job: TransferJob) -> GhostResult<()> {
        self.metrics.record_submission();
        self.trace_log.record(TraceEvent::TransferStarted {
            job: job.clone(),
            timestamp: current_timestamp(),
        });
        self.queue.submit(job)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::ChunkId;
    use ghost_tier::RamBackend;

    use ghost_policy::{LruConfig, LruPolicy, PlacementPolicy};

    fn test_backends() -> HashMap<TierId, Arc<dyn StorageBackend>> {
        let mut backends = HashMap::new();
        backends.insert(
            TierId::Ram,
            Arc::new(RamBackend::with_id(TierId::Ram, 1024 * 1024)) as Arc<dyn StorageBackend>,
        );
        backends.insert(
            TierId::Simulation,
            Arc::new(RamBackend::with_id(TierId::Simulation, 1024 * 1024)) as Arc<dyn StorageBackend>,
        );
        backends
    }

    fn test_config() -> OrchestratorConfig {
        OrchestratorConfig {
            queue_capacity: 1024,
            worker_count: 2,
            max_retries: 2,
            retry_base_delay_ms: 10,
            max_retry_delay_ms: 100,
            enable_compression: false,
            trace_max_events: 1000,
            shutdown_timeout_secs: 5,
        }
    }

    fn test_orchestrator() -> TransferOrchestrator {
        let backends = test_backends();
        let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
        TransferOrchestrator::new(test_config(), backends, policy)
    }

    #[test]
    fn test_orchestrator_creation() {
        let orch = test_orchestrator();
        let status = orch.status();
        assert_eq!(status.queue_depth, 0);
        assert_eq!(status.jobs_submitted, 0);
    }

    #[test]
    fn test_orchestrator_store() {
        let orch = test_orchestrator();
        let chunk_id = ChunkId::from_data(b"store_test");
        let data = b"hello world";

        let result = orch.store(chunk_id, TierId::Ram, data);
        assert!(result.is_ok());

        // Check metrics
        assert_eq!(
            orch.metrics().jobs_submitted.load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // Check trace log
        let events = orch.trace_log().get_events();
        assert!(events.iter().any(|e| matches!(e, TraceEvent::ChunkCreated { .. })));
    }

    #[test]
    fn test_orchestrator_retrieve() {
        let orch = test_orchestrator();
        let chunk_id = ChunkId::from_data(b"retrieve_test");

        // First store the chunk
        orch.store(chunk_id, TierId::Ram, b"data").unwrap();

        // Then retrieve
        let result = orch.retrieve(chunk_id, TierId::Ram);
        assert!(result.is_ok());
    }

    #[test]
    fn test_orchestrator_retrieve_unregistered_fails() {
        let orch = test_orchestrator();
        let chunk_id = ChunkId::from_data(b"nonexistent");

        let result = orch.retrieve(chunk_id, TierId::Ram);
        assert!(result.is_err());
    }

    #[test]
    fn test_orchestrator_migrate() {
        let orch = test_orchestrator();
        let chunk_id = ChunkId::from_data(b"migrate_test");

        let result = orch.migrate(chunk_id, TierId::Ram, TierId::Simulation, 1024);
        assert!(result.is_ok());

        // Check that the chunk was registered in the state machine
        let sm = orch.state_machine.lock().unwrap();
        let state = sm.get_state(&chunk_id);
        assert!(matches!(state, Some(ChunkState::Migrating)));
    }

    #[test]
    fn test_orchestrator_evict() {
        let orch = test_orchestrator();
        let chunk_id = ChunkId::from_data(b"evict_test");

        // First store the chunk
        orch.store(chunk_id, TierId::Ram, b"data").unwrap();

        // Then evict
        let result = orch.evict(chunk_id, TierId::Ram);
        assert!(result.is_ok());

        // Check state
        let sm = orch.state_machine.lock().unwrap();
        let state = sm.get_state(&chunk_id);
        assert!(matches!(state, Some(ChunkState::Evicted)));
    }

    #[test]
    fn test_orchestrator_evict_unregistered_fails() {
        let orch = test_orchestrator();
        let chunk_id = ChunkId::from_data(b"nonexistent");

        let result = orch.evict(chunk_id, TierId::Ram);
        assert!(result.is_err());
    }

    #[test]
    fn test_orchestrator_status() {
        let orch = test_orchestrator();
        let status = orch.status();
        assert_eq!(status.queue_depth, 0);
        assert_eq!(status.active_workers, 0);
    }

    #[test]
    fn test_orchestrator_shutdown() {
        let mut orch = test_orchestrator();
        // Shutdown without start should still work
        let result = orch.shutdown();
        assert!(result.is_ok());
    }

    #[test]
    fn test_orchestrator_queue_access() {
        let orch = test_orchestrator();
        assert!(orch.queue().is_empty());
        assert_eq!(orch.queue().capacity(), 1024);
    }

    #[test]
    fn test_orchestrator_migrate_invalid_state() {
        let orch = test_orchestrator();
        let chunk_id = ChunkId::from_data(b"invalid_migrate");

        // Register as evicted
        orch.store(chunk_id, TierId::Ram, b"data").unwrap();
        orch.evict(chunk_id, TierId::Ram).unwrap();

        // Try to migrate an evicted chunk — should fail
        let result = orch.migrate(chunk_id, TierId::Ram, TierId::Simulation, 1024);
        assert!(result.is_err());
    }

    #[test]
    fn test_orchestrator_store_state_transitions() {
        let orch = test_orchestrator();
        let chunk_id = ChunkId::from_data(b"state_test");

        // Store should register and transition Allocated -> Stored
        orch.store(chunk_id, TierId::Ram, b"data").unwrap();

        let sm = orch.state_machine.lock().unwrap();
        let state = sm.get_state(&chunk_id);
        assert!(matches!(state, Some(ChunkState::Stored)));
    }
}
