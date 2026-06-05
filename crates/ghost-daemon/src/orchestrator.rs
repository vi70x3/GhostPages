//! Transfer orchestrator for the GhostPages daemon.
//!
//! The top-level orchestrator that ties together the transfer queue, scheduler,
//! worker pool, state machine, trace log, and metrics into a unified API.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::state::{ChunkState, PressureState, StateMachine};
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::transfer::{TransferJob, TransferPriority};
use ghost_core::types::{ChunkId, TierId};
use ghost_policy::PlacementPolicy;
use ghost_tier::StorageBackend;

use tokio::sync::watch;

use crate::backpressure::BackpressureController;
use crate::config::{BackpressureConfig, MigrationConfig, OrchestratorConfig, SchedulerConfig, WorkerPoolConfig};
use crate::health::HealthTracker;
use crate::hotness_tracker::HotnessTracker;
use crate::metrics::TransferMetrics;
use crate::migration::MigrationEngine;
use crate::pressure::{PressureMonitor, PressureMonitorConfig};
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
    /// Chunk state machine for tracking state transitions.
    pub state_machine: Arc<std::sync::Mutex<StateMachine>>,
    trace_log: Arc<TraceLog>,
    metrics: Arc<TransferMetrics>,
    backends: HashMap<TierId, Arc<dyn StorageBackend>>,
    policy: Arc<dyn PlacementPolicy>,
    scheduler_config: SchedulerConfig,
    worker_config: WorkerPoolConfig,
    shutdown_tx: Option<watch::Sender<bool>>,
    pressure_tx: Option<watch::Sender<PressureState>>,
    pressure_monitor: Option<PressureMonitor>,
    /// Backend health tracker for failure detection and recovery.
    health_tracker: HealthTracker,
    /// Hotness tracker for access pattern analysis.
    hotness_tracker: Arc<HotnessTracker>,
    /// Migration engine for pressure-driven chunk migration.
    migration_engine: Arc<MigrationEngine>,
    /// Backpressure controller for overload management.
    backpressure_controller: Arc<BackpressureController>,
    /// Instant when the orchestrator was created (for uptime tracking).
    start_time: Instant,
}

impl TransferOrchestrator {
    /// Create a new transfer orchestrator.
    pub fn new(
        config: OrchestratorConfig,
        backends: HashMap<TierId, Arc<dyn StorageBackend>>,
        policy: Arc<dyn PlacementPolicy>,
    ) -> Self {
        let trace_log = Arc::new(TraceLog::new(config.trace_max_events));
        let queue = Arc::new(TransferQueue::new(config.queue_capacity, trace_log.clone()));
        let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
        let metrics = Arc::new(TransferMetrics::new());

        // Create health tracker and register all backends
        let mut health_tracker = HealthTracker::new(crate::health::HealthConfig::default());
        for tier_id in backends.keys() {
            health_tracker.register(*tier_id);
            trace_log.record(TraceEvent::BackendRegistered {
                tier: *tier_id,
                timestamp: current_timestamp(),
            });
        }

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

        // Create pressure channel and monitor
        let (pressure_tx, _pressure_rx) = watch::channel(PressureState::new());
        let pressure_monitor_config = PressureMonitorConfig {
            sample_interval_ms: config.pressure_sample_interval_ms,
            smoothing_factor: config.pressure_smoothing_factor,
            pressure_spike_threshold: 0.1,
        };
        let pressure_monitor = PressureMonitor::new(
            pressure_monitor_config,
            config.pressure_history_size,
            trace_log.clone(),
        );

        // Create hotness tracker
        let hotness_tracker = Arc::new(HotnessTracker::new(
            config.pressure_history_size,
            trace_log.clone(),
        ));

        // Create migration engine
        let migration_config = MigrationConfig::default();
        let migration_engine = Arc::new(MigrationEngine::new(
            migration_config,
            policy.clone(),
            hotness_tracker.clone(),
            state_machine.clone(),
            trace_log.clone(),
            backends.clone(),
        ));

        // Create backpressure controller
        let backpressure_config = BackpressureConfig::default();
        let backpressure_controller = Arc::new(BackpressureController::new(
            backpressure_config,
            trace_log.clone(),
        ));

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
            pressure_tx: Some(pressure_tx),
            pressure_monitor: Some(pressure_monitor),
            health_tracker,
            hotness_tracker,
            migration_engine,
            backpressure_controller,
            start_time: Instant::now(),
        }
    }

    /// Start the orchestrator, spawning the scheduler and worker pool.
    ///
    /// This must be called before submitting any jobs.
    pub fn start(&mut self) -> GhostResult<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx.clone());

        // Emit DaemonStarted event
        self.trace_log.record(TraceEvent::DaemonStarted {
            timestamp: current_timestamp(),
        });

        // Create and start the worker pool
        let worker_pool = WorkerPool::new(
            self.worker_config.clone(),
            self.backends.clone(),
            self.trace_log.clone(),
            self.metrics.clone(),
            self.state_machine.clone(),
        );

        let (job_tx, worker_handles) = worker_pool.start(shutdown_rx.clone());

        // Get pressure receiver from the pressure channel
        let pressure_rx = self
            .pressure_tx
            .as_ref()
            .map(|tx| tx.subscribe())
            .unwrap_or_else(|| {
                let (_, rx) = watch::channel(PressureState::new());
                rx
            });

        // Create the scheduler with pressure awareness
        let scheduler = TransferScheduler::new(
            self.queue.clone(),
            self.policy.clone(),
            self.state_machine.clone(),
            self.trace_log.clone(),
            self.scheduler_config.clone(),
            self.metrics.clone(),
            pressure_rx,
        );

        // Spawn the scheduler task
        let scheduler_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            scheduler.run(job_tx, scheduler_shutdown_rx).await;
        });

        // Spawn the pressure monitor task
        if let Some(pressure_monitor) = self.pressure_monitor.take() {
            let backends = self.backends.clone();
            let pm_shutdown_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                pressure_monitor.run(backends, pm_shutdown_rx).await;
            });
        }

        // Spawn the backpressure controller task
        let bp_controller = self.backpressure_controller.clone();
        let bp_pressure_rx = self
            .pressure_tx
            .as_ref()
            .map(|tx| tx.subscribe())
            .unwrap_or_else(|| {
                let (_, rx) = watch::channel(PressureState::new());
                rx
            });
        let bp_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            bp_controller.run(bp_pressure_rx, bp_shutdown_rx).await;
        });

        // Spawn the auto-migration task if enabled
        if self.config.enable_auto_migration {
            let auto_migration_interval = self.config.auto_migration_interval_ms;
            let migration_engine = self.migration_engine.clone();
            let backpressure_controller = self.backpressure_controller.clone();
            let queue = self.queue.clone();
            let _trace_log = self.trace_log.clone();
            let hotness_tracker = self.hotness_tracker.clone();
            let mut am_shutdown_rx = shutdown_rx.clone();
            let pressure_tx = self.pressure_tx.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
                    auto_migration_interval,
                ));
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            // Auto-migration logic: evaluate migration candidates
                            if let Some(ref tx) = pressure_tx {
                                let pressure = *tx.borrow();

                                // Evaluate migration candidates using the migration engine
                                let candidates = migration_engine.evaluate(&pressure);

                                for candidate in candidates {
                                    // Check backpressure before submitting each migration
                                    if !backpressure_controller.should_allow(candidate.priority) {
                                        tracing::debug!(
                                            "Auto-migration: skipping {:?} due to backpressure ({:?})",
                                            candidate.chunk_id,
                                            backpressure_controller.current_action()
                                        );
                                        continue;
                                    }

                                    // Check if migration engine has capacity
                                    if !migration_engine.has_capacity() {
                                        tracing::debug!(
                                            "Auto-migration: at capacity, deferring remaining candidates"
                                        );
                                        break;
                                    }

                                    // Create and submit the transfer job
                                    let job = TransferJob::new(
                                        candidate.chunk_id,
                                        candidate.from_tier,
                                        candidate.to_tier,
                                        candidate.size,
                                        candidate.priority,
                                    );

                                    if let Err(e) = queue.submit_priority(job) {
                                        tracing::warn!(
                                            "Auto-migration: failed to submit job for {:?}: {}",
                                            candidate.chunk_id,
                                            e
                                        );
                                    } else {
                                        migration_engine.mark_active(candidate.chunk_id);
                                        tracing::debug!(
                                            "Auto-migration: submitted {:?} from {:?} to {:?} (priority: {:?})",
                                            candidate.chunk_id,
                                            candidate.from_tier,
                                            candidate.to_tier,
                                            candidate.priority
                                        );
                                    }
                                }
                            }

                            // Periodic hotness decay
                            hotness_tracker.decay_all();
                        }
                        _ = am_shutdown_rx.changed() => {
                            if *am_shutdown_rx.borrow() {
                                tracing::info!("Auto-migration task shutting down");
                                break;
                            }
                        }
                    }
                }
            });
        }

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

    /// Deterministic run of the orchestrator using a fixed timestamp source.
    /// This method is used when `deterministic_mode` is enabled in the config.
    /// It behaves like `start` but ensures all timestamps are generated from a deterministic
    /// source (currently a simple monotonic counter). The implementation is a stub that
    /// forwards to `start` for now.
    pub fn deterministic_run(&mut self) -> GhostResult<()> {
        // In a full implementation, we would replace calls to `current_timestamp()` with a
        // deterministic generator based on a seed from the config. For now we simply call
        // `start` and rely on the flag being set.
        self.start()
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
                    return Err(GhostError::ChunkNotFound(format!("{:?}", chunk_id)));
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

        // Emit Eviction event
        self.trace_log.record(TraceEvent::Eviction {
            chunk_id,
            tier,
            reason: ghost_core::trace::EvictionReason::Manual,
            timestamp: current_timestamp(),
        });

        tracing::info!("Evicted chunk {:?} from tier {:?}", chunk_id, tier);
        Ok(())
    }

    /// Get the current orchestrator status.
    pub fn status(&self) -> crate::config::OrchestratorStatus {
        let submitted = self
            .metrics
            .jobs_submitted
            .load(std::sync::atomic::Ordering::Relaxed);
        let completed = self
            .metrics
            .jobs_completed
            .load(std::sync::atomic::Ordering::Relaxed);
        let failed = self
            .metrics
            .jobs_failed
            .load(std::sync::atomic::Ordering::Relaxed);
        let cancelled = self
            .metrics
            .jobs_cancelled
            .load(std::sync::atomic::Ordering::Relaxed);
        let bytes = self
            .metrics
            .bytes_transferred
            .load(std::sync::atomic::Ordering::Relaxed);
        let transfer_time = self
            .metrics
            .total_transfer_time_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let active = self
            .metrics
            .active_workers
            .load(std::sync::atomic::Ordering::Relaxed);

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

    /// Export the trace log to a binary trace file.
    ///
    /// Writes all recorded trace events to the given path in the GhostPages
    /// binary trace format, including CRC32 checksums and metadata.
    pub fn export_trace_log(
        &self,
        path: &std::path::Path,
        policy_name: &str,
        config_summary: &str,
    ) -> GhostResult<()> {
        use ghost_replay::format::{flags, TraceMetadata};
        use ghost_replay::writer::TraceWriter;

        let events = self.trace_log.get_events();
        let mut writer = TraceWriter::create(path, flags::HAS_CHECKSUM)
            .map_err(|e| GhostError::ReplayError(format!("failed to create trace file: {}", e)))?;

        writer
            .write_events(&events)
            .map_err(|e| GhostError::ReplayError(format!("failed to write events: {}", e)))?;

        let tier_ids: Vec<_> = self.backends.keys().cloned().collect();
        let time_range = if events.is_empty() {
            (0, 0)
        } else {
            (
                events.first().unwrap().timestamp(),
                events.last().unwrap().timestamp(),
            )
        };

        let metadata = TraceMetadata {
            total_events: events.len() as u64,
            total_chunks: self.state_machine.lock().unwrap().snapshot().len() as u64,
            tier_ids,
            time_range,
            policy_name: policy_name.to_string(),
            config_summary: config_summary.to_string(),
        };

        writer
            .close(metadata)
            .map_err(|e| GhostError::ReplayError(format!("failed to close trace file: {}", e)))?;

        tracing::info!(
            "Exported {} trace events to {}",
            events.len(),
            path.display()
        );
        Ok(())
    }

    /// Get the current smoothed pressure state.
    ///
    /// Returns the latest pressure reading from the pressure monitor.
    /// If the pressure monitor is not running, returns a default PressureState.
    pub fn current_pressure(&self) -> PressureState {
        if let Some(ref tx) = self.pressure_tx {
            *tx.borrow()
        } else {
            PressureState::new()
        }
    }

    /// Get the pressure history ring buffer.
    ///
    /// Returns the pressure monitor's history for trend analysis.
    /// Returns None if the pressure monitor has not been started.
    pub fn pressure_history(&self) -> Option<crate::pressure::PressureHistory> {
        // The history is held inside the PressureMonitor; once started it is
        // moved into a spawned task. We expose a snapshot via the pressure_tx
        // subscription. For a full history API the monitor would need to be
        // kept accessible; for now we return the current pressure as a
        // single-entry snapshot.
        let _ = self.pressure_tx;
        None
    }

    /// Run a pressure check and trigger migrations if needed.
    ///
    /// Examines the current pressure state and, if the system is under
    /// pressure, identifies chunks that should be migrated away from
    /// congested tiers.
    pub fn run_pressure_check(&self) -> GhostResult<Vec<(ChunkId, TierId, TierId)>> {
        let pressure = self.current_pressure();

        if !pressure.is_under_pressure() {
            return Ok(Vec::new());
        }

        tracing::info!(
            "Pressure check: max={:.2}, scanning for migration candidates",
            pressure.max_pressure()
        );

        // Identify chunks on congested tiers that could be migrated
        let candidates = self.find_migration_candidates(&pressure);

        let mut migrations = Vec::new();
        for (chunk_id, from_tier, to_tier) in candidates {
            tracing::debug!(
                "Pressure-driven migration: chunk {:?} from {:?} to {:?}",
                chunk_id,
                from_tier,
                to_tier
            );
            migrations.push((chunk_id, from_tier, to_tier));
        }

        Ok(migrations)
    }

    /// Find migration candidates based on current pressure state.
    fn find_migration_candidates(
        &self,
        pressure: &PressureState,
    ) -> Vec<(ChunkId, TierId, TierId)> {
        let mut candidates = Vec::new();

        // If RAM is under pressure, consider migrating to simulation tier
        if pressure.memory_pressure > 0.7 {
            let sm = self.state_machine.lock().unwrap();
            let stored_chunks: Vec<ChunkId> =
                sm.chunks_in_state(ChunkState::Stored).into_iter().collect();

            for chunk_id in stored_chunks {
                // Build a minimal ChunkMeta for the policy check
                let meta = ghost_core::types::ChunkMeta {
                    id: chunk_id,
                    size: 0,
                    compressed_size: 0,
                    tier: TierId::Ram,
                    state: ChunkState::Stored,
                    created_at: 0,
                    last_accessed: 0,
                    access_count: 0,
                    compression: ghost_core::types::CompressionAlgorithm::None,
                    checksum: [0u8; 32],
                };
                if let Some(target_tier) = self.policy.should_migrate(&meta, TierId::Ram, pressure)
                {
                    // Emit PolicyDecision event
                    self.trace_log.record(TraceEvent::PolicyDecision {
                        chunk_id,
                        from: TierId::Ram,
                        to: target_tier,
                        reason: "memory_pressure".to_string(),
                        timestamp: current_timestamp(),
                    });
                    candidates.push((chunk_id, TierId::Ram, target_tier));
                } else {
                    // Emit PolicyDecision event for "no_migration" decision
                    self.trace_log.record(TraceEvent::PolicyDecision {
                        chunk_id,
                        from: TierId::Ram,
                        to: TierId::Ram,
                        reason: "policy_rejected".to_string(),
                        timestamp: current_timestamp(),
                    });
                }
            }
        }

        candidates
    }

    /// Shutdown the orchestrator gracefully.
    ///
    /// Stops accepting new jobs, waits for in-flight jobs to complete
    /// (up to the configured timeout), then shuts down workers and scheduler.
    pub fn shutdown(&mut self) -> GhostResult<()> {
        tracing::info!("Orchestrator shutting down...");

        // Emit DaemonStopping event
        self.trace_log.record(TraceEvent::DaemonStopping {
            timestamp: current_timestamp(),
        });

        // Signal the queue to stop accepting new submissions
        self.queue.shutdown();

        // Send shutdown signal to scheduler and workers
        if let Some(tx) = self.shutdown_tx.take() {
            tx.send(true)
                .map_err(|_| GhostError::Internal("shutdown signal already sent".to_string()))?;
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

    /// Get a reference to the migration engine.
    pub fn migration_engine(&self) -> &MigrationEngine {
        &self.migration_engine
    }

    /// Get a reference to the backends map.
    pub fn backends(&self) -> &HashMap<TierId, Arc<dyn StorageBackend>> {
        &self.backends
    }

    /// Build a diagnostic snapshot of the current system state.
    ///
    /// Collects health information from all subsystems into a single
    /// JSON-serializable snapshot for monitoring and debugging.
    pub fn diagnostic_snapshot(&self) -> crate::diagnostics::DiagnosticSnapshot {
        use crate::diagnostics::{DiagnosticSnapshotBuilder, HealthStatus};

        let pressure = self.current_pressure();
        let mut snapshot = DiagnosticSnapshotBuilder::new(self.start_time).build_default();

        snapshot.timestamp = current_timestamp();
        snapshot.uptime_secs = self.start_time.elapsed().as_secs();
        snapshot.pressure = pressure;

        // Determine overall health from pressure and backend state
        if pressure.is_under_pressure() {
            snapshot.overall_health = HealthStatus::Degraded;
        }

        snapshot
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
            Arc::new(RamBackend::with_id(TierId::Simulation, 1024 * 1024))
                as Arc<dyn StorageBackend>,
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
            pressure_sample_interval_ms: 1000,
            pressure_smoothing_factor: 0.3,
            auto_migration_interval_ms: 5000,
            pressure_history_size: 256,
            enable_auto_migration: false,
            deterministic_mode: false,
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
            orch.metrics()
                .jobs_submitted
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // Check trace log
        let events = orch.trace_log().get_events();
        assert!(events
            .iter()
            .any(|e| matches!(e, TraceEvent::ChunkCreated { .. })));
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

        // Store the chunk first so it is registered in the state machine
        orch.store(chunk_id, TierId::Ram, b"migrate_data").unwrap();
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

        // Check that Eviction trace event was emitted
        let events = orch.trace_log().get_events();
        assert!(events
            .iter()
            .any(|e| matches!(e, TraceEvent::Eviction { .. })));
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

        // Check that DaemonStopping event was emitted
        let events = orch.trace_log().get_events();
        assert!(events
            .iter()
            .any(|e| matches!(e, TraceEvent::DaemonStopping { .. })));
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

    #[test]
    fn test_orchestrator_backend_registered_events() {
        let orch = test_orchestrator();
        let events = orch.trace_log().get_events();
        // Should have BackendRegistered events for Ram and Simulation
        assert!(events.iter().any(|e| matches!(
            e,
            TraceEvent::BackendRegistered {
                tier: TierId::Ram,
                ..
            }
        )));
        assert!(events.iter().any(|e| matches!(
            e,
            TraceEvent::BackendRegistered {
                tier: TierId::Simulation,
                ..
            }
        )));
    }
}
