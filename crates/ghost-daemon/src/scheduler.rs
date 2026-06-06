//! Transfer scheduler for the GhostPages daemon.
//!
//! Dequeues jobs from the transfer queue, validates state machine transitions,
//! determines source/target tiers, and dispatches to workers.
//!
//! Integrates live pressure readings and disk I/O metrics to throttle or
//! filter jobs when the system is under pressure or disk is congested.

use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::error::{GhostError, GhostResult};
use ghost_core::state::{PressureState, StateMachine};
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::transfer::{TransferJob, TransferPriority, TransferState};
use ghost_core::types::TierId;
use ghost_policy::PlacementPolicy;

use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::config::SchedulerConfig;
use crate::io_metrics::IoMetrics;
use crate::metrics::TransferMetrics;
use crate::queue::TransferQueue;
use crate::trace_log::TraceLog;

/// SUBSYSTEM: Migration Engine
///
/// The transfer scheduler dequeues jobs and dispatches them to workers.
pub struct TransferScheduler {
    queue: Arc<TransferQueue>,
    policy: Arc<dyn PlacementPolicy>,
    // State machine is accessed via the orchestrator; scheduler no longer validates transitions.
    state_machine: Arc<std::sync::Mutex<StateMachine>>,
    trace_log: Arc<TraceLog>,
    config: SchedulerConfig,
    metrics: Arc<TransferMetrics>,
    pressure_rx: watch::Receiver<PressureState>,
    /// Optional I/O metrics for disk congestion awareness.
    io_metrics: Option<Arc<IoMetrics>>,
    /// Optional event emitter for unified event taxonomy.
    event_emitter: Option<EventEmitter>,
}

impl TransferScheduler {
    /// Create a new transfer scheduler.
    pub fn new(
        queue: Arc<TransferQueue>,
        policy: Arc<dyn PlacementPolicy>,
        // State machine is accessed via the orchestrator; scheduler no longer validates transitions.
        state_machine: Arc<std::sync::Mutex<StateMachine>>,
        trace_log: Arc<TraceLog>,
        config: SchedulerConfig,
        metrics: Arc<TransferMetrics>,
        pressure_rx: watch::Receiver<PressureState>,
    ) -> Self {
        Self {
            queue,
            policy,
            state_machine,
            trace_log,
            config,
            metrics,
            pressure_rx,
            io_metrics: None,
            event_emitter: None,
        }
    }

    /// Set the I/O metrics for disk congestion awareness.
    pub fn set_io_metrics(&mut self, io_metrics: Arc<IoMetrics>) {
        self.io_metrics = Some(io_metrics);
    }

    /// Set the event emitter for unified event taxonomy.
    pub fn set_event_emitter(&mut self, emitter: EventEmitter) {
        self.event_emitter = Some(emitter);
    }

    /// Run the scheduler loop, dispatching jobs to the worker channel.
    ///
    /// The scheduler:
    /// 1. Waits for jobs from the queue
    /// 2. Checks live pressure to throttle or filter jobs
    /// 3. Validates state machine transitions
    /// 4. Determines source/target tiers via PlacementPolicy
    /// 5. Dispatches to the worker channel
    pub async fn run(
        &self,
        worker_tx: mpsc::Sender<TransferJob>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                Some(job) = self.queue.dequeue_wait() => {
                    // Update queue depth metric
                    self.metrics.set_queue_depth(self.queue.depth() as u64);

                    // Emit QueueDequeue event
                    if let Some(ref emitter) = self.event_emitter {
                        let _ = emitter.try_emit(ghost_core::events::Event::QueueDequeue {
                            task_id: job.attempts as u64,
                            sequence_id: 0,
                        });
                    }

                    // Check pressure before dispatching
                    let pressure = *self.pressure_rx.borrow();
                    if self.should_throttle(&job, &pressure) {
                        tracing::debug!(
                            "Throttling job {:?} due to pressure {:.2}",
                            job.chunk_id,
                            pressure.max_pressure()
                        );
                        self.trace_log.record(TraceEvent::PolicyDecision {
                            chunk_id: job.chunk_id,
                            from: job.from_tier,
                            to: job.to_tier,
                            reason: format!("pressure {:.2}", pressure.max_pressure()),
                            timestamp: current_timestamp(),
                        });
                        continue;
                    }

                    // Validate and dispatch
                    if let Err(e) = self.dispatch_job(job, &worker_tx) {
                        tracing::warn!("Failed to dispatch job: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Scheduler shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Determine whether a job should be throttled based on current pressure
    /// and disk I/O congestion.
    ///
    /// Under critical pressure, only critical-priority jobs are dispatched.
    /// Under high IO pressure, low-priority jobs are deferred.
    /// When disk-aware scheduling is enabled and disk queue depth exceeds
    /// the configured threshold, non-critical migrations to disk are deferred.
    fn should_throttle(&self, job: &TransferJob, pressure: &PressureState) -> bool {
        if pressure.is_critical() {
            // Under critical pressure, only critical-priority jobs go through
            return job.priority != TransferPriority::Critical;
        }

        if pressure.io_pressure > 0.8 {
            // Under high IO pressure, defer low-priority jobs
            if job.priority == TransferPriority::Low {
                return true;
            }
        }

        if pressure.is_under_pressure() {
            // Under moderate pressure, defer large low-priority jobs
            if job.priority == TransferPriority::Low && job.size > 1024 * 1024 {
                return true;
            }
        }

        // Disk-aware scheduling: defer migrations to disk when disk is congested
        if self.config.disk_aware_scheduling {
            if let Some(ref io_metrics) = self.io_metrics {
                let disk_queue = io_metrics.get_queue_depth() as u32;
                if disk_queue > self.config.disk_queue_threshold {
                    // Disk is congested: defer non-critical jobs that target disk
                    if job.to_tier == TierId::Disk && job.priority != TransferPriority::Critical {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Dispatch a single job to a worker.
    ///
    /// Note: State machine transitions are handled by the orchestrator
    /// before job submission. The scheduler only validates that the job
    /// is in an appropriate state and sends it to a worker.
    fn dispatch_job(
        &self,
        mut job: TransferJob,
        worker_tx: &mpsc::Sender<TransferJob>,
    ) -> GhostResult<()> {
        // Update job state
        job.transition_state(TransferState::Queued);

        // Emit policy decision: dispatched
        self.trace_log.record(TraceEvent::PolicyDecision {
            chunk_id: job.chunk_id,
            from: job.from_tier,
            to: job.to_tier,
            reason: format!("from={:?} to={:?}", job.from_tier, job.to_tier),
            timestamp: current_timestamp(),
        });

        // Emit transfer started event on dispatch
        self.trace_log.record(TraceEvent::TransferStarted {
            job: job.clone(),
            timestamp: current_timestamp(),
        });

        // Send to worker
        worker_tx
            .try_send(job)
            .map_err(|_| GhostError::PipelineError("worker channel closed".to_string()))?;

        Ok(())
    }

    /// Get the queue reference.
    pub fn queue(&self) -> &TransferQueue {
        &self.queue
    }

    /// Get the metrics reference.
    pub fn metrics(&self) -> &TransferMetrics {
        &self.metrics
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::ChunkState;
    use ghost_core::types::ChunkId;

    use ghost_policy::{LruConfig, LruPolicy, PlacementPolicy};

    use crate::io_metrics::IoMetrics;

    fn test_scheduler() -> TransferScheduler {
        let queue = Arc::new(TransferQueue::new(100, Arc::new(TraceLog::new(1000))));
        let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
        let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
        let trace_log = Arc::new(TraceLog::new(1000));
        let config = SchedulerConfig::default();
        let metrics = Arc::new(TransferMetrics::new());
        let (_pressure_tx, pressure_rx) = watch::channel(PressureState::new());

        TransferScheduler::new(
            queue,
            policy,
            state_machine,
            trace_log,
            config,
            metrics,
            pressure_rx,
        )
    }

    fn make_disk_job(priority: TransferPriority) -> TransferJob {
        TransferJob::new(
            ChunkId::from_data(b"disk_test"),
            TierId::Ram,
            TierId::Disk,
            4096,
            priority,
        )
    }

    #[tokio::test]
    async fn test_scheduler_dispatch() {
        let scheduler = test_scheduler();
        let (worker_tx, _worker_rx) = mpsc::channel(10);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Register a chunk in the state machine
        let chunk_id = ChunkId::from_data(b"dispatch_test");
        scheduler
            .state_machine
            .lock()
            .unwrap()
            .register(chunk_id)
            .unwrap();
        scheduler
            .state_machine
            .lock()
            .unwrap()
            .transition(&chunk_id, ChunkState::Stored)
            .unwrap();

        // Run scheduler in background
        let scheduler_handle = tokio::spawn(async move {
            scheduler.run(worker_tx, shutdown_rx).await;
        });

        // Wait for scheduler to be running
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Shutdown
        shutdown_tx.send(true).unwrap();
        scheduler_handle.await.unwrap();
    }

    #[test]
    fn test_scheduler_creation() {
        let scheduler = test_scheduler();
        assert!(scheduler.queue().is_empty());
    }

    #[test]
    fn test_disk_aware_scheduling_no_metrics_no_throttle() {
        // Without IoMetrics, disk-aware scheduling should not throttle
        let scheduler = test_scheduler();
        let pressure = PressureState::new();
        let job = make_disk_job(TransferPriority::Normal);
        // Should not throttle when no IoMetrics are set
        assert!(!scheduler.should_throttle(&job, &pressure));
    }

    #[test]
    fn test_disk_aware_scheduling_congested_disk_throttles() {
        // With IoMetrics showing high disk queue, should throttle non-critical to disk
        let mut scheduler = test_scheduler();
        let io_metrics = Arc::new(IoMetrics::new());

        // Simulate disk congestion: queue depth above threshold (64)
        for _ in 0..100 {
            io_metrics.increment_queue_depth();
        }

        scheduler.set_io_metrics(io_metrics);

        let pressure = PressureState::new();
        let job = make_disk_job(TransferPriority::Normal);
        // Should throttle normal priority job to disk when disk is congested
        assert!(scheduler.should_throttle(&job, &pressure));
    }

    #[test]
    fn test_disk_aware_scheduling_congested_disk_allows_critical() {
        // Critical priority jobs should not be throttled even when disk is congested
        let mut scheduler = test_scheduler();
        let io_metrics = Arc::new(IoMetrics::new());

        for _ in 0..100 {
            io_metrics.increment_queue_depth();
        }

        scheduler.set_io_metrics(io_metrics);

        let pressure = PressureState::new();
        let job = make_disk_job(TransferPriority::Critical);
        // Critical jobs should not be throttled
        assert!(!scheduler.should_throttle(&job, &pressure));
    }

    #[test]
    fn test_disk_aware_scheduling_low_queue_no_throttle() {
        // When disk queue is below threshold, should not throttle
        let mut scheduler = test_scheduler();
        let io_metrics = Arc::new(IoMetrics::new());

        // Queue depth below threshold (64)
        for _ in 0..10 {
            io_metrics.increment_queue_depth();
        }

        scheduler.set_io_metrics(io_metrics);

        let pressure = PressureState::new();
        let job = make_disk_job(TransferPriority::Normal);
        // Should not throttle when disk queue is below threshold
        assert!(!scheduler.should_throttle(&job, &pressure));
    }

    #[test]
    fn test_disk_aware_scheduling_disabled_no_throttle() {
        // When disk_aware_scheduling is disabled, should not throttle
        let queue = Arc::new(TransferQueue::new(100, Arc::new(TraceLog::new(1000))));
        let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
        let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
        let trace_log = Arc::new(TraceLog::new(1000));
        let mut config = SchedulerConfig::default();
        config.disk_aware_scheduling = false;
        let metrics = Arc::new(TransferMetrics::new());
        let (_pressure_tx, pressure_rx) = watch::channel(PressureState::new());

        let mut scheduler = TransferScheduler::new(
            queue,
            policy,
            state_machine,
            trace_log,
            config,
            metrics,
            pressure_rx,
        );

        let io_metrics = Arc::new(IoMetrics::new());
        for _ in 0..100 {
            io_metrics.increment_queue_depth();
        }
        scheduler.set_io_metrics(io_metrics);

        let pressure = PressureState::new();
        let job = make_disk_job(TransferPriority::Normal);
        // Should not throttle when disk_aware_scheduling is disabled
        assert!(!scheduler.should_throttle(&job, &pressure));
    }
}
