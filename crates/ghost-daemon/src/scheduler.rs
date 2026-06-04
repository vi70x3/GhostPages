//! Transfer scheduler for the GhostPages daemon.
//!
//! Dequeues jobs from the transfer queue, validates state machine transitions,
//! determines source/target tiers, and dispatches to workers.
//!
//! Integrates live pressure readings to throttle or filter jobs when
//! the system is under pressure.

use std::sync::Arc;

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::state::{ChunkState, PressureState, StateMachine};
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::transfer::{TransferJob, TransferPriority, TransferState};
use ghost_core::types::ChunkId;
use ghost_policy::PlacementPolicy;

use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::config::SchedulerConfig;
use crate::metrics::TransferMetrics;
use crate::queue::TransferQueue;
use crate::trace_log::TraceLog;

/// The transfer scheduler dequeues jobs and dispatches them to workers.
pub struct TransferScheduler {
    queue: Arc<TransferQueue>,
    policy: Arc<dyn PlacementPolicy>,
    state_machine: Arc<std::sync::Mutex<StateMachine>>,
    trace_log: Arc<TraceLog>,
    config: SchedulerConfig,
    metrics: Arc<TransferMetrics>,
    pressure_rx: watch::Receiver<PressureState>,
}

impl TransferScheduler {
    /// Create a new transfer scheduler.
    pub fn new(
        queue: Arc<TransferQueue>,
        policy: Arc<dyn PlacementPolicy>,
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
        }
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

                    // Check pressure before dispatching
                    let pressure = self.pressure_rx.borrow().clone();
                    if self.should_throttle(&job, &pressure) {
                        tracing::debug!(
                            "Throttling job {:?} due to pressure {:.2}",
                            job.chunk_id,
                            pressure.max_pressure()
                        );
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

    /// Determine whether a job should be throttled based on current pressure.
    ///
    /// Under critical pressure, only critical-priority jobs are dispatched.
    /// Under high IO pressure, low-priority jobs are deferred.
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

        false
    }

    /// Dispatch a single job to a worker.
    fn dispatch_job(
        &self,
        mut job: TransferJob,
        worker_tx: &mpsc::Sender<TransferJob>,
    ) -> GhostResult<()> {
        // Validate state machine transition
        {
            let mut sm = self.state_machine.lock().unwrap();
            let current_state = sm.get_state(&job.chunk_id);

            match current_state {
                Some(ChunkState::Stored) | Some(ChunkState::Cached) => {
                    // Valid source states for migration
                    sm.transition(&job.chunk_id, ChunkState::Migrating)?;
                    self.trace_log.record(TraceEvent::ChunkStateChanged {
                        chunk_id: job.chunk_id,
                        from: current_state.unwrap(),
                        to: ChunkState::Migrating,
                        timestamp: current_timestamp(),
                    });
                }
                Some(ChunkState::Failed) => {
                    // Retry: Failed → Migrating is not valid, go through Stored first
                    sm.transition(&job.chunk_id, ChunkState::Stored)?;
                    sm.transition(&job.chunk_id, ChunkState::Migrating)?;
                    self.trace_log.record(TraceEvent::ChunkStateChanged {
                        chunk_id: job.chunk_id,
                        from: ChunkState::Failed,
                        to: ChunkState::Migrating,
                        timestamp: current_timestamp(),
                    });
                }
                Some(ChunkState::Allocated) => {
                    // New chunk being stored — transition to Stored first
                    sm.transition(&job.chunk_id, ChunkState::Stored)?;
                    // If the target is different from source, migrate
                    if job.from_tier != job.to_tier {
                        sm.transition(&job.chunk_id, ChunkState::Migrating)?;
                    }
                    self.trace_log.record(TraceEvent::ChunkStateChanged {
                        chunk_id: job.chunk_id,
                        from: ChunkState::Allocated,
                        to: ChunkState::Migrating,
                        timestamp: current_timestamp(),
                    });
                }
                Some(state) => {
                    return Err(GhostError::InvalidStateTransition {
                        from: format!("{:?}", state),
                        to: "Migrating".to_string(),
                    });
                }
                None => {
                    // Chunk not registered — register it
                    sm.register(job.chunk_id)?;
                    if job.from_tier != job.to_tier {
                        sm.transition(&job.chunk_id, ChunkState::Stored)?;
                        sm.transition(&job.chunk_id, ChunkState::Migrating)?;
                    }
                }
            }
        }

        // Update job state
        job.transition_state(TransferState::Queued);

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
    use ghost_core::types::ChunkId;

    use ghost_policy::{LruConfig, LruPolicy, PlacementPolicy};

    fn test_scheduler() -> TransferScheduler {
        let queue = Arc::new(TransferQueue::new(100));
        let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
        let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
        let trace_log = Arc::new(TraceLog::new(1000));
        let config = SchedulerConfig::default();
        let metrics = Arc::new(TransferMetrics::new());
        let (_pressure_tx, pressure_rx) = watch::channel(PressureState::new());

        TransferScheduler::new(queue, policy, state_machine, trace_log, config, metrics, pressure_rx)
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
}
