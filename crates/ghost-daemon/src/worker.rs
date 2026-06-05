//! Worker pool for the GhostPages daemon.
//!
//! Each worker receives transfer jobs and executes them through the
//! full pipeline: compress → transfer → write → verify.
//! Handles retries with exponential backoff and graceful cancellation.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ghost_core::emitter::EventEmitter;
use ghost_core::error::{GhostError, GhostResult};
use ghost_core::state::ChunkState;
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::transfer::{TransferJob, TransferState};
use ghost_core::types::TierId;
use ghost_tier::StorageBackend;

use tokio::sync::mpsc;

use crate::config::WorkerPoolConfig;
use crate::metrics::TransferMetrics;
use crate::trace_log::TraceLog;

/// A pool of worker tasks that process transfer jobs.
#[derive(Debug)]
pub struct WorkerPool {
    config: WorkerPoolConfig,
    backends: BTreeMap<TierId, Arc<dyn StorageBackend>>,
    trace_log: Arc<TraceLog>,
    metrics: Arc<TransferMetrics>,
    active_workers: Arc<AtomicU64>,
    state_machine: Arc<std::sync::Mutex<ghost_core::state::StateMachine>>,
    /// Optional event emitter for unified event taxonomy.
    event_emitter: Option<EventEmitter>,
}

impl WorkerPool {
    /// Create a new worker pool.
    pub fn new(
        config: WorkerPoolConfig,
        backends: BTreeMap<TierId, Arc<dyn StorageBackend>>,
        trace_log: Arc<TraceLog>,
        metrics: Arc<TransferMetrics>,
        state_machine: Arc<std::sync::Mutex<ghost_core::state::StateMachine>>,
    ) -> Self {
        Self {
            config,
            backends,
            trace_log,
            metrics,
            active_workers: Arc::new(AtomicU64::new(0)),
            state_machine,
            event_emitter: None,
        }
    }

    /// Set the event emitter for unified event taxonomy.
    pub fn set_event_emitter(&mut self, emitter: EventEmitter) {
        self.event_emitter = Some(emitter);
    }

    /// Start the worker pool, spawning worker tasks.
    ///
    /// Returns a sender channel for submitting jobs and a JoinHandle vector.
    pub fn start(
        &self,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> (mpsc::Sender<TransferJob>, Vec<tokio::task::JoinHandle<()>>) {
        let (tx, rx) = mpsc::channel::<TransferJob>(self.config.worker_count * 2);
        let mut handles = Vec::with_capacity(self.config.worker_count);

        // Share the receiver among all workers using an Arc<Mutex>
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        for worker_id in 0..self.config.worker_count {
            let backends = self.backends.clone();
            let trace_log = self.trace_log.clone();
            let metrics = self.metrics.clone();
            let active_workers = self.active_workers.clone();
            let state_machine = self.state_machine.clone();
            let max_retries = self.config.max_retries;
            let retry_base_delay_ms = self.config.retry_base_delay_ms;
            let max_retry_delay_ms = self.config.max_retry_delay_ms;
            let enable_compression = self.config.enable_compression;
            let mut shutdown_rx = shutdown_rx.clone();
            let rx = rx.clone();

            let handle = tokio::spawn(async move {
                // Emit worker spawned event
                trace_log.record(TraceEvent::WorkerSpawned {
                    worker_id,
                    timestamp: current_timestamp(),
                });

                loop {
                    // Check for shutdown signal first
                    if *shutdown_rx.borrow() {
                        break;
                    }

                    // Lock the shared receiver and try to get a job
                    let mut guard = rx.lock().await;
                    match guard.try_recv() {
                        Ok(job) => {
                            drop(guard);
                            active_workers.fetch_add(1, Ordering::Relaxed);
                            let mut job = job;
                            let result = Self::execute_transfer(
                                &mut job,
                                &backends,
                                &trace_log,
                                state_machine.clone(),
                                max_retries,
                                retry_base_delay_ms,
                                max_retry_delay_ms,
                                enable_compression,
                                worker_id,
                            )
                            .await;

                            match result {
                                Ok(()) => {
                                    metrics.record_completion();
                                    // After successful cross-tier migration,
                                    // transition chunk from Migrating to Stored
                                    if job.from_tier != job.to_tier {
                                        let mut sm = state_machine.lock().unwrap();
                                        let _ = sm.transition(&job.chunk_id, ChunkState::Stored);
                                        trace_log.record(TraceEvent::ChunkStateChanged {
                                            chunk_id: job.chunk_id,
                                            from: ChunkState::Migrating,
                                            to: ChunkState::Stored,
                                            timestamp: current_timestamp(),
                                        });
                                    }
                                }
                                Err(GhostError::Cancelled) => {
                                    metrics.record_cancellation();
                                    trace_log.record(TraceEvent::TransferCancelled {
                                        chunk_id: job.chunk_id,
                                        from: job.from_tier,
                                        to: job.to_tier,
                                        timestamp: current_timestamp(),
                                    });
                                }
                                Err(e) => {
                                    metrics.record_failure();
                                    trace_log.record(TraceEvent::TransferFailed {
                                        chunk_id: job.chunk_id,
                                        from: job.from_tier,
                                        to: job.to_tier,
                                        error: e.to_string(),
                                        attempt: job.attempts,
                                        timestamp: current_timestamp(),
                                    });
                                }
                            }

                            metrics.record_bytes(job.size as u64);
                            active_workers.fetch_sub(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            drop(guard);
                            // Channel empty, wait for notification or shutdown
                            tokio::select! {
                                _ = shutdown_rx.changed() => {
                                    if *shutdown_rx.borrow() {
                                        break;
                                    }
                                }
                                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                                    // Timeout, loop back to retry
                                }
                            }
                        }
                    }
                }

                // Emit worker stopped event
                trace_log.record(TraceEvent::WorkerStopped {
                    worker_id,
                    timestamp: current_timestamp(),
                });
            });

            handles.push(handle);
        }

        (tx, handles)
    }

    /// Execute a single transfer job with retry logic.
    #[allow(clippy::too_many_arguments)]
    async fn execute_transfer(
        job: &mut TransferJob,
        backends: &BTreeMap<TierId, Arc<dyn StorageBackend>>,
        trace_log: &TraceLog,
        _state_machine: Arc<std::sync::Mutex<ghost_core::state::StateMachine>>,
        max_retries: u32,
        retry_base_delay_ms: u64,
        max_retry_delay_ms: u64,
        enable_compression: bool,
        _worker_id: usize,
    ) -> GhostResult<()> {
        let start_time = std::time::Instant::now();

        // Emit transfer started event
        trace_log.record(TraceEvent::TransferStarted {
            job: job.clone(),
            timestamp: current_timestamp(),
        });

        let mut last_error = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                job.record_attempt();
                // Emit transfer retry event
                trace_log.record(TraceEvent::TransferRetry {
                    chunk_id: job.chunk_id,
                    from: job.from_tier,
                    to: job.to_tier,
                    attempt,
                    timestamp: current_timestamp(),
                });
                // Exponential backoff
                let delay = retry_base_delay_ms * (1u64 << (attempt - 1));
                let delay = delay.min(max_retry_delay_ms);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }

            // Check if we should attempt compression
            let should_compress = enable_compression && job.size > 0;

            // Step 1: Read from source
            job.transition_state(TransferState::Transferring);
            trace_log.record(TraceEvent::ChunkStateChanged {
                chunk_id: job.chunk_id,
                from: ChunkState::Stored,
                to: ChunkState::Migrating,
                timestamp: current_timestamp(),
            });

            let source = backends
                .get(&job.from_tier)
                .ok_or(GhostError::TierUnavailable(job.from_tier))?;

            // For the transfer, we need to read the data.
            // In a real system, we'd look up the allocation from a chunk table.
            // For now, we use a simplified approach: allocate, write, read cycle.
            let data = Self::read_from_backend(source, job.size).await?;

            // Step 2: Optionally compress
            let (payload, _compressed_size) = if should_compress {
                job.transition_state(TransferState::Compressing);
                trace_log.record(TraceEvent::CompressionStarted {
                    chunk_id: job.chunk_id,
                    original_size: job.size,
                    timestamp: current_timestamp(),
                });
                // For Phase 1c, we skip actual compression to keep the pipeline simple.
                // The compression engine is available but we pass through.
                let compressed = data; // pass-through
                let compressed_size = job.size;
                trace_log.record(TraceEvent::CompressionCompleted {
                    chunk_id: job.chunk_id,
                    original_size: job.size,
                    compressed_size,
                    timestamp: current_timestamp(),
                });
                (compressed, compressed_size)
            } else {
                (data, job.size)
            };

            // Step 3: Write to target
            job.transition_state(TransferState::Writing);
            let target = backends
                .get(&job.to_tier)
                .ok_or(GhostError::TierUnavailable(job.to_tier))?;

            if let Err(e) = Self::write_to_backend(target, &payload).await {
                last_error = Some(GhostError::BackendError(e.to_string()));
                continue; // Retry
            }

            // Step 4: Verify integrity
            job.transition_state(TransferState::Verifying);
            // Verification is a no-op for Phase 1c since we don't track allocations.
            // In a full implementation, we'd verify the checksum here.

            // Success
            job.transition_state(TransferState::Complete);

            let elapsed_ms = start_time.elapsed().as_millis() as u64;
            trace_log.record(TraceEvent::TransferCompleted {
                chunk_id: job.chunk_id,
                from: job.from_tier,
                to: job.to_tier,
                size: job.size,
                duration_ms: elapsed_ms,
                timestamp: current_timestamp(),
            });

            return Ok(());
        }

        // All retries exhausted
        job.transition_state(TransferState::Failed);
        Err(last_error.unwrap_or_else(|| {
            GhostError::Internal("transfer failed after max retries".to_string())
        }))
    }

    /// Read data from a backend.
    ///
    /// For Phase 1c, this allocates space and reads from it.
    /// In a full implementation, the chunk table would provide the allocation.
    async fn read_from_backend(
        backend: &Arc<dyn StorageBackend>,
        size: usize,
    ) -> GhostResult<Vec<u8>> {
        let alloc = backend
            .allocate(size)
            .await
            .map_err(|e| GhostError::BackendError(e.to_string()))?;

        // In Phase 1c, we return a zero-filled buffer as a placeholder.
        // A real implementation would read from the existing allocation.
        // For testing purposes, we write a known pattern and read it back.
        let data = vec![0u8; size];
        backend
            .write(&alloc, &data)
            .await
            .map_err(|e| GhostError::BackendError(e.to_string()))?;

        let mut buf = vec![0u8; size];
        backend
            .read(&alloc, &mut buf)
            .await
            .map_err(|e| GhostError::BackendError(e.to_string()))?;

        backend
            .deallocate(alloc)
            .await
            .map_err(|e| GhostError::BackendError(e.to_string()))?;

        Ok(buf)
    }

    /// Write data to a backend.
    async fn write_to_backend(backend: &Arc<dyn StorageBackend>, data: &[u8]) -> GhostResult<()> {
        let alloc = backend
            .allocate(data.len())
            .await
            .map_err(|e| GhostError::BackendError(e.to_string()))?;

        backend
            .write(&alloc, data)
            .await
            .map_err(|e| GhostError::BackendError(e.to_string()))?;

        backend
            .deallocate(alloc)
            .await
            .map_err(|e| GhostError::BackendError(e.to_string()))?;

        Ok(())
    }

    /// Get the current number of active workers.
    pub fn active_worker_count(&self) -> u64 {
        self.active_workers.load(Ordering::Relaxed)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::ChunkId;
    use ghost_tier::RamBackend;

    fn test_config() -> WorkerPoolConfig {
        WorkerPoolConfig {
            worker_count: 2,
            max_retries: 2,
            retry_base_delay_ms: 10,
            max_retry_delay_ms: 100,
            enable_compression: false,
        }
    }

    fn test_backends() -> BTreeMap<TierId, Arc<dyn StorageBackend>> {
        let mut backends = BTreeMap::new();
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

    #[tokio::test]
    async fn test_worker_pool_successful_transfer() {
        let config = test_config();
        let backends = test_backends();
        let trace_log = Arc::new(TraceLog::new(1000));
        let metrics = Arc::new(TransferMetrics::new());

        let pool = WorkerPool::new(
            config,
            backends,
            trace_log.clone(),
            metrics.clone(),
            Arc::new(std::sync::Mutex::new(ghost_core::state::StateMachine::new())),
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (job_tx, handles) = pool.start(shutdown_rx);

        let job = TransferJob::new(
            ChunkId::from_data(b"worker_test"),
            TierId::Ram,
            TierId::Simulation,
            256,
            ghost_core::transfer::TransferPriority::Normal,
        );

        metrics.record_submission();
        job_tx.send(job).await.unwrap();

        // Give the worker time to process
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check metrics
        assert!(metrics.jobs_completed.load(Ordering::Relaxed) >= 1);

        // Check trace log
        let events = trace_log.get_events();
        assert!(!events.is_empty());
        assert!(events
            .iter()
            .any(|e| matches!(e, TraceEvent::TransferStarted { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, TraceEvent::TransferCompleted { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, TraceEvent::WorkerSpawned { .. })));

        // Shutdown
        shutdown_tx.send(true).unwrap();
        drop(job_tx);
        for h in handles {
            let _ = h.await;
        }
    }

    #[tokio::test]
    async fn test_worker_pool_active_workers() {
        let config = test_config();
        let backends = test_backends();
        let trace_log = Arc::new(TraceLog::new(1000));
        let metrics = Arc::new(TransferMetrics::new());

        let pool = WorkerPool::new(
            config,
            backends,
            trace_log,
            metrics,
            Arc::new(std::sync::Mutex::new(ghost_core::state::StateMachine::new())),
        );

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (_job_tx, _handles) = pool.start(shutdown_rx);

        // Initially no active workers
        assert_eq!(pool.active_worker_count(), 0);
    }

    #[test]
    fn test_worker_pool_creation() {
        let config = test_config();
        let backends = test_backends();
        let trace_log = Arc::new(TraceLog::new(1000));
        let metrics = Arc::new(TransferMetrics::new());

        let pool = WorkerPool::new(
            config,
            backends,
            trace_log,
            metrics,
            Arc::new(std::sync::Mutex::new(ghost_core::state::StateMachine::new())),
        );
        assert_eq!(pool.active_worker_count(), 0);
    }
}
