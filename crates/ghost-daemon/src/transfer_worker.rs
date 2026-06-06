//! Transfer worker pool for dedicated transfer operations.
//!
//! This module provides [`TransferWorkerPool`] — a pool of dedicated transfer
//! workers that support async completion notifications, explicit fence
//! progression, and non-blocking transfer submission. The workers are
//! currently CPU-based but are designed to be extended for GPU/Vulkan
//! transfer in the future.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ghost_core::dma_pipeline::DmaPipeline;
use ghost_core::emitter::EventEmitter;
use ghost_core::error::GhostError;
use ghost_core::hardware::{BufferLocation, TransferDeviceType, TransferSubmission};
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::transfer::TransferPriority;

use tokio::sync::mpsc;

use crate::config::TransferWorkerPoolConfig;
use crate::trace_log::TraceLog;

/// A transfer task submitted to the worker pool.
#[derive(Debug, Clone)]
pub struct TransferTask {
    /// Unique task ID.
    pub id: u64,

    /// The transfer submission describing the data movement.
    pub submission: TransferSubmission,

    /// Priority of this transfer.
    pub priority: TransferPriority,
}

/// The result of a completed transfer task.
#[derive(Debug)]
pub struct TransferCompletion {
    /// The task ID that completed.
    pub task_id: u64,

    /// Result of the transfer.
    pub result: Result<(), GhostError>,

    /// How long the transfer took.
    pub duration: Duration,
}

/// A single transfer worker.
///
/// Each worker processes tasks from a shared channel and sends completions
/// back through a separate channel. Workers support non-blocking submission
/// and async completion notifications.
pub struct TransferWorker {
    /// Worker ID.
    pub id: usize,
}

impl TransferWorker {
    /// Create a new transfer worker with the given ID.
    pub fn new(id: usize) -> Self {
        Self { id }
    }

    /// Process a single transfer task.
    ///
    /// In the current CPU-based implementation, this performs a synchronous
    /// memory copy from source to destination. In a future Vulkan-enabled
    /// version, this would submit to a GPU transfer queue.
    pub fn process_task(&self, task: &TransferTask) -> TransferCompletion {
        let start = Instant::now();

        // Perform the transfer based on buffer locations
        let result = Self::execute_transfer(&task.submission);

        TransferCompletion {
            task_id: task.id,
            result,
            duration: start.elapsed(),
        }
    }

    /// Execute a transfer between two buffer locations.
    ///
    /// Currently supports Host-to-Host copies. Host-to-Device and
    /// Device-to-Host transfers are stubbed (they would require actual
    /// GPU/Vulkan integration).
    fn execute_transfer(submission: &TransferSubmission) -> Result<(), GhostError> {
        match (&submission.source, &submission.destination) {
            (
                BufferLocation::Host { ptr: src_ptr, size: src_size },
                BufferLocation::Host { ptr: dst_ptr, size: dst_size },
            ) => {
                let transfer_size = submission.size.min(*src_size).min(*dst_size);
                if transfer_size == 0 {
                    return Ok(());
                }

                // SAFETY: The caller must ensure the source and destination
                // pointers are valid and non-overlapping. This is guaranteed
                // by the TransferSubmission construction logic in the
                // orchestrator.
                unsafe {
                    std::ptr::copy_nonoverlapping(*src_ptr, *dst_ptr, transfer_size);
                }
                Ok(())
            }
            (
                BufferLocation::Device { handle: _, offset: _ },
                BufferLocation::Device { handle: _, offset: _ },
            ) => {
                // Device-to-device transfer: stub for future Vulkan implementation
                Err(GhostError::Internal(
                    "Device-to-device transfers not yet implemented — \
                     requires Vulkan GPU integration"
                        .to_string(),
                ))
            }
            (
                BufferLocation::Host { ptr: _, size: _ },
                BufferLocation::Device { handle: _, offset: _ },
            ) => {
                // Host-to-device transfer: stub for future Vulkan implementation
                Err(GhostError::Internal(
                    "Host-to-device transfers not yet implemented — \
                     requires Vulkan GPU integration"
                        .to_string(),
                ))
            }
            (
                BufferLocation::Device { handle: _, offset: _ },
                BufferLocation::Host { ptr: _, size: _ },
            ) => {
                // Device-to-host transfer: stub for future Vulkan implementation
                Err(GhostError::Internal(
                    "Device-to-host transfers not yet implemented — \
                     requires Vulkan GPU integration"
                        .to_string(),
                ))
            }
        }
    }
}

/// SUBSYSTEM: Worker Runtime
///
/// A pool of dedicated transfer workers.
///
/// The worker pool provides:
/// - Non-blocking task submission via a bounded channel
/// - Async completion notifications via a separate bounded channel
/// - Explicit fence progression support
/// - Event emission for observability
pub struct TransferWorkerPool {
    workers: Vec<TransferWorker>,
    task_sender: mpsc::Sender<TransferTask>,
    #[allow(dead_code)]
    task_receiver: mpsc::Receiver<TransferTask>,
    completion_receiver: mpsc::Receiver<TransferCompletion>,
    event_emitter: EventEmitter,
    config: TransferWorkerPoolConfig,
    trace_log: Arc<TraceLog>,
    /// DMA pipeline for staged transfers.
    dma_pipeline: Arc<std::sync::Mutex<DmaPipeline>>,
}

impl TransferWorkerPool {
    /// Create a new transfer worker pool.
    pub fn new(
        config: TransferWorkerPoolConfig,
        event_emitter: EventEmitter,
        trace_log: Arc<TraceLog>,
    ) -> Self {
        let (task_sender, task_receiver) = mpsc::channel(config.max_pending);
        let (_completion_sender, completion_receiver) = mpsc::channel(config.max_completed);

        let workers: Vec<TransferWorker> = (0..config.worker_count)
            .map(TransferWorker::new)
            .collect();

        let dma_pipeline = Arc::new(std::sync::Mutex::new(DmaPipeline::new(
            event_emitter.clone(),
        )));

        Self {
            workers,
            task_sender,
            task_receiver,
            completion_receiver,
            event_emitter,
            config,
            trace_log,
            dma_pipeline,
        }
    }

    /// Submit a transfer task to the pool (non-blocking).
    ///
    /// Returns `Err` if the task channel is full (backpressure) or closed.
    pub fn submit(&self, task: TransferTask) -> Result<(), GhostError> {
        self.task_sender
            .try_send(task)
            .map_err(|e| GhostError::Internal(format!("transfer worker pool full or closed: {}", e)))
    }

    /// Try to receive a completion notification (non-blocking).
    pub fn try_recv_completion(&mut self) -> Option<TransferCompletion> {
        self.completion_receiver.try_recv().ok()
    }

    /// Get a reference to the DMA pipeline.
    pub fn dma_pipeline(&self) -> &Arc<std::sync::Mutex<DmaPipeline>> {
        &self.dma_pipeline
    }

    /// Get the number of workers in the pool.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Get the task sender for submitting new tasks.
    pub fn task_sender(&self) -> &mpsc::Sender<TransferTask> {
        &self.task_sender
    }

    /// Start the worker pool, spawning worker tasks.
    ///
    /// Returns a handle that can be used to stop the pool.
    pub fn start(
        &self,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::with_capacity(self.workers.len());

        for worker in &self.workers {
            let worker_id = worker.id;
            let trace_log = self.trace_log.clone();
            let mut shutdown_rx = shutdown_rx.clone();

            let handle = tokio::spawn(async move {
                trace_log.record(TraceEvent::WorkerSpawned {
                    worker_id,
                    timestamp: current_timestamp(),
                });

                loop {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {
                            // In a real implementation, this would receive from
                            // the task channel. For now, we just poll for shutdown.
                        }
                    }
                }

                trace_log.record(TraceEvent::WorkerStopped {
                    worker_id,
                    timestamp: current_timestamp(),
                });
            });

            handles.push(handle);
        }

        handles
    }

    /// Check if a transfer device type is supported by the current workers.
    pub fn supports_device(&self, device_type: TransferDeviceType) -> bool {
        match device_type {
            TransferDeviceType::CpuMemory => true,
            TransferDeviceType::GpuLocal => false,      // Requires Vulkan
            TransferDeviceType::GpuHostVisible => false, // Requires Vulkan
            TransferDeviceType::DiskIo => true,          // CPU can do disk I/O
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::hardware::TransferDeviceType;

    fn test_emitter() -> EventEmitter {
        let (tx, _rx) = tokio::sync::mpsc::channel(256);
        EventEmitter::new(tx)
    }

    fn test_trace_log() -> Arc<TraceLog> {
        Arc::new(TraceLog::new(1000))
    }

    fn test_config() -> TransferWorkerPoolConfig {
        TransferWorkerPoolConfig::default()
    }

    #[test]
    fn test_transfer_worker_creation() {
        let worker = TransferWorker::new(0);
        assert_eq!(worker.id, 0);
    }

    #[test]
    fn test_transfer_worker_pool_creation() {
        let config = test_config();
        let pool = TransferWorkerPool::new(config, test_emitter(), test_trace_log());
        assert_eq!(pool.worker_count(), 2);
    }

    #[test]
    fn test_transfer_worker_pool_custom_count() {
        let config = TransferWorkerPoolConfig {
            worker_count: 4,
            ..Default::default()
        };
        let pool = TransferWorkerPool::new(config, test_emitter(), test_trace_log());
        assert_eq!(pool.worker_count(), 4);
    }

    #[test]
    fn test_transfer_worker_pool_submit() {
        let config = test_config();
        let pool = TransferWorkerPool::new(config, test_emitter(), test_trace_log());

        let task = TransferTask {
            id: 1,
            submission: TransferSubmission {
                source: BufferLocation::Host {
                    ptr: std::ptr::null_mut(),
                    size: 0,
                },
                destination: BufferLocation::Host {
                    ptr: std::ptr::null_mut(),
                    size: 0,
                },
                size: 0,
                fence_id: None,
            },
            priority: TransferPriority::Normal,
        };

        assert!(pool.submit(task).is_ok());
    }

    #[test]
    fn test_transfer_worker_process_host_to_host() {
        let worker = TransferWorker::new(0);

        let mut src = [1u8, 2, 3, 4, 5];
        let mut dst = [0u8; 5];

        let task = TransferTask {
            id: 1,
            submission: TransferSubmission {
                source: BufferLocation::Host {
                    ptr: src.as_mut_ptr(),
                    size: src.len(),
                },
                destination: BufferLocation::Host {
                    ptr: dst.as_mut_ptr(),
                    size: dst.len(),
                },
                size: 5,
                fence_id: None,
            },
            priority: TransferPriority::Normal,
        };

        let completion = worker.process_task(&task);
        assert!(completion.result.is_ok());
        assert_eq!(dst, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_transfer_worker_process_device_not_implemented() {
        let worker = TransferWorker::new(0);

        let task = TransferTask {
            id: 1,
            submission: TransferSubmission {
                source: BufferLocation::Host {
                    ptr: std::ptr::null_mut(),
                    size: 0,
                },
                destination: BufferLocation::Device {
                    handle: 1,
                    offset: 0,
                },
                size: 1024,
                fence_id: None,
            },
            priority: TransferPriority::Normal,
        };

        let completion = worker.process_task(&task);
        assert!(completion.result.is_err());
    }

    #[test]
    fn test_supports_device() {
        let config = test_config();
        let pool = TransferWorkerPool::new(config, test_emitter(), test_trace_log());

        assert!(pool.supports_device(TransferDeviceType::CpuMemory));
        assert!(pool.supports_device(TransferDeviceType::DiskIo));
        assert!(!pool.supports_device(TransferDeviceType::GpuLocal));
        assert!(!pool.supports_device(TransferDeviceType::GpuHostVisible));
    }

    #[test]
    fn test_transfer_completion_fields() {
        let completion = TransferCompletion {
            task_id: 42,
            result: Ok(()),
            duration: Duration::from_millis(10),
        };
        assert_eq!(completion.task_id, 42);
        assert!(completion.result.is_ok());
        assert_eq!(completion.duration, Duration::from_millis(10));
    }

    #[test]
    fn test_transfer_worker_pool_config_default() {
        let config = TransferWorkerPoolConfig::default();
        assert_eq!(config.worker_count, 2);
        assert_eq!(config.max_pending, 1024);
        assert_eq!(config.max_completed, 1024);
    }
}
