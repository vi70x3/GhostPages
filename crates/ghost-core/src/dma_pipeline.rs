//! DMA-oriented transfer pipeline for backend-neutral data movement.
//!
//! This module provides [`DmaTransfer`] and [`DmaPipeline`] for managing
//! staged transfers through a DMA-oriented pipeline. The pipeline is
//! backend-neutral — it does not depend on any specific GPU or DMA engine.
//!
//! The pipeline tracks transfer lifecycle stages (Pending → Submitted →
//! InProgress → Completed/Failed) and emits events via an [`EventEmitter`].

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::emitter::EventEmitter;
use crate::error::GhostError;
use crate::events::Event;
use crate::hardware::{TransferSubmission, TransferFence};

/// A staged transfer in the DMA-oriented pipeline.
#[derive(Debug, Clone)]
pub struct DmaTransfer {
    /// Unique transfer ID.
    pub id: u64,

    /// Current stage of the transfer.
    pub stage: DmaStage,

    /// The original submission for this transfer.
    pub submission: TransferSubmission,

    /// When this transfer was submitted.
    pub submitted_at: Instant,
}

/// Lifecycle stages for a DMA transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DmaStage {
    /// Waiting to be submitted to the transfer device.
    Pending,

    /// Submitted to the transfer device, awaiting execution.
    Submitted,

    /// Transfer is actively in progress.
    InProgress,

    /// Transfer completed successfully.
    Completed,

    /// Transfer failed with the given error message.
    Failed(String),
}

impl DmaStage {
    /// Check if this stage is terminal (no further transitions expected).
    pub fn is_terminal(&self) -> bool {
        matches!(self, DmaStage::Completed | DmaStage::Failed(_))
    }

    /// Check if this stage indicates the transfer is in progress.
    pub fn is_active(&self) -> bool {
        matches!(self, DmaStage::Submitted | DmaStage::InProgress)
    }
}

/// DMA transfer pipeline (backend-neutral).
///
/// Manages the lifecycle of DMA transfers from submission through completion.
/// The pipeline emits events for each stage transition, enabling observability
/// and replay.
pub struct DmaPipeline {
    transfers: BTreeMap<u64, DmaTransfer>,
    next_id: Arc<AtomicU64>,
    event_emitter: EventEmitter,
}

impl DmaPipeline {
    /// Create a new DMA pipeline with the given event emitter.
    pub fn new(event_emitter: EventEmitter) -> Self {
        Self {
            transfers: BTreeMap::new(),
            next_id: Arc::new(AtomicU64::new(1)),
            event_emitter,
        }
    }

    /// Submit a new transfer to the pipeline.
    ///
    /// Returns the unique transfer ID assigned to this submission.
    /// The transfer starts in the [`DmaStage::Pending`] stage.
    pub fn submit(&mut self, submission: TransferSubmission) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let transfer = DmaTransfer {
            id,
            stage: DmaStage::Pending,
            submission,
            submitted_at: Instant::now(),
        };

        self.transfers.insert(id, transfer);

        // Emit QueueEnqueue event for the DMA pipeline
        let _ = self.event_emitter.try_emit(Event::QueueEnqueue {
            task_id: id,
            sequence_id: 0,
        });

        id
    }

    /// Poll the pipeline for completed transfers.
    ///
    /// Returns a list of transfers that have reached a terminal stage
    /// (Completed or Failed) since the last poll.
    ///
    /// In a real implementation, this would check hardware completion
    /// status. In this abstraction, it returns transfers that have been
    /// explicitly marked as completed or failed via [`Self::mark_completed`]
    /// or [`Self::mark_failed`].
    pub fn poll(&mut self) -> Vec<DmaTransfer> {
        let completed_ids: Vec<u64> = self
            .transfers
            .iter()
            .filter(|(_, t)| t.stage.is_terminal())
            .map(|(id, _)| *id)
            .collect();

        let mut completed = Vec::new();
        for id in completed_ids {
            if let Some(transfer) = self.transfers.remove(&id) {
                // Emit QueueDequeue event
                let _ = self.event_emitter.try_emit(Event::QueueDequeue {
                    task_id: id,
                    sequence_id: 0,
                });
                completed.push(transfer);
            }
        }

        completed
    }

    /// Get a reference to a transfer by ID.
    pub fn get(&self, id: u64) -> Option<&DmaTransfer> {
        self.transfers.get(&id)
    }

    /// Get a mutable reference to a transfer by ID.
    pub fn get_mut(&mut self, id: u64) -> Option<&mut DmaTransfer> {
        self.transfers.get_mut(&id)
    }

    /// Mark a transfer as submitted (moves from Pending to Submitted).
    pub fn mark_submitted(&mut self, id: u64) -> Result<(), GhostError> {
        let transfer = self
            .transfers
            .get_mut(&id)
            .ok_or_else(|| GhostError::Internal(format!("DMA transfer {} not found", id)))?;

        if transfer.stage != DmaStage::Pending {
            return Err(GhostError::Internal(format!(
                "DMA transfer {} is not in Pending stage (current: {:?})",
                id, transfer.stage
            )));
        }

        transfer.stage = DmaStage::Submitted;
        Ok(())
    }

    /// Mark a transfer as in progress (moves from Submitted to InProgress).
    pub fn mark_in_progress(&mut self, id: u64) -> Result<(), GhostError> {
        let transfer = self
            .transfers
            .get_mut(&id)
            .ok_or_else(|| GhostError::Internal(format!("DMA transfer {} not found", id)))?;

        if transfer.stage != DmaStage::Submitted {
            return Err(GhostError::Internal(format!(
                "DMA transfer {} is not in Submitted stage (current: {:?})",
                id, transfer.stage
            )));
        }

        transfer.stage = DmaStage::InProgress;
        Ok(())
    }

    /// Mark a transfer as completed (moves to Completed).
    pub fn mark_completed(&mut self, id: u64) -> Result<(), GhostError> {
        let transfer = self
            .transfers
            .get_mut(&id)
            .ok_or_else(|| GhostError::Internal(format!("DMA transfer {} not found", id)))?;

        if !transfer.stage.is_active() {
            return Err(GhostError::Internal(format!(
                "DMA transfer {} is not in an active stage (current: {:?})",
                id, transfer.stage
            )));
        }

        transfer.stage = DmaStage::Completed;

        // Emit TransferCompleted event
        let _ = self.event_emitter.try_emit(Event::TransferCompleted {
            chunk_id: crate::types::ChunkId::from_data(format!("dma-{}", id).as_bytes()),
            from: crate::types::TierId::Ram,
            to: crate::types::TierId::GpuVram,
            duration_ms: transfer.submitted_at.elapsed().as_millis() as u64,
            sequence_id: 0,
        });

        Ok(())
    }

    /// Mark a transfer as failed (moves to Failed).
    pub fn mark_failed(&mut self, id: u64, reason: impl Into<String>) -> Result<(), GhostError> {
        let transfer = self
            .transfers
            .get_mut(&id)
            .ok_or_else(|| GhostError::Internal(format!("DMA transfer {} not found", id)))?;

        if !transfer.stage.is_active() {
            return Err(GhostError::Internal(format!(
                "DMA transfer {} is not in an active stage (current: {:?})",
                id, transfer.stage
            )));
        }

        let reason = reason.into();
        transfer.stage = DmaStage::Failed(reason.clone());

        // Emit TransferFailed event
        let _ = self.event_emitter.try_emit(Event::TransferFailed {
            chunk_id: crate::types::ChunkId::from_data(format!("dma-{}", id).as_bytes()),
            from: crate::types::TierId::Ram,
            to: crate::types::TierId::GpuVram,
            reason,
            sequence_id: 0,
        });

        Ok(())
    }

    /// Get the number of transfers currently tracked.
    pub fn len(&self) -> usize {
        self.transfers.len()
    }

    /// Check if the pipeline has no tracked transfers.
    pub fn is_empty(&self) -> bool {
        self.transfers.is_empty()
    }

    /// Get the number of transfers in a given stage.
    pub fn count_in_stage(&self, stage: DmaStage) -> usize {
        self.transfers
            .values()
            .filter(|t| t.stage == stage)
            .count()
    }

    /// Wait for a fence to be signaled.
    ///
    /// This is a convenience method that polls the fence with the given
    /// timeout. The fence is provided as a trait object, allowing different
    /// backend implementations.
    pub fn wait_fence(fence: &dyn TransferFence, timeout: std::time::Duration) -> Result<(), GhostError> {
        fence.wait(timeout)
    }
}

impl std::fmt::Debug for DmaPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DmaPipeline")
            .field("transfer_count", &self.transfers.len())
            .field("next_id", &self.next_id.load(Ordering::SeqCst))
            .finish()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hardware::BufferLocation;

    fn test_emitter() -> (EventEmitter, tokio::sync::mpsc::Receiver<Event>) {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        (EventEmitter::new(tx), rx)
    }

    fn test_submission() -> TransferSubmission {
        TransferSubmission {
            source: BufferLocation::Host {
                ptr: std::ptr::null_mut(),
                size: 1024,
            },
            destination: BufferLocation::Device {
                handle: 1,
                offset: 0,
            },
            size: 1024,
            fence_id: None,
        }
    }

    #[test]
    fn test_dma_pipeline_new() {
        let (emitter, _rx) = test_emitter();
        let pipeline = DmaPipeline::new(emitter);
        assert!(pipeline.is_empty());
        assert_eq!(pipeline.len(), 0);
    }

    #[test]
    fn test_dma_pipeline_submit() {
        let (emitter, _rx) = test_emitter();
        let mut pipeline = DmaPipeline::new(emitter);
        let id = pipeline.submit(test_submission());
        assert_eq!(id, 1);
        assert_eq!(pipeline.len(), 1);
        assert!(!pipeline.is_empty());
    }

    #[test]
    fn test_dma_pipeline_get() {
        let (emitter, _rx) = test_emitter();
        let mut pipeline = DmaPipeline::new(emitter);
        let id = pipeline.submit(test_submission());

        let transfer = pipeline.get(id).unwrap();
        assert_eq!(transfer.id, id);
        assert_eq!(transfer.stage, DmaStage::Pending);
    }

    #[test]
    fn test_dma_pipeline_lifecycle() {
        let (emitter, _rx) = test_emitter();
        let mut pipeline = DmaPipeline::new(emitter);
        let id = pipeline.submit(test_submission());

        // Pending → Submitted
        pipeline.mark_submitted(id).unwrap();
        assert_eq!(pipeline.get(id).unwrap().stage, DmaStage::Submitted);

        // Submitted → InProgress
        pipeline.mark_in_progress(id).unwrap();
        assert_eq!(pipeline.get(id).unwrap().stage, DmaStage::InProgress);

        // InProgress → Completed
        pipeline.mark_completed(id).unwrap();

        // Completed transfers are removed on poll
        let completed = pipeline.poll();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].stage, DmaStage::Completed);
        assert!(pipeline.is_empty());
    }

    #[test]
    fn test_dma_pipeline_failure() {
        let (emitter, _rx) = test_emitter();
        let mut pipeline = DmaPipeline::new(emitter);
        let id = pipeline.submit(test_submission());

        pipeline.mark_submitted(id).unwrap();
        pipeline.mark_in_progress(id).unwrap();
        pipeline
            .mark_failed(id, "device timeout")
            .unwrap();

        let completed = pipeline.poll();
        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0].stage,
            DmaStage::Failed("device timeout".to_string())
        );
    }

    #[test]
    fn test_dma_pipeline_poll_empty() {
        let (emitter, _rx) = test_emitter();
        let mut pipeline = DmaPipeline::new(emitter);
        let completed = pipeline.poll();
        assert!(completed.is_empty());
    }

    #[test]
    fn test_dma_pipeline_count_in_stage() {
        let (emitter, _rx) = test_emitter();
        let mut pipeline = DmaPipeline::new(emitter);

        let id1 = pipeline.submit(test_submission());
        let id2 = pipeline.submit(test_submission());
        let id3 = pipeline.submit(test_submission());

        assert_eq!(pipeline.count_in_stage(DmaStage::Pending), 3);

        pipeline.mark_submitted(id1).unwrap();
        pipeline.mark_submitted(id2).unwrap();

        assert_eq!(pipeline.count_in_stage(DmaStage::Pending), 1);
        assert_eq!(pipeline.count_in_stage(DmaStage::Submitted), 2);

        pipeline.mark_in_progress(id1).unwrap();
        assert_eq!(pipeline.count_in_stage(DmaStage::Submitted), 1);
        assert_eq!(pipeline.count_in_stage(DmaStage::InProgress), 1);
    }

    #[test]
    fn test_dma_stage_is_terminal() {
        assert!(DmaStage::Completed.is_terminal());
        assert!(DmaStage::Failed("err".to_string()).is_terminal());
        assert!(!DmaStage::Pending.is_terminal());
        assert!(!DmaStage::Submitted.is_terminal());
        assert!(!DmaStage::InProgress.is_terminal());
    }

    #[test]
    fn test_dma_stage_is_active() {
        assert!(DmaStage::Submitted.is_active());
        assert!(DmaStage::InProgress.is_active());
        assert!(!DmaStage::Pending.is_active());
        assert!(!DmaStage::Completed.is_active());
        assert!(!DmaStage::Failed("err".to_string()).is_active());
    }

    #[test]
    fn test_dma_pipeline_invalid_transition() {
        let (emitter, _rx) = test_emitter();
        let mut pipeline = DmaPipeline::new(emitter);
        let id = pipeline.submit(test_submission());

        // Cannot mark_in_progress from Pending (must go through Submitted)
        let result = pipeline.mark_in_progress(id);
        assert!(result.is_err());

        // Cannot mark_completed from Pending
        let result = pipeline.mark_completed(id);
        assert!(result.is_err());
    }

    #[test]
    fn test_dma_pipeline_unknown_id() {
        let (emitter, _rx) = test_emitter();
        let mut pipeline = DmaPipeline::new(emitter);

        assert!(pipeline.mark_submitted(999).is_err());
        assert!(pipeline.mark_in_progress(999).is_err());
        assert!(pipeline.mark_completed(999).is_err());
        assert!(pipeline.mark_failed(999, "err").is_err());
    }
}
