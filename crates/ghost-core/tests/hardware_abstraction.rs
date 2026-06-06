//! Tests for hardware abstraction contracts (Vulkan readiness layer).
//!
//! These tests verify that the hardware abstraction traits are correctly
//! defined and can be used for backend-neutral transfer operations.

use ghost_core::dma_pipeline::{DmaPipeline, DmaStage, DmaTransfer};
use ghost_core::emitter::EventEmitter;
use ghost_core::hardware::{
    BufferLocation, TransferBuffer, TransferDevice, TransferDeviceType, TransferFence,
    TransferSubmission,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ─── Test: TransferDevice trait is object-safe ──────────────────────────────

/// A mock CPU transfer device for testing.
struct CpuTransferDevice {
    max_size: usize,
    async_supported: bool,
}

impl TransferDevice for CpuTransferDevice {
    fn device_type(&self) -> TransferDeviceType {
        TransferDeviceType::CpuMemory
    }

    fn max_transfer_size(&self) -> usize {
        self.max_size
    }

    fn supports_async(&self) -> bool {
        self.async_supported
    }
}

/// A mock GPU transfer device for testing.
struct GpuTransferDevice {
    max_size: usize,
}

impl TransferDevice for GpuTransferDevice {
    fn device_type(&self) -> TransferDeviceType {
        TransferDeviceType::GpuLocal
    }

    fn max_transfer_size(&self) -> usize {
        self.max_size
    }

    fn supports_async(&self) -> bool {
        true
    }
}

#[test]
fn test_transfer_device_trait() {
    // Verify trait is object-safe (can use Box<dyn TransferDevice>)
    let cpu: Box<dyn TransferDevice> = Box::new(CpuTransferDevice {
        max_size: 1024 * 1024,
        async_supported: false,
    });

    assert_eq!(cpu.device_type(), TransferDeviceType::CpuMemory);
    assert_eq!(cpu.max_transfer_size(), 1024 * 1024);
    assert!(!cpu.supports_async());

    let gpu: Box<dyn TransferDevice> = Box::new(GpuTransferDevice {
        max_size: 256 * 1024 * 1024,
    });

    assert_eq!(gpu.device_type(), TransferDeviceType::GpuLocal);
    assert_eq!(gpu.max_transfer_size(), 256 * 1024 * 1024);
    assert!(gpu.supports_async());
}

#[test]
fn test_transfer_device_type_variants() {
    let types = [
        TransferDeviceType::CpuMemory,
        TransferDeviceType::GpuLocal,
        TransferDeviceType::GpuHostVisible,
        TransferDeviceType::DiskIo,
    ];

    // All variants should be distinct
    for i in 0..types.len() {
        for j in 0..types.len() {
            if i == j {
                assert_eq!(types[i], types[j]);
            } else {
                assert_ne!(types[i], types[j]);
            }
        }
    }
}

// ─── Test: TransferBuffer trait ─────────────────────────────────────────────

/// A mock host buffer for testing.
struct HostBuffer {
    size: usize,
    mapped: bool,
}

impl TransferBuffer for HostBuffer {
    fn size(&self) -> usize {
        self.size
    }

    fn is_mapped(&self) -> bool {
        self.mapped
    }
}

#[test]
fn test_transfer_buffer_trait() {
    let buf: Box<dyn TransferBuffer> = Box::new(HostBuffer {
        size: 4096,
        mapped: true,
    });

    assert_eq!(buf.size(), 4096);
    assert!(buf.is_mapped());
}

// ─── Test: BufferLocation enum ──────────────────────────────────────────────

#[test]
fn test_buffer_location_host_variant() {
    let mut data = [0u8; 128];
    let loc = BufferLocation::Host {
        ptr: data.as_mut_ptr(),
        size: data.len(),
    };

    match loc {
        BufferLocation::Host { ptr, size } => {
            assert_eq!(size, 128);
            assert!(!ptr.is_null());
        }
        _ => panic!("expected Host variant"),
    }
}

#[test]
fn test_buffer_location_device_variant() {
    let loc = BufferLocation::Device {
        handle: 0xCAFE_BABE,
        offset: 8192,
    };

    match loc {
        BufferLocation::Device { handle, offset } => {
            assert_eq!(handle, 0xCAFE_BABE);
            assert_eq!(offset, 8192);
        }
        _ => panic!("expected Device variant"),
    }
}

#[test]
fn test_buffer_location_send_sync() {
    // Verify BufferLocation is Send + Sync (required for multi-threaded use)
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<BufferLocation>();
    assert_sync::<BufferLocation>();
}

// ─── Test: TransferSubmission ───────────────────────────────────────────────

#[test]
fn test_transfer_submission_with_fence() {
    let submission = TransferSubmission {
        source: BufferLocation::Host {
            ptr: std::ptr::null_mut(),
            size: 2048,
        },
        destination: BufferLocation::Device {
            handle: 1,
            offset: 0,
        },
        size: 2048,
        fence_id: Some(100),
    };

    assert_eq!(submission.size, 2048);
    assert_eq!(submission.fence_id, Some(100));
}

#[test]
fn test_transfer_submission_without_fence() {
    let submission = TransferSubmission {
        source: BufferLocation::Device {
            handle: 2,
            offset: 4096,
        },
        destination: BufferLocation::Host {
            ptr: std::ptr::null_mut(),
            size: 512,
        },
        size: 512,
        fence_id: None,
    };

    assert_eq!(submission.fence_id, None);
}

// ─── Test: TransferFence trait ──────────────────────────────────────────────

/// A mock fence for testing.
struct MockFence {
    signaled: Arc<AtomicBool>,
}

impl TransferFence for MockFence {
    fn is_signaled(&self) -> bool {
        self.signaled.load(Ordering::SeqCst)
    }

    fn wait(&self, _timeout: Duration) -> Result<(), ghost_core::error::GhostError> {
        if self.is_signaled() {
            Ok(())
        } else {
            Err(ghost_core::error::GhostError::Internal(
                "fence not signaled".to_string(),
            ))
        }
    }
}

#[test]
fn test_transfer_fence_trait() {
    let signaled = Arc::new(AtomicBool::new(true));
    let fence: Box<dyn TransferFence> = Box::new(MockFence {
        signaled: signaled.clone(),
    });

    assert!(fence.is_signaled());
    assert!(fence.wait(Duration::from_secs(1)).is_ok());
}

#[test]
fn test_transfer_fence_not_signaled() {
    let signaled = Arc::new(AtomicBool::new(false));
    let fence = MockFence {
        signaled: signaled.clone(),
    };

    assert!(!fence.is_signaled());
    assert!(fence.wait(Duration::from_secs(1)).is_err());
}

// ─── Test: DmaPipeline lifecycle ────────────────────────────────────────────

#[test]
fn test_dma_pipeline_lifecycle() {
    let (tx, _rx) = tokio::sync::mpsc::channel(256);
    let emitter = EventEmitter::new(tx);
    let mut pipeline = DmaPipeline::new(emitter);

    // Submit a transfer
    let submission = TransferSubmission {
        source: BufferLocation::Host {
            ptr: std::ptr::null_mut(),
            size: 1024,
        },
        destination: BufferLocation::Host {
            ptr: std::ptr::null_mut(),
            size: 1024,
        },
        size: 1024,
        fence_id: None,
    };

    let id = pipeline.submit(submission);
    assert_eq!(pipeline.len(), 1);
    assert_eq!(pipeline.get(id).unwrap().stage, DmaStage::Pending);

    // Progress through stages
    pipeline.mark_submitted(id).unwrap();
    assert_eq!(pipeline.get(id).unwrap().stage, DmaStage::Submitted);

    pipeline.mark_in_progress(id).unwrap();
    assert_eq!(pipeline.get(id).unwrap().stage, DmaStage::InProgress);

    pipeline.mark_completed(id).unwrap();

    // Poll should return the completed transfer
    let completed = pipeline.poll();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].stage, DmaStage::Completed);
    assert!(pipeline.is_empty());
}

#[test]
fn test_dma_pipeline_failure_path() {
    let (tx, _rx) = tokio::sync::mpsc::channel(256);
    let emitter = EventEmitter::new(tx);
    let mut pipeline = DmaPipeline::new(emitter);

    let submission = TransferSubmission {
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
    };

    let id = pipeline.submit(submission);
    pipeline.mark_submitted(id).unwrap();
    pipeline.mark_in_progress(id).unwrap();
    pipeline.mark_failed(id, "timeout").unwrap();

    let completed = pipeline.poll();
    assert_eq!(completed.len(), 1);
    assert_eq!(
        completed[0].stage,
        DmaStage::Failed("timeout".to_string())
    );
}

// ─── Test: DmaTransfer fields ──────────────────────────────────────────────

#[test]
fn test_dma_transfer_fields() {
    let submission = TransferSubmission {
        source: BufferLocation::Host {
            ptr: std::ptr::null_mut(),
            size: 4096,
        },
        destination: BufferLocation::Device {
            handle: 42,
            offset: 0,
        },
        size: 4096,
        fence_id: Some(7),
    };

    let transfer = DmaTransfer {
        id: 123,
        stage: DmaStage::Pending,
        submission,
        submitted_at: std::time::Instant::now(),
    };

    assert_eq!(transfer.id, 123);
    assert_eq!(transfer.stage, DmaStage::Pending);
    assert_eq!(transfer.submission.size, 4096);
    assert_eq!(transfer.submission.fence_id, Some(7));
}

// ─── Test: DmaStage transitions ─────────────────────────────────────────────

#[test]
fn test_dma_stage_terminal_states() {
    assert!(DmaStage::Completed.is_terminal());
    assert!(DmaStage::Failed("err".to_string()).is_terminal());
    assert!(!DmaStage::Pending.is_terminal());
    assert!(!DmaStage::Submitted.is_terminal());
    assert!(!DmaStage::InProgress.is_terminal());
}

#[test]
fn test_dma_stage_active_states() {
    assert!(DmaStage::Submitted.is_active());
    assert!(DmaStage::InProgress.is_active());
    assert!(!DmaStage::Pending.is_active());
    assert!(!DmaStage::Completed.is_active());
    assert!(!DmaStage::Failed("err".to_string()).is_active());
}

// ─── Test: TransferDeviceType is Send + Sync ────────────────────────────────

#[test]
fn test_transfer_device_type_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<TransferDeviceType>();
    assert_sync::<TransferDeviceType>();
}

// ─── Test: TransferSubmission is Send + Sync ────────────────────────────────

#[test]
fn test_transfer_submission_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<TransferSubmission>();
    assert_sync::<TransferSubmission>();
}
