//! Hardware abstraction contracts for GPU/Vulkan readiness.
//!
//! This module provides backend-neutral traits for transfer devices, buffers,
//! submissions, and fences. These abstractions prepare the system for Vulkan
//! GPU integration without implementing actual GPU code.

use std::time::Duration;

use crate::error::GhostError;

// ─── Transfer Device ─────────────────────────────────────────────────────────

/// Backend-neutral transfer device abstraction.
///
/// Represents a device capable of performing data transfers (CPU memory,
/// GPU local memory, GPU host-visible memory, or disk I/O). This trait is
/// object-safe and can be used with dynamic dispatch.
pub trait TransferDevice: Send + Sync {
    /// The type of this transfer device.
    fn device_type(&self) -> TransferDeviceType;

    /// Maximum single transfer size in bytes.
    fn max_transfer_size(&self) -> usize;

    /// Whether this device supports asynchronous transfers.
    fn supports_async(&self) -> bool;
}

/// Types of transfer devices in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransferDeviceType {
    /// Standard CPU memory (RAM).
    CpuMemory,

    /// GPU local memory (VRAM, not directly accessible from CPU).
    GpuLocal,

    /// GPU host-visible memory (mapped CPU-GPU shared memory).
    GpuHostVisible,

    /// Disk I/O (block device or file system).
    DiskIo,
}

// ─── Transfer Buffer ─────────────────────────────────────────────────────────

/// Backend-neutral transfer buffer abstraction.
///
/// Represents a buffer that can be used as a source or destination for
/// data transfers. The buffer may be host-mapped or device-local.
pub trait TransferBuffer: Send + Sync {
    /// Size of the buffer in bytes.
    fn size(&self) -> usize;

    /// Whether the buffer is host-mapped (CPU-accessible).
    fn is_mapped(&self) -> bool;
}

// ─── Buffer Location ─────────────────────────────────────────────────────────

/// Location of a buffer in the system.
///
/// Used to specify the source and destination of a transfer operation.
/// The pointer/handle is opaque — the actual interpretation depends on
/// the transfer device.
#[derive(Debug, Clone, Copy)]
pub enum BufferLocation {
    /// Host (CPU) memory location.
    Host {
        /// Pointer to the host memory.
        ptr: *mut u8,
        /// Size of the host memory region.
        size: usize,
    },

    /// Device (GPU) memory location.
    Device {
        /// Opaque device handle (e.g., VkBuffer, VkDeviceMemory).
        handle: u64,
        /// Offset within the device allocation.
        offset: u64,
    },
}

// Safety: BufferLocation contains a raw pointer, but it is never dereferenced
// by the abstraction layer. It is passed through to concrete backends that
// handle the actual memory access. The Send/Sync bounds are enforced by the
// concrete types that produce these locations.
unsafe impl Send for BufferLocation {}
unsafe impl Sync for BufferLocation {}

// ─── Transfer Submission ─────────────────────────────────────────────────────

/// A transfer submission describing a data movement operation.
///
/// This is the unit of work submitted to a [`crate::dma_pipeline::DmaPipeline`]
/// or a future Vulkan transfer queue.
#[derive(Debug, Clone)]
pub struct TransferSubmission {
    /// Source buffer location.
    pub source: BufferLocation,

    /// Destination buffer location.
    pub destination: BufferLocation,

    /// Number of bytes to transfer.
    pub size: usize,

    /// Optional fence ID for synchronization.
    ///
    /// If `Some(id)`, the transfer will signal the fence with the given ID
    /// upon completion. If `None`, no fence signaling is requested.
    pub fence_id: Option<u64>,
}

// ─── Transfer Fence ──────────────────────────────────────────────────────────

/// Backend-neutral transfer fence abstraction.
///
/// A fence is a synchronization primitive that is signaled when a transfer
/// operation completes. It can be used to coordinate between transfer
/// workers and the main pipeline.
pub trait TransferFence: Send + Sync {
    /// Check if the fence has been signaled (non-blocking).
    fn is_signaled(&self) -> bool;

    /// Wait for the fence to be signaled, with a timeout.
    ///
    /// Returns `Ok(())` if the fence was signaled within the timeout,
    /// or an error if the timeout elapsed or the fence failed.
    fn wait(&self, timeout: Duration) -> Result<(), GhostError>;
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_device_type_equality() {
        assert_eq!(TransferDeviceType::CpuMemory, TransferDeviceType::CpuMemory);
        assert_ne!(TransferDeviceType::CpuMemory, TransferDeviceType::GpuLocal);
    }

    #[test]
    fn test_buffer_location_host() {
        let mut data = [0u8; 64];
        let loc = BufferLocation::Host {
            ptr: data.as_mut_ptr(),
            size: data.len(),
        };
        match loc {
            BufferLocation::Host { ptr, size } => {
                assert_eq!(size, 64);
                assert!(!ptr.is_null());
            }
            _ => panic!("expected Host variant"),
        }
    }

    #[test]
    fn test_buffer_location_device() {
        let loc = BufferLocation::Device {
            handle: 0xDEAD_BEEF,
            offset: 4096,
        };
        match loc {
            BufferLocation::Device { handle, offset } => {
                assert_eq!(handle, 0xDEAD_BEEF);
                assert_eq!(offset, 4096);
            }
            _ => panic!("expected Device variant"),
        }
    }

    #[test]
    fn test_transfer_submission_creation() {
        let mut src = [0u8; 128];
        let mut dst = [0u8; 128];
        let submission = TransferSubmission {
            source: BufferLocation::Host {
                ptr: src.as_mut_ptr(),
                size: src.len(),
            },
            destination: BufferLocation::Host {
                ptr: dst.as_mut_ptr(),
                size: dst.len(),
            },
            size: 128,
            fence_id: Some(42),
        };
        assert_eq!(submission.size, 128);
        assert_eq!(submission.fence_id, Some(42));
    }

    #[test]
    fn test_transfer_submission_no_fence() {
        let submission = TransferSubmission {
            source: BufferLocation::Device {
                handle: 1,
                offset: 0,
            },
            destination: BufferLocation::Device {
                handle: 2,
                offset: 0,
            },
            size: 512,
            fence_id: None,
        };
        assert_eq!(submission.fence_id, None);
        assert_eq!(submission.size, 512);
    }
}
