//! Vulkan VRAM backend for GhostPages.
//!
//! This crate provides a Vulkan-based GPU VRAM storage backend.
//! It implements the [`StorageBackend`] trait from `ghost-tier` using
//! Vulkan memory allocation and DMA transfers.
//!
//! # Phase 0 Status
//!
//! This is a skeleton implementation. Full Vulkan device enumeration,
//! VRAM allocation, and DMA transfer operations will be implemented in
//! Phase 4.

#![warn(missing_docs)]

/// Vulkan backend module.
pub mod backend;

pub use backend::VulkanBackend;
