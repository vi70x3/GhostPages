//! # ghost-linux
//!
//! Linux-specific system integration for GhostPages.
//!
//! Provides read-only observation of Linux kernel subsystems including
//! Pressure Stall Information (PSI) for memory, I/O, and CPU pressure monitoring.

pub mod psi;

pub use psi::{PsiReader, PsiSample, PsiResource, SimulatedPsiReader};
