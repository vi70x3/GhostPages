//! # ghost-linux
//!
//! Linux-specific system integration for GhostPages.
//!
//! Provides read-only observation of Linux kernel subsystems including
//! Pressure Stall Information (PSI) for memory, I/O, and CPU pressure monitoring,
//! memory statistics from `/proc/meminfo`, and VM statistics from `/proc/vmstat`.

pub mod meminfo;
pub mod policy;
pub mod policy_rules;
pub mod psi;
pub mod recorder;
pub mod replayer;
pub mod scanner;
pub mod swaps;
pub mod tier_inventory;
pub mod vmstat;
pub mod zram;
pub mod hotness_provider;

pub use meminfo::{MeminfoReader, MeminfoSnapshot, SimulatedMeminfoReader};
pub use policy::{Recommendation, PolicyRuntime};
pub use policy_rules::{PolicyRules, SystemState};
pub use psi::{PsiReader, PsiSample, PsiResource, SimulatedPsiReader};
pub use recorder::{LinuxRecorder, LinuxSnapshot};
pub use replayer::{LinuxReplayer, ReplayVerificationResult};
pub use scanner::SystemScanner;
pub use vmstat::{SimulatedVmstatReader, VmstatReader, VmstatSnapshot};
pub use swaps::{SimulatedSwapReader, SwapDevice, SwapKind, SwapReader, SwapTopology};
pub use zram::{SimulatedZramReader, ZramDevice, ZramReader, ZramSnapshot};
pub use tier_inventory::{SimulatedTierInventory, TierInfo, TierInventory, TierKind};
