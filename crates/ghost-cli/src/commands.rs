//! CLI command definitions.
//!
//! Skeleton implementation for Phase 0. Full CLI commands
//! will be implemented in Phase 6.

use clap::Parser;

/// GhostPages CLI.
#[derive(Debug, Parser)]
#[command(name = "ghostpages", version, about = "GhostPages memory-tiering system")]
pub struct Cli {
    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: CliCommand,
}

/// CLI subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum CliCommand {
    /// Start the daemon.
    Start {
        /// Socket path for IPC.
        #[arg(long, default_value = "/tmp/ghostpages.sock")]
        socket: String,
    },

    /// Stop the daemon.
    Stop,

    /// Store data.
    Store {
        /// File to store.
        file: String,
    },

    /// Retrieve data.
    Retrieve {
        /// Chunk ID to retrieve.
        chunk_id: String,
    },

    /// Delete data.
    Delete {
        /// Chunk ID to delete.
        chunk_id: String,
    },

    /// Query daemon status.
    Status,

    /// Run a trace replay.
    Replay {
        /// Path to trace file.
        trace_path: String,
        /// Replay speed multiplier.
        #[arg(long, default_value = "1.0")]
        speed: f64,
    },
}
