//! CLI tool for interacting with the GhostPages daemon.
//!
//! Connects to the daemon via Unix domain sockets and provides
//! human-readable output for all operations.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use ghost_core::state::PressureState;
use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, TierId};
use ghost_ipc::client::IpcClient;
use ghost_ipc::protocol::{IpcResponse, TierInfo};

// ─── CLI Definition ────────────────────────────────────────────────────────────

/// GhostPages CLI — command-line interface to the daemon.
#[derive(Debug, Parser)]
#[command(name = "ghost-cli", version, about = "GhostPages memory-tiering CLI")]
struct Cli {
    /// Path to the daemon's Unix socket.
    #[arg(long, default_value = "/tmp/ghostpages.sock", global = true)]
    socket: PathBuf,

    /// Subcommand to execute.
    #[command(subcommand)]
    command: Commands,
}

/// CLI subcommands.
#[derive(Debug, Subcommand)]
enum Commands {
    /// Store a file in the daemon.
    Store {
        /// Path to the file to store.
        file: PathBuf,
        /// Target tier.
        #[arg(long)]
        tier: Option<String>,
    },

    /// Retrieve data by ChunkId.
    Retrieve {
        /// Chunk ID (hex string) to retrieve.
        chunk_id: String,
        /// Output file path.
        file: PathBuf,
    },

    /// Delete a chunk.
    Delete {
        /// Chunk ID (hex string) to delete.
        chunk_id: String,
    },

    /// Migrate a chunk between tiers.
    Migrate {
        /// Chunk ID (hex string) to migrate.
        chunk_id: String,
        /// Source tier.
        from: String,
        /// Destination tier.
        to: String,
    },

    /// Show chunk metadata.
    Info {
        /// Chunk ID (hex string) to query.
        chunk_id: String,
    },

    /// List all chunks.
    List {
        /// Filter by tier.
        #[arg(long)]
        tier: Option<String>,
    },

    /// Show system status.
    Status,

    /// Show current pressure state.
    Pressure,

    /// Show recent trace events.
    Trace {
        /// Number of events to show.
        count: Option<usize>,
    },

    /// Trigger a pressure check.
    PressureCheck,

    /// Graceful daemon shutdown.
    Shutdown,

    /// Ping the daemon.
    Ping,
}

// ─── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();

    let mut client = IpcClient::connect(&cli.socket)
        .await
        .context("failed to connect to daemon")?;

    match cli.command {
        Commands::Store { file, tier } => {
            let data = std::fs::read(&file)
                .with_context(|| format!("failed to read file: {}", file.display()))?;
            let tier = tier.map(|t| parse_tier(&t)).transpose()?;
            let chunk_id = client
                .store(data, tier)
                .await
                .context("store request failed")?;
            println!("Stored chunk: {}", chunk_id);
        }

        Commands::Retrieve { chunk_id, file } => {
            let id = parse_chunk_id(&chunk_id)?;
            let data = client
                .retrieve(&id)
                .await
                .context("retrieve request failed")?;
            std::fs::write(&file, &data)
                .with_context(|| format!("failed to write file: {}", file.display()))?;
            println!("Retrieved {} bytes to {}", data.len(), file.display());
        }

        Commands::Delete { chunk_id } => {
            let id = parse_chunk_id(&chunk_id)?;
            client.delete(&id).await.context("delete request failed")?;
            println!("Deleted chunk {}", chunk_id);
        }

        Commands::Migrate { chunk_id, from, to } => {
            let id = parse_chunk_id(&chunk_id)?;
            let from = parse_tier(&from)?;
            let to = parse_tier(&to)?;
            client
                .migrate(&id, from, to)
                .await
                .context("migrate request failed")?;
            println!("Migrating chunk {} from {} to {}", chunk_id, from, to);
        }

        Commands::Info { chunk_id } => {
            let id = parse_chunk_id(&chunk_id)?;
            let meta = client.info(&id).await.context("info request failed")?;
            print_chunk_meta(&meta);
        }

        Commands::List { tier } => {
            let tier = tier.map(|t| parse_tier(&t)).transpose()?;
            let chunks = client.list(tier).await.context("list request failed")?;
            if chunks.is_empty() {
                println!("No chunks found.");
            } else {
                println!("Found {} chunks:", chunks.len());
                for (id, meta) in &chunks {
                    println!("  {}  size={}  tier={}  state={}", id, meta.size, meta.tier, meta.state);
                }
            }
        }

        Commands::Status => {
            let status = client.status().await.context("status request failed")?;
            print_status(&status);
        }

        Commands::Pressure => {
            let state = client.pressure().await.context("pressure request failed")?;
            print_pressure(&state);
        }

        Commands::Trace { count } => {
            let events = client.trace(count).await.context("trace request failed")?;
            if events.is_empty() {
                println!("No trace events.");
            } else {
                println!("Recent trace events ({}):", events.len());
                for event in &events {
                    print_trace_event(event);
                }
            }
        }

        Commands::PressureCheck => {
            let response = client
                .send_request(ghost_ipc::protocol::IpcRequest::PressureCheck)
                .await
                .context("pressure check failed")?;
            match response {
                IpcResponse::PressureCheck { jobs_created } => {
                    println!("Pressure check: {} migration jobs created", jobs_created);
                }
                IpcResponse::Error { code, message } => {
                    eprintln!("Error ({:?}): {}", code, message);
                    std::process::exit(1);
                }
                other => {
                    eprintln!("Unexpected response: {:?}", other);
                    std::process::exit(1);
                }
            }
        }

        Commands::Shutdown => {
            client.shutdown().await.context("shutdown request failed")?;
            println!("Shutdown initiated.");
        }

        Commands::Ping => {
            client.ping().await.context("ping failed")?;
            println!("Pong! Daemon is alive.");
        }
    }

    Ok(())
}

// ─── Parsing Helpers ───────────────────────────────────────────────────────────

fn parse_chunk_id(s: &str) -> Result<ChunkId> {
    let bytes = hex::decode(s).context("invalid hex string for chunk ID")?;
    if bytes.len() != 32 {
        anyhow::bail!("chunk ID must be 32 bytes (64 hex chars), got {} bytes", bytes.len());
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(ChunkId(arr))
}

fn parse_tier(s: &str) -> Result<TierId> {
    match s.to_lowercase().as_str() {
        "ram" => Ok(TierId::Ram),
        "gpu" | "vram" | "gpuvram" => Ok(TierId::GpuVram),
        "disk" | "ssd" | "nvme" => Ok(TierId::Disk),
        "sim" | "simulation" => Ok(TierId::Simulation),
        other => anyhow::bail!(
            "unknown tier '{}'. Valid tiers: ram, gpu, disk, sim",
            other
        ),
    }
}

// ─── Display Helpers ───────────────────────────────────────────────────────────

fn print_chunk_meta(meta: &ghost_core::types::ChunkMeta) {
    println!("Chunk ID:      {}", meta.id);
    println!("Size:          {} bytes", meta.size);
    println!("Compressed:    {} bytes", meta.compressed_size);
    println!("Tier:          {}", meta.tier);
    println!("State:         {}", meta.state);
    println!("Compression:   {}", meta.compression);
    println!("Access count:  {}", meta.access_count);
    println!("Created at:    {}", meta.created_at);
    println!("Last accessed: {}", meta.last_accessed);
}

fn print_status(status: &ghost_ipc::client::StatusResponse) {
    println!("=== GhostPages Daemon Status ===");
    println!("Uptime:        {}s", status.uptime_secs);
    println!("Total chunks:  {}", status.chunks_total);
    println!("Queue depth:   {}", status.queue_depth);
    println!("Active workers:{}", status.active_workers);
    println!();
    println!("Tiers:");
    for tier in &status.tiers {
        print_tier_info(tier);
    }
}

fn print_tier_info(info: &TierInfo) {
    let used_pct = if info.capacity_bytes > 0 {
        (info.used_bytes as f64 / info.capacity_bytes as f64) * 100.0
    } else {
        0.0
    };
    println!(
        "  {}: {} / {} bytes ({:.1}%) — {} chunks",
        info.tier_id,
        info.used_bytes,
        info.capacity_bytes,
        used_pct,
        info.chunk_count
    );
}

fn print_pressure(state: &PressureState) {
    println!("=== GhostPages Pressure State ===");
    println!("Memory:  {:.2}", state.memory_pressure);
    println!("VRAM:    {:.2}", state.vram_pressure);
    println!("I/O:     {:.2}", state.io_pressure);
    println!("Queue:   {}", state.queue_depth);
    println!("Throughput: {} B/s", state.throughput_bps);

    if state.is_critical() {
        println!("Status:  CRITICAL");
    } else if state.is_under_pressure() {
        println!("Status:  UNDER PRESSURE");
    } else {
        println!("Status:  Normal");
    }
}

fn print_trace_event(event: &TraceEvent) {
    let ts = event.timestamp();
    let chunk = event.chunk_id().map(|id| id.to_string()).unwrap_or_default();
    let event_type = event.event_type();

    match event {
        TraceEvent::ChunkCreated { size, tier, .. } => {
            println!("  [{}] {} chunk={} size={} tier={}", ts, event_type, chunk, size, tier);
        }
        TraceEvent::ChunkStateChanged { from, to, .. } => {
            println!("  [{}] {} chunk={} from={} to={}", ts, event_type, chunk, from, to);
        }
        TraceEvent::TransferStarted { job, .. } => {
            println!(
                "  [{}] {} chunk={} from={} to={} priority={:?}",
                ts, event_type, chunk, job.from_tier, job.to_tier, job.priority
            );
        }
        TraceEvent::TransferCompleted {
            from, to, duration_ms, ..
        } => {
            println!(
                "  [{}] {} chunk={} from={} to={} duration={}ms",
                ts, event_type, chunk, from, to, duration_ms
            );
        }
        TraceEvent::TransferFailed { error, .. } => {
            println!("  [{}] {} chunk={} error={}", ts, event_type, chunk, error);
        }
        TraceEvent::PressureSample { state, .. } => {
            println!(
                "  [{}] {} mem={:.2} vram={:.2} io={:.2}",
                ts, event_type, state.memory_pressure, state.vram_pressure, state.io_pressure
            );
        }
        TraceEvent::Eviction {
            tier, reason, ..
        } => {
            println!(
                "  [{}] {} chunk={} tier={} reason={}",
                ts, event_type, chunk, tier, reason
            );
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tier_ram() {
        assert_eq!(parse_tier("ram").unwrap(), TierId::Ram);
        assert_eq!(parse_tier("RAM").unwrap(), TierId::Ram);
    }

    #[test]
    fn test_parse_tier_gpu() {
        assert_eq!(parse_tier("gpu").unwrap(), TierId::GpuVram);
        assert_eq!(parse_tier("vram").unwrap(), TierId::GpuVram);
        assert_eq!(parse_tier("gpuvram").unwrap(), TierId::GpuVram);
    }

    #[test]
    fn test_parse_tier_disk() {
        assert_eq!(parse_tier("disk").unwrap(), TierId::Disk);
        assert_eq!(parse_tier("ssd").unwrap(), TierId::Disk);
        assert_eq!(parse_tier("nvme").unwrap(), TierId::Disk);
    }

    #[test]
    fn test_parse_tier_sim() {
        assert_eq!(parse_tier("sim").unwrap(), TierId::Simulation);
        assert_eq!(parse_tier("simulation").unwrap(), TierId::Simulation);
    }

    #[test]
    fn test_parse_tier_invalid() {
        assert!(parse_tier("invalid").is_err());
        assert!(parse_tier("").is_err());
    }

    #[test]
    fn test_parse_chunk_id_valid() {
        let hex = "a".repeat(64);
        let id = parse_chunk_id(&hex).unwrap();
        assert_eq!(id, ChunkId([0xaa; 32]));
    }

    #[test]
    fn test_parse_chunk_id_wrong_length() {
        let hex = "a".repeat(32); // Too short
        assert!(parse_chunk_id(&hex).is_err());
    }

    #[test]
    fn test_parse_chunk_id_invalid_hex() {
        assert!(parse_chunk_id("zzzz").is_err());
    }
}
