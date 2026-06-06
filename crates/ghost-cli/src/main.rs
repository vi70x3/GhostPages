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

    /// Replay a trace file and validate state transitions.
    Replay {
        /// Path to the trace file.
        file: std::path::PathBuf,
        /// Maximum number of events to replay (0 = all).
        #[arg(long)]
        max_events: Option<u64>,
        /// Stop on the first validation error.
        #[arg(long)]
        stop_on_error: bool,
    },

    /// Verify replay determinism and stability of a trace file.
    ReplayVerify {
        /// Path to the trace file.
        file: std::path::PathBuf,
        /// Number of determinism rounds (replay N times and compare checksums).
        #[arg(long, default_value = "3")]
        rounds: u32,
        /// Stop on first validation error during replay.
        #[arg(long)]
        stop_on_error: bool,
    },

    /// Compare two trace files for divergence.
    ReplayCompare {
        /// Path to the baseline trace file.
        baseline: std::path::PathBuf,
        /// Path to the candidate trace file.
        candidate: std::path::PathBuf,
    },

    /// Run invariant validation on a trace file.
    ReplayInvariants {
        /// Path to the trace file.
        file: std::path::PathBuf,
    },

    /// Compute and display the deterministic checksum of a trace file.
    ReplayChecksum {
        /// Path to the trace file.
        file: std::path::PathBuf,
    },

    /// Export a trace file to another format.
    ExportTrace {
        /// Path to the input trace file.
        file: std::path::PathBuf,
        /// Output file path.
        output: std::path::PathBuf,
        /// Export format (json, jsonl, csv).
        #[arg(long, default_value = "json")]
        format: String,
    },

    /// Show replay metrics for a trace file.
    ReplayMetrics {
        /// Path to the trace file.
        file: std::path::PathBuf,
    },

    /// Show comprehensive diagnostic snapshot.
    Diagnostics,

    /// Show queue status.
    Queue,

    /// Show migration status.
    Migration,

    /// Show allocator status.
    Allocator,

    /// Show backend health status.
    Backends,

    /// Show replay status.
    ReplayStatus,

    /// Linux observation commands.
    Linux {
        /// Linux subcommand to execute.
        #[command(subcommand)]
        action: LinuxAction,
    },
}


/// Linux observation subcommands.
#[derive(Debug, Subcommand)]
enum LinuxAction {
    /// Perform a full system scan.
    Scan,

    /// Record observations to file.
    Record {
        /// Path to the output file.
        path: PathBuf,
    },

    /// Replay observations from file.
    Replay {
        /// Path to the input file.
        path: PathBuf,
    },

    /// Show current tier inventory and pressure.
    Status,
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
                    println!(
                        "  {}  size={}  tier={}  state={}",
                        id, meta.size, meta.tier, meta.state
                    );
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

        Commands::Replay {
            file,
            max_events,
            stop_on_error,
        } => {
            use ghost_replay::{ReplayConfig, ReplayEngine};

            let config = ReplayConfig {
                validate_transitions: true,
                stop_on_error,
                max_events: max_events.unwrap_or(0),
            };

            let (_engine, summary) = ReplayEngine::load(&file, config)
                .with_context(|| format!("failed to replay trace file: {}", file.display()))?;

            println!("=== Replay Summary ===");
            println!("Events replayed: {}", summary.events_replayed);
            println!("Chunks created: {}", summary.chunks_created);
            println!("Chunks deleted: {}", summary.chunks_deleted);
            println!("State transitions: {}", summary.state_transitions);
            println!("Transfers completed: {}", summary.transfers_completed);
            println!("Transfers failed: {}", summary.transfers_failed);
            println!("Evictions: {}", summary.evictions);
            println!("Pressure alerts: {}", summary.pressure_alerts);
            println!("Policy decisions: {}", summary.policy_decisions);
            println!("Unique chunks: {}", summary.unique_chunks);
            println!("Validation errors: {}", summary.validation_errors);

            if !summary.errors.is_empty() {
                println!();
                println!(
                    "=== Validation Errors (first {}) ===",
                    summary.errors.len().min(10)
                );
                for err in summary.errors.iter().take(10) {
                    println!("  Event {}: {}", err.event_index, err.message);
                }
            }
        }

        Commands::ReplayVerify {
            file,
            rounds,
            stop_on_error,
        } => {
            use ghost_replay::{ReplayConfig, ReplayVerifier, VerifierConfig};

            let verify_config = VerifierConfig {
                iterations: rounds as usize,
                verify_checksums: true,
                verify_invariants: true,
                stop_on_failure: stop_on_error,
                replay_config: ReplayConfig::default(),
            };

            let verifier = ReplayVerifier::new(verify_config);

            let result = verifier
                .verify_stability(&file)
                .with_context(|| format!("verification failed for: {}", file.display()))?;

            println!("=== Replay Verification ===");
            println!("File: {}", file.display());
            println!("Determinism rounds: {}", rounds);
            println!(
                "Status: {}",
                if result.passed() {
                    "PASSED"
                } else {
                    "FAILED"
                }
            );
            println!("Iterations run: {}", result.iterations_run);
            println!();

            if result.deterministic {
                println!("✓ Determinism: PASSED (all rounds produced identical checksums)");
            } else {
                println!("✗ Determinism: FAILED (checksum mismatch between rounds)");
            }

            if result.violations.is_empty() {
                println!("✓ Invariants: PASSED (no violations)");
            } else {
                println!(
                    "✗ Invariants: FAILED ({} violations)",
                    result.violations.len()
                );
            }

            if let Some(ref div) = result.divergence {
                println!();
                println!("=== Divergence Report ===");
                println!("{}", div.summary());
                for d in &div.divergences {
                    println!("  {:?}", d);
                }
            }

            if !result.violations.is_empty() {
                println!();
                println!("=== Invariant Violations ===");
                for v in &result.violations {
                    println!("  [{:?}] {}", v.severity, v.message);
                }
            }

            if !result.passed() {
                std::process::exit(1);
            }
        }

        Commands::ReplayCompare {
            baseline,
            candidate,
        } => {
            use ghost_replay::divergence::compare_traces;

            let report = compare_traces(&baseline, &candidate)
                .with_context(|| {
                    format!(
                        "failed to compare traces: {} vs {}",
                        baseline.display(),
                        candidate.display()
                    )
                })?;

            println!("=== Trace Comparison ===");
            println!(
                "Baseline:  {} ({} events)",
                baseline.display(),
                report.events_compared
            );
            println!(
                "Candidate: {} ({} events)",
                candidate.display(),
                report.events_compared
            );
            println!();

            if report.identical {
                println!(
                    "✓ Traces are IDENTICAL ({} events compared)",
                    report.events_compared
                );
            } else {
                println!(
                    "✗ Traces DIVERGE at event {}",
                    report
                        .first_divergence_index
                        .map(|i| i.to_string())
                        .unwrap_or_else(|| "?".to_string())
                );
                println!("  Events compared: {}", report.events_compared);
                println!("  Divergences found: {}", report.divergences.len());
                println!();

                for d in &report.divergences {
                    match d {
                        ghost_replay::DivergenceType::LengthMismatch {
                            baseline_len,
                            candidate_len,
                        } => {
                            println!(
                                "  Length mismatch: baseline={} events, candidate={} events",
                                baseline_len, candidate_len
                            );
                        }
                        ghost_replay::DivergenceType::ContentMismatch { index, category } => {
                            println!(
                                "  Event {}: content mismatch (category: {:?})",
                                index, category
                            );
                        }
                        ghost_replay::DivergenceType::TimestampMismatch {
                            index,
                            baseline_ts,
                            candidate_ts,
                        } => {
                            println!(
                                "  Event {}: timestamp mismatch (baseline={}, candidate={})",
                                index, baseline_ts, candidate_ts
                            );
                        }
                        ghost_replay::DivergenceType::ChunkIdMismatch {
                            index,
                            baseline_chunk,
                            candidate_chunk,
                        } => {
                            println!(
                                "  Event {}: chunk ID mismatch (baseline={:?}, candidate={:?})",
                                index, baseline_chunk, candidate_chunk
                            );
                        }
                        ghost_replay::DivergenceType::TypeMismatch {
                            index,
                            baseline_type,
                            candidate_type,
                        } => {
                            println!(
                                "  Event {}: type mismatch (baseline={}, candidate={})",
                                index, baseline_type, candidate_type
                            );
                        }
                        ghost_replay::DivergenceType::TimingOnly { .. } => {
                            println!("  Timing-only difference");
                        }
                        ghost_replay::DivergenceType::DecisionDifference { .. } => {
                            println!("  Decision difference");
                        }
                        ghost_replay::DivergenceType::OrderingDifference { .. } => {
                            println!("  Ordering difference");
                        }
                        ghost_replay::DivergenceType::MissingEvent { .. } => {
                            println!("  Missing event");
                        }
                        ghost_replay::DivergenceType::ChecksumMismatch { .. } => {
                            println!("  Checksum mismatch");
                        }
                    }
                }
                std::process::exit(1);
            }
        }

        Commands::ReplayInvariants { file } => {
            use ghost_replay::{InvariantValidator, TraceReader};

            let mut reader = TraceReader::open(&file)
                .with_context(|| format!("failed to open trace file: {}", file.display()))?;
            let events = reader
                .read_all()
                .with_context(|| format!("failed to read trace file: {}", file.display()))?;

            let validator = InvariantValidator::with_defaults();
            let violations = validator.validate(&events);

            println!("=== Invariant Validation ===");
            println!(
                "File: {} ({} events)",
                file.display(),
                events.len()
            );
            println!("Invariants checked: {}", validator.len());
            println!();

            if violations.is_empty() {
                println!("✓ All invariants passed.");
            } else {
                println!("✗ {} invariant violation(s) found:", violations.len());
                println!();
                for v in &violations {
                    println!("  [{:?}] {}", v.severity, v.message);
                    if let Some(idx) = v.event_index {
                        println!("    at event index: {}", idx);
                    }
                    if let Some(ref chunk_id) = v.chunk_id {
                        println!("    chunk: {}", chunk_id.short_hex());
                    }
                }
                std::process::exit(1);
            }
        }

        Commands::ReplayChecksum { file } => {
            use ghost_replay::from_file;

            let checksum = from_file(&file)
                .with_context(|| format!("failed to compute checksum for: {}", file.display()))?;

            fn hex8_display(bytes: &[u8]) -> String {
                bytes
                    .iter()
                    .take(8)
                    .map(|b| format!("{:02x}", b))
                    .collect()
            }

            println!("=== Trace Checksum ===");
            println!("File: {}", file.display());
            println!("Events: {}", checksum.event_count);
            println!();
            println!("Total:     {}", hex8_display(&checksum.total));
            println!("Content:   {}", hex8_display(&checksum.content));
            println!("Migration: {}", hex8_display(&checksum.migration));
            println!("State:     {}", hex8_display(&checksum.state));
            println!("Other:     {}", hex8_display(&checksum.other));
        }

        Commands::ExportTrace {
            file,
            output,
            format,
        } => {
            use ghost_replay::{export_trace, ExportFormat, TraceReader};

            let mut reader = TraceReader::open(&file)
                .with_context(|| format!("failed to open trace file: {}", file.display()))?;
            let events = reader
                .read_all()
                .with_context(|| format!("failed to read trace file: {}", file.display()))?;

            let format = format
                .parse::<ExportFormat>()
                .with_context(|| format!("invalid export format: {}", format))?;

            let mut out_file = std::fs::File::create(&output)
                .with_context(|| format!("failed to create output file: {}", output.display()))?;
            export_trace(&events, &mut out_file, format).with_context(|| "export failed")?;

            println!(
                "Exported {} events to {} ({})",
                events.len(),
                output.display(),
                format
            );
        }

        Commands::ReplayMetrics { file } => {
            use ghost_replay::{ReplayMetrics, TraceReader};

            let mut reader = TraceReader::open(&file)
                .with_context(|| format!("failed to open trace file: {}", file.display()))?;
            let events = reader
                .read_all()
                .with_context(|| format!("failed to read trace file: {}", file.display()))?;

            let metrics = ReplayMetrics::from_events(&events);

            println!("=== Replay Metrics ===");
            println!("Total events: {}", metrics.total_events);
            println!("Chunks created: {}", metrics.chunks_created);
            println!("Chunks deleted: {}", metrics.chunks_deleted);
            println!("State transitions: {}", metrics.state_transitions);
            println!(
                "Avg transitions/chunk: {:.2}",
                metrics.avg_transitions_per_chunk
            );
            println!("Transfers completed: {}", metrics.transfers_completed);
            println!("Transfers failed: {}", metrics.transfers_failed);
            println!(
                "Transfer success rate: {:.1}%",
                metrics.transfer_success_rate * 100.0
            );
            println!("Evictions: {}", metrics.evictions);
            println!("Pressure alerts: {}", metrics.pressure_alerts);
            println!("Peak memory pressure: {:.2}", metrics.peak_memory_pressure);
            println!("Peak VRAM pressure: {:.2}", metrics.peak_vram_pressure);
            println!("Peak I/O pressure: {:.2}", metrics.peak_io_pressure);
            println!("Policy decisions: {}", metrics.policy_decisions);
            println!("Migrations decided: {}", metrics.migrations_decided);
            println!("Unique chunks: {}", metrics.unique_chunks);
            println!(
                "Time range: {} - {}",
                metrics.time_range.0, metrics.time_range.1
            );

            if !metrics.tier_distribution.is_empty() {
                println!();
                println!("=== Tier Distribution ===");
                for (tier, count) in &metrics.tier_distribution {
                    println!("  {}: {} chunks", tier, count);
                }
            }

            if !metrics.evictions_by_reason.is_empty() {
                println!();
                println!("=== Evictions by Reason ===");
                for (reason, count) in &metrics.evictions_by_reason {
                    println!("  {}: {}", reason, count);
                }
            }
        }

    // ─── Diagnostic Commands ─────────────────────────────────────────────────────

    Commands::Diagnostics => {
        let response = client
            .send_request(ghost_ipc::protocol::IpcRequest::Diagnostics)
            .await
            .context("diagnostics request failed")?;
        match response {
            IpcResponse::Diagnostics { snapshot_json } => {
                // Pretty-print the JSON diagnostic snapshot
                let snapshot: serde_json::Value = serde_json::from_str(&snapshot_json)
                    .context("failed to parse diagnostics JSON")?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&snapshot).unwrap_or(snapshot_json)
                );
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

    Commands::Queue => {
        let response = client
            .send_request(ghost_ipc::protocol::IpcRequest::QueueStatus)
            .await
            .context("queue status request failed")?;
        match response {
            IpcResponse::QueueStatus {
                depth,
                capacity,
                is_full,
                submitted_total,
                dequeued_total,
            } => {
                println!("=== Queue Status ===");
                println!("Depth: {}/{}", depth, capacity);
                println!("Full: {}", is_full);
                println!("Total submitted: {}", submitted_total);
                println!("Total dequeued: {}", dequeued_total);
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

    Commands::Migration => {
        let response = client
            .send_request(ghost_ipc::protocol::IpcRequest::MigrationStatus)
            .await
            .context("migration status request failed")?;
        match response {
            IpcResponse::MigrationStatus {
                active_migrations,
                promotions_total,
                evictions_total,
                failures_total,
                bytes_migrated_total,
            } => {
                println!("=== Migration Status ===");
                println!("Active migrations: {}", active_migrations);
                println!("Total promotions: {}", promotions_total);
                println!("Total evictions: {}", evictions_total);
                println!("Total failures: {}", failures_total);
                println!("Total bytes migrated: {}", bytes_migrated_total);
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

    Commands::Allocator => {
        let response = client
            .send_request(ghost_ipc::protocol::IpcRequest::AllocatorStatus)
            .await
            .context("allocator status request failed")?;
        match response {
            IpcResponse::AllocatorStatus {
                allocated_bytes,
                peak_allocated_bytes,
                allocations_total,
                deallocations_total,
                active_allocations,
            } => {
                println!("=== Allocator Status ===");
                println!("Allocated bytes: {}", allocated_bytes);
                println!("Peak allocated bytes: {}", peak_allocated_bytes);
                println!("Total allocations: {}", allocations_total);
                println!("Total deallocations: {}", deallocations_total);
                println!("Active allocations: {}", active_allocations);
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

    Commands::Backends => {
        let response = client
            .send_request(ghost_ipc::protocol::IpcRequest::BackendStatus)
            .await
            .context("backend status request failed")?;
        match response {
            IpcResponse::BackendStatus { tiers } => {
                println!("=== Backend Health ===");
                if tiers.is_empty() {
                    println!("No backends registered.");
                } else {
                    for tier in &tiers {
                        println!(
                            "  {}: health={}, successes={}, failures={}, consecutive_failures={}",
                            tier.tier_id,
                            tier.health,
                            tier.health_check_successes,
                            tier.health_check_failures,
                            tier.consecutive_failures
                        );
                    }
                }
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

    Commands::ReplayStatus => {
        let response = client
            .send_request(ghost_ipc::protocol::IpcRequest::ReplayStatus)
            .await
            .context("replay status request failed")?;
        match response {
            IpcResponse::ReplayStatus {
                replay_ops_total,
                events_replayed_total,
                validation_errors_total,
                active_replays,
            } => {
                println!("=== Replay Status ===");
                println!("Total replay operations: {}", replay_ops_total);
                println!("Total events replayed: {}", events_replayed_total);
                println!("Total validation errors: {}", validation_errors_total);
                println!("Active replays: {}", active_replays);
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

        Commands::Linux { action } => {
            use ghost_linux::{LinuxRecorder, LinuxReplayer, SystemScanner};
            use ghost_core::time::RealTimeProvider;
            use std::sync::Arc;
            use std::time::Duration;

            let time_provider: Arc<dyn ghost_core::time::TimeProvider> =
                Arc::new(RealTimeProvider);
            let emitter = {
                let (tx, _rx) = tokio::sync::mpsc::channel(256);
                ghost_core::emitter::EventEmitter::new(tx)
            };

            match action {
                LinuxAction::Scan => {
                    let mut scanner = SystemScanner::new(
                        time_provider,
                        emitter,
                        42,
                    );
                    let snapshot = scanner.scan().context("system scan failed")?;
                    println!("=== System Scan ===");
                    println!("Timestamp: {}", snapshot.timestamp);
                    if let Some(ref psi) = snapshot.psi {
                        println!("PSI samples: {}", psi.len());
                        for p in psi {
                            println!("  {:?}: avg10={:.2} avg60={:.2} avg300={:.2} total={}",
                                p.resource, p.avg10, p.avg60, p.avg300, p.total);
                        }
                    }
                    if let Some(ref mem) = snapshot.meminfo {
                        println!("Memory: {} MB total, {} MB available",
                            mem.total_kb / 1024, mem.available_kb / 1024);
                    }
                    if let Some(ref swap) = snapshot.swap {
                        println!("Swap: {} MB total, {} MB used",
                            swap.total_kb / 1024, swap.used_kb / 1024);
                    }
                    if let Some(ref zram) = snapshot.zram {
                        println!("ZRAM: {} devices, ratio={:.2}",
                            zram.devices.len(), zram.compression_ratio);
                    }
                    if let Some(ref tiers) = snapshot.tier_inventory {
                        println!("Tiers: {}", tiers.len());
                        for tier in tiers {
                            println!("  {}: {} / {} bytes ({:.1}%)",
                                tier.name, tier.used_bytes, tier.total_bytes,
                                tier.utilization() * 100.0);
                        }
                    }
                    println!("Recommendations:");
                    for rec in &snapshot.recommendations {
                        println!("  {}", rec);
                    }
                }

                LinuxAction::Record { path } => {
                    let mut scanner = SystemScanner::new(
                        time_provider,
                        emitter,
                        42,
                    );
                    let mut recorder = LinuxRecorder::new(&path)
                        .context("failed to create recorder")?;
                    let _snapshot = scanner.scan_and_record(&mut recorder)
                        .context("scan and record failed")?;
                    recorder.close().context("failed to close recorder")?;
                    println!("Recorded observations to: {}", path.display());
                }

                LinuxAction::Replay { path } => {
                    let mut replayer = LinuxReplayer::new(&path)
                        .context("failed to open replay file")?;
                    replayer.load().context("failed to load replay file")?;
                    println!("=== Replay: {} ===", path.display());
                    println!("Events: {}", replayer.event_count());
                    replayer.reset();
                    while let Some(event) = replayer.next() {
                        println!("  [{}] seq={} ts={}",
                            event.event_name(),
                            event.sequence_id,
                            event.timestamp);
                    }
                }

                LinuxAction::Status => {
                    let mut scanner = SystemScanner::new(
                        time_provider,
                        emitter,
                        42,
                    );
                    let snapshot = scanner.scan().context("status scan failed")?;
                    println!("=== Linux System Status ===");
                    println!("Timestamp: {}", snapshot.timestamp);
                    if let Some(ref tiers) = snapshot.tier_inventory {
                        println!("Tier Inventory ({} tiers):", tiers.len());
                        for tier in tiers {
                            let pressure = tier.pressure.memory_pressure;
                            println!("  {}: used={}/{} bytes ({:.1}%) pressure={:.2}",
                                tier.name, tier.used_bytes, tier.total_bytes,
                                tier.utilization() * 100.0, pressure);
                        }
                    }
                    if let Some(ref psi) = snapshot.psi {
                        for p in psi {
                            println!("PSI {:?}: avg10={:.2}%", p.resource, p.avg10);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}


// ─── Parsing Helpers ───────────────────────────────────────────────────────────

fn parse_chunk_id(s: &str) -> Result<ChunkId> {
    let bytes = hex::decode(s).context("invalid hex string for chunk ID")?;
    if bytes.len() != 32 {
        anyhow::bail!(
            "chunk ID must be 32 bytes (64 hex chars), got {} bytes",
            bytes.len()
        );
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
        other => anyhow::bail!("unknown tier '{}'. Valid tiers: ram, gpu, disk, sim", other),
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
        info.tier_id, info.used_bytes, info.capacity_bytes, used_pct, info.chunk_count
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
    let chunk = event
        .chunk_id()
        .map(|id| id.to_string())
        .unwrap_or_default();
    let event_type = event.event_type();

    match event {
        TraceEvent::ChunkCreated { size, tier, .. } => {
            println!(
                "  [{}] {} chunk={} size={} tier={}",
                ts, event_type, chunk, size, tier
            );
        }
        TraceEvent::ChunkStateChanged { from, to, .. } => {
            println!(
                "  [{}] {} chunk={} from={} to={}",
                ts, event_type, chunk, from, to
            );
        }
        TraceEvent::TransferStarted { job, .. } => {
            println!(
                "  [{}] {} chunk={} from={} to={} priority={:?}",
                ts, event_type, chunk, job.from_tier, job.to_tier, job.priority
            );
        }
        TraceEvent::TransferCompleted {
            from,
            to,
            duration_ms,
            ..
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
        TraceEvent::Eviction { tier, reason, .. } => {
            println!(
                "  [{}] {} chunk={} tier={} reason={}",
                ts, event_type, chunk, tier, reason
            );
        }
        _ => {
            println!("  [{}] {} {:?}", ts, event_type, event);
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
