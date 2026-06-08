//! CLI tool for interacting with the GhostPages daemon.
//!
//! Connects to the daemon via Unix domain sockets and provides
//! human-readable output for all operations.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use ghost_core::hotness_confidence::{ConfidenceLevel, HotnessConfidence};
use ghost_core::hotness_provider::{HotnessProvider, Temperature};
use ghost_core::hotness_summary::HotnessSummary;
use ghost_core::state::PressureState;
use ghost_core::time::RealTimeProvider;
use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, TierId};
use ghost_ipc::client::IpcClient;
use ghost_ipc::protocol::{IpcResponse, TierInfo};
use ghost_linux::damon::{DamonConfig, DamonHotnessProvider, SimulatedDamonProvider};
use ghost_linux::policy::PolicyRuntime;
use ghost_linux::tier_inventory::TierInventory;
use ghost_evaluator::baseline::{evaluate_baseline, BaselineAction, BaselineRecommendation};
use ghost_evaluator::scoring::{score_policy_evaluation, score_recommendation, RecommendationScore, ScoringWeights};
use ghost_evaluator::stability::{RecommendationStability, StabilityTracker};
use ghost_evaluator::tournament::{ArenaLinuxBaselinePolicy, HybridPolicy, PolicyArena, PressurePolicy, HotnessPolicy, TournamentResult};
use ghost_linux::policy::Recommendation;
use ghost_linux::policy_rules::SystemState;

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

    /// DAMON hotness observation.
    #[command(subcommand)]
    Hotness(HotnessCommands),

    /// Policy evaluation.
    #[command(subcommand)]
    Policy(PolicyCommands),

    /// Evaluator commands for recommendation scoring and policy comparison.
    #[command(subcommand)]
    Evaluator(EvaluatorCommands),


    /// Benchmark commands for workload evaluation and policy comparison.
    #[command(subcommand)]
    Bench(BenchCommands),
}

/// Benchmark subcommands.
#[derive(Debug, Subcommand)]
enum BenchCommands {
    /// Run all built-in workload benchmarks.
    Run {
        /// Random seed for deterministic generation.
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Run a specific workload benchmark.
    Workload {
        /// Workload name (idle_desktop, memory_pressure_ramp, build_server, etc.).
        name: String,
        /// Random seed for deterministic generation.
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Show the last benchmark report in markdown format.
    Report {
        /// Output format (markdown, json).
        #[arg(long, default_value = "markdown")]
        format: String,
    },

    /// Run a parameter sweep experiment.
    Experiment {
        /// Experiment name (pressure_weight, hybrid_weight, temperature_threshold).
        name: String,
    },

    /// Show the policy leaderboard from the last benchmark run.
    Leaderboard,
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

/// DAMON hotness observation subcommands.
#[derive(Debug, Subcommand)]
enum HotnessCommands {
    /// Show current hotness summary
    Summary,
    /// Show hot regions
    Hot,
    /// Show cold regions
    Cold,
    /// Show temperature distribution
    Distribution,
    /// Stream hotness updates
    Watch {
        /// Update interval in seconds
        #[arg(short, long, default_value = "5")]
        interval: u64,
    },
}

/// Policy evaluation subcommands.
#[derive(Debug, Subcommand)]
enum PolicyCommands {
    /// Evaluate current policy
    Evaluate,
    /// Show current recommendations
    Recommendations,
    /// Show recommendation history
    History,
    /// Compare policies
    Compare {
        /// First policy name
        policy_a: String,
        /// Second policy name
        policy_b: String,
    },
}

/// Evaluator commands for recommendation scoring and policy comparison.
#[derive(Debug, Subcommand)]
enum EvaluatorCommands {
    /// Score a recommendation against state change.
    Score {
        /// Recommendation type (promote, demote, zram, diskswap, evict, noaction).
        #[arg(long)]
        recommendation: String,
        /// DRAM pressure before (0.0-1.0).
        #[arg(long, default_value = "0.5")]
        dram_pressure_before: f32,
        /// DRAM pressure after (0.0-1.0).
        #[arg(long, default_value = "0.3")]
        dram_pressure_after: f32,
    },

    /// Compare baseline Linux behavior vs GhostPages.
    Baseline {
        /// DRAM utilization (0.0-1.0).
        #[arg(long, default_value = "0.85")]
        dram_utilization: f32,
        /// Swap utilization (0.0-1.0).
        #[arg(long, default_value = "0.5")]
        swap_utilization: f32,
        /// DRAM pressure (0.0-1.0).
        #[arg(long, default_value = "0.7")]
        dram_pressure: f32,
    },

    /// Run a policy tournament with all built-in policies.
    Tournament {
        /// Number of rounds to run.
        #[arg(long, default_value = "5")]
        rounds: usize,
    },

    /// Show recommendation stability metrics.
    Stability {
        /// Window size for stability tracking.
        #[arg(long, default_value = "100")]
        window_size: usize,
    },
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

        // ─── Hotness Commands ─────────────────────────────────────────────────────

        Commands::Hotness(ref hotness_cmd) => {
            let time_provider: Arc<dyn ghost_core::time::TimeProvider> =
                Arc::new(RealTimeProvider);
            let emitter = {
                let (tx, _rx) = tokio::sync::mpsc::channel(256);
                ghost_core::emitter::EventEmitter::new(tx)
            };

            // Try DAMON first, fall back to simulated provider
            let config = DamonConfig::default();
            let damon_provider = DamonHotnessProvider::new(
                config.clone(),
                time_provider.clone(),
                emitter.clone(),
            );

            let snapshot = if damon_provider.check_availability() {
                damon_provider.sample().context("DAMON sample failed")?
            } else {
                // Fall back to simulated DAMON provider
                let sim_provider = SimulatedDamonProvider::new(
                    config,
                    time_provider.clone(),
                    emitter.clone(),
                    42,
                    80,
                );
                sim_provider.sample().context("simulated DAMON sample failed")?
            };

            let summary = HotnessSummary::from_snapshot(&snapshot);
            let confidence = HotnessConfidence::calculate(&snapshot, &[]);

            match hotness_cmd {
                HotnessCommands::Summary => {
                    print_hotness_summary(&summary, &confidence, snapshot.timestamp);
                }

                HotnessCommands::Hot => {
                    print_hot_regions(&snapshot);
                }

                HotnessCommands::Cold => {
                    print_cold_regions(&snapshot);
                }

                HotnessCommands::Distribution => {
                    print_distribution(&summary);
                }

                HotnessCommands::Watch { interval } => {
                    run_hotness_watch(&time_provider, &emitter, *interval);
                }
            }
        }

        // ─── Policy Commands ──────────────────────────────────────────────────────

        Commands::Policy(ref policy_cmd) => {
            let time_provider: Arc<dyn ghost_core::time::TimeProvider> =
                Arc::new(RealTimeProvider);
            let emitter = {
                let (tx, _rx) = tokio::sync::mpsc::channel(256);
                ghost_core::emitter::EventEmitter::new(tx)
            };

            // Build a policy runtime with simulated tier inventory
            let tier_inventory = Arc::new(parking_lot::RwLock::new(
                TierInventory::new(time_provider.clone(), emitter.clone()),
            ));
            let runtime = PolicyRuntime::new(
                tier_inventory,
                emitter.clone(),
                time_provider.clone(),
            );

            // Get hotness data for policy evaluation
            let config = DamonConfig::default();
            let sim_provider = SimulatedDamonProvider::new(
                config,
                time_provider.clone(),
                emitter.clone(),
                42,
                80,
            );
            let hotness_snapshot = sim_provider.sample().ok();
            let hotness_summary = hotness_snapshot.as_ref().map(|s| HotnessSummary::from_snapshot(s));
            let hotness_confidence = hotness_snapshot.as_ref().map(|s| HotnessConfidence::calculate(s, &[]));

            match policy_cmd {
                PolicyCommands::Evaluate => {
                    print_policy_evaluation(&runtime, &hotness_summary, &hotness_confidence);
                }

                PolicyCommands::Recommendations => {
                    print_recommendations(&runtime);
                }

                PolicyCommands::History => {
                    print_recommendation_history();
                }

                PolicyCommands::Compare { policy_a, policy_b } => {
                    print_policy_comparison(policy_a, policy_b);
                }
            }
        }

        // ─── Evaluator Commands ────────────────────────────────────────────────────

        Commands::Evaluator(ref evaluator_cmd) => {
            use ghost_core::state::PressureState;

            match evaluator_cmd {
                EvaluatorCommands::Score {
                    recommendation,
                    dram_pressure_before,
                    dram_pressure_after,
                } => {
                    // Build before/after SystemState from CLI args
                    let before = SystemState {
                        dram_pressure: PressureState {
                            memory_pressure: *dram_pressure_before,
                            ..Default::default()
                        },
                        dram_utilization: *dram_pressure_before,
                        swap_utilization: 0.3,
                        zram_utilization: Some(0.4),
                        io_pressure: PressureState::new(),
                        hotness_summary: None,
                        hotness_confidence: None,
                    };

                    let after = SystemState {
                        dram_pressure: PressureState {
                            memory_pressure: *dram_pressure_after,
                            ..Default::default()
                        },
                        dram_utilization: *dram_pressure_after,
                        swap_utilization: 0.2,
                        zram_utilization: Some(0.5),
                        io_pressure: PressureState::new(),
                        hotness_summary: None,
                        hotness_confidence: None,
                    };

                    // Parse recommendation type
                    let rec = parse_recommendation_type(recommendation);

                    let weights = ScoringWeights::default();
                    let score = score_recommendation(&rec, &before, &after, &weights);

                    println!("=== Recommendation Score ===");
                    println!("Type: {}", recommendation);
                    println!("DRAM pressure: {} → {}", dram_pressure_before, dram_pressure_after);
                    println!();
                    print_recommendation_score(&score);
                }

                EvaluatorCommands::Baseline {
                    dram_utilization,
                    swap_utilization,
                    dram_pressure,
                } => {
                    let state = SystemState {
                        dram_pressure: PressureState {
                            memory_pressure: *dram_pressure,
                            ..Default::default()
                        },
                        dram_utilization: *dram_utilization,
                        swap_utilization: *swap_utilization,
                        zram_utilization: Some(0.4),
                        io_pressure: PressureState::new(),
                        hotness_summary: None,
                        hotness_confidence: None,
                    };

                    let baseline_recs = evaluate_baseline(&state);

                    println!("=== Baseline Linux Evaluation ===");
                    println!("DRAM utilization: {:.0}%", dram_utilization * 100.0);
                    println!("Swap utilization: {:.0}%", swap_utilization * 100.0);
                    println!("DRAM pressure: {:.0}%", dram_pressure * 100.0);
                    println!();
                    print_baseline_recommendations(&baseline_recs);

                    // Also show scores for the baseline recommendations
                    let after = SystemState {
                        dram_pressure: PressureState {
                            memory_pressure: (dram_pressure - 0.2).max(0.0),
                            ..Default::default()
                        },
                        dram_utilization: (dram_utilization - 0.15).max(0.0),
                        swap_utilization: (swap_utilization - 0.1).max(0.0),
                        zram_utilization: Some(0.5),
                        io_pressure: PressureState::new(),
                        hotness_summary: None,
                        hotness_confidence: None,
                    };

                    let recommendations: Vec<Recommendation> =
                        baseline_recs.into_iter().map(Recommendation::from).collect();
                    let score = score_policy_evaluation(&recommendations, &state, &after);

                    println!();
                    println!("=== Baseline Score (simulated improvement) ===");
                    print_recommendation_score(&score);
                }

                EvaluatorCommands::Tournament { rounds } => {
                    // Build simulated pressure scenarios
                    let scenarios = build_tournament_scenarios(*rounds);

                    let mut arena = PolicyArena::new();
                    arena
                        .add_policy(Box::new(ArenaLinuxBaselinePolicy))
                        .add_policy(Box::new(PressurePolicy))
                        .add_policy(Box::new(HotnessPolicy))
                        .add_policy(Box::new(HybridPolicy));

                    let result = arena.run_tournament(&scenarios);

                    println!("=== Policy Tournament ===");
                    println!("Rounds: {}", rounds);
                    println!("Policies: LinuxBaseline, Pressure, Hotness, Hybrid");
                    println!();
                    print_tournament_result(&result);
                }

                EvaluatorCommands::Stability { window_size } => {
                    let mut tracker = StabilityTracker::new(*window_size);

                    // Feed simulated recommendations: mostly stable with occasional changes
                    let state = SystemState {
                        dram_pressure: PressureState::new(),
                        dram_utilization: 0.4,
                        swap_utilization: 0.15,
                        zram_utilization: Some(0.3),
                        io_pressure: PressureState::new(),
                        hotness_summary: None,
                        hotness_confidence: None,
                    };

                    let chunk_id = ChunkId::from_data(b"test_chunk");

                    for i in 0..*window_size {
                        let rec = if i % 20 == 0 && i > 0 {
                            // Occasional non-NoAction recommendation
                            Recommendation::PromoteToDram {
                                chunk_id,
                                reason: "periodic hot chunk".to_string(),
                                confidence: 0.8,
                                factors: vec!["periodic".to_string()],
                            }
                        } else {
                            Recommendation::NoAction {
                                reason: "system stable".to_string(),
                                confidence: 1.0,
                                factors: vec![],
                            }
                        };
                        tracker.record(rec, &state, i as u64 * 60);
                    }

                    let stability = tracker.evaluate();

                    println!("=== Recommendation Stability ===");
                    println!("Window size: {}", window_size);
                    println!("Entries: {}", window_size);
                    println!();
                    print_stability(&stability);
                }
            }
        }

        // ─── Benchmark Commands ────────────────────────────────────────────────────

        Commands::Bench(ref bench_cmd) => {
            use ghost_bench::{
                all_builtin_workloads, leaderboard::from_report,
                report::format_report_markdown, report::format_report_json,
                runner::BenchmarkRunner, workload::WorkloadGenerator,
            };
            use ghost_evaluator::tournament::{
                ArenaLinuxBaselinePolicy, HotnessPolicy, HybridPolicy, PressurePolicy,
            };

            // Build a runner with all built-in policies
            let mut runner = BenchmarkRunner::new(42);
            runner.with_policies(vec![
                Box::new(ArenaLinuxBaselinePolicy),
                Box::new(PressurePolicy),
                Box::new(HotnessPolicy),
                Box::new(HybridPolicy),
            ]);

            match bench_cmd {
                BenchCommands::Run { seed } => {
                    let mut seeded_runner = BenchmarkRunner::new(*seed);
                    seeded_runner.with_policies(vec![
                        Box::new(ArenaLinuxBaselinePolicy),
                        Box::new(PressurePolicy),
                        Box::new(HotnessPolicy),
                        Box::new(HybridPolicy),
                    ]);
                    let report = seeded_runner.run_all_builtin();
                    let md = format_report_markdown(&report);
                    println!("{}", md);
                }

                BenchCommands::Workload { name, seed } => {
                    let gen = WorkloadGenerator::new(*seed);
                    let definitions = all_builtin_workloads();
                    let def = definitions.iter().find(|d| d.name == *name);

                    match def {
                        Some(def) => {
                            let scenario = gen.generate(def);
                            let policies: Vec<Box<dyn ghost_evaluator::tournament::Policy>> = vec![
                                Box::new(ArenaLinuxBaselinePolicy),
                                Box::new(PressurePolicy),
                                Box::new(HotnessPolicy),
                                Box::new(HybridPolicy),
                            ];
                            let weights = ghost_evaluator::scoring::ScoringWeights::default();
                            let comparison =
                                ghost_bench::comparison::run_workload_comparison(&scenario, &policies, &weights);

                            println!("=== Benchmark: {} ===", name);
                            println!("Class: {}", def.class);
                            println!("Description: {}", def.description);
                            println!("Snapshots: {}", scenario.snapshots.len());
                            println!("Peak DRAM pressure: {:.2}", scenario.metadata.peak_dram_pressure);
                            println!("Peak DRAM utilization: {:.1}%", scenario.metadata.peak_dram_utilization * 100.0);
                            println!();
                            println!("{:<16} {:<12} {:<12} {:<10}", "Policy", "Score", "Recs", "Active");
                            println!("{:<16} {:<12} {:<12} {:<10}", "────────────────", "────────────", "────────────", "──────────");
                            for run in &comparison.runs {
                                println!(
                                    "{:<16} {:<12.4} {:<12} {:<10}",
                                    run.policy_name,
                                    run.average_score.overall_score,
                                    run.recommendation_count,
                                    run.active_recommendation_count
                                );
                            }
                            println!();
                            println!("★ Winner: {} ({:.4})", comparison.winner, comparison.winner_score);
                        }
                        None => {
                            eprintln!("Unknown workload: '{}'", name);
                            eprintln!("Available workloads:");
                            for d in &definitions {
                                eprintln!("  {} — {}", d.name, d.description);
                            }
                            std::process::exit(1);
                        }
                    }
                }

                BenchCommands::Report { format } => {
                    let report = runner.run_all_builtin();
                    match format.as_str() {
                        "json" => {
                            let json = format_report_json(&report);
                            println!("{}", json);
                        }
                        _ => {
                            let md = format_report_markdown(&report);
                            println!("{}", md);
                        }
                    }
                }

                BenchCommands::Experiment { name } => {
                    use ghost_bench::experiment::{pressure_weight_experiment, hybrid_weight_experiment, temperature_threshold_experiment, run_experiment};

                    let base_policy = PressurePolicy;
                    let experiment = match name.as_str() {
                        "pressure_weight" => pressure_weight_experiment(),
                        "hybrid_weight" => hybrid_weight_experiment(),
                        "temperature_threshold" => temperature_threshold_experiment(),
                        other => {
                            eprintln!("Unknown experiment: '{}'", other);
                            eprintln!("Available: pressure_weight, hybrid_weight, temperature_threshold");
                            std::process::exit(1);
                        }
                    };

                    let definitions = all_builtin_workloads();
                    let scenarios: Vec<_> = definitions.iter().map(|def| {
                        let gen = WorkloadGenerator::new(def.seed);
                        gen.generate(def)
                    }).collect();

                    let result = run_experiment(&experiment, &scenarios, &base_policy);

                    println!("=== Experiment: {} ===", result.name);
                    println!("Description: {}", result.description);
                    println!("Parameter: {}", result.parameter_name);
                    println!("Baseline value: {}", result.baseline_value);
                    println!("Best value: {}", result.best_value);
                    println!("Best score: {:.4}", result.best_score);
                    println!("Improvement: {:.1}%", result.improvement * 100.0);
                    println!();
                    println!("{:<12} {:<12} {:<12} {:<12}", "Value", "Avg Score", "Win Rate", "Stability");
                    println!("{:<12} {:<12} {:<12} {:<12}", "────────────", "────────────", "────────────", "────────────");
                    for r in &result.results {
                        println!(
                            "{:<12.2} {:<12.4} {:<12.1} {:<12.4}",
                            r.parameter_value,
                            r.average_score,
                            r.win_rate * 100.0,
                            r.stability_index
                        );
                    }
                }

                BenchCommands::Leaderboard => {
                    let report = runner.run_all_builtin();
                    let leaderboard = from_report(&report, 1);

                    println!("=== Policy Leaderboard ===");
                    println!("Version: {}", leaderboard.version);
                    println!("Last updated: {}", leaderboard.last_updated);
                    println!();

                    let top = leaderboard.top_policies(10);
                    println!("{:<6} {:<16} {:<12} {:<12}", "Rank", "Policy", "Score", "Stability");
                    println!("{:<6} {:<16} {:<12} {:<12}", "──────", "────────────────", "────────────", "────────────");
                    for (i, entry) in top.iter().enumerate() {
                        println!(
                            "{:<6} {:<16} {:<12.4} {:<12.4}",
                            i + 1,
                            entry.policy_name,
                            entry.score,
                            entry.stability_index
                        );
                    }
                }
            }
        }
    }
    Ok(())
}


// ─── Hotness Display Helpers ───────────────────────────────────────────────────

fn print_hotness_summary(summary: &HotnessSummary, confidence: &HotnessConfidence, timestamp: u64) {
    let workload = if summary.is_hot_workload() {
        "Hot"
    } else if summary.is_cold_workload() {
        "Cold"
    } else {
        "Mixed"
    };

    let dominant = match summary.dominant_temperature() {
        Temperature::Hot => "Hot",
        Temperature::Warm => "Warm",
        Temperature::Cold => "Cold",
        Temperature::Frozen => "Frozen",
    };

    let confidence_level = match confidence.level() {
        ConfidenceLevel::High => "High",
        ConfidenceLevel::Medium => "Medium",
        ConfidenceLevel::Low => "Low",
        ConfidenceLevel::Unknown => "Unknown",
    };

    let datetime = format_timestamp(timestamp);

    println!("Hotness Summary");
    println!("===============");
    println!("Hot regions:    {} ({:.1}%)", summary.hot_count, summary.hot_percentage);
    println!("Warm regions:   {} ({:.1}%)", summary.warm_count, summary.warm_percentage);
    println!("Cold regions:   {} ({:.1}%)", summary.cold_count, summary.cold_percentage);
    println!("Frozen regions: {} ({:.1}%)", summary.frozen_count, summary.frozen_percentage);
    println!("Total:          {}", summary.total_regions);
    println!();
    println!("Confidence:     {:.2} ({})", confidence.score, confidence_level);
    println!("Dominant:       {}", dominant);
    println!("Workload:       {}", workload);
    println!();
    println!("Last update:    {}", datetime);
}

fn print_hot_regions(snapshot: &ghost_core::hotness_provider::HotnessSnapshot) {
    let hot_samples: Vec<_> = snapshot
        .samples
        .iter()
        .filter(|s| s.temperature == Temperature::Hot)
        .collect();

    println!("Hot Regions");
    println!("===========");
    if hot_samples.is_empty() {
        println!("No hot regions found.");
    } else {
        println!("{:<12} {:<12} {:<10} {}", "Start", "End", "Accesses", "Temperature");
        for sample in &hot_samples {
            println!(
                "0x{:010x} 0x{:010x} {:<10} {:?}",
                sample.address_range.start,
                sample.address_range.end,
                sample.access_count,
                sample.temperature
            );
        }
    }

    let cold_samples: Vec<_> = snapshot
        .samples
        .iter()
        .filter(|s| s.temperature == Temperature::Cold || s.temperature == Temperature::Frozen)
        .collect();

    println!();
    println!("Cold Regions");
    println!("============");
    if cold_samples.is_empty() {
        println!("No cold regions found.");
    } else {
        println!("{:<12} {:<12} {:<10} {}", "Start", "End", "Accesses", "Temperature");
        for sample in &cold_samples {
            println!(
                "0x{:010x} 0x{:010x} {:<10} {:?}",
                sample.address_range.start,
                sample.address_range.end,
                sample.access_count,
                sample.temperature
            );
        }
    }
}

fn print_cold_regions(snapshot: &ghost_core::hotness_provider::HotnessSnapshot) {
    let cold_samples: Vec<_> = snapshot
        .samples
        .iter()
        .filter(|s| s.temperature == Temperature::Cold)
        .collect();

    let frozen_samples: Vec<_> = snapshot
        .samples
        .iter()
        .filter(|s| s.temperature == Temperature::Frozen)
        .collect();

    println!("Cold Regions");
    println!("============");
    if cold_samples.is_empty() && frozen_samples.is_empty() {
        println!("No cold or frozen regions found.");
    } else {
        println!("{:<12} {:<12} {:<10} {}", "Start", "End", "Accesses", "Temperature");
        for sample in &cold_samples {
            println!(
                "0x{:010x} 0x{:010x} {:<10} {:?}",
                sample.address_range.start,
                sample.address_range.end,
                sample.access_count,
                sample.temperature
            );
        }
        for sample in &frozen_samples {
            println!(
                "0x{:010x} 0x{:010x} {:<10} {:?}",
                sample.address_range.start,
                sample.address_range.end,
                sample.access_count,
                sample.temperature
            );
        }
    }
}

fn print_distribution(summary: &HotnessSummary) {
    println!("Temperature Distribution");
    println!("========================");
    println!();
    print_distribution_bar("Hot    ", summary.hot_count, summary.total_regions, summary.hot_percentage);
    print_distribution_bar("Warm   ", summary.warm_count, summary.total_regions, summary.warm_percentage);
    print_distribution_bar("Cold   ", summary.cold_count, summary.total_regions, summary.cold_percentage);
    print_distribution_bar("Frozen ", summary.frozen_count, summary.total_regions, summary.frozen_percentage);
    println!();
    println!("Total regions: {}", summary.total_regions);
}

fn print_distribution_bar(label: &str, count: usize, total: usize, percentage: f32) {
    let bar_width = 40;
    let filled = if total > 0 {
        ((count as f64 / total as f64) * bar_width as f64).round() as usize
    } else {
        0
    };
    let empty = bar_width - filled;
    let bar: String = std::iter::repeat('█').take(filled)
        .chain(std::iter::repeat('░').take(empty))
        .collect();
    println!("{} [{}] {} ({:.1}%)", label, bar, count, percentage);
}

fn run_hotness_watch(
    time_provider: &Arc<dyn ghost_core::time::TimeProvider>,
    emitter: &ghost_core::emitter::EventEmitter,
    interval: u64,
) {
    use std::io::Write;

    let config = DamonConfig::default();
    let sim_provider = SimulatedDamonProvider::new(
        config,
        time_provider.clone(),
        emitter.clone(),
        42,
        80,
    );

    println!("Watching hotness (interval: {}s) — Ctrl+C to stop", interval);
    println!();

    loop {
        match sim_provider.sample() {
            Ok(snapshot) => {
                let summary = HotnessSummary::from_snapshot(&snapshot);
                let confidence = HotnessConfidence::calculate(&snapshot, &[]);
                let timestamp = snapshot.timestamp;

                // Clear screen and print update
                print!("\x1B[2J\x1B[H");
                io::stdout().flush().ok();
                print_hotness_summary(&summary, &confidence, timestamp);

                // Show temperature change indicators
                println!();
                println!("Active regions:  {} ({:.1}%)", summary.active_count(), summary.active_percentage());
                println!("Inactive regions: {} ({:.1}%)", summary.inactive_count(), summary.inactive_percentage());
            }
            Err(e) => {
                eprintln!("Error sampling hotness: {}", e);
            }
        }

        std::thread::sleep(Duration::from_secs(interval));
    }
}

// ─── Policy Display Helpers ────────────────────────────────────────────────────

fn print_policy_evaluation(
    runtime: &PolicyRuntime,
    hotness_summary: &Option<HotnessSummary>,
    hotness_confidence: &Option<HotnessConfidence>,
) {
    let recommendations = runtime.evaluate().unwrap_or_default();

    println!("Policy Evaluation");
    println!("=================");
    println!();

    // System State section
    println!("System State:");
    // Read pressure from the runtime's tier inventory
    let inventory = runtime.tier_inventory().read();
    let mut dram_pressure_val = 0.0f32;
    let mut swap_util = 0.0f32;
    let mut zram_util = 0.0f32;
    for tier in inventory.all_tiers().values() {
        match tier.kind {
            ghost_linux::tier_inventory::TierKind::Dram => {
                dram_pressure_val = tier.pressure.memory_pressure;
            }
            ghost_linux::tier_inventory::TierKind::Swap | ghost_linux::tier_inventory::TierKind::DiskSwap => {
                swap_util = tier.utilization() as f32;
            }
            ghost_linux::tier_inventory::TierKind::Zram => {
                zram_util = tier.utilization() as f32;
            }
            _ => {}
        }
    }
    let pressure_level = if dram_pressure_val >= 0.7 {
        "Medium"
    } else if dram_pressure_val >= 0.9 {
        "High"
    } else {
        "Low"
    };
    println!("  DRAM pressure:    {} (avg10: {:.1})", pressure_level, dram_pressure_val * 10.0);
    println!("  Swap utilization: {:.0}%", swap_util * 100.0);
    println!("  ZRAM utilization: {:.0}%", zram_util * 100.0);
    println!("  IO pressure:      Low");

    // Hotness section
    println!();
    println!("Hotness:");
    if let Some(summary) = hotness_summary {
        let cold_count = summary.cold_count + summary.frozen_count;
        let cold_pct = summary.cold_percentage + summary.frozen_percentage;
        println!("  Hot regions:      {} ({:.0}%)", summary.hot_count, summary.hot_percentage);
        println!("  Cold regions:     {} ({:.0}%)", cold_count, cold_pct);
    } else {
        println!("  Hot regions:      N/A");
        println!("  Cold regions:     N/A");
    }
    if let Some(confidence) = hotness_confidence {
        let level = match confidence.level() {
            ConfidenceLevel::High => "High",
            ConfidenceLevel::Medium => "Medium",
            ConfidenceLevel::Low => "Low",
            ConfidenceLevel::Unknown => "Unknown",
        };
        println!("  Confidence:       {:.2} ({})", confidence.score, level);
    } else {
        println!("  Confidence:       N/A");
    }

    // Recommendations section
    println!();
    println!("Recommendations:");
    if recommendations.is_empty() {
        println!("  No recommendations at this time.");
    } else {
        for (i, rec) in recommendations.iter().enumerate() {
            let kind_short = match rec {
                ghost_linux::policy::Recommendation::MoveToDiskSwap { .. } => "MoveToDiskSwap",
                ghost_linux::policy::Recommendation::PromoteToDram { .. } => "PromoteToDram",
                ghost_linux::policy::Recommendation::NoAction { .. } => "NoAction",
                ghost_linux::policy::Recommendation::MoveToZram { .. } => "MoveToZram",
                ghost_linux::policy::Recommendation::EvictCold { .. } => "EvictCold",
                ghost_linux::policy::Recommendation::DemoteHot { .. } => "DemoteHot",
            };
            println!("  {}. [{:.2}] {} — {}", i + 1, rec.confidence(), kind_short, rec.reason());
        }
    }

    // Stability section
    println!();
    println!("Stability:");
    let cooldowns_active = runtime.active_cooldowns();
    let suppressed = runtime.suppressed_count();
    let last_eval = runtime.last_evaluation_time();
    println!("  Cooldowns active: {}", cooldowns_active);
    println!("  Suppressed:       {}", suppressed);
    println!("  Last evaluation:  {}", format_timestamp(last_eval));
}

fn print_recommendations(runtime: &PolicyRuntime) {
    let recommendations = runtime.evaluate().unwrap_or_default();

    println!("Current Recommendations");
    println!("=======================");
    println!();

    if recommendations.is_empty() {
        println!("No recommendations at this time.");
    } else {
        for (i, rec) in recommendations.iter().enumerate() {
            println!("{}. {}", i + 1, rec);
            println!("   Confidence: {:.2}", rec.confidence());
            let factors = rec.factors();
            if !factors.is_empty() {
                println!("   Factors: {}", factors.join(", "));
            }
            println!();
        }
    }
}

fn print_recommendation_history() {
    println!("Recommendation History");
    println!("======================");
    println!();
    println!("(History tracking requires persistent storage — showing current session only)");
    println!();
    println!("No historical data available in this session.");
}

fn print_policy_comparison(policy_a: &str, policy_b: &str) {
    println!("Policy Comparison: {} vs {}", policy_a, policy_b);
    println!("==========================================");
    println!();
    println!("Comparing policy configurations...");
    println!();
    println!("  {:<20} {:<15} {:<15}", "Metric", policy_a, policy_b);
    println!("  {:<20} {:<15} {:<15}", "────────────────────", "───────────────", "───────────────");
    println!("  {:<20} {:<15.2} {:<15.2}", "DRAM high thresh", 0.7, 0.7);
    println!("  {:<20} {:<15.2} {:<15.2}", "DRAM crit thresh", 0.9, 0.9);
    println!("  {:<20} {:<15.2} {:<15.2}", "Swap high thresh", 0.8, 0.8);
    println!("  {:<20} {:<15.2} {:<15.2}", "Hotness weight", 0.3, 0.3);
    println!("  {:<20} {:<15.2} {:<15.2}", "Pressure weight", 0.7, 0.7);
    println!("  {:<20} {:<15} {:<15}", "Cooldown (s)", "60", "60");
    println!();
    println!("(Full policy comparison requires named policy configurations)");
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

/// Format a Unix timestamp as a human-readable UTC datetime string.
fn format_timestamp(timestamp: u64) -> String {
    // Simple formatting without chrono dependency
    let days_since_epoch = timestamp / 86400;
    let seconds_of_day = timestamp % 86400;
    let hours = seconds_of_day / 3600;
    let minutes = (seconds_of_day % 3600) / 60;
    let seconds = seconds_of_day % 60;

    // Approximate date calculation (good enough for display)
    let year = 1970 + days_since_epoch / 365;
    let day_of_year = days_since_epoch % 365;
    let month = (day_of_year / 30) + 1;
    let day = (day_of_year % 30) + 1;

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC",
        year, month, day, hours, minutes, seconds
    )
}


// ─── Evaluator Display Helpers ─────────────────────────────────────────────────

fn print_recommendation_score(score: &RecommendationScore) {
    println!("  Fault reduction:    {:.4}", score.fault_reduction);
    println!("  Swap reduction:     {:.4}", score.swap_reduction);
    println!("  ZRAM efficiency:    {:.4}", score.zram_efficiency);
    println!("  Pressure reduction: {:.4}", score.pressure_reduction);
    println!("  Tier balance:       {:.4}", score.tier_balance);
    println!("  Stability:          {:.4}", score.stability);
    println!("  ─────────────────────────────");
    println!("  Overall score:      {:.4}", score.overall_score);
}

fn print_baseline_recommendations(recs: &[BaselineRecommendation]) {
    if recs.is_empty() {
        println!("  No baseline recommendations.");
        return;
    }
    for (i, rec) in recs.iter().enumerate() {
        let action_str = match rec.action {
            BaselineAction::Evict => "Evict",
            BaselineAction::SwapOut => "SwapOut",
            BaselineAction::NoAction => "NoAction",
        };
        println!(
            "  {}. [{}] {} (confidence={:.2})",
            i + 1,
            action_str,
            rec.reason,
            rec.confidence
        );
    }
}

fn print_tournament_result(result: &TournamentResult) {
    // Print each round
    for round in &result.rounds {
        println!("── Round {} ──", round.round_index + 1);
        for r in &round.results {
            println!(
                "  {:<16} overall={:.4}  recs={}",
                r.policy_name,
                r.score.overall_score,
                r.recommendations.len()
            );
        }
        if let Some(winner) = round.round_winner {
            println!("  Winner: {}", winner);
        }
        println!();
    }

    // Print leaderboard
    println!("── Leaderboard ──");
    let mut entries: Vec<_> = result.summary.average_scores.iter().collect();
    entries.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
    for (i, (name, avg_score)) in entries.iter().enumerate() {
        let wins = result.summary.policy_wins.get(*name).unwrap_or(&0);
        println!("  {}. {:<16} avg={:.4}  wins={}", i + 1, name, avg_score, wins);
    }

    println!();
    if let Some(winner) = result.winner {
        println!("★ Overall winner: {}", winner);
    } else {
        println!("★ No winner (empty tournament).");
    }

    println!();
    println!(
        "  Best score:  {:.4}",
        result.summary.best_overall_score
    );
    println!(
        "  Worst score: {:.4}",
        result.summary.worst_overall_score
    );
}

fn print_stability(stability: &RecommendationStability) {
    println!("  Recommendations/hour: {:.2}", stability.recommendations_per_hour);
    println!("  Temperature flips:    {}", stability.temperature_flips);
    println!("  Tier oscillations:    {}", stability.tier_oscillations);
    println!("  Confidence variance:  {:.4}", stability.confidence_variance);
    println!("  ─────────────────────────────");
    println!("  Stability index:      {:.4}", stability.stability_index);

    let assessment = if stability.stability_index >= 0.8 {
        "Excellent"
    } else if stability.stability_index >= 0.6 {
        "Good"
    } else if stability.stability_index >= 0.4 {
        "Fair"
    } else if stability.stability_index >= 0.2 {
        "Poor"
    } else {
        "Critical"
    };
    println!("  Assessment:           {}", assessment);
}

// ─── Evaluator Helper Functions ─────────────────────────────────────────────────

fn parse_recommendation_type(s: &str) -> Recommendation {
    let chunk_id = ChunkId::from_data(b"cli_chunk");
    match s.to_lowercase().as_str() {
        "promote" => Recommendation::PromoteToDram {
            chunk_id,
            reason: "CLI: promote to DRAM".to_string(),
            confidence: 0.9,
            factors: vec!["cli_request".to_string()],
        },
        "demote" => Recommendation::DemoteHot {
            tier: TierId::Ram,
            target: TierId::Disk,
            confidence: 0.8,
            factors: vec!["cli_request".to_string()],
        },
        "zram" => Recommendation::MoveToZram {
            chunk_id,
            reason: "CLI: move to ZRAM".to_string(),
            confidence: 0.85,
            factors: vec!["cli_request".to_string()],
        },
        "diskswap" => Recommendation::MoveToDiskSwap {
            chunk_id,
            reason: "CLI: move to disk swap".to_string(),
            confidence: 0.85,
            factors: vec!["cli_request".to_string()],
        },
        "evict" => Recommendation::EvictCold {
            tier: TierId::Ram,
            count: 8,
            confidence: 1.0,
            factors: vec!["cli_request".to_string()],
        },
        _ => Recommendation::NoAction {
            reason: "CLI: no action".to_string(),
            confidence: 1.0,
            factors: vec!["cli_request".to_string()],
        },
    }
}

fn build_tournament_scenarios(rounds: usize) -> Vec<(&'static SystemState, &'static SystemState)> {
    // Use a static array of predefined scenarios that cycle
    use std::sync::OnceLock;

    static IDLE: OnceLock<SystemState> = OnceLock::new();
    static MEDIUM: OnceLock<SystemState> = OnceLock::new();
    static HIGH: OnceLock<SystemState> = OnceLock::new();
    static CRITICAL: OnceLock<SystemState> = OnceLock::new();
    static IMPROVED: OnceLock<SystemState> = OnceLock::new();

    let idle = IDLE.get_or_init(|| SystemState {
        dram_pressure: PressureState::new(),
        dram_utilization: 0.3,
        swap_utilization: 0.1,
        zram_utilization: Some(0.2),
        io_pressure: PressureState::new(),
        hotness_summary: None,
        hotness_confidence: None,
    });

    let medium = MEDIUM.get_or_init(|| SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.55,
            ..Default::default()
        },
        dram_utilization: 0.7,
        swap_utilization: 0.2,
        zram_utilization: Some(0.3),
        io_pressure: PressureState::new(),
        hotness_summary: None,
        hotness_confidence: None,
    });

    let high = HIGH.get_or_init(|| SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
        hotness_summary: None,
        hotness_confidence: None,
    });

    let critical = CRITICAL.get_or_init(|| SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.95,
            ..Default::default()
        },
        dram_utilization: 0.97,
        swap_utilization: 0.5,
        zram_utilization: Some(0.6),
        io_pressure: PressureState::new(),
        hotness_summary: None,
        hotness_confidence: None,
    });

    let improved = IMPROVED.get_or_init(|| SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.4,
            ..Default::default()
        },
        dram_utilization: 0.5,
        swap_utilization: 0.15,
        zram_utilization: Some(0.5),
        io_pressure: PressureState::new(),
        hotness_summary: None,
        hotness_confidence: None,
    });

    // Cycle through scenarios: idle→idle, medium→idle, high→improved, critical→high, high→medium
    let scenario_pairs: Vec<(&SystemState, &SystemState)> = vec![
        (idle, idle),
        (medium, idle),
        (high, improved),
        (critical, high),
        (high, medium),
    ];

    let mut result = Vec::new();
    for i in 0..rounds {
        result.push(scenario_pairs[i % scenario_pairs.len()]);
    }
    result
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

    // ─── Hotness CLI Tests ──────────────────────────────────────────────────────

    #[test]
    fn test_hotness_summary_command() {
        use ghost_core::hotness_provider::{AddressRange, HotnessSample};

        let snapshot = ghost_core::hotness_provider::HotnessSnapshot {
            samples: vec![
                HotnessSample {
                    address_range: AddressRange::new(0x1000000, 0x2000000),
                    temperature: Temperature::Hot,
                    access_count: 150,
                },
                HotnessSample {
                    address_range: AddressRange::new(0x2000000, 0x3000000),
                    temperature: Temperature::Warm,
                    access_count: 50,
                },
                HotnessSample {
                    address_range: AddressRange::new(0x3000000, 0x4000000),
                    temperature: Temperature::Cold,
                    access_count: 5,
                },
                HotnessSample {
                    address_range: AddressRange::new(0x4000000, 0x5000000),
                    temperature: Temperature::Frozen,
                    access_count: 0,
                },
            ],
            timestamp: 1_700_000_000,
        };

        let summary = HotnessSummary::from_snapshot(&snapshot);
        let confidence = HotnessConfidence::calculate(&snapshot, &[]);

        // Verify summary counts
        assert_eq!(summary.hot_count, 1);
        assert_eq!(summary.warm_count, 1);
        assert_eq!(summary.cold_count, 1);
        assert_eq!(summary.frozen_count, 1);
        assert_eq!(summary.total_regions, 4);

        // Verify percentages
        assert!((summary.hot_percentage - 25.0).abs() < f32::EPSILON);
        assert!((summary.warm_percentage - 25.0).abs() < f32::EPSILON);
        assert!((summary.cold_percentage - 25.0).abs() < f32::EPSILON);
        assert!((summary.frozen_percentage - 25.0).abs() < f32::EPSILON);

        // Verify confidence is calculated
        assert!(confidence.score >= 0.0 && confidence.score <= 1.0);
    }

    #[test]
    fn test_hot_regions_command() {
        use ghost_core::hotness_provider::{AddressRange, HotnessSample};

        let snapshot = ghost_core::hotness_provider::HotnessSnapshot {
            samples: vec![
                HotnessSample {
                    address_range: AddressRange::new(0x1000000, 0x2000000),
                    temperature: Temperature::Hot,
                    access_count: 15234,
                },
                HotnessSample {
                    address_range: AddressRange::new(0x3000000, 0x4000000),
                    temperature: Temperature::Hot,
                    access_count: 12891,
                },
                HotnessSample {
                    address_range: AddressRange::new(0x5000000, 0x6000000),
                    temperature: Temperature::Cold,
                    access_count: 23,
                },
            ],
            timestamp: 1_700_000_000,
        };

        let hot_samples: Vec<_> = snapshot
            .samples
            .iter()
            .filter(|s| s.temperature == Temperature::Hot)
            .collect();

        assert_eq!(hot_samples.len(), 2);
        assert_eq!(hot_samples[0].access_count, 15234);
        assert_eq!(hot_samples[1].access_count, 12891);
    }

    #[test]
    fn test_cold_regions_command() {
        use ghost_core::hotness_provider::{AddressRange, HotnessSample};

        let snapshot = ghost_core::hotness_provider::HotnessSnapshot {
            samples: vec![
                HotnessSample {
                    address_range: AddressRange::new(0x1000000, 0x2000000),
                    temperature: Temperature::Hot,
                    access_count: 150,
                },
                HotnessSample {
                    address_range: AddressRange::new(0x5000000, 0x6000000),
                    temperature: Temperature::Cold,
                    access_count: 23,
                },
                HotnessSample {
                    address_range: AddressRange::new(0x7000000, 0x8000000),
                    temperature: Temperature::Frozen,
                    access_count: 0,
                },
            ],
            timestamp: 1_700_000_000,
        };

        let cold_samples: Vec<_> = snapshot
            .samples
            .iter()
            .filter(|s| s.temperature == Temperature::Cold || s.temperature == Temperature::Frozen)
            .collect();

        assert_eq!(cold_samples.len(), 2);
        assert_eq!(cold_samples[0].access_count, 23);
        assert_eq!(cold_samples[1].access_count, 0);
    }

    #[test]
    fn test_distribution_command() {
        let snapshot = ghost_core::hotness_provider::HotnessSnapshot {
            samples: vec![
                ghost_core::hotness_provider::HotnessSample {
                    address_range: ghost_core::hotness_provider::AddressRange::new(0, 4096),
                    temperature: Temperature::Hot,
                    access_count: 100,
                },
                ghost_core::hotness_provider::HotnessSample {
                    address_range: ghost_core::hotness_provider::AddressRange::new(4096, 8192),
                    temperature: Temperature::Warm,
                    access_count: 50,
                },
                ghost_core::hotness_provider::HotnessSample {
                    address_range: ghost_core::hotness_provider::AddressRange::new(8192, 12288),
                    temperature: Temperature::Cold,
                    access_count: 5,
                },
                ghost_core::hotness_provider::HotnessSample {
                    address_range: ghost_core::hotness_provider::AddressRange::new(12288, 16384),
                    temperature: Temperature::Frozen,
                    access_count: 0,
                },
            ],
            timestamp: 0,
        };

        let summary = HotnessSummary::from_snapshot(&snapshot);

        // Verify distribution percentages sum to 100%
        let total_pct = summary.hot_percentage
            + summary.warm_percentage
            + summary.cold_percentage
            + summary.frozen_percentage;
        assert!((total_pct - 100.0).abs() < 0.01);

        // Each should be 25%
        assert!((summary.hot_percentage - 25.0).abs() < f32::EPSILON);
        assert!((summary.warm_percentage - 25.0).abs() < f32::EPSILON);
        assert!((summary.cold_percentage - 25.0).abs() < f32::EPSILON);
        assert!((summary.frozen_percentage - 25.0).abs() < f32::EPSILON);
    }

    // ─── Policy CLI Tests ───────────────────────────────────────────────────────

    #[test]
    fn test_policy_evaluate_command() {
        let time_provider: Arc<dyn ghost_core::time::TimeProvider> =
            Arc::new(ghost_core::time::DeterministicTimeProvider::new(
                1_700_000_000,
                Duration::from_secs(1),
            ));
        let emitter = {
            let (tx, _rx) = tokio::sync::mpsc::channel(256);
            ghost_core::emitter::EventEmitter::new(tx)
        };

        let tier_inventory = Arc::new(parking_lot::RwLock::new(
            TierInventory::new(time_provider.clone(), emitter.clone()),
        ));
        let runtime = PolicyRuntime::new(
            tier_inventory,
            emitter,
            time_provider,
        );

        let result = runtime.evaluate();
        assert!(result.is_ok());

        let recommendations = result.unwrap();
        // Should produce at least one recommendation
        assert!(!recommendations.is_empty());
    }

    #[test]
    fn test_recommendations_command() {
        let time_provider: Arc<dyn ghost_core::time::TimeProvider> =
            Arc::new(ghost_core::time::DeterministicTimeProvider::new(
                1_700_000_000,
                Duration::from_secs(1),
            ));
        let emitter = {
            let (tx, _rx) = tokio::sync::mpsc::channel(256);
            ghost_core::emitter::EventEmitter::new(tx)
        };

        let tier_inventory = Arc::new(parking_lot::RwLock::new(
            TierInventory::new(time_provider.clone(), emitter.clone()),
        ));
        let runtime = PolicyRuntime::new(
            tier_inventory,
            emitter,
            time_provider,
        );

        let recommendations = runtime.evaluate().unwrap();

        // Verify recommendations have valid confidence scores
        for rec in &recommendations {
            let conf = rec.confidence();
            assert!(conf >= 0.0 && conf <= 1.0);
        }
    }

    #[test]
    fn test_format_timestamp() {
        let ts = 1_700_000_000u64;
        let formatted = format_timestamp(ts);
        assert!(formatted.contains("UTC"));
        assert!(formatted.contains("2023") || formatted.contains("2024"));
    }
}
