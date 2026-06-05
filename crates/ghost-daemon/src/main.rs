//! GhostPages daemon binary.
//!
//! Entry point for the GhostPages memory-tiering daemon. Binds a Unix
//! socket, starts the transfer pipeline, and serves IPC clients until
//! SIGTERM/SIGINT triggers a graceful shutdown.

use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::ipc_server::{IpcServer, IpcServerConfig};
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_policy::pressure::PressureAwareConfig;
use ghost_policy::pressure::PressureAwarePolicy;
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use ghost_core::types::TierId;
use ghost_tier::backend::StorageBackend;

/// Default socket path for the daemon.
const DEFAULT_SOCKET_PATH: &str = "/tmp/ghostpages/ghostpages.sock";

/// Build a default orchestrator config for the MVP daemon.
fn default_config() -> OrchestratorConfig {
    OrchestratorConfig::default()
}

/// Construct all storage backends for the MVP.
fn build_backends() -> BTreeMap<TierId, Arc<dyn StorageBackend>> {
    let mut backends = BTreeMap::new();

    // RAM tier
    let ram: Arc<dyn StorageBackend> = Arc::new(RamBackend::new(4 * 1024 * 1024)); // 4 MiB
    backends.insert(TierId::Ram, ram);

    // Simulation tier (primary development backend)
    let sim_config = SimConfig::with_capacity(16 * 1024 * 1024) // 16 MiB
        .with_seed(42);
    let sim: Arc<dyn StorageBackend> = Arc::new(SimBackend::new(sim_config));
    backends.insert(TierId::Simulation, sim);

    backends
}

/// Construct the placement policy for the MVP.
fn build_policy() -> Arc<dyn ghost_policy::PlacementPolicy> {
    let config = PressureAwareConfig::default();
    Arc::new(PressureAwarePolicy::new(config))
}

/// Run the GhostPages daemon until SIGTERM/SIGINT.
///
/// This is the main entry point for the daemon. It:
/// 1. Sets up tracing
/// 2. Creates backends, policy, and orchestrator
/// 3. Starts the pipeline (workers, scheduler, pressure monitor)
/// 4. Binds the Unix socket and serves IPC clients
/// 5. On SIGTERM/SIGINT, performs graceful shutdown
pub async fn run_daemon(socket_path: PathBuf) -> ghost_core::error::GhostResult<()> {
    // 1. Tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!("GhostPages daemon starting");

    // 2. Build components
    let config = default_config();
    let backends = build_backends();
    let policy = build_policy();

    let mut orchestrator = TransferOrchestrator::new(config, backends, policy);

    // 3. Start pipeline
    orchestrator.start()?;
    info!("Pipeline started");

    // 4. IPC server
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ipc_config = IpcServerConfig {
        socket_path: socket_path.to_path_buf(),
        ..Default::default()
    };
    let server = IpcServer::new(
        ipc_config,
        Arc::new(orchestrator),
        Arc::new(ghost_daemon::trace_log::TraceLog::new(10_000)),
        shutdown_rx,
    );

    // Spawn server task
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            warn!("IPC server error: {}", e);
        }
    });

    // 5. Wait for shutdown signal
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("failed to install SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => info!("Received SIGTERM"),
        _ = sigint.recv() => info!("Received SIGINT"),
    }

    // Signal shutdown
    let _ = shutdown_tx.send(true);

    // Graceful shutdown
    server_handle.abort();
    info!("Daemon stopped");

    Ok(())
}

#[tokio::main]
async fn main() -> ghost_core::error::GhostResult<()> {
    let socket_path = PathBuf::from(DEFAULT_SOCKET_PATH);

    // Ensure parent directory exists
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    // Clean up stale socket
    if socket_path.exists() {
        tokio::fs::remove_file(&socket_path).await.ok();
    }

    run_daemon(socket_path).await
}
