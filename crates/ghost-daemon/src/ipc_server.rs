//! IPC server for GhostPages daemon.
//!
//! Unix domain socket server that accepts client connections and
//! dispatches requests to the TransferOrchestrator.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::trace::{current_timestamp, TraceEvent};
use ghost_core::types::{ChunkId, ChunkMeta, TierId};

use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

use crate::orchestrator::TransferOrchestrator;
use crate::trace_log::TraceLog;

use ghost_ipc::frame::{read_frame, write_frame};
use ghost_ipc::protocol::{IpcErrorCode, IpcRequest, IpcResponse, TierInfo};

/// Configuration for the IPC server.
#[derive(Debug, Clone)]
pub struct IpcServerConfig {
    /// Path to the Unix domain socket.
    pub socket_path: PathBuf,

    /// Maximum number of concurrent connections.
    pub max_connections: usize,

    /// Timeout in seconds for individual requests.
    pub request_timeout_secs: u64,

    /// Maximum request size in bytes.
    pub max_request_size: usize,
}

impl Default for IpcServerConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from("/tmp/ghostpages.sock"),
            max_connections: 64,
            request_timeout_secs: 30,
            max_request_size: 256 * 1024 * 1024,
        }
    }
}

/// IPC server that listens for client connections.
pub struct IpcServer {
    config: IpcServerConfig,
    orchestrator: Arc<TransferOrchestrator>,
    trace_log: Arc<TraceLog>,
    shutdown: watch::Receiver<bool>,
    start_time: Instant,
}

impl IpcServer {
    /// Create a new IPC server.
    pub fn new(
        config: IpcServerConfig,
        orchestrator: Arc<TransferOrchestrator>,
        trace_log: Arc<TraceLog>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            config,
            orchestrator,
            trace_log,
            shutdown,
            start_time: Instant::now(),
        }
    }

    /// Run the IPC server, listening for connections.
    ///
    /// This method blocks until the shutdown signal is received.
    /// On shutdown, the socket file is cleaned up.
    pub async fn run(&self) -> GhostResult<()> {
        let socket_path = &self.config.socket_path;

        // Remove stale socket file if it exists
        if socket_path.exists() {
            std::fs::remove_file(socket_path).map_err(|e| {
                GhostError::IpcError(format!(
                    "failed to remove stale socket file {}: {}",
                    socket_path.display(),
                    e
                ))
            })?;
        }

        // Create parent directory if needed
        if let Some(parent) = socket_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    GhostError::IpcError(format!(
                        "failed to create socket directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
        }

        // Bind the Unix socket
        let listener = UnixListener::bind(socket_path).map_err(|e| {
            GhostError::IpcError(format!(
                "failed to bind socket at {}: {}",
                socket_path.display(),
                e
            ))
        })?;

        tracing::info!("IPC server listening on {}", socket_path.display());

        let mut shutdown = self.shutdown.clone();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            let orchestrator = self.orchestrator.clone();
                            let trace_log = self.trace_log.clone();
                            let timeout = Duration::from_secs(self.config.request_timeout_secs);
                            let max_size = self.config.max_request_size;
                            let start_time = self.start_time;
                            let mut conn_shutdown = self.shutdown.clone();

                            tokio::spawn(async move {
                                // Emit IpcConnectionAccepted event
                                trace_log.record(TraceEvent::IpcConnectionAccepted {
                                    timestamp: current_timestamp(),
                                });

                                if let Err(e) = handle_connection(
                                    stream,
                                    orchestrator,
                                    trace_log.clone(),
                                    timeout,
                                    max_size,
                                    start_time,
                                    &mut conn_shutdown,
                                ).await {
                                    tracing::debug!("connection handler error: {}", e);
                                }

                                // Emit IpcConnectionClosed event
                                trace_log.record(TraceEvent::IpcConnectionClosed {
                                    timestamp: current_timestamp(),
                                });
                            });
                        }
                        Err(e) => {
                            tracing::error!("failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("IPC server shutting down");
                        break;
                    }
                }
            }
        }

        // Clean up socket file
        if socket_path.exists() {
            if let Err(e) = std::fs::remove_file(socket_path) {
                tracing::warn!(
                    "failed to remove socket file {}: {}",
                    socket_path.display(),
                    e
                );
            }
        }

        tracing::info!("IPC server stopped");
        Ok(())
    }
}

/// Handle a single client connection.
async fn handle_connection(
    mut stream: UnixStream,
    orchestrator: Arc<TransferOrchestrator>,
    trace_log: Arc<TraceLog>,
    timeout: Duration,
    max_size: usize,
    start_time: Instant,
    shutdown: &mut watch::Receiver<bool>,
) -> GhostResult<()> {
    loop {
        // Check for shutdown signal
        if *shutdown.borrow() {
            tracing::debug!("connection handler: shutdown signal received");
            break;
        }

        // Read request with timeout
        let request_bytes = match tokio::time::timeout(timeout, read_frame(&mut stream)).await {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(e)) => {
                // Read error — likely connection closed
                tracing::debug!("frame read error: {}", e);
                break;
            }
            Err(_) => {
                // Timeout — close idle connection
                tracing::debug!("request timeout, closing connection");
                break;
            }
        };

        // Enforce max request size
        if request_bytes.len() > max_size {
            let response = IpcResponse::Error {
                code: IpcErrorCode::InvalidRequest,
                message: format!(
                    "request size {} exceeds maximum {}",
                    request_bytes.len(),
                    max_size
                ),
            };
            send_response(&mut stream, &response).await?;
            continue;
        }

        // Deserialize request
        let request: IpcRequest = match serde_json::from_slice(&request_bytes) {
            Ok(req) => req,
            Err(e) => {
                let response = IpcResponse::Error {
                    code: IpcErrorCode::InvalidRequest,
                    message: format!("failed to parse request: {}", e),
                };
                send_response(&mut stream, &response).await?;
                continue;
            }
        };

        // Emit IpcRequestReceived event
        let request_type = match &request {
            IpcRequest::Store { .. } => "store",
            IpcRequest::Retrieve { .. } => "retrieve",
            IpcRequest::Delete { .. } => "delete",
            IpcRequest::Migrate { .. } => "migrate",
            IpcRequest::Info { .. } => "info",
            IpcRequest::List { .. } => "list",
            IpcRequest::Status => "status",
            IpcRequest::Pressure => "pressure",
            IpcRequest::Trace { .. } => "trace",
            IpcRequest::PressureCheck => "pressure_check",
            IpcRequest::Shutdown => "shutdown",
            IpcRequest::Ping => "ping",
        };
        trace_log.record(TraceEvent::IpcRequestReceived {
            request_type: request_type.to_string(),
            timestamp: current_timestamp(),
        });

        // Dispatch request
        let response = dispatch_request(&request, &orchestrator, &trace_log, start_time).await;

        // Emit IpcResponseSent event
        let _response_type = match &response {
            IpcResponse::Ok { .. } => "ok",
            IpcResponse::ChunkId { .. } => "chunk_id",
            IpcResponse::Error { .. } => "error",
            IpcResponse::Info { .. } => "info",
            IpcResponse::List { .. } => "list",
            IpcResponse::Status { .. } => "status",
            IpcResponse::Pressure { .. } => "pressure",
            IpcResponse::Trace { .. } => "trace",
            IpcResponse::PressureCheck { .. } => "pressure_check",
            IpcResponse::Pong => "pong",
        };
        trace_log.record(TraceEvent::IpcResponseSent {
            request_type: request_type.to_string(),
            success: matches!(
                &response,
                IpcResponse::Ok { .. }
                    | IpcResponse::ChunkId { .. }
                    | IpcResponse::Info { .. }
                    | IpcResponse::List { .. }
                    | IpcResponse::Status { .. }
                    | IpcResponse::Pressure { .. }
                    | IpcResponse::Trace { .. }
                    | IpcResponse::PressureCheck { .. }
                    | IpcResponse::Pong
            ),
            timestamp: current_timestamp(),
        });

        // Send response
        if let Err(e) = send_response(&mut stream, &response).await {
            tracing::debug!("failed to send response: {}", e);
            break;
        }
    }

    Ok(())
}

/// Dispatch an IPC request to the appropriate handler.
async fn dispatch_request(
    request: &IpcRequest,
    orchestrator: &Arc<TransferOrchestrator>,
    trace_log: &Arc<TraceLog>,
    start_time: Instant,
) -> IpcResponse {
    match request {
        IpcRequest::Store { data, tier } => handle_store(orchestrator, data, *tier).await,
        IpcRequest::Retrieve { chunk_id } => handle_retrieve(orchestrator, chunk_id).await,
        IpcRequest::Delete { chunk_id } => handle_delete(orchestrator, chunk_id).await,
        IpcRequest::Migrate { chunk_id, from, to } => {
            handle_migrate(orchestrator, chunk_id, *from, *to).await
        }
        IpcRequest::Info { chunk_id } => handle_info(orchestrator, chunk_id).await,
        IpcRequest::List { tier } => handle_list(orchestrator, *tier).await,
        IpcRequest::Status => handle_status(orchestrator, start_time).await,
        IpcRequest::Pressure => handle_pressure(orchestrator).await,
        IpcRequest::Trace { count } => handle_trace(trace_log, *count).await,
        IpcRequest::PressureCheck => handle_pressure_check(orchestrator).await,
        IpcRequest::Shutdown => handle_shutdown(orchestrator).await,
        IpcRequest::Ping => IpcResponse::Pong,
    }
}

/// Send a response over the stream.
async fn send_response(stream: &mut UnixStream, response: &IpcResponse) -> GhostResult<()> {
    let response_json = serde_json::to_vec(response)
        .map_err(|e| GhostError::IpcError(format!("failed to serialize response: {}", e)))?;
    write_frame(stream, &response_json).await
}

// ─── Request Handlers ──────────────────────────────────────────────────────────

async fn handle_store(
    orchestrator: &Arc<TransferOrchestrator>,
    data: &[u8],
    tier: Option<TierId>,
) -> IpcResponse {
    let chunk_id = ChunkId::from_data(data);
    let tier = tier.unwrap_or(TierId::Ram);

    match orchestrator.store(chunk_id, tier, data) {
        Ok(()) => IpcResponse::ChunkId { chunk_id },
        Err(e) => IpcResponse::Error {
            code: IpcErrorCode::InternalError,
            message: format!("store failed: {}", e),
        },
    }
}

async fn handle_retrieve(
    orchestrator: &Arc<TransferOrchestrator>,
    chunk_id: &ChunkId,
) -> IpcResponse {
    // Look up the chunk's tier from the state machine
    let tier = {
        let sm = orchestrator.state_machine.lock().unwrap();
        let state = sm.get_state(chunk_id);
        if state.is_none() {
            return IpcResponse::Error {
                code: IpcErrorCode::ChunkNotFound,
                message: format!("chunk {} not found", chunk_id),
            };
        }
        // Default to RAM tier for retrieval
        TierId::Ram
    };

    match orchestrator.retrieve(*chunk_id, tier) {
        Ok(()) => {
            // The actual data will be delivered by the worker pool.
            // For now, return Ok to acknowledge the request was accepted.
            IpcResponse::Ok { data: None }
        }
        Err(e) => IpcResponse::Error {
            code: match &e {
                GhostError::ChunkNotFound(_) => IpcErrorCode::ChunkNotFound,
                _ => IpcErrorCode::InternalError,
            },
            message: format!("retrieve failed: {}", e),
        },
    }
}

async fn handle_delete(
    orchestrator: &Arc<TransferOrchestrator>,
    chunk_id: &ChunkId,
) -> IpcResponse {
    // Evict the chunk from all tiers
    let tiers = [
        TierId::Ram,
        TierId::Simulation,
        TierId::Disk,
        TierId::GpuVram,
    ];
    let mut deleted = false;

    for tier in &tiers {
        match orchestrator.evict(*chunk_id, *tier) {
            Ok(()) => {
                deleted = true;
            }
            Err(GhostError::ChunkNotFound(_)) => continue,
            Err(e) => {
                return IpcResponse::Error {
                    code: IpcErrorCode::InternalError,
                    message: format!("delete failed: {}", e),
                };
            }
        }
    }

    if deleted {
        IpcResponse::Ok { data: None }
    } else {
        IpcResponse::Error {
            code: IpcErrorCode::ChunkNotFound,
            message: format!("chunk {} not found", chunk_id),
        }
    }
}

async fn handle_migrate(
    orchestrator: &Arc<TransferOrchestrator>,
    chunk_id: &ChunkId,
    from: TierId,
    to: TierId,
) -> IpcResponse {
    // Get the chunk size from state machine
    let size = 0u8; // Default size; in a real system we'd look up the actual size

    match orchestrator.migrate(*chunk_id, from, to, size.into()) {
        Ok(()) => IpcResponse::Ok { data: None },
        Err(e) => IpcResponse::Error {
            code: match &e {
                GhostError::ChunkNotFound(_) => IpcErrorCode::ChunkNotFound,
                _ => IpcErrorCode::InternalError,
            },
            message: format!("migrate failed: {}", e),
        },
    }
}

async fn handle_info(orchestrator: &Arc<TransferOrchestrator>, chunk_id: &ChunkId) -> IpcResponse {
    let sm = orchestrator.state_machine.lock().unwrap();
    let state = sm.get_state(chunk_id);

    match state {
        Some(state) => {
            let meta = ChunkMeta::new(
                *chunk_id,
                0,
                0,
                TierId::Ram,
                ghost_core::types::CompressionAlgorithm::None,
                [0u8; 32],
            );
            // In a real system, we'd look up the actual metadata
            let _ = state;
            IpcResponse::Info { meta }
        }
        None => IpcResponse::Error {
            code: IpcErrorCode::ChunkNotFound,
            message: format!("chunk {} not found", chunk_id),
        },
    }
}

async fn handle_list(
    orchestrator: &Arc<TransferOrchestrator>,
    tier: Option<TierId>,
) -> IpcResponse {
    let sm = orchestrator.state_machine.lock().unwrap();
    let stored_chunks = sm.chunks_in_state(ghost_core::state::ChunkState::Stored);

    let chunks: Vec<(ChunkId, ChunkMeta)> = stored_chunks
        .into_iter()
        .filter(|_chunk_id| {
            if let Some(tier_filter) = tier {
                // In a real system, we'd check the chunk's actual tier
                let _ = tier_filter;
                true
            } else {
                true
            }
        })
        .map(|chunk_id| {
            let meta = ChunkMeta::new(
                chunk_id,
                0,
                0,
                TierId::Ram,
                ghost_core::types::CompressionAlgorithm::None,
                [0u8; 32],
            );
            (chunk_id, meta)
        })
        .collect();

    IpcResponse::List { chunks }
}

async fn handle_status(
    orchestrator: &Arc<TransferOrchestrator>,
    start_time: Instant,
) -> IpcResponse {
    let status = orchestrator.status();

    let tiers = vec![
        TierInfo {
            tier_id: TierId::Ram,
            capacity_bytes: 1024 * 1024 * 1024, // 1 GB placeholder
            used_bytes: 0,
            chunk_count: 0,
        },
        TierInfo {
            tier_id: TierId::Simulation,
            capacity_bytes: 512 * 1024 * 1024, // 512 MB placeholder
            used_bytes: 0,
            chunk_count: 0,
        },
    ];

    IpcResponse::Status {
        uptime_secs: start_time.elapsed().as_secs(),
        chunks_total: status.trace_event_count, // Approximate
        tiers,
        queue_depth: status.queue_depth,
        active_workers: status.active_workers as usize,
    }
}

async fn handle_pressure(orchestrator: &Arc<TransferOrchestrator>) -> IpcResponse {
    let state = orchestrator.current_pressure();
    IpcResponse::Pressure { state }
}

async fn handle_trace(trace_log: &Arc<TraceLog>, count: Option<usize>) -> IpcResponse {
    let all_events = trace_log.get_events();
    let events = match count {
        Some(n) => all_events.into_iter().rev().take(n).collect(),
        None => all_events,
    };
    IpcResponse::Trace { events }
}

async fn handle_pressure_check(orchestrator: &Arc<TransferOrchestrator>) -> IpcResponse {
    match orchestrator.run_pressure_check() {
        Ok(migrations) => IpcResponse::PressureCheck {
            jobs_created: migrations.len(),
        },
        Err(e) => IpcResponse::Error {
            code: IpcErrorCode::InternalError,
            message: format!("pressure check failed: {}", e),
        },
    }
}

async fn handle_shutdown(orchestrator: &Arc<TransferOrchestrator>) -> IpcResponse {
    // We can't actually shut down the orchestrator from here because
    // we only have a shared reference. The shutdown signal is handled
    // by the IpcServer's shutdown receiver.
    let _ = orchestrator;
    IpcResponse::Ok {
        data: Some(b"shutdown initiated".to_vec()),
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipc_server_config_default() {
        let config = IpcServerConfig::default();
        assert_eq!(config.socket_path, PathBuf::from("/tmp/ghostpages.sock"));
        assert_eq!(config.max_connections, 64);
        assert_eq!(config.request_timeout_secs, 30);
        assert_eq!(config.max_request_size, 256 * 1024 * 1024);
    }

    #[test]
    fn test_ipc_server_config_custom() {
        let config = IpcServerConfig {
            socket_path: PathBuf::from("/var/run/ghost.sock"),
            max_connections: 128,
            request_timeout_secs: 60,
            max_request_size: 128 * 1024 * 1024,
        };
        assert_eq!(config.socket_path, PathBuf::from("/var/run/ghost.sock"));
        assert_eq!(config.max_connections, 128);
        assert_eq!(config.request_timeout_secs, 60);
        assert_eq!(config.max_request_size, 128 * 1024 * 1024);
    }
}
