//! IPC client for GhostPages.
//!
//! Provides a client that connects to the GhostPages daemon via Unix
//! domain sockets and sends length-prefixed JSON requests.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::state::PressureState;
use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, ChunkMeta, TierId};

use tokio::net::UnixStream;

use crate::frame::{read_frame, write_frame};
use crate::protocol::{IpcRequest, IpcResponse, TierInfo};

/// Timeout for individual request-response cycles.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// IPC client for connecting to the GhostPages daemon.
#[derive(Debug)]
pub struct IpcClient {
    socket_path: PathBuf,
    stream: UnixStream,
    timeout: Duration,
}

impl IpcClient {
    /// Connect to the daemon at the given socket path.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails.
    pub async fn connect(socket_path: &Path) -> GhostResult<Self> {
        let stream = UnixStream::connect(socket_path)
            .await
            .map_err(|e| GhostError::IpcError(format!("failed to connect: {}", e)))?;

        Ok(Self {
            socket_path: socket_path.to_path_buf(),
            stream,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        })
    }

    /// Connect with a custom timeout.
    pub async fn connect_with_timeout(socket_path: &Path, timeout: Duration) -> GhostResult<Self> {
        let stream = UnixStream::connect(socket_path)
            .await
            .map_err(|e| GhostError::IpcError(format!("failed to connect: {}", e)))?;

        Ok(Self {
            socket_path: socket_path.to_path_buf(),
            stream,
            timeout,
        })
    }

    /// Send a request and wait for a response.
    ///
    /// This is the core request-response method. All convenience methods
    /// delegate to this.
    pub async fn send_request(&mut self, request: IpcRequest) -> GhostResult<IpcResponse> {
        // Serialize request to JSON
        let request_json = serde_json::to_vec(&request)
            .map_err(|e| GhostError::IpcError(format!("failed to serialize request: {}", e)))?;

        // Write length-prefixed frame
        write_frame(&mut self.stream, &request_json).await?;

        // Read response with timeout
        let response_bytes = tokio::time::timeout(self.timeout, read_frame(&mut self.stream))
            .await
            .map_err(|_| GhostError::Timeout)??;

        // Deserialize response
        let response: IpcResponse = serde_json::from_slice(&response_bytes)
            .map_err(|e| GhostError::IpcError(format!("failed to deserialize response: {}", e)))?;

        Ok(response)
    }

    // ─── Convenience Methods ─────────────────────────────────────────────────

    /// Store data in the daemon.
    ///
    /// Returns the ChunkId of the stored data.
    pub async fn store(&mut self, data: Vec<u8>, tier: Option<TierId>) -> GhostResult<ChunkId> {
        let response = self.send_request(IpcRequest::Store { data, tier }).await?;
        match response {
            IpcResponse::ChunkId { chunk_id } => Ok(chunk_id),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "store failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to store: {:?}",
                other
            ))),
        }
    }

    /// Retrieve data by ChunkId.
    pub async fn retrieve(&mut self, chunk_id: &ChunkId) -> GhostResult<Vec<u8>> {
        let response = self
            .send_request(IpcRequest::Retrieve {
                chunk_id: *chunk_id,
            })
            .await?;
        match response {
            IpcResponse::Ok { data } => data.ok_or_else(|| {
                GhostError::IpcError("retrieve returned Ok with no data".to_string())
            }),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "retrieve failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to retrieve: {:?}",
                other
            ))),
        }
    }

    /// Delete a chunk.
    pub async fn delete(&mut self, chunk_id: &ChunkId) -> GhostResult<()> {
        let response = self
            .send_request(IpcRequest::Delete {
                chunk_id: *chunk_id,
            })
            .await?;
        match response {
            IpcResponse::Ok { .. } => Ok(()),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "delete failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to delete: {:?}",
                other
            ))),
        }
    }

    /// Migrate a chunk between tiers.
    pub async fn migrate(
        &mut self,
        chunk_id: &ChunkId,
        from: TierId,
        to: TierId,
    ) -> GhostResult<()> {
        let response = self
            .send_request(IpcRequest::Migrate {
                chunk_id: *chunk_id,
                from,
                to,
            })
            .await?;
        match response {
            IpcResponse::Ok { .. } => Ok(()),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "migrate failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to migrate: {:?}",
                other
            ))),
        }
    }

    /// Get chunk metadata.
    pub async fn info(&mut self, chunk_id: &ChunkId) -> GhostResult<ChunkMeta> {
        let response = self
            .send_request(IpcRequest::Info {
                chunk_id: *chunk_id,
            })
            .await?;
        match response {
            IpcResponse::Info { meta } => Ok(meta),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "info failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to info: {:?}",
                other
            ))),
        }
    }

    /// List chunks, optionally filtered by tier.
    pub async fn list(&mut self, tier: Option<TierId>) -> GhostResult<Vec<(ChunkId, ChunkMeta)>> {
        let response = self.send_request(IpcRequest::List { tier }).await?;
        match response {
            IpcResponse::List { chunks } => Ok(chunks),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "list failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to list: {:?}",
                other
            ))),
        }
    }

    /// Get system status.
    pub async fn status(&mut self) -> GhostResult<StatusResponse> {
        let response = self.send_request(IpcRequest::Status).await?;
        match response {
            IpcResponse::Status {
                uptime_secs,
                chunks_total,
                tiers,
                queue_depth,
                active_workers,
            } => Ok(StatusResponse {
                uptime_secs,
                chunks_total,
                tiers,
                queue_depth,
                active_workers,
            }),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "status failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to status: {:?}",
                other
            ))),
        }
    }

    /// Get current pressure state.
    pub async fn pressure(&mut self) -> GhostResult<PressureState> {
        let response = self.send_request(IpcRequest::Pressure).await?;
        match response {
            IpcResponse::Pressure { state } => Ok(state),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "pressure failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to pressure: {:?}",
                other
            ))),
        }
    }

    /// Get recent trace events.
    pub async fn trace(&mut self, count: Option<usize>) -> GhostResult<Vec<TraceEvent>> {
        let response = self.send_request(IpcRequest::Trace { count }).await?;
        match response {
            IpcResponse::Trace { events } => Ok(events),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "trace failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to trace: {:?}",
                other
            ))),
        }
    }

    /// Ping the daemon.
    pub async fn ping(&mut self) -> GhostResult<()> {
        let response = self.send_request(IpcRequest::Ping).await?;
        match response {
            IpcResponse::Pong => Ok(()),
            IpcResponse::Error { code, message } => Err(GhostError::IpcError(format!(
                "ping failed ({:?}): {}",
                code, message
            ))),
            other => Err(GhostError::IpcError(format!(
                "unexpected response to ping: {:?}",
                other
            ))),
        }
    }

    /// Request graceful shutdown.
    pub async fn shutdown(&mut self) -> GhostResult<()> {
        let response = self.send_request(IpcRequest::Shutdown).await?;
        match response {
            IpcResponse::Ok { .. } | IpcResponse::Pong => Ok(()),
            IpcResponse::Error { code: _, message } => {
                // Shutdown may close the connection before responding
                if message.contains("shutting down") || message.contains("connection") {
                    Ok(())
                } else {
                    Err(GhostError::IpcError(format!(
                        "shutdown failed: {}",
                        message
                    )))
                }
            }
            other => Err(GhostError::IpcError(format!(
                "unexpected response to shutdown: {:?}",
                other
            ))),
        }
    }

    /// Get the socket path this client is connected to.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

/// Status response from the daemon.
#[derive(Debug, Clone)]
pub struct StatusResponse {
    /// Daemon uptime in seconds.
    pub uptime_secs: u64,
    /// Total number of chunks.
    pub chunks_total: usize,
    /// Per-tier information.
    pub tiers: Vec<TierInfo>,
    /// Current queue depth.
    pub queue_depth: usize,
    /// Number of active workers.
    pub active_workers: usize,
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::IpcRequest;

    /// Test that IpcClient can be constructed (without connecting).
    #[test]
    fn test_client_construction() {
        // We can't easily test the full client without a running server,
        // but we can verify the types compile correctly.
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<IpcClient>();
        _assert_send_sync::<StatusResponse>();
    }

    /// Test that convenience methods map to correct request types.
    #[test]
    fn test_request_types() {
        let store_req = IpcRequest::Store {
            data: b"test".to_vec(),
            tier: Some(TierId::Ram),
        };
        assert!(matches!(store_req, IpcRequest::Store { .. }));

        let chunk_id = ChunkId::from_data(b"test");
        let retrieve_req = IpcRequest::Retrieve { chunk_id };
        assert!(matches!(retrieve_req, IpcRequest::Retrieve { .. }));

        let delete_req = IpcRequest::Delete { chunk_id };
        assert!(matches!(delete_req, IpcRequest::Delete { .. }));

        let migrate_req = IpcRequest::Migrate {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Disk,
        };
        assert!(matches!(migrate_req, IpcRequest::Migrate { .. }));

        let info_req = IpcRequest::Info { chunk_id };
        assert!(matches!(info_req, IpcRequest::Info { .. }));

        let list_req = IpcRequest::List {
            tier: Some(TierId::Ram),
        };
        assert!(matches!(list_req, IpcRequest::List { .. }));

        let status_req = IpcRequest::Status;
        assert!(matches!(status_req, IpcRequest::Status));

        let pressure_req = IpcRequest::Pressure;
        assert!(matches!(pressure_req, IpcRequest::Pressure));

        let trace_req = IpcRequest::Trace { count: Some(10) };
        assert!(matches!(trace_req, IpcRequest::Trace { .. }));

        let ping_req = IpcRequest::Ping;
        assert!(matches!(ping_req, IpcRequest::Ping));

        let shutdown_req = IpcRequest::Shutdown;
        assert!(matches!(shutdown_req, IpcRequest::Shutdown));
    }

    /// Test StatusResponse construction.
    #[test]
    fn test_status_response() {
        let resp = StatusResponse {
            uptime_secs: 60,
            chunks_total: 10,
            tiers: vec![TierInfo {
                tier_id: TierId::Ram,
                capacity_bytes: 1024,
                used_bytes: 512,
                chunk_count: 5,
            }],
            queue_depth: 2,
            active_workers: 1,
        };
        assert_eq!(resp.uptime_secs, 60);
        assert_eq!(resp.chunks_total, 10);
        assert_eq!(resp.tiers.len(), 1);
        assert_eq!(resp.queue_depth, 2);
        assert_eq!(resp.active_workers, 1);
    }
}
