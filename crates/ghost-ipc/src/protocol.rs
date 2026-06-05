//! IPC protocol message types.
//!
//! Defines the wire protocol for communication between the GhostPages
//! daemon and CLI tools. All messages are serialized as JSON with
//! length-prefixed framing.

use serde::{Deserialize, Serialize};

use ghost_core::state::PressureState;
use ghost_core::trace::TraceEvent;
use ghost_core::types::{ChunkId, ChunkMeta, TierId};

// ─── Request Types ─────────────────────────────────────────────────────────────

/// IPC request types sent from client to daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcRequest {
    /// Store data, returns the ChunkId.
    Store {
        /// Raw data to store.
        data: Vec<u8>,
        /// Optional tier override.
        tier: Option<TierId>,
    },

    /// Retrieve data by ChunkId.
    Retrieve {
        /// Chunk ID to retrieve.
        chunk_id: ChunkId,
    },

    /// Delete a chunk from all tiers.
    Delete {
        /// Chunk ID to delete.
        chunk_id: ChunkId,
    },

    /// Migrate a chunk between tiers.
    Migrate {
        /// Chunk ID to migrate.
        chunk_id: ChunkId,
        /// Source tier.
        from: TierId,
        /// Destination tier.
        to: TierId,
    },

    /// Get chunk metadata without retrieving data.
    Info {
        /// Chunk ID to query.
        chunk_id: ChunkId,
    },

    /// List all chunks (optionally filtered by tier).
    List {
        /// Optional tier filter.
        tier: Option<TierId>,
    },

    /// Get system status.
    Status,

    /// Get current pressure state.
    Pressure,

    /// Get recent trace events.
    Trace {
        /// Number of recent events to return.
        count: Option<usize>,
    },

    /// Trigger a pressure check.
    PressureCheck,

    /// Graceful shutdown.
    Shutdown,

    /// Ping for liveness check.
    Ping,
}

// ─── Response Types ────────────────────────────────────────────────────────────

/// IPC response types sent from daemon to client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcResponse {
    /// Operation succeeded with optional data.
    Ok {
        /// Response data, if any.
        data: Option<Vec<u8>>,
    },

    /// Operation succeeded with chunk ID.
    ChunkId {
        /// The chunk ID of the stored chunk.
        chunk_id: ChunkId,
    },

    /// Operation succeeded with metadata.
    Info {
        /// Chunk metadata.
        meta: ChunkMeta,
    },

    /// Operation succeeded with list.
    List {
        /// List of chunk IDs and their metadata.
        chunks: Vec<(ChunkId, ChunkMeta)>,
    },

    /// Operation succeeded with status.
    Status {
        /// Daemon uptime in seconds.
        uptime_secs: u64,
        /// Total number of chunks.
        chunks_total: usize,
        /// Per-tier information.
        tiers: Vec<TierInfo>,
        /// Current queue depth.
        queue_depth: usize,
        /// Number of active workers.
        active_workers: usize,
    },

    /// Operation succeeded with pressure.
    Pressure {
        /// Current pressure state.
        state: PressureState,
    },

    /// Operation succeeded with trace events.
    Trace {
        /// Recent trace events.
        events: Vec<TraceEvent>,
    },

    /// Operation succeeded with migration result.
    PressureCheck {
        /// Number of migration jobs created.
        jobs_created: usize,
    },

    /// Pong response to ping.
    Pong,

    /// Operation failed.
    Error {
        /// Error code.
        code: IpcErrorCode,
        /// Human-readable error message.
        message: String,
    },
}

// ─── Tier Info ─────────────────────────────────────────────────────────────────

/// Information about a single storage tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierInfo {
    /// Tier identifier.
    pub tier_id: TierId,
    /// Total capacity in bytes.
    pub capacity_bytes: u64,
    /// Used bytes.
    pub used_bytes: u64,
    /// Number of chunks stored in this tier.
    pub chunk_count: usize,
}

// ─── Error Codes ────────────────────────────────────────────────────────────────

/// IPC error codes for typed error handling.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum IpcErrorCode {
    /// Chunk not found in any tier.
    ChunkNotFound,

    /// Specified tier does not exist.
    TierNotFound,

    /// Malformed or invalid request.
    InvalidRequest,

    /// Transfer queue is full.
    QueueFull,

    /// Internal daemon error.
    InternalError,

    /// Request timed out.
    Timeout,

    /// Daemon is shutting down.
    ShuttingDown,
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::types::CompressionAlgorithm;

    #[test]
    fn test_ipc_request_store_roundtrip() {
        let req = IpcRequest::Store {
            data: b"hello world".to_vec(),
            tier: Some(TierId::Ram),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: IpcRequest = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcRequest::Store { data, tier } => {
                assert_eq!(data, b"hello world");
                assert_eq!(tier, Some(TierId::Ram));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_request_retrieve_roundtrip() {
        let chunk_id = ChunkId::from_data(b"test");
        let req = IpcRequest::Retrieve { chunk_id };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: IpcRequest = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcRequest::Retrieve { chunk_id: id } => {
                assert_eq!(id, chunk_id);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_request_migrate_roundtrip() {
        let chunk_id = ChunkId::from_data(b"migrate_me");
        let req = IpcRequest::Migrate {
            chunk_id,
            from: TierId::Ram,
            to: TierId::Disk,
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: IpcRequest = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcRequest::Migrate {
                chunk_id: id,
                from,
                to,
            } => {
                assert_eq!(id, chunk_id);
                assert_eq!(from, TierId::Ram);
                assert_eq!(to, TierId::Disk);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_response_chunk_id_roundtrip() {
        let chunk_id = ChunkId::from_data(b"response_test");
        let resp = IpcResponse::ChunkId { chunk_id };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: IpcResponse = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcResponse::ChunkId { chunk_id: id } => {
                assert_eq!(id, chunk_id);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_response_info_roundtrip() {
        let meta = ChunkMeta::new(
            ChunkId::from_data(b"meta_test"),
            1024,
            512,
            TierId::Ram,
            CompressionAlgorithm::Zstd,
            [0u8; 32],
        );
        let resp = IpcResponse::Info { meta };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: IpcResponse = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcResponse::Info { meta: m } => {
                assert_eq!(m.id, ChunkId::from_data(b"meta_test"));
                assert_eq!(m.size, 1024);
                assert_eq!(m.compressed_size, 512);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_response_error_roundtrip() {
        let resp = IpcResponse::Error {
            code: IpcErrorCode::ChunkNotFound,
            message: "chunk not found".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: IpcResponse = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcResponse::Error { code, message } => {
                assert_eq!(code, IpcErrorCode::ChunkNotFound);
                assert_eq!(message, "chunk not found");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_response_status_roundtrip() {
        let resp = IpcResponse::Status {
            uptime_secs: 3600,
            chunks_total: 42,
            tiers: vec![TierInfo {
                tier_id: TierId::Ram,
                capacity_bytes: 1024 * 1024,
                used_bytes: 512 * 1024,
                chunk_count: 20,
            }],
            queue_depth: 5,
            active_workers: 3,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: IpcResponse = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcResponse::Status {
                uptime_secs,
                chunks_total,
                tiers,
                queue_depth,
                active_workers,
            } => {
                assert_eq!(uptime_secs, 3600);
                assert_eq!(chunks_total, 42);
                assert_eq!(tiers.len(), 1);
                assert_eq!(queue_depth, 5);
                assert_eq!(active_workers, 3);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_response_pressure_roundtrip() {
        let state = PressureState {
            memory_pressure: 0.5,
            vram_pressure: 0.3,
            io_pressure: 0.1,
            queue_depth: 10,
            throughput_bps: 1000,
        };
        let resp = IpcResponse::Pressure { state };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: IpcResponse = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcResponse::Pressure { state: s } => {
                assert!((s.memory_pressure - 0.5).abs() < f32::EPSILON);
                assert!((s.vram_pressure - 0.3).abs() < f32::EPSILON);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_response_trace_roundtrip() {
        let events = vec![TraceEvent::ChunkCreated {
            chunk_id: ChunkId::from_data(b"trace_test"),
            size: 100,
            tier: TierId::Ram,
            timestamp: 12345,
        }];
        let resp = IpcResponse::Trace { events };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: IpcResponse = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcResponse::Trace { events: evts } => {
                assert_eq!(evts.len(), 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_tier_info_roundtrip() {
        let info = TierInfo {
            tier_id: TierId::Ram,
            capacity_bytes: 1024 * 1024 * 1024,
            used_bytes: 512 * 1024 * 1024,
            chunk_count: 100,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: TierInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.tier_id, TierId::Ram);
        assert_eq!(deserialized.capacity_bytes, 1024 * 1024 * 1024);
        assert_eq!(deserialized.used_bytes, 512 * 1024 * 1024);
        assert_eq!(deserialized.chunk_count, 100);
    }

    #[test]
    fn test_ipc_error_code_roundtrip() {
        for code in [
            IpcErrorCode::ChunkNotFound,
            IpcErrorCode::TierNotFound,
            IpcErrorCode::InvalidRequest,
            IpcErrorCode::QueueFull,
            IpcErrorCode::InternalError,
            IpcErrorCode::Timeout,
            IpcErrorCode::ShuttingDown,
        ] {
            let json = serde_json::to_string(&code).unwrap();
            let deserialized: IpcErrorCode = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, code);
        }
    }

    #[test]
    fn test_ipc_request_ping_roundtrip() {
        let req = IpcRequest::Ping;
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: IpcRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, IpcRequest::Ping));
    }

    #[test]
    fn test_ipc_response_pong_roundtrip() {
        let resp = IpcResponse::Pong;
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: IpcResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, IpcResponse::Pong));
    }

    #[test]
    fn test_ipc_request_list_roundtrip() {
        let req = IpcRequest::List {
            tier: Some(TierId::Ram),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: IpcRequest = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcRequest::List { tier } => {
                assert_eq!(tier, Some(TierId::Ram));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_request_list_all_roundtrip() {
        let req = IpcRequest::List { tier: None };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: IpcRequest = serde_json::from_str(&json).unwrap();
        match deserialized {
            IpcRequest::List { tier } => {
                assert_eq!(tier, None);
            }
            _ => panic!("wrong variant"),
        }
    }
}
