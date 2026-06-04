//! IPC protocol message types.
//!
//! Skeleton implementation for Phase 0. Full message protocol
//! will be implemented in Phase 3.

use serde::{Deserialize, Serialize};

/// IPC message types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Client request to store data.
    StoreRequest {
        /// Data to store.
        data: Vec<u8>,
    },

    /// Client request to retrieve data.
    RetrieveRequest {
        /// Chunk ID to retrieve.
        chunk_id: [u8; 32],
    },

    /// Client request to delete data.
    DeleteRequest {
        /// Chunk ID to delete.
        chunk_id: [u8; 32],
    },

    /// Server response with stored chunk ID.
    StoreResponse {
        /// ID of the stored chunk.
        chunk_id: [u8; 32],
    },

    /// Server response with retrieved data.
    RetrieveResponse {
        /// Retrieved data, if found.
        data: Option<Vec<u8>>,
    },

    /// Server response confirming deletion.
    DeleteResponse {
        /// Whether the chunk was found and deleted.
        success: bool,
    },

    /// Server error response.
    Error {
        /// Error message.
        message: String,
    },
}
